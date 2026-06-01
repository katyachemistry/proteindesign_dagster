"""
Asset definitions for RFdiffusion → ProteinMPNN → ColabFold (+ Boltz-2 YAML inputs).

Partitioning
------------
`rfdiffusion_yamls` is a non-partitioned seed asset that:
  1. Generates all configured (design × hotspot_file) YAML configs
  2. Writes a manifest JSON:  partition_key → {config_dir, config_name, target_pdb}
  3. Registers all keys in DynamicPartitionsDefinition("design_configs")

Every downstream asset (rfdiffusion → proteinmpnn → boltz2 inputs / colabfold) is partitioned
by "design_configs". All outputs for one partition live under:

  {outputs_dir}/{partition_key}/
      pdbs/                  ← RFdiffusion PDB outputs
      proteinmpnn/
          parsed_pdbs.jsonl
          assigned_chains.jsonl
          seqs/
          combined_seqs/
          combined_yamls/    ← Boltz-2 predict inputs (A binder; B,C,… target by ":")
      colabfold/             ← ColabFold predictions
      boltz2/                ← Boltz-2 predictions (when run)
      filters/               ← Post-fold metrics (e.g. pairwise backbone RMSD)

Partition key format
--------------------
  {design_name}__{target_pdb_stem}__{hotspot_txts_dir_name}__{yaml_stem}
  e.g.  designs_1__truncated_renumbered_loops__loops__scaffold_guided_loop1_loop3_set1

Docker notes
------------
• -it is omitted (no TTY under Dagster).
• outputs_dir parent and target PDB parent are mounted as themselves so
  container paths == host paths.
• Each asset that writes via Docker runs a chown step afterwards so the
  next asset (running as the host user) can read the files.
"""

import json
import os
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Dict, List, Optional, Sequence

import yaml
from dagster import (
    AssetExecutionContext,
    Config,
    DynamicPartitionsDefinition,
    MaterializeResult,
    MetadataValue,
    asset,
)
from pydantic import Field

from dagster_pipeline.resources import (
    DEFAULT_OUTPUTS_DIR,
    PIPELINE_CONFIG_PATH,
    Boltz2ToolConfig,
    ColabFoldToolConfig,
    FiltersToolConfig,
    PipelinePathsResource,
    ProteinMPNNToolConfig,
    RFDiffusionToolConfig,
)

# Make /storage/proteindesign importable so we can use utils/
_PROTEINDESIGN = Path(__file__).resolve().parents[2]
if str(_PROTEINDESIGN) not in sys.path:
    sys.path.insert(0, str(_PROTEINDESIGN))

from utils.generate_hotspot_yamls import (  # noqa: E402
    generate_yamls,
    generate_yamls_from_template_text,
    output_name_for_hotspot_file,
)
from utils.merge_fasta import explode_fasta_records_with_append, read_fasta_sequence_flat  # noqa: E402
from utils.write_boltz2_yamls import (  # noqa: E402
    iter_exploded_binder_target_mpnn,
    write_yaml_for_pair,
)


# ---------------------------------------------------------------------------
# Dynamic partitions – one per generated RFdiffusion config
# ---------------------------------------------------------------------------

design_configs = DynamicPartitionsDefinition(name="design_configs")

# ---------------------------------------------------------------------------
# Pipeline-wide tool config (written/read as pipeline_config.yaml)
# ---------------------------------------------------------------------------

class PipelineAllToolsConfig(Config):
    """All tool parameters for the full RFdiffusion → ProteinMPNN → ColabFold pipeline.

    Materialize the ``pipeline_config`` asset with this config to persist the
    settings to ``pipeline_config.yaml``.  All downstream tool assets read from
    that file at runtime — no per-partition configuration needed.
    """

    rfdiffusion: RFDiffusionToolConfig = Field(default_factory=RFDiffusionToolConfig)
    proteinmpnn: ProteinMPNNToolConfig = Field(default_factory=ProteinMPNNToolConfig)
    boltz2: Boltz2ToolConfig = Field(default_factory=Boltz2ToolConfig)
    colabfold: ColabFoldToolConfig = Field(default_factory=ColabFoldToolConfig)
    filters: FiltersToolConfig = Field(default_factory=FiltersToolConfig)


def _read_pipeline_config() -> PipelineAllToolsConfig:
    """Load tool parameters from pipeline_config.yaml written by the pipeline_config asset."""
    if not PIPELINE_CONFIG_PATH.is_file():
        raise FileNotFoundError(
            f"Pipeline config not found at {PIPELINE_CONFIG_PATH}. "
            "Materialize the `pipeline_config` asset (run `generate_configs` job) first."
        )
    data = yaml.safe_load(PIPELINE_CONFIG_PATH.read_text(encoding="utf-8"))
    return PipelineAllToolsConfig.model_validate(data)


# ---------------------------------------------------------------------------
# Low-level helpers
# ---------------------------------------------------------------------------

def _run(context: AssetExecutionContext, cmd: List[str], cwd: Optional[Path] = None) -> None:
    """Stream command output line-by-line to the Dagster log."""
    context.log.info("$ " + " ".join(cmd))
    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        cwd=cwd,
    )
    assert proc.stdout is not None
    for line in proc.stdout:
        stripped = line.rstrip()
        if stripped:
            context.log.info(stripped)
    proc.wait()
    if proc.returncode != 0:
        raise RuntimeError(f"Command exited with code {proc.returncode}: {cmd[0]} ...")


def _chown_dir(context: AssetExecutionContext, host_dir: Path, inner_dir: str) -> None:
    """Restore file ownership after a Docker write using a minimal alpine container."""
    uid, gid = os.getuid(), os.getgid()
    _run(
        context,
        [
            "docker", "run", "--rm",
            "-v", f"{host_dir}:{inner_dir}",
            "alpine",
            "chown", "-R", f"{uid}:{gid}", inner_dir,
        ],
    )


def _chmod_mount_writable(context: AssetExecutionContext, mount_source: str | Path, inner_dir: str) -> None:
    """Allow a container's default user to write to a mounted host path or Docker volume."""
    _run(
        context,
        [
            "docker", "run", "--rm",
            "-v", f"{mount_source}:{inner_dir}",
            "alpine",
            "chmod", "-R", "a+rwX", inner_dir,
        ],
    )


def _docker_gpu_args(gpus: str) -> List[str]:
    """Build Docker GPU args, treating comma-separated values as explicit device IDs."""
    value = str(gpus).strip()
    if not value:
        return []
    if value == "all" or value.startswith(("device=", "count=", '"device=', '"count=')):
        return ["--gpus", value]
    return ["--gpus", f'"device={value}"']


def _manifest_path(outputs_dir: str) -> Path:
    return Path(outputs_dir) / "design_configs_manifest.json"


def _read_manifest(outputs_dir: str) -> Dict[str, dict]:
    p = _manifest_path(outputs_dir)
    if not p.is_file():
        raise FileNotFoundError(
            f"Manifest not found at {p}. Materialize `rfdiffusion_yamls` first."
        )
    return json.loads(p.read_text())


def _partition_root(outputs_dir: str, partition_key: str) -> Path:
    """Isolated output directory for one design config partition."""
    return Path(outputs_dir) / partition_key


def _make_partition_key(
    design_name: str,
    target_pdb: str,
    hotspot_txts_dir: str,
    yaml_name: str,
) -> str:
    """Build a filesystem-safe, human-readable partition key from path components."""
    pdb_stem = Path(target_pdb).stem
    hdir_name = Path(hotspot_txts_dir).name
    yaml_stem = Path(yaml_name).stem

    def sanitize(s: str) -> str:
        return "".join(c if (c.isalnum() or c == "_") else "_" for c in s)

    return (
        f"{sanitize(design_name)}__{sanitize(pdb_stem)}__"
        f"{sanitize(hdir_name)}__{sanitize(yaml_stem)}"
    )


def _collect_fold_structure_paths(proot: Path) -> tuple[list[Path], list[Path]]:
    """ColabFold PDBs and Boltz-2 structures (.pdb / .cif / .mmcif) under the partition root."""
    cf_dir = proot / "colabfold"
    bz_dir = proot / "boltz2"
    cf_pdbs: list[Path] = []
    bz_structs: list[Path] = []
    if cf_dir.is_dir():
        cf_pdbs = sorted(cf_dir.rglob("*.pdb"))
    if bz_dir.is_dir():
        for pattern in ("*.pdb", "*.cif", "*.mmcif"):
            bz_structs.extend(bz_dir.rglob(pattern))
        bz_structs = sorted(set(bz_structs), key=lambda p: str(p))
    return cf_pdbs, bz_structs


def _pick_structure_for_key(key: str, paths: Sequence[Path]) -> Optional[Path]:
    """Pick one structure file whose stem best matches a combined-FASTA key (e.g. ``stem__r3``)."""
    if not paths:
        return None
    for p in paths:
        st = p.stem
        if st == key or st.startswith(f"{key}_") or st.startswith(key):
            return p
    key_l = key.lower()
    for p in paths:
        if key in p.stem or key_l in p.stem.lower():
            return p
    return None


def _design_keys_from_combined_fastas(
    proot: Path, fasta_extensions: Optional[Sequence[str]] = None
) -> List[str]:
    d = proot / "proteinmpnn" / "combined_seqs"
    if not d.is_dir():
        return []
    if fasta_extensions:
        exts = {e.lower() if e.startswith(".") else f".{e.lower()}" for e in fasta_extensions}
    else:
        exts = {".fa", ".fasta", ".faa"}
    return sorted(
        p.stem for p in d.iterdir() if p.is_file() and p.suffix.lower() in exts
    )


def _safe_token_for_filename(s: str) -> str:
    return "".join(c if (c.isalnum() or c in ("_", "-")) else "_" for c in s)


# ---------------------------------------------------------------------------
# configs group  (non-partitioned seed asset)
# ---------------------------------------------------------------------------

@asset(
    group_name="configs",
    description=(
        "Saves all RFdiffusion, ProteinMPNN, Boltz-2, ColabFold, and filter tool parameters to "
        "``pipeline_config.yaml``.  Materialize this asset once (with your desired "
        "settings) before running ``design_pipeline`` — all partitioned tool assets "
        "read their parameters from that file at runtime."
    ),
)
def pipeline_config(
    context: AssetExecutionContext,
    config: PipelineAllToolsConfig,
) -> MaterializeResult:
    data = config.model_dump()
    PIPELINE_CONFIG_PATH.parent.mkdir(parents=True, exist_ok=True)
    PIPELINE_CONFIG_PATH.write_text(
        yaml.dump(data, default_flow_style=False, sort_keys=False, allow_unicode=True),
        encoding="utf-8",
    )
    context.log.info(f"Pipeline config saved → {PIPELINE_CONFIG_PATH}")
    return MaterializeResult(
        metadata={
            "config_path": MetadataValue.path(str(PIPELINE_CONFIG_PATH)),
            "rfdiffusion_outputs_dir": config.rfdiffusion.outputs_dir,
            "proteinmpnn_num_seq_per_target": config.proteinmpnn.num_seq_per_target,
            "colabfold_model_type": config.colabfold.model_type,
            "colabfold_pair_mode": config.colabfold.pair_mode,
            "preview": MetadataValue.md(
                f"```yaml\n{PIPELINE_CONFIG_PATH.read_text(encoding='utf-8')}\n```"
            ),
        }
    )


class RfdiffusionYamlsConfig(Config):
    """Manifest output directory for the YAML-generation step.

    Must match ``rfdiffusion.outputs_dir`` in the pipeline_config asset.
    """
    outputs_dir: str = DEFAULT_OUTPUTS_DIR

def _yaml_preview(output_dir: Path) -> str:
    yamls = sorted(output_dir.glob("*.yaml"))
    if not yamls:
        return f"_No YAML files found in `{output_dir}`._"
    lines = [f"**{len(yamls)} file(s)** in `{output_dir}`\n"]
    for y in yamls[:3]:
        lines.append(f"#### `{y.name}`\n```yaml\n{y.read_text().strip()}\n```")
    if len(yamls) > 3:
        lines.append(f"_…and {len(yamls) - 3} more._")
    return "\n\n".join(lines)


@asset(
    group_name="configs",
    description=(
        "Generates RFdiffusion config YAMLs. It accepts a directory with hotspot txt files, template RFdiffusion config, "
        "truncated (or non-truncated) target pdb, and output base directory."
        "It then turns each hotspot txt file into yaml, based on template - basically, it replaces hotspot"
        " residues in template."
        "It writes a manifest to "
        "`rfdiffusion_backbones.config.outputs_dir/design_configs_manifest.json`, "
        "and registers one dynamic partition per generated YAML. "
        "Arguments: `paths` provides design bundles "
        "(hotspots_txts_dir + template path or inline YAML + target_pdb + output_base_dir)."
    ),
)
def rfdiffusion_yamls(
    context: AssetExecutionContext,
    config: RfdiffusionYamlsConfig,
    paths: PipelinePathsResource,
) -> MaterializeResult:
    manifest: Dict[str, dict] = {}
    preview_sections: List[str] = []
    total_files = 0

    for design_name, design in paths.epitopes_hotspots.designs_config.items():
        hotspot_txts_dir = Path(design.hotspots_txts_dir)
        out_dir = Path(design.output_base_dir)

        if design.template_rfdiffusion_yaml:
            tmpl_path = Path(design.template_rfdiffusion_yaml)
            context.log.info(
                f"Generating YAMLs for {design_name}: "
                f"{tmpl_path.name} × {hotspot_txts_dir.name} → {out_dir}"
            )
            n = generate_yamls(
                tmpl_path,
                hotspot_txts_dir,
                out_dir,
                Path(design.target_pdb),
            )
        else:
            tmpl_label = design.naming_prefix
            context.log.info(
                f"Generating YAMLs for {design_name}: "
                f"inline template ({tmpl_label}) × {hotspot_txts_dir.name} → {out_dir}"
            )
            n = generate_yamls_from_template_text(
                design.template_rfdiffusion_yaml_inline or "",
                design.naming_prefix,
                hotspot_txts_dir,
                out_dir,
                Path(design.target_pdb),
            )
        total_files += n
        preview_sections.append(_yaml_preview(out_dir))

        # Build one manifest entry per hotspot .txt file in this design dir.
        for txt_file in sorted(hotspot_txts_dir.rglob("*.txt")):
            yaml_name = output_name_for_hotspot_file(txt_file)
            partition_key = _make_partition_key(
                design_name,
                design.target_pdb,
                design.hotspots_txts_dir,
                yaml_name,
            )
            manifest[partition_key] = {
                "design_name": design_name,
                "config_dir": str(out_dir),
                "config_name": Path(yaml_name).stem,
                "target_pdb": design.target_pdb,
            }

    outputs_dir = config.outputs_dir

    # Persist manifest so downstream partitioned assets can look up paths
    mp = _manifest_path(outputs_dir)
    mp.parent.mkdir(parents=True, exist_ok=True)
    mp.write_text(json.dumps(manifest, indent=2))
    context.log.info(f"Manifest written → {mp}  ({len(manifest)} entries)")

    # Register all partition keys with Dagster (silently skips existing keys)
    context.instance.add_dynamic_partitions("design_configs", list(manifest.keys()))
    context.log.info(f"Registered {len(manifest)} partition(s) in 'design_configs'")

    return MaterializeResult(
        metadata={
            "total_yamls": total_files,
            "partition_count": len(manifest),
            "manifest_path": MetadataValue.path(str(mp)),
            "partition_keys": MetadataValue.text("\n".join(sorted(manifest.keys()))),
            "preview": MetadataValue.md("\n\n---\n\n".join(preview_sections)),
        }
    )


# ---------------------------------------------------------------------------
# rfdiffusion group  (partitioned)
# ---------------------------------------------------------------------------

@asset(
    group_name="rfdiffusion",
    partitions_def=design_configs,
    deps=[rfdiffusion_yamls, pipeline_config],
    description=(
        "Runs RFdiffusion for one partition key. All parameters (GPUs, outputs dir, "
        "Docker image, model mounts, extra Hydra args) are read from ``pipeline_config.yaml`` "
        "written by the ``pipeline_config`` asset."
    ),
)
def rfdiffusion_backbones(
    context: AssetExecutionContext,
) -> MaterializeResult:
    rfd = _read_pipeline_config().rfdiffusion
    partition_key = context.partition_key
    manifest = _read_manifest(rfd.outputs_dir)
    if partition_key not in manifest:
        raise KeyError(
            f"Partition '{partition_key}' not found in manifest. "
            "Re-run `rfdiffusion_yamls` to regenerate it."
        )
    entry = manifest[partition_key]
    config_dir: str = entry["config_dir"]
    config_name: str = entry["config_name"]
    target_pdb: str = entry["target_pdb"]
    if not Path(target_pdb).is_file():
        raise FileNotFoundError(f"Target PDB from manifest does not exist on host: {target_pdb}")

    proot = _partition_root(rfd.outputs_dir, partition_key)
    pdbs_dir = proot / "pdbs"
    pdbs_dir.mkdir(parents=True, exist_ok=True)

    # Override inference.output_prefix so PDBs land in the partition's pdbs/ dir.
    # Mounts below are path-preserving, so inner container paths match host paths.
    output_prefix = str(pdbs_dir / "design")
    target_parent = Path(target_pdb).parent

    cmd: List[str] = [
        "docker", "run", "--rm",
        "--runtime=nvidia", *_docker_gpu_args(rfd.gpus),
        "-v", f"{Path(rfd.outputs_dir).parent}:{Path(rfd.outputs_dir).parent}",
        "-v", f"{target_parent}:{target_parent}",
        "-v", f"{rfd.rfdiffusion_models_host}:{rfd.rfdiffusion_models_host}",
        rfd.docker_image,
        f"--config-dir={config_dir}",
        f"--config-name={config_name}",
        f"inference.model_directory_path={rfd.model_directory_path}",
        f"inference.output_prefix={output_prefix}",
    ]
    cmd.extend(rfd.extra_hydra_args)
    _run(context, cmd)
    _chown_dir(context, proot, "/partition_root")

    pdb_count = sum(1 for _ in pdbs_dir.rglob("*.pdb"))
    return MaterializeResult(
        metadata={
            "partition_key": partition_key,
            "config_dir": MetadataValue.path(config_dir),
            "config_name": config_name,
            "pdbs_dir": MetadataValue.path(str(pdbs_dir)),
            "pdb_count": pdb_count,
        }
    )


# ---------------------------------------------------------------------------
# proteinmpnn group  (partitioned)
# ---------------------------------------------------------------------------

@asset(
    group_name="proteinmpnn",
    partitions_def=design_configs,
    deps=[rfdiffusion_backbones],
    description=(
        "Parses RFdiffusion PDB outputs into ProteinMPNN JSONL input for one partition. "
        "Docker image is read from ``pipeline_config.yaml``."
    ),
)
def proteinmpnn_parsed(
    context: AssetExecutionContext,
) -> MaterializeResult:
    pc = _read_pipeline_config()
    proot = _partition_root(pc.rfdiffusion.outputs_dir, context.partition_key)
    pdbs_dir = proot / "pdbs"
    mpnn_dir = proot / "proteinmpnn"
    mpnn_dir.mkdir(parents=True, exist_ok=True)

    _run(
        context,
        [
            "docker", "run", "--rm",
            "--entrypoint", "python",
            "-v", f"{pdbs_dir}:/input/pdbs",
            "-v", f"{mpnn_dir}:/output",
            pc.proteinmpnn.docker_image,
            "/app/proteinmpnn/helper_scripts/parse_multiple_chains.py",
            "--input_path=/input/pdbs",
            "--output_path=/output/parsed_pdbs.jsonl",
        ],
    )
    _chown_dir(context, mpnn_dir, "/mpnn_out")

    j = mpnn_dir / "parsed_pdbs.jsonl"
    return MaterializeResult(
        metadata={
            "parsed_jsonl": MetadataValue.path(str(j)),
            "size_bytes": j.stat().st_size if j.is_file() else 0,
        }
    )


@asset(
    group_name="proteinmpnn",
    partitions_def=design_configs,
    deps=[proteinmpnn_parsed],
    description=(
        "Creates ``assigned_chains.jsonl`` for one partition. "
        "Chain list and Docker image are read from ``pipeline_config.yaml``."
    ),
)
def proteinmpnn_assigned_chains(
    context: AssetExecutionContext,
) -> MaterializeResult:
    pc = _read_pipeline_config()
    mpnn_dir = _partition_root(pc.rfdiffusion.outputs_dir, context.partition_key) / "proteinmpnn"
    mpnn = pc.proteinmpnn
    _run(
        context,
        [
            "docker", "run", "--rm",
            "--entrypoint", "python",
            "-v", f"{mpnn_dir}:/output",
            mpnn.docker_image,
            "/app/proteinmpnn/helper_scripts/assign_fixed_chains.py",
            "--input_path=/output/parsed_pdbs.jsonl",
            "--output_path=/output/assigned_chains.jsonl",
            "--chain_list", mpnn.chain_list,
        ],
    )
    _chown_dir(context, mpnn_dir, "/mpnn_out")

    p = mpnn_dir / "assigned_chains.jsonl"
    return MaterializeResult(
        metadata={
            "assigned_jsonl": MetadataValue.path(str(p)),
            "chain_list": mpnn.chain_list,
        }
    )


@asset(
    group_name="proteinmpnn",
    partitions_def=design_configs,
    deps=[proteinmpnn_assigned_chains],
    description=(
        "Generates ProteinMPNN sequences for one partition and writes FASTA files "
        "to the partition's ``proteinmpnn/seqs`` directory. "
        "All parameters are read from ``pipeline_config.yaml``."
    ),
)
def proteinmpnn_sequences(
    context: AssetExecutionContext,
) -> MaterializeResult:
    pc = _read_pipeline_config()
    mpnn_dir = _partition_root(pc.rfdiffusion.outputs_dir, context.partition_key) / "proteinmpnn"
    mpnn = pc.proteinmpnn
    cmd: List[str] = [
        "docker", "run", "--rm",
        "--runtime=nvidia", *_docker_gpu_args(mpnn.gpus),
        "-v", f"{mpnn_dir}:/output",
        mpnn.docker_image,
        "--jsonl_path", "/output/parsed_pdbs.jsonl",
        "--chain_id_jsonl", "/output/assigned_chains.jsonl",
        "--out_folder", "/output",
        "--num_seq_per_target", str(mpnn.num_seq_per_target),
        "--omit_AAs", mpnn.omit_AAs,
        "--sampling_temp", mpnn.sampling_temp,
    ]
    if mpnn.fixed_positions_jsonl:
        cmd.extend(["--fixed_positions_jsonl", mpnn.fixed_positions_jsonl])
    if mpnn.use_soluble_model:
        cmd.append("--use_soluble_model")
    cmd.extend(mpnn.extra_args)
    _run(context, cmd)
    _chown_dir(context, mpnn_dir, "/mpnn_out")

    seq_dir = mpnn_dir / "seqs"
    fa_files = sorted(seq_dir.glob("*.fa")) if seq_dir.is_dir() else []
    return MaterializeResult(
        metadata={
            "seq_dir": MetadataValue.path(str(seq_dir)),
            "fasta_count": len(fa_files),
            "num_seq_per_target": mpnn.num_seq_per_target,
            "fixed_positions_jsonl": mpnn.fixed_positions_jsonl,
            "omit_AAs": mpnn.omit_AAs,
            "sampling_temp": mpnn.sampling_temp,
            "use_soluble_model": mpnn.use_soluble_model,
            "sample_files": MetadataValue.text("\n".join(f.name for f in fa_files[:10])),
        }
    )


# ---------------------------------------------------------------------------
# boltz2 group  (partitioned; YAML inputs after ProteinMPNN)
# ---------------------------------------------------------------------------

@asset(
    group_name="boltz2",
    partitions_def=design_configs,
    deps=[proteinmpnn_sequences],
    description=(
        "Writes Boltz-2 ``predict`` input YAMLs to ``proteinmpnn/combined_yamls`` "
        "(chain A = binder; B, C, … = epitope segments split on ``:``), using the same "
        "ProteinMPNN FASTAs and epitope pairing as ColabFold. "
        "``colabfold.target_epitope_fasta``, ``colabfold.fasta_extensions``, and "
        "``boltz2.template_yaml`` are read from ``pipeline_config.yaml``."
    ),
)
def boltz2_input_yamls(
    context: AssetExecutionContext,
) -> MaterializeResult:
    pc = _read_pipeline_config()
    proot = _partition_root(pc.rfdiffusion.outputs_dir, context.partition_key)
    seq_dir = proot / "proteinmpnn" / "seqs"
    yaml_dir = proot / "proteinmpnn" / "combined_yamls"
    cf = pc.colabfold
    bz = pc.boltz2
    append_path = Path(cf.target_epitope_fasta)
    template_path = Path(bz.template_yaml)

    append_seq = read_fasta_sequence_flat(append_path)
    if not append_seq:
        raise RuntimeError(f"No sequence residues found in {append_path}")
    if not template_path.is_file():
        raise RuntimeError(f"Boltz-2 template YAML not found: {template_path}")

    template_text = template_path.read_text(encoding="utf-8")
    exts = tuple(e if e.startswith(".") else f".{e}" for e in cf.fasta_extensions)
    files = sorted(p for p in seq_dir.iterdir() if p.is_file() and p.suffix.lower() in exts)
    if not files:
        raise RuntimeError(f"No FASTA files found in {seq_dir} with extensions {exts}")

    yaml_dir.mkdir(parents=True, exist_ok=True)
    written = 0
    for src in files:
        n = 0
        for base, binder, target in iter_exploded_binder_target_mpnn(src, append_seq):
            write_yaml_for_pair(template_text, yaml_dir, base, binder, target)
            n += 1
        written += n
        context.log.info(f"Boltz-2 YAMLs from {src.name}: {n} file(s)")

    yaml_files = sorted(yaml_dir.glob("*.yaml")) if yaml_dir.is_dir() else []
    return MaterializeResult(
        metadata={
            "combined_yamls_dir": MetadataValue.path(str(yaml_dir)),
            "source_fastas": len(files),
            "yaml_files_written": written,
            "template_yaml": MetadataValue.path(bz.template_yaml),
            "append_fasta": MetadataValue.path(cf.target_epitope_fasta),
            "sample_files": MetadataValue.text("\n".join(f.name for f in yaml_files[:10])),
        }
    )


@asset(
    group_name="boltz2",
    partitions_def=design_configs,
    deps=[boltz2_input_yamls],
    description=(
        "Runs Boltz-2 structure predictions for one partition using input YAMLs from "
        "``proteinmpnn/combined_yamls`` and writes results to ``boltz2/``. "
        "All parameters are read from ``pipeline_config.yaml`` (``boltz2`` section)."
    ),
)
def boltz2_predictions(
    context: AssetExecutionContext,
) -> MaterializeResult:
    pc = _read_pipeline_config()
    proot = _partition_root(pc.rfdiffusion.outputs_dir, context.partition_key)
    bz = pc.boltz2
    yaml_dir = proot / "proteinmpnn" / "combined_yamls"
    out_dir = proot / "boltz2"
    out_dir.mkdir(parents=True, exist_ok=True)

    yaml_files = sorted(yaml_dir.glob("*.yaml")) if yaml_dir.is_dir() else []
    if not yaml_files:
        raise RuntimeError(f"No YAML input files found in {yaml_dir}")

    chunk_size = bz.query_chunk_size if bz.query_chunk_size > 0 else len(yaml_files)
    chunk_root = proot / "proteinmpnn" / "combined_yamls_chunks"
    if chunk_root.exists():
        shutil.rmtree(chunk_root)
    chunk_root.mkdir(parents=True, exist_ok=True)

    _chmod_mount_writable(context, out_dir, "/output")
    _chmod_mount_writable(context, bz.cache_volume, "/cache")

    chunk_count = 0
    for i in range(0, len(yaml_files), chunk_size):
        chunk_count += 1
        chunk_files = yaml_files[i : i + chunk_size]
        chunk_dir = chunk_root / f"chunk_{chunk_count:04d}"
        chunk_dir.mkdir(parents=True, exist_ok=True)
        for src in chunk_files:
            dst = chunk_dir / src.name
            if dst.exists():
                dst.unlink()
            # Mount the whole partition root as /work so these links resolve in Docker.
            os.symlink(os.path.relpath(src, start=chunk_dir), dst)

        context.log.info(
            f"Boltz-2 chunk {chunk_count}: {len(chunk_files)} YAML file(s) "
            f"({i + 1}-{i + len(chunk_files)} of {len(yaml_files)})"
        )

        cmd = [
            "docker", "run", "--rm",
            "--runtime=nvidia",
            "--ipc=host",
            *_docker_gpu_args(bz.gpus),
            "-e",
            "LD_LIBRARY_PATH=/usr/local/lib/python3.11/dist-packages/nvidia/cu13/lib:/usr/local/nvidia/lib:/usr/local/nvidia/lib64",
            "-v", f"{proot}:/work:rw",
            "-v", f"{bz.cache_volume}:/cache:rw",
            bz.docker_image,
            "predict", f"/work/proteinmpnn/combined_yamls_chunks/chunk_{chunk_count:04d}",
            "--cache", "/cache",
            "--out_dir", "/work/boltz2",
            "--recycling_steps", str(bz.recycling_steps),
            "--devices", str(bz.devices),
            "--diffusion_samples", str(bz.diffusion_samples),
            "--num_workers", str(bz.num_workers),
        ]
        if bz.use_msa_server:
            cmd.append("--use_msa_server")
        if bz.use_potentials:
            cmd.append("--use_potentials")
        if bz.override:
            cmd.append("--override")
        if bz.no_kernels:
            cmd.append("--no_kernels")

        _run(context, cmd)
    _chown_dir(context, out_dir, "/output")

    cif_files = list(out_dir.rglob("*.cif"))
    pdb_files = list(out_dir.rglob("*.pdb"))
    json_files = list(out_dir.rglob("*.json"))
    structure_files = cif_files or pdb_files
    if not structure_files:
        raise RuntimeError(
            f"Boltz-2 completed without producing any structure files in {out_dir}. "
            "Check the Boltz-2 log for errors."
        )

    return MaterializeResult(
        metadata={
            "predictions_dir": MetadataValue.path(str(out_dir)),
            "input_yaml_count": len(yaml_files),
            "query_chunk_size": chunk_size,
            "chunk_count": chunk_count,
            "structure_file_count": len(structure_files),
            "json_file_count": len(json_files),
            "recycling_steps": bz.recycling_steps,
            "diffusion_samples": bz.diffusion_samples,
            "devices": bz.devices,
            "use_msa_server": bz.use_msa_server,
        }
    )


# ---------------------------------------------------------------------------
# colabfold group  (partitioned)
# ---------------------------------------------------------------------------

@asset(
    group_name="colabfold",
    partitions_def=design_configs,
    deps=[proteinmpnn_sequences],
    description=(
        "Builds ColabFold input FASTAs by appending the target epitope sequence to "
        "each ProteinMPNN FASTA in a partition. "
        "``target_epitope_fasta`` and ``fasta_extensions`` are read from ``pipeline_config.yaml``."
    ),
)
def colabfold_input_fastas(
    context: AssetExecutionContext,
) -> MaterializeResult:
    pc = _read_pipeline_config()
    proot = _partition_root(pc.rfdiffusion.outputs_dir, context.partition_key)
    seq_dir = proot / "proteinmpnn" / "seqs"
    combined_dir = proot / "proteinmpnn" / "combined_seqs"
    cf = pc.colabfold
    append_path = Path(cf.target_epitope_fasta)

    append_seq = read_fasta_sequence_flat(append_path)
    if not append_seq:
        raise RuntimeError(f"No sequence residues found in {append_path}")

    exts = tuple(e if e.startswith(".") else f".{e}" for e in cf.fasta_extensions)
    files = sorted(p for p in seq_dir.iterdir() if p.is_file() and p.suffix.lower() in exts)
    if not files:
        raise RuntimeError(f"No FASTA files found in {seq_dir} with extensions {exts}")

    combined_dir.mkdir(parents=True, exist_ok=True)
    written = 0
    for src in files:
        n = explode_fasta_records_with_append(src, combined_dir, append_seq)
        written += n
        context.log.info(f"Merged {src.name} into {n} single-record FASTA(s)")

    return MaterializeResult(
        metadata={
            "combined_dir": MetadataValue.path(str(combined_dir)),
            "source_fastas": len(files),
            "files_written": written,
            "append_fasta": MetadataValue.path(cf.target_epitope_fasta),
            "file_list": MetadataValue.text("\n".join(f.name for f in files)),
        }
    )


@asset(
    group_name="colabfold",
    partitions_def=design_configs,
    deps=[colabfold_input_fastas],
    description=(
        "Runs ColabFold predictions for one partition using merged FASTAs from "
        "``proteinmpnn/combined_seqs`` and writes results to ``colabfold/``. "
        "All parameters are read from ``pipeline_config.yaml``."
    ),
)
def colabfold_predictions(
    context: AssetExecutionContext,
) -> MaterializeResult:
    pc = _read_pipeline_config()
    proot = _partition_root(pc.rfdiffusion.outputs_dir, context.partition_key)
    cf = pc.colabfold
    out_dir = proot / "colabfold"
    out_dir.mkdir(parents=True, exist_ok=True)
    combined_dir = proot / "proteinmpnn" / "combined_seqs"
    query_files = sorted(p for p in combined_dir.iterdir() if p.is_file())
    if not query_files:
        raise RuntimeError(f"No FASTA query files found in {combined_dir}")

    patch = ""
    if cf.apply_alphafold_numpy_patch:
        patch = (
            "sed -i 's/np\\.sum(x for x in feats)/sum(x for x in feats)/g' "
            "/usr/local/lib/python3.12/site-packages/alphafold/data/msa_pairing.py && "
        )

    cf_args = " ".join(cf.extra_colabfold_args)
    chunk_size = cf.query_chunk_size if cf.query_chunk_size > 0 else len(query_files)
    chunk_root = proot / "proteinmpnn" / "combined_seqs_chunks"
    if chunk_root.exists():
        shutil.rmtree(chunk_root)
    chunk_root.mkdir(parents=True, exist_ok=True)

    chunk_count = 0
    for i in range(0, len(query_files), chunk_size):
        chunk_count += 1
        chunk_files = query_files[i : i + chunk_size]
        chunk_dir = chunk_root / f"chunk_{chunk_count:04d}"
        chunk_dir.mkdir(parents=True, exist_ok=True)
        for src in chunk_files:
            dst = chunk_dir / src.name
            if dst.exists():
                dst.unlink()
            # Use relative links so they still resolve after the partition root
            # is mounted at /work inside the ColabFold container.
            os.symlink(os.path.relpath(src, start=chunk_dir), dst)

        context.log.info(
            f"ColabFold chunk {chunk_count}: {len(chunk_files)} query file(s) "
            f"({i + 1}-{i + len(chunk_files)} of {len(query_files)})"
        )

        # Mount the whole partition root as /work so inner paths are predictable:
        #   /work/proteinmpnn/combined_seqs_chunks/chunk_xxxx  →  chunk FASTAs
        #   /work/colabfold                                    →  outputs
        inner_cmd = (
            f"{patch}colabfold_batch --model-type {cf.model_type} "
            f"--pair-mode {cf.pair_mode} "
            f"--num-models {cf.num_models} --num-recycle {cf.num_recycle} "
            f"--num-relax {cf.num_relax} {cf_args} "
            f"/work/proteinmpnn/combined_seqs_chunks/chunk_{chunk_count:04d} /work/colabfold"
        ).strip()

        _run(
            context,
            [
                "docker", "run", "--rm",
                "--runtime=nvidia", *_docker_gpu_args(cf.gpus),
                "-v", f"{cf.colabfold_cache_host}:/cache:rw",
                "-v", f"{proot}:/work:rw",
                cf.colabfold_image,
                "bash", "-c", inner_cmd,
            ],
        )
    _chown_dir(context, proot, "/work")

    pdb_files = list(out_dir.rglob("*.pdb"))
    json_files = list(out_dir.rglob("*.json"))
    result_files = pdb_files + json_files
    if not pdb_files:
        raise RuntimeError(
            f"ColabFold completed without producing any PDB files in {out_dir}. "
            "Check the ColabFold log for skipped or unreadable query FASTAs."
        )

    return MaterializeResult(
        metadata={
            "predictions_dir": MetadataValue.path(str(out_dir)),
            "model_type": cf.model_type,
            "pair_mode": cf.pair_mode,
            "num_models": cf.num_models,
            "num_recycle": cf.num_recycle,
            "num_relax": cf.num_relax,
            "query_count": len(query_files),
            "query_chunk_size": chunk_size,
            "chunk_count": chunk_count,
            "pdb_file_count": len(pdb_files),
            "json_file_count": len(json_files),
            "result_file_count": len(result_files),
        }
    )


# ---------------------------------------------------------------------------
# filters group  (partitioned; optional pairing of ColabFold vs Boltz-2 structures)
# ---------------------------------------------------------------------------


@asset(
    group_name="filters",
    partitions_def=design_configs,
    deps=[colabfold_predictions, boltz2_predictions],
    description=(
        "Runs ``chain_b_align_chain_a_rmsd.py`` from the filters image for each "
        "``proteinmpnn/combined_seqs`` FASTA stem when both a ColabFold PDB and a "
        "Boltz-2 structure file can be matched. "
        "Dagster dependency on **both** fold assets keeps ordering when both are in the "
        "same job; if one output directory is empty at runtime, pairwise RMSD is "
        "skipped and ``filters/summary.json`` records what was found. "
        "Parameters come from ``pipeline_config.yaml`` (``filters`` section)."
    ),
)
def filter_chain_backbone_rmsd(context: AssetExecutionContext) -> MaterializeResult:
    pc = _read_pipeline_config()
    proot = _partition_root(pc.rfdiffusion.outputs_dir, context.partition_key)
    ft = pc.filters

    cf_paths, bz_paths = _collect_fold_structure_paths(proot)
    filters_dir = proot / "filters"
    filters_dir.mkdir(parents=True, exist_ok=True)

    keys = _design_keys_from_combined_fastas(proot, pc.colabfold.fasta_extensions)
    tasks: List[tuple[str, Optional[Path], Optional[Path]]] = [
        (key, _pick_structure_for_key(key, cf_paths), _pick_structure_for_key(key, bz_paths))
        for key in keys
    ]
    if not tasks and len(cf_paths) == 1 and len(bz_paths) == 1:
        tasks.append(("single", cf_paths[0], bz_paths[0]))

    summary: Dict[str, object] = {
        "partition_key": context.partition_key,
        "design_keys_from_combined_seqs": keys,
        "colabfold_pdb_paths": [str(p.relative_to(proot)) for p in cf_paths],
        "boltz_structure_paths": [str(p.relative_to(proot)) for p in bz_paths],
        "pairwise_rmsd": [],
        "skipped_pairwise": [],
    }

    if not cf_paths and not bz_paths:
        raise RuntimeError(
            f"No ColabFold PDBs and no Boltz-2 structures under {proot}. "
            "Expected files under colabfold/ and/or boltz2/."
        )

    script_inner = "/filters/scripts/chain_b_align_chain_a_rmsd.py"
    pairwise_runs: List[Dict[str, str]] = []
    skipped: List[Dict[str, str]] = []

    for key, cf_p, bz_p in tasks:
        if cf_p is not None and bz_p is not None:
            safe = _safe_token_for_filename(key)
            out_name = f"rmsd_{safe}.json"
            out_host = filters_dir / out_name
            ref_in = f"/work/{cf_p.relative_to(proot).as_posix()}"
            mob_in = f"/work/{bz_p.relative_to(proot).as_posix()}"
            out_in = f"/work/filters/{out_name}"
            cmd = [
                "docker",
                "run",
                "--rm",
                "-v",
                f"{proot}:/work:rw",
                ft.docker_image,
                "python",
                script_inner,
                ref_in,
                mob_in,
                "--align-chain",
                ft.align_chain,
                "--rmsd-chains",
                ft.rmsd_chains,
                "-o",
                out_in,
            ]
            _run(context, cmd)
            pairwise_runs.append(
                {
                    "key": key,
                    "reference": str(cf_p.relative_to(proot)),
                    "mobile": str(bz_p.relative_to(proot)),
                    "output": str(out_host.relative_to(proot)),
                }
            )
        else:
            skipped.append(
                {
                    "key": key,
                    "colabfold": str(cf_p.relative_to(proot)) if cf_p else "",
                    "boltz2": str(bz_p.relative_to(proot)) if bz_p else "",
                    "reason": "Need both ColabFold PDB and Boltz structure for pairwise RMSD",
                }
            )

    summary["pairwise_rmsd"] = pairwise_runs
    summary["skipped_pairwise"] = skipped
    summary_path = filters_dir / "summary.json"
    summary_path.write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")

    _chown_dir(context, proot, "/work")

    return MaterializeResult(
        metadata={
            "filters_dir": MetadataValue.path(str(filters_dir)),
            "pairwise_runs": len(pairwise_runs),
            "skipped_pairwise": len(skipped),
            "colabfold_pdbs": len(cf_paths),
            "boltz_structures": len(bz_paths),
            "summary": MetadataValue.path(str(summary_path)),
        }
    )


# ---------------------------------------------------------------------------
# Exported list consumed by definitions.py
# ---------------------------------------------------------------------------

all_assets = [
    pipeline_config,
    rfdiffusion_yamls,
    rfdiffusion_backbones,
    proteinmpnn_parsed,
    proteinmpnn_assigned_chains,
    proteinmpnn_sequences,
    boltz2_input_yamls,
    boltz2_predictions,
    colabfold_input_fastas,
    colabfold_predictions,
    filter_chain_backbone_rmsd,
]
