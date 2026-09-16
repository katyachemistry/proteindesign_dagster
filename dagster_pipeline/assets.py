"""
Asset definitions for design backends (RFdiffusion / BoltzGen) → ProteinMPNN → Boltz-2 / ESMFold.

Partitioning
------------
`pipeline_config` is a non-partitioned seed asset that:
  1. Persists tool parameters to ``{outputs_root}/{run_id}/pipeline_config.yaml``
  2. Generates configs for enabled design tools under
     ``{outputs_root}/{run_id}/design_configs/{tool}/{design_name}/``
  3. Writes a manifest JSON:  partition_key → {tool, config paths, target_pdb, …}
  4. Registers all keys in DynamicPartitionsDefinition("design_configs")

Every downstream asset is partitioned by "design_configs". Each ``generate_configs``
run writes to a fresh directory:

  {outputs_root}/{run_id}/{partition_suffix}/
      designs/{tool}/        ← design_*.pdb (BoltzGen also keeps raw/ CIFs)
      antihotspot_contacts/  ← binder–target CA contact reports
      structure_filter/      ← per-design filter JSON + summary
      pdbs_filtered/         ← designs passing hotspot/antihotspot rules
      proteinmpnn/
          parsed_pdbs.jsonl
          assigned_chains.jsonl
          seqs/
          seqs_filtered/     ← SoluProt-passing MPNN sequences
          soluprot/          ← SoluProt predictions + filter summary
          boltz2_msas/       ← per-design Boltz CSVs (global search cache is separate)
          combined_seqs/
          combined_yamls/    ← Boltz-2 predict inputs (A binder; B,C,… target by ":")
          esmfold_inputs/    ← ESMFold2 predict inputs (ProteinInput JSON per design)
      esmfold/               ← ESMFold predictions
      esmfold_renumbered/    ← ESMFold with target chains renumbered to reference PDB
      boltz2/                ← Boltz-2 predictions (when run)
      boltz2_renumbered/     ← Boltz-2 with target chains renumbered to reference PDB

Partition key format
--------------------
  {run_id}__{tool}__{design_name}__{target_pdb_stem}__{hotspot_or_spec_stem}__{yaml_stem}
  e.g.  2026-06-23_143052__rfdiffusion__designs_1__truncated_renumbered_loops__loops__scaffold_guided_nanobody_loop1_loop3_set1

On-disk partition directories omit the ``{run_id}__`` prefix because ``outputs_dir`` is
already ``{outputs_root}/{run_id}/``.

Docker notes
------------
• -it is omitted (no TTY under Dagster).
• Tool containers pass ``--user {uid}:{gid}`` of the Dagster process so bind
  mounts are written as the host user. A synthetic ``/etc/passwd`` + ``HOME=/tmp``
  is also injected so libraries that call ``pwd.getpwuid`` work for any host UID
  (images often only know a built-in user such as boltz uid 1000).
• outputs_dir parent and target PDB parent are mounted as themselves so
  container paths == host paths.
• BoltzGen also needs ``--shm-size``, ``--group-add`` (gpuusers), ``HF_TOKEN``,
  and CUDA 13 NVRTC on ``LD_LIBRARY_PATH`` (see ``boltzgen_generation``).
"""

import csv
import json
import os
import shlex
import shutil
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml
from dagster import (
    AssetExecutionContext,
    Config,
    DynamicPartitionsDefinition,
    Failure,
    MaterializeResult,
    MetadataValue,
    Output,
    asset,
)
from pydantic import Field, model_validator
from tqdm import tqdm

from dagster_pipeline.resources import (
    DEFAULT_BOLTZ2_CACHE_VOLUME,
    DEFAULT_BOLTZGEN_CACHE_DIR,
    DEFAULT_BOLTZGEN_GPU_GROUP_ID,
    DEFAULT_BOLTZGEN_SHM_SIZE,
    DEFAULT_ESMFOLD2_HF_CACHE_DIR,
    DEFAULT_MSA_CACHE_DIR,
    DEFAULT_OUTPUTS_ROOT,
    RegisterSequencePartitionsConfig,
    MSAToolConfig,
    load_pipeline_config_dict,
    Boltz2ToolConfig,
    BoltzGenToolConfig,
    DesignSpec,
    ProteinMPNNToolConfig,
    RFDiffusionToolConfig,
    PromeraToolConfig,
    SoluProtToolConfig,
    ESMFoldToolConfig,
    FinalScoresToolConfig,
    FilteredDesignsToolConfig,
    LhOfftargetConfig,
    LhAcOfftargetConfig,
    RuleBPresentationConfig,
    RuleAcPresentationConfig,
    StructureFiltersToolConfig,
    _normalize_structure_filters_yaml,
)

# Make /storage/proteindesign importable so we can use utils/
_PROTEINDESIGN = Path(__file__).resolve().parents[2]
if str(_PROTEINDESIGN) not in sys.path:
    sys.path.insert(0, str(_PROTEINDESIGN))

from utils.filter_rfdiffusion_contacts import filter_partition_contacts  # noqa: E402
from utils.generate_boltzgen_yamls import generate_boltzgen_yamls  # noqa: E402
from utils.generate_hotspot_yamls import (  # noqa: E402
    generate_yamls,
    generate_yamls_from_template_text,
    iter_generated_yaml_specs,
)
from utils.load_rfdiffusion_antihotspots import (  # noqa: E402
    load_antihotspots_config,
    rfdiffusion_config_path,
)
from utils.generate_promera_yamls import generate_promera_task_configs  # noqa: E402
from utils.normalize_promera_designs import normalize_promera_designs  # noqa: E402
from utils.import_promera_sequences import import_promera_fasta_to_mpnn_seqs  # noqa: E402
from utils.merge_fasta import explode_fasta_records_with_append, read_fasta_sequence_flat  # noqa: E402
from utils.import_external_sequences import import_external_fasta_to_mpnn_seqs  # noqa: E402
from utils.soluprot_filter import (  # noqa: E402
    filter_partition_soluprot,
    list_mpnn_fasta_files,
    write_soluprot_input_fasta,
)
from utils.boltz2_msas import (  # noqa: E402
    ComplexSpec,
    MsaServerClient,
    build_boltz_msa_bundle,
)
from utils.write_boltz2_yamls import (  # noqa: E402
    iter_exploded_binder_target_mpnn,
    split_target_segments,
    write_yaml_for_pair,
)
from utils.write_esmfold2_jsons import write_json_for_pair  # noqa: E402
from utils.proteinmpnn_fixed_positions import (  # noqa: E402
    primary_designed_chain,
    write_fixed_positions_jsonl,
)
from utils.proteinmpnn_omit_aas import resolve_omit_aas_for_names  # noqa: E402


# ---------------------------------------------------------------------------
# Dynamic partitions – one per generated design-tool config
# ---------------------------------------------------------------------------

design_configs = DynamicPartitionsDefinition(name="design_configs")

DESIGN_TOOL_RFDIFFUSION = "rfdiffusion"
DESIGN_TOOL_BOLTZGEN = "boltzgen"
DESIGN_TOOL_PROMERA = "promera"

# ---------------------------------------------------------------------------
# Pipeline-wide tool config (Launchpad template + run-scoped copy under each run dir)
# ---------------------------------------------------------------------------

class PipelineAllToolsConfig(Config):
    """All tool parameters for design backends → ProteinMPNN → Boltz-2 / ESMFold.

    Materialize the ``pipeline_config`` asset with this config to persist settings
    to ``{outputs_root}/{run_id}/pipeline_config.yaml``, generate configs for enabled
    design tools (``rfdiffusion`` / ``boltzgen``), and register design partitions.
    Downstream tool assets read the run-scoped config file.
    """

    gpus: str = "1,2"
    run_id: str = ""
    outputs_root: str = DEFAULT_OUTPUTS_ROOT
    # Order matches the design pipeline: design → filter → MPNN → SoluProt → MSA → predict → score.
    rfdiffusion: RFDiffusionToolConfig = Field(default_factory=RFDiffusionToolConfig)
    boltzgen: BoltzGenToolConfig = Field(default_factory=BoltzGenToolConfig)
    promera: PromeraToolConfig = Field(default_factory=PromeraToolConfig)
    structure_filters: StructureFiltersToolConfig = Field(
        default_factory=StructureFiltersToolConfig
    )
    proteinmpnn: ProteinMPNNToolConfig = Field(default_factory=ProteinMPNNToolConfig)
    soluprot: SoluProtToolConfig = Field(default_factory=SoluProtToolConfig)
    msa: MSAToolConfig = Field(default_factory=MSAToolConfig)
    boltz2: Boltz2ToolConfig = Field(default_factory=Boltz2ToolConfig)
    esmfold: ESMFoldToolConfig = Field(default_factory=ESMFoldToolConfig)
    final_scores: FinalScoresToolConfig = Field(default_factory=FinalScoresToolConfig)
    filtered_designs: FilteredDesignsToolConfig = Field(default_factory=FilteredDesignsToolConfig)
    lh_offtarget_b: LhOfftargetConfig = Field(default_factory=LhOfftargetConfig)
    lh_offtarget_ac: LhAcOfftargetConfig = Field(default_factory=LhAcOfftargetConfig)
    rule_b_presentation: RuleBPresentationConfig = Field(
        default_factory=RuleBPresentationConfig
    )
    rule_ac_presentation: RuleAcPresentationConfig = Field(
        default_factory=RuleAcPresentationConfig
    )

    @model_validator(mode="before")
    @classmethod
    def _hoist_legacy_fields(cls, data: Any) -> Any:
        if not isinstance(data, dict):
            return data
        data = dict(data)
        if not str(data.get("gpus", "")).strip():
            for key in ("rfdiffusion", "proteinmpnn", "boltz2"):
                section = data.get(key)
                if isinstance(section, dict) and str(section.get("gpus", "")).strip():
                    data["gpus"] = section["gpus"]
                    break

        colabfold = data.get("colabfold")
        boltz2 = data.setdefault("boltz2", {})
        esmfold = data.setdefault("esmfold", {})
        if isinstance(colabfold, dict) and isinstance(boltz2, dict):
            legacy_fasta = colabfold.pop("target_epitope_fasta", None)
            if legacy_fasta and not str(boltz2.get("target_fasta", "")).strip():
                boltz2["target_fasta"] = legacy_fasta
            legacy_exts = colabfold.pop("fasta_extensions", None)
            if legacy_exts and not boltz2.get("fasta_extensions"):
                boltz2["fasta_extensions"] = legacy_exts

        if isinstance(boltz2, dict) and isinstance(esmfold, dict):
            legacy_ckpt_host = esmfold.pop("checkpoints_host", None)
            esmfold.pop("checkpoints_container_path", None)
            esmfold.pop("checkpoints_dir", None)
            esmfold.pop("fold_script", None)
            for legacy_key in (
                "num_recycles",
                "max_tokens_per_batch",
                "chunk_size",
                "cpu_offload",
            ):
                esmfold.pop(legacy_key, None)
            if legacy_ckpt_host and not str(esmfold.get("hf_cache_dir", "")).strip():
                pass  # v1 checkpoints are not used by ESMFold2

        # These runtime/implementation settings are intentionally not part of
        # the Launchpad schema. Drop them when reading older run configs.
        hidden_fields = {
            "rfdiffusion": ("outputs_dir",),
            "boltzgen": (
                "outputs_dir",
                "cache_dir",
                "use_kernels",
                "shm_size",
                "gpu_group_id",
            ),
            "proteinmpnn": ("fixed_positions_jsonl",),
            "msa": ("cache_dir",),
            "boltz2": ("cache_volume", "no_kernels"),
            "esmfold": ("target_fasta", "predict_script", "hf_cache_dir"),
            "final_scores": ("boltz_subdir", "esmfold_subdir"),
        }
        for section_name, keys in hidden_fields.items():
            section = data.get(section_name)
            if isinstance(section, dict):
                for key in keys:
                    section.pop(key, None)

        structure_filters = data.setdefault("structure_filters", {})
        if isinstance(structure_filters, dict):
            for legacy_key in ("rfdiffusion", "boltz2"):
                legacy_section = data.get(legacy_key)
                if not isinstance(legacy_section, dict):
                    continue
                legacy_image = legacy_section.pop("structure_docker_image", None)
                if legacy_image and not str(
                    structure_filters.get("structure_docker_image", "")
                ).strip():
                    structure_filters["structure_docker_image"] = legacy_image

        # Older runs used ``lh_offtarget:``; prefer ``lh_offtarget_b:``.
        if "lh_offtarget_b" not in data and isinstance(data.get("lh_offtarget"), dict):
            data["lh_offtarget_b"] = data.pop("lh_offtarget")
        else:
            data.pop("lh_offtarget", None)

        return data


def _manifest_entry(context: AssetExecutionContext, pc: PipelineAllToolsConfig) -> dict[str, Any]:
    manifest = _read_manifest(_run_outputs_dir(pc))
    return manifest.get(context.partition_key, {})


def _run_outputs_dir(pc: PipelineAllToolsConfig) -> str:
    """Resolve the run directory from pipeline-wide outputs root and run ID."""
    if str(pc.run_id or "").strip():
        return str(Path(pc.outputs_root or DEFAULT_OUTPUTS_ROOT) / _sanitize_run_id(pc.run_id))
    raise Failure(
        description=(
            "Cannot resolve run outputs directory: set run_id / outputs_root or materialize "
            "pipeline_config first."
        )
    )


def _designs_dir(proot: Path, tool: str) -> Path:
    return Path(proot) / "designs" / tool


def _is_sequence_import_partition(context: AssetExecutionContext, pc: PipelineAllToolsConfig) -> bool:
    return _manifest_entry(context, pc).get("source") == "sequence_import"


def _pipeline_target_pdb(pc: PipelineAllToolsConfig) -> str:
    """Reference PDB for Boltz-2 renumbering from the run-scoped pipeline config."""
    target_pdb = str(pc.boltz2.target_pdb or "").strip()
    if target_pdb and Path(target_pdb).is_file():
        return target_pdb
    raise FileNotFoundError(
        "boltz2.target_pdb is missing or not found on host "
        f"({target_pdb!r}). Set it in the run-scoped pipeline_config.yaml."
    )


def _sanitize_run_id(run_id: str) -> str:
    return "".join(c if (c.isalnum() or c in "-_") else "_" for c in run_id.strip())


def _resolve_run_outputs(outputs_root: str, run_id: str) -> tuple[str, str]:
    """Return (run_id, outputs_dir). Empty run_id → timestamped run folder."""
    resolved_run_id = _sanitize_run_id(run_id) or datetime.now().strftime("%Y-%m-%d_%H%M%S")
    outputs_dir = str(Path(outputs_root) / resolved_run_id)
    return resolved_run_id, outputs_dir


def _run_config_path(outputs_dir: str) -> Path:
    return Path(outputs_dir) / "pipeline_config.yaml"


def _default_outputs_root() -> str:
    data = load_pipeline_config_dict()
    root = str(data.get("outputs_root") or "").strip()
    return root or DEFAULT_OUTPUTS_ROOT


def _run_id_from_partition_key(partition_key: str) -> str:
    run_id, sep, _rest = partition_key.partition("__")
    if not sep:
        raise ValueError(
            f"Invalid partition key (expected '{{run_id}}__{{suffix}}'): {partition_key!r}"
        )
    return run_id


def _resolve_outputs_dir(
    *,
    partition_key: str | None = None,
    outputs_dir: str | None = None,
) -> Path:
    if outputs_dir:
        return Path(outputs_dir)
    if partition_key:
        run_id = _run_id_from_partition_key(partition_key)
        return Path(_default_outputs_root()) / run_id
    data = load_pipeline_config_dict()
    run_id = _sanitize_run_id(str(data.get("run_id") or ""))
    if not run_id:
        raise FileNotFoundError(
            "Cannot resolve run outputs_dir: set run_id in the Launchpad template "
            "(dagster_pipeline/pipeline_config.yaml) or materialize a partitioned asset."
        )
    outputs_root = str(data.get("outputs_root") or DEFAULT_OUTPUTS_ROOT)
    return Path(outputs_root) / run_id


def _read_pipeline_config(
    *,
    partition_key: str | None = None,
    outputs_dir: str | None = None,
) -> PipelineAllToolsConfig:
    """Load tool parameters from the run-scoped pipeline config on disk."""
    run_dir = _resolve_outputs_dir(partition_key=partition_key, outputs_dir=outputs_dir)
    config_path = _run_config_path(str(run_dir))
    if not config_path.is_file():
        raise FileNotFoundError(
            f"Pipeline config not found at {config_path}. "
            "Materialize the `pipeline_config` asset (run `generate_configs` job) first."
        )
    data = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    if isinstance(data, dict):
        _normalize_structure_filters_yaml(data)
    return PipelineAllToolsConfig.model_validate(data)


def _branch_status(has_candidates: bool) -> str:
    return "has_candidates" if has_candidates else "no_candidates"


def _empty_branch_output(
    context: AssetExecutionContext,
    *,
    reason: str,
    branch_status: str = "no_candidates",
    value: dict | None = None,
    **extra_metadata,
) -> Output:
    """Materialize a no-op / empty branch so asset backfills clear requested partitions.

    Assets that previously used ``output_required=False`` and bare ``return`` left
    Dagster backfills stuck in REQUESTED forever (skipped steps never emit
    ASSET_MATERIALIZATION). Always yield this instead of returning without output.
    """
    payload = {
        "partition_key": context.partition_key,
        "has_candidates": False,
        **(value or {}),
    }
    return Output(
        payload,
        metadata={
            "partition_key": context.partition_key,
            "has_candidates": False,
            "branch_status": branch_status,
            "skip_reason": reason,
            **extra_metadata,
        },
    )


def _empty_branch_result(
    context: AssetExecutionContext,
    *,
    reason: str,
    branch_status: str = "no_candidates",
    **extra_metadata,
) -> MaterializeResult:
    """Same as ``_empty_branch_output`` for assets that return ``MaterializeResult``."""
    return MaterializeResult(
        metadata={
            "partition_key": context.partition_key,
            "has_candidates": False,
            "branch_status": branch_status,
            "skip_reason": reason,
            **extra_metadata,
        }
    )


def _renumber_predicted_structures(
    context: AssetExecutionContext,
    pc: PipelineAllToolsConfig,
    *,
    proot: Path,
    target_pdb: str,
    target_fasta: str,
    input_dir: Path,
    output_dir: Path,
    log_label: str,
) -> tuple[int, Optional[Path]]:
    """Renumber target chains in predicted structures to match the reference PDB."""
    if output_dir.exists():
        shutil.rmtree(output_dir)

    context.log.info(
        f"[{log_label}] renumbering target chains  "
        f"target_pdb={target_pdb}  target_fasta={target_fasta}  "
        f"input={input_dir}  output={output_dir}"
    )

    renumber_cmd: List[str] = [
        "docker", "run", "--rm", *_docker_user_args(),
        "-v", f"{_PROTEINDESIGN}:{_PROTEINDESIGN}",
        "-v", f"{proot}:{proot}",
        "-v", f"{Path(target_pdb).parent}:{Path(target_pdb).parent}",
        "-v", f"{Path(target_fasta).parent}:{Path(target_fasta).parent}",
        pc.structure_filters.structure_docker_image,
        "python", "/storage/proteindesign/utils/renumber_boltz2_structures.py",
        target_pdb,
        target_fasta,
        str(input_dir),
        str(output_dir),
    ]
    _run(context, renumber_cmd)

    renumbered_structure_count = len(
        list(output_dir.rglob("*.cif")) + list(output_dir.rglob("*.pdb"))
    )
    candidate_mapping = output_dir / "target_renumber_mapping.json"
    mapping_path = candidate_mapping if candidate_mapping.is_file() else None
    return renumbered_structure_count, mapping_path


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


def _docker_user_args() -> List[str]:
    """Run tool containers as the Dagster host user to avoid root-owned bind mounts.

    Images such as ``boltz2`` only have a fixed passwd entry (e.g. uid 1000). Running
    with ``--user 1005:...`` alone makes ``pwd.getpwuid`` raise KeyError. Inject a
    synthetic passwd/group and a writable HOME so any host UID works.
    """
    import grp
    import pwd

    uid, gid = os.getuid(), os.getgid()
    try:
        name = pwd.getpwuid(uid).pw_name
    except KeyError:
        name = f"u{uid}"
    try:
        gname = grp.getgrgid(gid).gr_name
    except KeyError:
        gname = f"g{gid}"

    identity_dir = Path(f"/tmp/dagster_docker_id_{uid}_{gid}")
    identity_dir.mkdir(parents=True, exist_ok=True)
    passwd_path = identity_dir / "passwd"
    group_path = identity_dir / "group"
    home = "/tmp"
    passwd_path.write_text(
        f"root:x:0:0:root:/root:/bin/sh\n{name}:x:{uid}:{gid}::{home}:/bin/sh\n",
        encoding="utf-8",
    )
    group_path.write_text(
        f"root:x:0:\n{gname}:x:{gid}:\n",
        encoding="utf-8",
    )
    return [
        "--user",
        f"{uid}:{gid}",
        "-v",
        f"{passwd_path}:/etc/passwd:ro",
        "-v",
        f"{group_path}:/etc/group:ro",
        "-e",
        f"HOME={home}",
        "-e",
        f"USER={name}",
    ]


def _colabfold_alphafold_patch_prefix() -> str:
    """Shell prefix: copy AlphaFold to writable /work and apply the numpy patch."""
    site = "/usr/local/lib/python3.12/site-packages/alphafold"
    return (
        "patch_root=/work/.alphafold_patch && "
        f'if [ ! -d "${{patch_root}}/alphafold" ]; then '
        f'mkdir -p "${{patch_root}}" && cp -a {site} "${{patch_root}}/"; fi && '
        "sed -i 's/np\\.sum(x for x in feats)/sum(x for x in feats)/g' "
        "${patch_root}/alphafold/data/msa_pairing.py && "
        "export PYTHONPATH=${patch_root}:${PYTHONPATH:-} && "
    )


def _docker_gpu_args(gpus: str) -> List[str]:
    """Build Docker GPU args, treating comma-separated values as explicit device IDs."""
    value = str(gpus).strip()
    if not value:
        return []
    if value == "all" or value.startswith(("device=", "count=", '"device=', '"count=')):
        return ["--gpus", value]
    return ["--gpus", f'"device={value}"']


def _gpu_device_count(gpus: str) -> int:
    """Number of GPUs implied by the pipeline ``gpus`` setting (for Boltz-2 ``--devices``)."""
    value = str(gpus).strip().strip('"')
    if not value or value == "all":
        return 1
    if value.startswith("device="):
        value = value[len("device=") :].strip('"')
    elif value.startswith("count="):
        try:
            return max(1, int(value[len("count=") :].strip('"')))
        except ValueError:
            return 1
    parts = [part.strip() for part in value.split(",") if part.strip()]
    return max(1, len(parts))


def _select_gpu_ids(gpus: str, count: int) -> str:
    """First ``count`` GPU IDs from ``gpus`` for tools that use fewer devices than listed."""
    value = str(gpus).strip().strip('"')
    if not value or value == "all" or value.startswith("count="):
        return value or "all"
    if value.startswith("device="):
        value = value[len("device=") :].strip('"')
    parts = [part.strip() for part in value.split(",") if part.strip()]
    if not parts:
        return value
    return ",".join(parts[: max(1, count)])


def _manifest_path(outputs_dir: str) -> Path:
    return Path(outputs_dir) / "design_configs_manifest.json"


def _read_manifest(outputs_dir: str) -> Dict[str, dict]:
    p = _manifest_path(outputs_dir)
    if not p.is_file():
        raise FileNotFoundError(
            f"Manifest not found at {p}. Materialize `pipeline_config` first (run `generate_configs`)."
        )
    return json.loads(p.read_text())


def _partition_subdir(partition_key: str, run_id: str) -> str:
    """Filesystem directory name under ``outputs_dir`` (drops ``run_id__`` Dagster prefix)."""
    prefix = f"{_sanitize_run_id(run_id)}__"
    if run_id and partition_key.startswith(prefix):
        return partition_key[len(prefix) :]
    return partition_key


def _partition_root(outputs_dir: str, partition_key: str, run_id: str = "") -> Path:
    """Isolated output directory for one design config partition."""
    return Path(outputs_dir) / _partition_subdir(partition_key, run_id)


def _design_partition_suffix(
    tool: str,
    design_name: str,
    target_pdb: str,
    hotspot_or_spec_stem: str,
    yaml_name: str,
) -> str:
    """Design-specific partition suffix (without run_id), including design tool."""
    pdb_stem = Path(target_pdb).stem
    hdir_name = Path(hotspot_or_spec_stem).name
    yaml_stem = Path(yaml_name).stem

    def sanitize(s: str) -> str:
        return "".join(c if (c.isalnum() or c == "_") else "_" for c in s)

    return (
        f"{sanitize(tool)}__{sanitize(design_name)}__{sanitize(pdb_stem)}__"
        f"{sanitize(hdir_name)}__{sanitize(yaml_stem)}"
    )


def _make_partition_key(
    run_id: str,
    tool: str,
    design_name: str,
    target_pdb: str,
    hotspot_or_spec_stem: str,
    yaml_name: str,
) -> str:
    """Build a Dagster partition key scoped to one pipeline run."""
    suffix = _design_partition_suffix(
        tool, design_name, target_pdb, hotspot_or_spec_stem, yaml_name
    )
    return f"{_sanitize_run_id(run_id)}__{suffix}"


def _sync_design_partitions(
    context: AssetExecutionContext,
    manifest: Dict[str, dict],
) -> tuple[int, int]:
    """Replace the dynamic partition set with keys from the current manifest."""
    new_keys = set(manifest.keys())
    existing_keys = set(context.instance.get_dynamic_partitions("design_configs"))
    stale_keys = existing_keys - new_keys
    for key in sorted(stale_keys):
        context.instance.delete_dynamic_partition("design_configs", key)
    if new_keys:
        context.instance.add_dynamic_partitions("design_configs", sorted(new_keys))
    return len(stale_keys), len(new_keys)


def _sanitize_partition_label(name: str) -> str:
    return "".join(c if (c.isalnum() or c == "_") else "_" for c in name.strip())


def _sequence_partition_prefix(run_id: str) -> str:
    return f"{_sanitize_run_id(run_id)}__seq__"


def _make_sequence_partition_key(run_id: str, fasta_stem: str) -> str:
    return f"{_sequence_partition_prefix(run_id)}{_sanitize_partition_label(fasta_stem)}"


def _sync_sequence_partitions_additive(
    context: AssetExecutionContext,
    run_id: str,
    sequence_keys: set[str],
) -> tuple[int, int]:
    """Add sequence-import partition keys; drop stale ``{run_id}__seq__*`` keys only."""
    prefix = _sequence_partition_prefix(run_id)
    existing_keys = set(context.instance.get_dynamic_partitions("design_configs"))
    stale_keys = {
        key for key in existing_keys if key.startswith(prefix) and key not in sequence_keys
    }
    for key in sorted(stale_keys):
        context.instance.delete_dynamic_partition("design_configs", key)
    to_add = sequence_keys - existing_keys
    if to_add:
        context.instance.add_dynamic_partitions("design_configs", sorted(to_add))
    return len(stale_keys), len(to_add)


def _load_or_create_run_config_data(
    outputs_dir: str,
    run_id: str,
    outputs_root: str,
    target_fasta: str,
    *,
    hotspots: list[str] | None = None,
    antihotspots: list[str] | None = None,
) -> dict[str, Any]:
    run_cfg_path = _run_config_path(outputs_dir)
    if run_cfg_path.is_file():
        data = yaml.safe_load(run_cfg_path.read_text(encoding="utf-8")) or {}
    else:
        data = load_pipeline_config_dict() or {}
    data["run_id"] = run_id
    data["outputs_root"] = outputs_root
    boltz2 = data.setdefault("boltz2", {})
    if isinstance(boltz2, dict):
        boltz2["target_fasta"] = target_fasta
    final_scores = data.setdefault("final_scores", {})
    if isinstance(final_scores, dict):
        if hotspots is not None:
            final_scores["hotspots"] = hotspots
        if antihotspots is not None:
            final_scores["antihotspots"] = antihotspots
    return data


def _merge_sequence_manifest(
    outputs_dir: str,
    run_id: str,
    sequence_entries: Dict[str, dict],
) -> Dict[str, dict]:
    manifest_path = _manifest_path(outputs_dir)
    manifest: Dict[str, dict] = {}
    if manifest_path.is_file():
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))

    prefix = _sequence_partition_prefix(run_id)
    for key in list(manifest):
        if key.startswith(prefix) and key not in sequence_entries:
            del manifest[key]

    manifest.update(sequence_entries)
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    return manifest


# ---------------------------------------------------------------------------
# configs group  (non-partitioned seed asset)
# ---------------------------------------------------------------------------


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


def _generate_rfdiffusion_yamls(
    context: AssetExecutionContext,
    designs_config: Dict[str, DesignSpec],
    run_id: str,
    outputs_dir: str,
) -> tuple[Dict[str, dict], int, List[str]]:
    """Generate RFdiffusion YAMLs and build the partition manifest entries."""
    manifest: Dict[str, dict] = {}
    preview_sections: List[str] = []
    total_files = 0
    rfdiffusion_configs_root = Path(outputs_dir) / "design_configs" / DESIGN_TOOL_RFDIFFUSION

    for design_name, design in designs_config.items():
        hotspot_txts_dir = Path(design.hotspots_txts_dir)
        out_dir = rfdiffusion_configs_root / design_name
        scaffolds = design.scaffolds or None
        scaffolds_root_dir = (
            Path(design.scaffolds_root_dir) if design.scaffolds_root_dir else None
        )
        insertions_for_scaffolds = {
            scaffold: settings.model_dump()
            for scaffold, settings in design.insertions_for_scaffolds.items()
        }
        scaffold_label = (
            f" × {len(scaffolds)} scaffold(s)" if scaffolds else ""
        )

        if design.template_rfdiffusion_yaml:
            tmpl_path = Path(design.template_rfdiffusion_yaml)
            context.log.info(
                f"Generating YAMLs for {design_name}: "
                f"{tmpl_path.name} × {hotspot_txts_dir.name}{scaffold_label} → {out_dir}"
            )
            n = generate_yamls(
                tmpl_path,
                hotspot_txts_dir,
                out_dir,
                Path(design.target_pdb),
                scaffolds_root_dir=scaffolds_root_dir,
                scaffolds=scaffolds,
                insertions_for_scaffolds=insertions_for_scaffolds or None,
            )
        else:
            tmpl_label = design.naming_prefix
            context.log.info(
                f"Generating YAMLs for {design_name}: "
                f"inline template ({tmpl_label}) × {hotspot_txts_dir.name}{scaffold_label} → {out_dir}"
            )
            n = generate_yamls_from_template_text(
                design.template_rfdiffusion_yaml_inline or "",
                design.naming_prefix,
                hotspot_txts_dir,
                out_dir,
                Path(design.target_pdb),
                scaffolds_root_dir=scaffolds_root_dir,
                scaffolds=scaffolds,
                insertions_for_scaffolds=insertions_for_scaffolds or None,
            )
        total_files += n
        preview_sections.append(_yaml_preview(out_dir))

        for _, scaffold, yaml_name in iter_generated_yaml_specs(
            hotspot_txts_dir,
            design.naming_prefix,
            scaffolds,
        ):
            partition_key = _make_partition_key(
                run_id,
                DESIGN_TOOL_RFDIFFUSION,
                design_name,
                design.target_pdb,
                design.hotspots_txts_dir,
                yaml_name,
            )
            subdir = _design_partition_suffix(
                DESIGN_TOOL_RFDIFFUSION,
                design_name,
                design.target_pdb,
                design.hotspots_txts_dir,
                yaml_name,
            )
            config_name = Path(yaml_name).stem
            manifest[partition_key] = {
                "run_id": run_id,
                "tool": DESIGN_TOOL_RFDIFFUSION,
                "subdir": subdir,
                "design_name": design_name,
                "config_dir": str(out_dir),
                "config_name": config_name,
                "filter_config_yaml": str(rfdiffusion_config_path(out_dir, config_name)),
                "target_pdb": design.target_pdb,
                "binder_chain": "A",
                "target_chains": ["B"],
                "scaffold": scaffold,
            }

    return manifest, total_files, preview_sections


def _write_boltzgen_filter_sidecar(
    out_path: Path,
    *,
    hotspot_res: list[str],
    antihotspots: dict[str, Any],
) -> None:
    """Write an RFdiffusion-shaped YAML so existing contact-filter loaders work."""
    payload = {
        "ppi": {"hotspot_res": list(hotspot_res)},
        "antihotspots": {
            "enabled": bool(antihotspots.get("enabled", False)),
            "contact_distance": float(antihotspots.get("contact_distance", 10.0)),
            "max_antihotspot_contact_fraction": float(
                antihotspots.get("max_antihotspot_contact_fraction", 0.05)
            ),
            "res": list(antihotspots.get("res") or []),
        },
    }
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(
        yaml.dump(payload, default_flow_style=False, sort_keys=False, allow_unicode=True),
        encoding="utf-8",
    )


def _generate_boltzgen_configs(
    context: AssetExecutionContext,
    designs_config: Dict[str, Any],
    run_id: str,
    outputs_dir: str,
    *,
    num_designs: int,
) -> tuple[Dict[str, dict], int, List[str]]:
    """Generate BoltzGen specs + filter sidecars; build partition manifest entries."""
    manifest: Dict[str, dict] = {}
    preview_sections: List[str] = []
    total_files = 0
    configs_root = Path(outputs_dir) / "design_configs" / DESIGN_TOOL_BOLTZGEN

    for design_name, design in designs_config.items():
        # designs_config values are BoltzGenDesignSpec after validation
        spec_src = Path(design.design_spec_yaml)
        out_dir = configs_root / design_name
        naming_prefix = str(design.naming_prefix or design_name)
        generated = generate_boltzgen_yamls(
            spec_src,
            Path(design.hotspots_txts_dir),
            out_dir,
            Path(design.target_pdb),
            naming_prefix,
            binder_sequence_file=design.binder_sequence_file,
            binder_chain=design.binder_chain,
        )
        anti = design.antihotspots.model_dump()
        for _, dest_spec, hotspot_res in generated:
            filter_yaml = (
                out_dir / f"{dest_spec.stem}_structure_filter.yaml"
            )
            _write_boltzgen_filter_sidecar(
                filter_yaml,
                hotspot_res=hotspot_res,
                antihotspots=anti,
            )
            total_files += 2

            yaml_name = dest_spec.name
            partition_key = _make_partition_key(
                run_id,
                DESIGN_TOOL_BOLTZGEN,
                design_name,
                design.target_pdb,
                design.hotspots_txts_dir,
                yaml_name,
            )
            subdir = _design_partition_suffix(
                DESIGN_TOOL_BOLTZGEN,
                design_name,
                design.target_pdb,
                design.hotspots_txts_dir,
                yaml_name,
            )
            manifest[partition_key] = {
                "run_id": run_id,
                "tool": DESIGN_TOOL_BOLTZGEN,
                "subdir": subdir,
                "design_name": design_name,
                "config_dir": str(out_dir),
                "config_name": dest_spec.stem,
                "design_spec_yaml": str(dest_spec),
                "filter_config_yaml": str(filter_yaml),
                "target_pdb": design.target_pdb,
                "protocol": design.protocol,
                "num_designs": int(num_designs),
                "binder_chain": design.binder_chain,
                "target_chains": list(design.target_chains),
                "binder_sequence_file": design.binder_sequence_file,
            }
        preview_sections.append(_yaml_preview(out_dir))

    return manifest, total_files, preview_sections


def _generate_promera_yamls(
    context: AssetExecutionContext,
    designs_config: Dict[str, Any],
    run_id: str,
    outputs_dir: str,
) -> tuple[Dict[str, dict], int, List[str]]:
    """Generate Promera task_config YAMLs + filter sidecars; build partition manifest entries.

    Regenerates ``input`` (target/*.json) and ``epitope_residues`` per hotspot file
    (see ``generate_promera_yamls.py``) instead of hand-editing one task_config per
    campaign, mirroring ``_generate_rfdiffusion_yamls`` / ``_generate_boltzgen_configs``.
    The filter sidecar reuses ``_write_boltzgen_filter_sidecar`` so
    ``design_structure_filter`` needs no promera-specific code path.
    """
    manifest: Dict[str, dict] = {}
    preview_sections: List[str] = []
    total_files = 0
    configs_root = Path(outputs_dir) / "design_configs" / DESIGN_TOOL_PROMERA

    for design_name, design in designs_config.items():
        out_dir = configs_root / design_name
        context.log.info(
            f"Generating promera task_configs for {design_name}: "
            f"{Path(design.task_config_yaml).name} × "
            f"{Path(design.hotspots_txts_dir).name} → {out_dir}"
        )
        generated = generate_promera_task_configs(
            Path(design.task_config_yaml),
            Path(design.hotspots_txts_dir),
            out_dir,
            Path(design.target_fasta),
            design_name,
            epitope_chain=design.epitope_chain,
            hotspot_files=design.hotspot_files or None,
        )
        anti = design.antihotspots.model_dump()
        for _hotspot_file, task_config_path, _target_dir, epitope_residues in generated:
            # ppi.hotspot_res for the shared filter uses chain-prefixed labels
            # (e.g. "B21"), like RFdiffusion/BoltzGen — not promera's own
            # plain-int epitope_residues.
            hotspot_res = [f"{design.epitope_chain}{n}" for n in epitope_residues]
            filter_yaml = out_dir / f"{task_config_path.stem}_structure_filter.yaml"
            _write_boltzgen_filter_sidecar(
                filter_yaml,
                hotspot_res=hotspot_res,
                antihotspots=anti,
            )
            total_files += 2

            yaml_name = task_config_path.name
            partition_key = _make_partition_key(
                run_id,
                DESIGN_TOOL_PROMERA,
                design_name,
                design.target_pdb,
                design.hotspots_txts_dir,
                yaml_name,
            )
            subdir = _design_partition_suffix(
                DESIGN_TOOL_PROMERA,
                design_name,
                design.target_pdb,
                design.hotspots_txts_dir,
                yaml_name,
            )
            manifest[partition_key] = {
                "run_id": run_id,
                "tool": DESIGN_TOOL_PROMERA,
                "subdir": subdir,
                "design_name": design_name,
                "config_dir": str(out_dir),
                "config_name": task_config_path.stem,
                "task_config_yaml": str(task_config_path),
                "filter_config_yaml": str(filter_yaml),
                "target_pdb": design.target_pdb,
                "epitope_chain": design.epitope_chain,
            }
        preview_sections.append(_yaml_preview(out_dir))

    return manifest, total_files, preview_sections


@asset(
    group_name="configs",
    description=(
        "Saves tool parameters to ``{outputs_root}/{run_id}/pipeline_config.yaml``, "
        "generates configs for enabled design tools under "
        "``design_configs/{rfdiffusion|boltzgen}/``, writes "
        "``design_configs_manifest.json``, and registers design partitions. "
        "Each materialization creates a new run directory under ``outputs_root`` "
        "(timestamped when ``run_id`` is empty). Materialize before ``design_pipeline``."
    ),
)
def pipeline_config(
    context: AssetExecutionContext,
    config: PipelineAllToolsConfig,
) -> MaterializeResult:
    run_id, outputs_dir = _resolve_run_outputs(config.outputs_root, config.run_id)
    context.log.info(f"Run outputs_dir → {outputs_dir}")

    manifest: Dict[str, dict] = {}
    preview_sections: List[str] = []
    total_files = 0

    if config.rfdiffusion.enabled:
        rfd_manifest, n, previews = _generate_rfdiffusion_yamls(
            context,
            config.rfdiffusion.designs_config,
            run_id,
            outputs_dir,
        )
        manifest.update(rfd_manifest)
        total_files += n
        preview_sections.extend(previews)
    else:
        context.log.info("rfdiffusion.enabled=false — skipping RFdiffusion config generation")

    if config.boltzgen.enabled:
        bg_manifest, n, previews = _generate_boltzgen_configs(
            context,
            config.boltzgen.designs_config,
            run_id,
            outputs_dir,
            num_designs=int(config.boltzgen.num_designs),
        )
        manifest.update(bg_manifest)
        total_files += n
        preview_sections.extend(previews)
    else:
        context.log.info("boltzgen.enabled=false — skipping BoltzGen config generation")

    if config.promera.enabled:
        pm_manifest, n, previews = _generate_promera_yamls(
            context,
            config.promera.designs_config,
            run_id,
            outputs_dir,
        )
        manifest.update(pm_manifest)
        total_files += n
        preview_sections.extend(previews)
    else:
        context.log.info("promera.enabled=false — skipping Promera config generation")

    if not manifest:
        raise Failure(
            description=(
                "No design partitions generated. Enable at least one of "
                "rfdiffusion.enabled, boltzgen.enabled, or promera.enabled."
            )
        )

    mp = _manifest_path(outputs_dir)
    mp.parent.mkdir(parents=True, exist_ok=True)
    mp.write_text(json.dumps(manifest, indent=2))
    context.log.info(f"Manifest written → {mp}  ({len(manifest)} entries)")

    removed_count, registered_count = _sync_design_partitions(context, manifest)
    context.log.info(
        f"Synced design_configs partitions: {registered_count} active, "
        f"{removed_count} stale removed"
    )

    data = config.model_dump()
    data["run_id"] = run_id
    Path(outputs_dir).mkdir(parents=True, exist_ok=True)
    run_config_path = _run_config_path(outputs_dir)
    run_config_path.write_text(
        yaml.dump(data, default_flow_style=False, sort_keys=False, allow_unicode=True),
        encoding="utf-8",
    )
    context.log.info(
        f"Pipeline config saved → {run_config_path}  "
        f"(run_id={run_id}, outputs_dir={outputs_dir})"
    )
    return MaterializeResult(
        metadata={
            "config_path": MetadataValue.path(str(run_config_path)),
            "gpus": config.gpus,
            "run_id": run_id,
            "outputs_root": config.outputs_root,
            "outputs_dir": outputs_dir,
            "design_configs_dir": MetadataValue.path(str(Path(outputs_dir) / "design_configs")),
            "rfdiffusion_enabled": config.rfdiffusion.enabled,
            "boltzgen_enabled": config.boltzgen.enabled,
            "promera_enabled": config.promera.enabled,
            "proteinmpnn_num_seq_per_target": config.proteinmpnn.num_seq_per_target,
            "total_yamls": total_files,
            "partition_count": len(manifest),
            "partitions_removed": removed_count,
            "partitions_registered": registered_count,
            "manifest_path": MetadataValue.path(str(mp)),
            "partition_keys": MetadataValue.text("\n".join(sorted(manifest.keys()))),
            "yaml_preview": MetadataValue.md("\n\n---\n\n".join(preview_sections)),
            "preview": MetadataValue.md(
                f"```yaml\n{run_config_path.read_text(encoding='utf-8')}\n```"
            ),
        }
    )


# ---------------------------------------------------------------------------
# sequences group  (external FASTA import branch)
# ---------------------------------------------------------------------------

@asset(
    group_name="sequences",
    description=(
        "Scan ``input_path`` for FASTA files and add one partition per file under "
        "``{outputs_root}/{add_to_run_id}/``. Partition keys are "
        "``{run_id}__seq__{fasta_stem}``. Merges into the run manifest, writes "
        "``final_scores.hotspots`` / ``final_scores.antihotspots`` into the run "
        "``pipeline_config.yaml``, and registers partitions without removing "
        "RFdiffusion or other-run keys. Configure via ``register_sequences_config.yaml``."
    ),
)
def register_sequence_partitions(
    context: AssetExecutionContext,
    config: RegisterSequencePartitionsConfig,
) -> MaterializeResult:
    run_id = _sanitize_run_id(config.add_to_run_id)
    if not run_id:
        raise Failure(description="add_to_run_id is required.")

    input_dir = Path(config.input_path)
    if not input_dir.is_dir():
        raise Failure(description=f"input_path is not a directory: {input_dir}")

    target_fasta = Path(config.target_fasta)
    if not target_fasta.is_file():
        raise Failure(description=f"target_fasta not found: {target_fasta}")

    outputs_root = config.outputs_root or DEFAULT_OUTPUTS_ROOT
    outputs_dir = str(Path(outputs_root) / run_id)
    run_dir = Path(outputs_dir)
    run_dir.mkdir(parents=True, exist_ok=True)

    fasta_files = sorted(
        p
        for p in input_dir.iterdir()
        if p.is_file() and p.suffix.lower() in {".fa", ".fasta", ".faa"}
    )
    if not fasta_files:
        raise Failure(description=f"No FASTA files found in {input_dir}")

    sequence_entries: Dict[str, dict] = {}
    for fasta_path in fasta_files:
        stem = _sanitize_partition_label(fasta_path.stem)
        partition_key = _make_sequence_partition_key(run_id, stem)
        subdir = f"seq__{stem}"
        proot = Path(outputs_dir) / subdir
        proot.mkdir(parents=True, exist_ok=True)

        sequence_entries[partition_key] = {
            "run_id": run_id,
            "subdir": subdir,
            "source": "sequence_import",
            "input_fasta": str(fasta_path.resolve()),
            "fasta_filename": fasta_path.name,
            "target_pdb": "",
            "config_dir": "",
            "config_name": "",
        }
        context.log.info(
            f"[register_sequence_partitions] partition={partition_key}  "
            f"input={fasta_path}  output_dir={proot}"
        )

    manifest = _merge_sequence_manifest(outputs_dir, run_id, sequence_entries)
    removed_count, added_count = _sync_sequence_partitions_additive(
        context, run_id, set(sequence_entries.keys())
    )

    run_config_data = _load_or_create_run_config_data(
        outputs_dir,
        run_id,
        outputs_root,
        str(target_fasta.resolve()),
        hotspots=list(config.hotspots),
        antihotspots=list(config.antihotspots),
    )
    PipelineAllToolsConfig.model_validate(run_config_data)
    run_config_path = _run_config_path(outputs_dir)
    run_config_path.write_text(
        yaml.dump(run_config_data, default_flow_style=False, sort_keys=False, allow_unicode=True),
        encoding="utf-8",
    )

    context.log.info(
        f"Sequence partitions registered: {len(sequence_entries)} new/updated, "
        f"{added_count} added to Launchpad, {removed_count} stale seq keys removed"
    )
    return MaterializeResult(
        metadata={
            "run_id": run_id,
            "outputs_dir": MetadataValue.path(outputs_dir),
            "manifest_path": MetadataValue.path(str(_manifest_path(outputs_dir))),
            "run_config_path": MetadataValue.path(str(run_config_path)),
            "target_fasta": MetadataValue.path(str(target_fasta)),
            "hotspots": MetadataValue.text(", ".join(config.hotspots) or "(none)"),
            "antihotspots": MetadataValue.text(", ".join(config.antihotspots) or "(none)"),
            "input_path": MetadataValue.path(str(input_dir)),
            "sequence_partition_count": len(sequence_entries),
            "partitions_added": added_count,
            "partitions_removed": removed_count,
            "partition_keys": MetadataValue.text("\n".join(sorted(sequence_entries.keys()))),
        }
    )


@asset(
    group_name="sequences",
    partitions_def=design_configs,
    deps=[register_sequence_partitions],
    output_required=False,
    description=(
        "Import external binder FASTAs for one ``sequence_import`` partition. Requires "
        "``register_sequence_partitions`` to have run for the target run. Writes "
        "MPNN-style files to ``proteinmpnn/seqs/`` using sanitized protein names from "
        "FASTA headers (one output file per sequence). For non-sequence_import partitions, "
        "materializes as skipped (not a Dagster skip) so asset backfills still complete."
    ),
)
def import_binder_sequences(
    context: AssetExecutionContext,
):
    partition_key = context.partition_key
    pc = _read_pipeline_config(partition_key=partition_key)
    if not _is_sequence_import_partition(context, pc):
        context.log.info(
            f"[import_binder_sequences] partition={partition_key}  "
            "not a sequence_import partition; materializing as skipped"
        )
        yield _empty_branch_output(
            context,
            reason="not a sequence_import partition",
            branch_status="skipped",
        )
        return

    manifest = _read_manifest(_run_outputs_dir(pc))
    if partition_key not in manifest:
        raise Failure(
            description=(
                f"Partition {partition_key!r} not found in manifest. "
                "Run register_sequence_partitions first."
            )
        )

    entry = manifest[partition_key]
    if entry.get("source") != "sequence_import":
        raise Failure(
            description=(
                f"Partition {partition_key!r} is not a sequence_import entry "
                f"(source={entry.get('source')!r})."
            )
        )

    input_fasta = Path(entry["input_fasta"])
    if not input_fasta.is_file():
        raise Failure(description=f"Input FASTA from manifest not found: {input_fasta}")

    proot = _partition_root(_run_outputs_dir(pc), partition_key, pc.run_id)
    seq_dir = proot / "proteinmpnn" / "seqs"
    context.log.info(
        f"[import_binder_sequences] partition={partition_key}  "
        f"input={input_fasta}  output={seq_dir}"
    )
    written = import_external_fasta_to_mpnn_seqs(input_fasta, seq_dir)

    fa_files = sorted(seq_dir.glob("*.fa"))
    yield Output(
        {
            "partition_key": partition_key,
            "seq_dir": str(seq_dir),
            "sequence_count": written,
        },
        metadata={
            "partition_key": partition_key,
            "input_fasta": MetadataValue.path(str(input_fasta)),
            "seq_dir": MetadataValue.path(str(seq_dir)),
            "sequence_count": written,
            "fasta_file_count": len(fa_files),
            "sample_files": MetadataValue.text("\n".join(f.name for f in fa_files[:20])),
        },
    )


# ---------------------------------------------------------------------------
# Design generation (partitioned)
# ---------------------------------------------------------------------------

@asset(
    group_name="rfdiffusion",
    partitions_def=design_configs,
    deps=[pipeline_config],
    description=(
        "Runs RFdiffusion for partitions with ``tool=rfdiffusion``. "
        "No-ops successfully for BoltzGen / sequence-import partitions. "
        "PDBs are written to ``designs/rfdiffusion/design_*.pdb``."
    ),
)
def rfdiffusion_generation(
    context: AssetExecutionContext,
) -> MaterializeResult:
    pc = _read_pipeline_config(partition_key=context.partition_key)
    rfd = pc.rfdiffusion
    partition_key = context.partition_key
    manifest = _read_manifest(_run_outputs_dir(pc))
    if partition_key not in manifest:
        raise KeyError(
            f"Partition '{partition_key}' not found in manifest. "
            "Re-run `generate_configs` to regenerate it."
        )
    entry = manifest[partition_key]
    tool = entry.get("tool", DESIGN_TOOL_RFDIFFUSION)
    if tool != DESIGN_TOOL_RFDIFFUSION:
        context.log.info(
            f"[rfdiffusion_generation] partition={partition_key} tool={tool} — skipping"
        )
        return MaterializeResult(
            metadata={
                "partition_key": partition_key,
                "tool": tool,
                "skipped": True,
                "reason": f"tool is {tool}, not {DESIGN_TOOL_RFDIFFUSION}",
            }
        )

    config_dir: str = entry["config_dir"]
    config_name: str = entry["config_name"]
    target_pdb: str = entry["target_pdb"]
    context.log.info(
        f"[rfdiffusion_generation] partition={partition_key}  "
        f"config={config_name}  config_dir={config_dir}  target_pdb={target_pdb}"
    )
    if not Path(target_pdb).is_file():
        raise FileNotFoundError(f"Target PDB from manifest does not exist on host: {target_pdb}")

    proot = _partition_root(_run_outputs_dir(pc), partition_key, pc.run_id)
    pdbs_dir = _designs_dir(proot, DESIGN_TOOL_RFDIFFUSION)
    pdbs_dir.mkdir(parents=True, exist_ok=True)

    # Override inference.output_prefix so PDBs land in designs/rfdiffusion/.
    output_prefix = str(pdbs_dir / "design")
    target_parent = Path(target_pdb).parent

    cmd: List[str] = [
        "docker", "run", "--rm", *_docker_user_args(),
        "--runtime=nvidia", *_docker_gpu_args(pc.gpus),
        "-v", f"{Path(_run_outputs_dir(pc)).parent}:{Path(_run_outputs_dir(pc)).parent}",
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

    pdb_count = sum(1 for _ in pdbs_dir.glob("design_*.pdb"))
    context.log.info(f"RFdiffusion done → {pdbs_dir}  ({pdb_count} PDB(s))")
    return MaterializeResult(
        metadata={
            "partition_key": partition_key,
            "tool": DESIGN_TOOL_RFDIFFUSION,
            "skipped": False,
            "config_dir": MetadataValue.path(config_dir),
            "config_name": config_name,
            "designs_dir": MetadataValue.path(str(pdbs_dir)),
            "pdb_count": pdb_count,
        }
    )


def _load_hf_token() -> str:
    """Return HF_TOKEN from the environment, or from the repo ``.env`` if present."""
    token = str(os.environ.get("HF_TOKEN") or os.environ.get("HUGGING_FACE_HUB_TOKEN") or "").strip()
    if token:
        return token
    env_path = _PROTEINDESIGN / ".env"
    if not env_path.is_file():
        return ""
    for line in env_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        if key.strip() in {"HF_TOKEN", "HUGGING_FACE_HUB_TOKEN"}:
            return value.strip().strip('"').strip("'")
    return ""


@asset(
    group_name="boltzgen",
    partitions_def=design_configs,
    deps=[pipeline_config],
    description=(
        "Runs BoltzGen ``--steps design`` for partitions with ``tool=boltzgen``. "
        "Writes CIFs under ``designs/boltzgen/raw/``, then normalizes to "
        "``designs/boltzgen/design_*.pdb`` (binder=A). No-ops for other tools."
    ),
)
def boltzgen_generation(
    context: AssetExecutionContext,
) -> MaterializeResult:
    pc = _read_pipeline_config(partition_key=context.partition_key)
    bg = pc.boltzgen
    partition_key = context.partition_key
    manifest = _read_manifest(_run_outputs_dir(pc))
    if partition_key not in manifest:
        raise KeyError(
            f"Partition '{partition_key}' not found in manifest. "
            "Re-run `generate_configs` to regenerate it."
        )
    entry = manifest[partition_key]
    tool = entry.get("tool")
    if tool != DESIGN_TOOL_BOLTZGEN:
        context.log.info(
            f"[boltzgen_generation] partition={partition_key} tool={tool} — skipping"
        )
        return MaterializeResult(
            metadata={
                "partition_key": partition_key,
                "tool": tool or "",
                "skipped": True,
                "reason": f"tool is {tool}, not {DESIGN_TOOL_BOLTZGEN}",
            }
        )

    design_spec = Path(entry["design_spec_yaml"])
    target_pdb = entry["target_pdb"]
    protocol = str(entry.get("protocol") or "protein-anything")
    # Shared BoltzGen setting; manifest value is a snapshot from pipeline_config.
    num_designs = int(bg.num_designs)
    binder_hint = str(entry.get("binder_chain") or "A")
    target_chains = list(entry.get("target_chains") or ["B"])

    if not design_spec.is_file():
        raise FileNotFoundError(f"BoltzGen design spec missing: {design_spec}")
    if not Path(target_pdb).is_file():
        raise FileNotFoundError(f"Target PDB from manifest does not exist on host: {target_pdb}")

    proot = _partition_root(_run_outputs_dir(pc), partition_key, pc.run_id)
    designs_dir = _designs_dir(proot, DESIGN_TOOL_BOLTZGEN)
    raw_dir = designs_dir / "raw"
    run_dir = designs_dir / "boltzgen_run"
    designs_dir.mkdir(parents=True, exist_ok=True)
    if raw_dir.exists():
        shutil.rmtree(raw_dir)
    raw_dir.mkdir(parents=True, exist_ok=True)

    cache_dir = Path(DEFAULT_BOLTZGEN_CACHE_DIR)
    cache_dir.mkdir(parents=True, exist_ok=True)

    hf_token = _load_hf_token()
    if hf_token and not str(os.environ.get("HF_TOKEN") or "").strip():
        # Make token available to Docker via ``-e HF_TOKEN`` without embedding it in argv logs.
        os.environ["HF_TOKEN"] = hf_token

    ld_library_path = (
        "/usr/local/lib/python3.11/dist-packages/nvidia/cu13/lib:"
        "/usr/local/lib/python3.11/dist-packages/nvidia/cuda_nvrtc/lib"
    )

    cmd: List[str] = [
        "docker", "run", "--rm",
        f"--shm-size={DEFAULT_BOLTZGEN_SHM_SIZE}",
        "--group-add", str(DEFAULT_BOLTZGEN_GPU_GROUP_ID),
        *_docker_user_args(),
        "--runtime=nvidia", *_docker_gpu_args(pc.gpus),
        "-e", f"HF_HOME={cache_dir}",
        "-e", f"TORCHINDUCTOR_CACHE_DIR={cache_dir / 'torch'}",
        "-e", f"LD_LIBRARY_PATH={ld_library_path}",
    ]
    if str(os.environ.get("HF_TOKEN") or "").strip():
        cmd.extend(["-e", "HF_TOKEN"])
    else:
        context.log.warning(
            "[boltzgen_generation] HF_TOKEN not set; HuggingFace downloads may be rate-limited"
        )

    # Path-preserving mounts for outputs, cache, design spec, and target PDB.
    mount_roots = {
        Path(_run_outputs_dir(pc)).parent,
        cache_dir,
        design_spec.parent,
        Path(target_pdb).parent,
        _PROTEINDESIGN,
    }
    for root in sorted({str(p.resolve()) for p in mount_roots}):
        cmd.extend(["-v", f"{root}:{root}"])

    cmd.extend(
        [
            bg.docker_image,
            "boltzgen", "run", str(design_spec),
            "--output", str(run_dir),
            "--steps", "design",
            "--protocol", protocol,
            "--num_designs", str(num_designs),
            "--budget", str(max(1, num_designs)),
            "--devices", "1",
            "--cache", str(cache_dir),
            "--use_kernels", "false",
        ]
    )
    cmd.extend(bg.extra_args)
    _run(context, cmd)

    intermediate = run_dir / "intermediate_designs"
    if not intermediate.is_dir():
        raise FileNotFoundError(
            f"BoltzGen intermediate_designs missing after design step: {intermediate}"
        )
    for cif in intermediate.glob("*.cif"):
        shutil.copy2(cif, raw_dir / cif.name)
        npz = cif.with_suffix(".npz")
        if npz.is_file():
            shutil.copy2(npz, raw_dir / npz.name)

    # Normalize inside structure_tools so Biopython/numpy are available.
    normalize_cmd: List[str] = [
        "docker", "run", "--rm", *_docker_user_args(),
        "-v", f"{_PROTEINDESIGN}:{_PROTEINDESIGN}",
        "-v", f"{proot}:{proot}",
        "-v", f"{design_spec.parent}:{design_spec.parent}",
        "-v", f"{Path(target_pdb).parent}:{Path(target_pdb).parent}",
        pc.structure_filters.structure_docker_image,
        "python", "/storage/proteindesign/utils/boltzgen_designs_to_pdb.py",
        str(raw_dir),
        str(designs_dir),
        "--design-spec", str(design_spec),
        "--binder-chain-hint", binder_hint,
        "--target-chains", ",".join(target_chains),
        "--target-pdb", str(target_pdb),
    ]
    _run(context, normalize_cmd)

    pdb_count = len(list(designs_dir.glob("design_*.pdb")))
    context.log.info(f"BoltzGen design done → {designs_dir}  ({pdb_count} PDB(s))")
    return MaterializeResult(
        metadata={
            "partition_key": partition_key,
            "tool": DESIGN_TOOL_BOLTZGEN,
            "skipped": False,
            "design_spec_yaml": MetadataValue.path(str(design_spec)),
            "designs_dir": MetadataValue.path(str(designs_dir)),
            "raw_dir": MetadataValue.path(str(raw_dir)),
            "pdb_count": pdb_count,
            "protocol": protocol,
            "num_designs": num_designs,
        }
    )


@asset(
    group_name="promera",
    partitions_def=design_configs,
    deps=[pipeline_config],
    description=(
        "Runs Promera (``python -m promera --task_config ...``) for partitions with "
        "``tool=promera``, then normalizes ``sample*/backbone.pdb`` into "
        "``designs/promera/design_N.pdb`` (chain A=binder, B=target — already correct, "
        "no remap needed, unlike BoltzGen). No-ops for other tools."
    ),
)
def promera_generation(
    context: AssetExecutionContext,
) -> MaterializeResult:
    pc = _read_pipeline_config(partition_key=context.partition_key)
    pm = pc.promera
    partition_key = context.partition_key
    manifest = _read_manifest(_run_outputs_dir(pc))
    if partition_key not in manifest:
        raise KeyError(
            f"Partition '{partition_key}' not found in manifest. "
            "Re-run `generate_configs` to regenerate it."
        )
    entry = manifest[partition_key]
    tool = entry.get("tool")
    if tool != DESIGN_TOOL_PROMERA:
        context.log.info(
            f"[promera_generation] partition={partition_key} tool={tool} — skipping"
        )
        return MaterializeResult(
            metadata={
                "partition_key": partition_key,
                "tool": tool or "",
                "skipped": True,
                "reason": f"tool is {tool}, not {DESIGN_TOOL_PROMERA}",
            }
        )

    task_config = Path(entry["task_config_yaml"])
    config_dir = Path(entry["config_dir"])
    if not task_config.is_file():
        raise FileNotFoundError(f"Promera task_config missing: {task_config}")

    context.log.info(
        f"[promera_generation] partition={partition_key}  task_config={task_config}"
    )

    proot = _partition_root(_run_outputs_dir(pc), partition_key, pc.run_id)
    designs_dir = _designs_dir(proot, DESIGN_TOOL_PROMERA)
    raw_out = designs_dir / "promera_run"
    designs_dir.mkdir(parents=True, exist_ok=True)
    raw_out.mkdir(parents=True, exist_ok=True)

    weights_host = Path(pm.weights_host).resolve()
    cache_host = Path(pm.tinyprot_cache_host).resolve()
    cache_host.mkdir(parents=True, exist_ok=True)
    ligandmpnn_dir = Path(pm.ligandmpnn_dir).resolve()

    cmd: List[str] = [
        "docker", "run", "--rm", *_docker_user_args(),
        "--group-add", str(pm.shared_group_gid),
        "--runtime=nvidia", *_docker_gpu_args(pc.gpus),
        "-v", f"{weights_host}:/weights:ro",
        "-v", f"{cache_host}:/cache:rw",
        "-v", f"{ligandmpnn_dir}:/ligandmpnn:ro",
        # task_config's own `input:` (target/*.json) lives under config_dir too —
        # one mount covers both, same "host path == container path" convention
        # used for target_pdb/model dirs elsewhere in this module.
        "-v", f"{config_dir}:{config_dir}",
        "-v", f"{raw_out}:{raw_out}",
        "-e", f"PROMERA_WEIGHTS=/weights/{pm.weights_file}",
        "-e", "TINYPROT_CACHE=/cache",
        "-e", "LIGANDMPNN_DIR=/ligandmpnn",
        "-e", "ABMPNN_CHECKPOINT=/ligandmpnn/model_params/abmpnn.pt",
        pm.docker_image,
        "--task", "promera.inference.Design",
        "--task_config", str(task_config),
        f"output={raw_out}",  # override the YAML's own `output:` so it lands under proot
        f"trainer.devices={_gpu_device_count(pc.gpus)}",
    ]
    _run(context, cmd)

    # Promera writes {output}/{target_name}/sample*/backbone.pdb, where target_name
    # is the stem of the target/*.json file — which generate_promera_task_configs
    # names identically to the task_config itself (one target per generated config).
    promera_raw_target_dir = raw_out / task_config.stem
    index = normalize_promera_designs(promera_raw_target_dir, designs_dir)
    pdb_count = index["design_count"]

    context.log.info(f"Promera design done → {designs_dir}  ({pdb_count} PDB(s))")
    return MaterializeResult(
        metadata={
            "partition_key": partition_key,
            "tool": DESIGN_TOOL_PROMERA,
            "skipped": False,
            "task_config_yaml": MetadataValue.path(str(task_config)),
            "designs_dir": MetadataValue.path(str(designs_dir)),
            "raw_dir": MetadataValue.path(str(promera_raw_target_dir)),
            "pdb_count": pdb_count,
        }
    )


# ---------------------------------------------------------------------------
# structure filter group 
# ---------------------------------------------------------------------------

@asset(
    group_name="design_filter",
    partitions_def=design_configs,
    deps=[rfdiffusion_generation, boltzgen_generation, promera_generation],
    description=(
        "Shared structure filter for RFdiffusion, BoltzGen, and Promera design PDBs. "
        "Optionally computes binder–target CA contact reports "
        "(when ``antihotspots.enabled`` and ``structure_filters.enabled``), then applies "
        "hotspot / antihotspot rules (100% hotspot coverage required; antihotspot "
        "contact fraction capped per tool). Binder–alpha contacts are allowed and "
        "only reported. Passing PDBs are copied to ``pdbs_filtered/`` "
        "for ProteinMPNN."
    ),
)
def design_structure_filter(
    context: AssetExecutionContext,
) -> MaterializeResult:
    pc = _read_pipeline_config(partition_key=context.partition_key)
    partition_key = context.partition_key
    manifest = _read_manifest(_run_outputs_dir(pc))
    if partition_key not in manifest:
        raise KeyError(
            f"Partition '{partition_key}' not found in manifest. "
            "Re-run `generate_configs` to regenerate it."
        )
    entry = manifest[partition_key]
    if entry.get("source") == "sequence_import":
        context.log.info(
            f"[design_structure_filter] partition={partition_key} sequence_import — skipping"
        )
        return MaterializeResult(
            metadata={
                "partition_key": partition_key,
                "skipped": True,
                "reason": "sequence_import partition",
            }
        )

    tool = str(entry.get("tool") or DESIGN_TOOL_RFDIFFUSION)
    target_pdb: str = entry["target_pdb"]
    config_yaml = Path(
        entry.get("filter_config_yaml")
        or rfdiffusion_config_path(entry["config_dir"], entry["config_name"])
    )
    antihotspots = load_antihotspots_config(config_yaml)
    proot = _partition_root(_run_outputs_dir(pc), partition_key, pc.run_id)
    pdbs_dir = _designs_dir(proot, tool)
    contacts_dir = proot / "antihotspot_contacts"
    filter_results_dir = proot / "structure_filter"
    filtered_pdbs_dir = proot / "pdbs_filtered"

    structure_tools_enabled = pc.structure_filters.enabled
    structural_filter_enabled = (
        antihotspots["enabled"] and structure_tools_enabled
    )
    contacts_skipped = not structural_filter_enabled
    report_count = 0

    if contacts_skipped:
        if not structure_tools_enabled:
            context.log.info(
                "[design_structure_filter] structure_filters.enabled=false in "
                "pipeline_config — skipping contact analysis and structural filtering"
            )
        else:
            context.log.info(
                f"[design_structure_filter] antihotspots.enabled=false in "
                f"{config_yaml} — skipping contact analysis"
            )
    else:
        if not Path(target_pdb).is_file():
            raise FileNotFoundError(f"Target PDB from manifest does not exist on host: {target_pdb}")
        if contacts_dir.exists():
            shutil.rmtree(contacts_dir)
        contacts_dir.mkdir(parents=True, exist_ok=True)

        context.log.info(
            f"[design_structure_filter] partition={partition_key} tool={tool}  "
            f"config={config_yaml}  target_pdb={target_pdb}  "
            f"contact_distance={antihotspots['contact_distance']}  "
            f"max_antihotspot_contact_fraction="
            f"{antihotspots.get('max_antihotspot_contact_fraction')}  "
            f"antihotspot_res={antihotspots['res']}  "
            f"pdbs_dir={pdbs_dir}  contacts={contacts_dir}"
        )

        cmd: List[str] = [
            "docker", "run", "--rm", *_docker_user_args(),
            "-v", f"{_PROTEINDESIGN}:{_PROTEINDESIGN}",
            "-v", f"{proot}:{proot}",
            "-v", f"{Path(config_yaml).parent}:{Path(config_yaml).parent}",
            "-v", f"{Path(target_pdb).parent}:{Path(target_pdb).parent}",
            pc.structure_filters.structure_docker_image,
            "python", "/storage/proteindesign/utils/antihotspot_contacts.py",
            target_pdb,
            str(pdbs_dir),
            "--distance", str(antihotspots["contact_distance"]),
            "-o", str(contacts_dir),
        ]
        if antihotspots["res"]:
            cmd.extend(["--antihotspot-res", ",".join(antihotspots["res"])])
        if antihotspots["target_chains"]:
            cmd.extend(["--target-chains", ",".join(antihotspots["target_chains"])])
        _run(context, cmd)
        report_count = len(sorted(contacts_dir.glob("*_antihotspot_contacts.json")))

    context.log.info(
        f"[design_structure_filter] partition={partition_key} tool={tool}  "
        f"structural_filter_enabled={structural_filter_enabled}  "
        f"contacts_dir={contacts_dir}  output={filtered_pdbs_dir}"
    )

    summary = filter_partition_contacts(
        contacts_dir,
        config_yaml,
        pdbs_dir,
        filter_results_dir,
        filtered_pdbs_dir,
        structural_filter_enabled=structural_filter_enabled,
        max_antihotspot_contact_fraction=antihotspots.get(
            "max_antihotspot_contact_fraction"
        ),
        tool=tool,
    )

    has_candidates = summary["passed_design_count"] > 0
    if summary["structural_filter_enabled"] and not has_candidates:
        context.log.warning(
            f"No designs passed the structural filter for partition {partition_key!r}. "
            f"ProteinMPNN and downstream steps will be skipped. "
            f"See {filter_results_dir / 'structure_filter_summary.json'}."
        )

    context.log.info(
        f"Structure filter done → {filtered_pdbs_dir}  "
        f"({summary['passed_design_count']}/{summary['input_design_count']} passed)"
    )
    return MaterializeResult(
        metadata={
            "partition_key": partition_key,
            "tool": tool,
            "config_yaml": MetadataValue.path(str(config_yaml)),
            "contacts_skipped": contacts_skipped,
            "contacts_dir": MetadataValue.path(str(contacts_dir)),
            "report_count": report_count,
            "contact_distance_angstrom": antihotspots["contact_distance"],
            "max_antihotspot_contact_fraction": summary.get(
                "max_antihotspot_contact_fraction"
            ),
            "antihotspot_res": MetadataValue.text(", ".join(antihotspots["res"]) or "(none)"),
            "structure_filters_enabled": structure_tools_enabled,
            "structure_docker_image": pc.structure_filters.structure_docker_image,
            "structural_filter_enabled": structural_filter_enabled,
            "filter_results_dir": MetadataValue.path(str(filter_results_dir)),
            "filtered_pdbs_dir": MetadataValue.path(str(filtered_pdbs_dir)),
            "designs_dir": MetadataValue.path(str(pdbs_dir)),
            "input_design_count": summary["input_design_count"],
            "passed_design_count": summary["passed_design_count"],
            "failed_design_count": summary["failed_design_count"],
            "has_candidates": has_candidates,
            "branch_status": _branch_status(has_candidates),
            "summary_path": MetadataValue.path(
                str(filter_results_dir / "structure_filter_summary.json")
            ),
        }
    )


# ---------------------------------------------------------------------------
# proteinmpnn group  (partitioned)
# ---------------------------------------------------------------------------

def _resolve_boltzgen_binder_sequence_file(
    pc: PipelineAllToolsConfig,
    entry: dict,
) -> str:
    """Return the BoltzGen design-fragment path for ProteinMPNN fixed positions."""
    from_manifest = str(entry.get("binder_sequence_file") or "").strip()
    if from_manifest:
        return from_manifest

    design_name = str(entry.get("design_name") or "").strip()
    if design_name and design_name in pc.boltzgen.designs_config:
        from_config = pc.boltzgen.designs_config[design_name].binder_sequence_file
        if from_config:
            return str(from_config)

    raise Failure(
        description=(
            f"BoltzGen partition {entry.get('subdir')!r} needs binder_sequence_file "
            "in the manifest or boltzgen.designs_config.<design>.binder_sequence_file "
            "to fix non-designed scaffold residues for ProteinMPNN."
        )
    )


@asset(
    group_name="proteinmpnn",
    partitions_def=design_configs,
    deps=[design_structure_filter],
    output_required=False,
    description=(
        "Prepares ProteinMPNN inputs for one partition when structure filtering left "
        "at least one design in ``pdbs_filtered/``. Parses PDBs into ``parsed_pdbs.jsonl`` "
        "and writes ``assigned_chains.jsonl``. For BoltzGen partitions, also writes "
        "``fixed_positions.jsonl`` from the design-fragment scaffold so non-designed "
        "binder residues stay fixed during inverse folding. When no designs passed the "
        "structure filter (or this partition does not use ProteinMPNN), materializes "
        "``branch_status=no_candidates`` / ``skipped`` so asset backfills complete."
    ),
)
def proteinmpnn_parsed(context: AssetExecutionContext):
    pc = _read_pipeline_config(partition_key=context.partition_key)
    if _is_sequence_import_partition(context, pc):
        context.log.info(
            f"[proteinmpnn_parsed] partition={context.partition_key}  "
            "sequence_import partition; materializing ProteinMPNN parse as skipped"
        )
        yield _empty_branch_output(
            context,
            reason="sequence_import partition",
            branch_status="skipped",
        )
        return

    entry = _manifest_entry(context, pc)
    tool = str(entry.get("tool") or "")
    if tool == DESIGN_TOOL_PROMERA:
        context.log.info(
            f"[proteinmpnn_parsed] partition={context.partition_key}  "
            "promera partition; materializing ProteinMPNN parse as skipped "
            "(promera already did its own AbMPNN CDR redesign — see "
            "proteinmpnn_soluprot_filter)"
        )
        yield _empty_branch_output(
            context,
            reason="promera partition uses its own AbMPNN sequences",
            branch_status="skipped",
            tool=tool,
        )
        return
    mpnn = pc.proteinmpnn
    proot = _partition_root(_run_outputs_dir(pc), context.partition_key, pc.run_id)
    pdbs_dir = proot / "pdbs_filtered"
    mpnn_dir = proot / "proteinmpnn"
    mpnn_dir.mkdir(parents=True, exist_ok=True)

    if not pdbs_dir.is_dir():
        raise Failure(
            description=(
                f"Filtered PDB directory missing for partition {context.partition_key!r}: "
                f"{pdbs_dir}. Re-materialize design_structure_filter."
            )
        )

    passed_pdbs = sorted(pdbs_dir.glob("design_*.pdb"))
    if not passed_pdbs:
        context.log.warning(
            f"[proteinmpnn_parsed] partition={context.partition_key}  "
            f"no filtered PDBs in {pdbs_dir}; materializing empty ProteinMPNN branch"
        )
        yield _empty_branch_output(
            context,
            reason=f"no filtered PDBs in {pdbs_dir}",
            value={"candidate_count": 0},
            pdbs_dir=MetadataValue.path(str(pdbs_dir)),
            candidate_count=0,
        )
        return

    context.log.info(
        f"[proteinmpnn_parsed] partition={context.partition_key}  "
        f"pdbs_dir={pdbs_dir}  output={mpnn_dir / 'parsed_pdbs.jsonl'}  "
        f"candidate_count={len(passed_pdbs)}"
    )
    _run(
        context,
        [
            "docker", "run", "--rm", *_docker_user_args(),
            "--entrypoint", "python",
            "-v", f"{pdbs_dir}:/input/pdbs",
            "-v", f"{mpnn_dir}:/output",
            mpnn.docker_image,
            "/app/proteinmpnn/helper_scripts/parse_multiple_chains.py",
            "--input_path=/input/pdbs",
            "--output_path=/output/parsed_pdbs.jsonl",
        ],
    )

    parsed_jsonl = mpnn_dir / "parsed_pdbs.jsonl"
    context.log.info(
        f"Parsed JSONL written → {parsed_jsonl}  "
        f"({parsed_jsonl.stat().st_size if parsed_jsonl.is_file() else 0} bytes)"
    )

    context.log.info(
        f"[proteinmpnn_parsed] assigning fixed chains  chain_list={mpnn.chain_list}"
    )
    _run(
        context,
        [
            "docker", "run", "--rm", *_docker_user_args(),
            "--entrypoint", "python",
            "-v", f"{mpnn_dir}:/output",
            mpnn.docker_image,
            "/app/proteinmpnn/helper_scripts/assign_fixed_chains.py",
            "--input_path=/output/parsed_pdbs.jsonl",
            "--output_path=/output/assigned_chains.jsonl",
            "--chain_list", mpnn.chain_list,
        ],
    )

    assigned_jsonl = mpnn_dir / "assigned_chains.jsonl"
    context.log.info(f"Assigned chains JSONL written → {assigned_jsonl}")

    fixed_jsonl = mpnn_dir / "fixed_positions.jsonl"
    fixed_count = 0
    binder_sequence_file = ""
    if fixed_jsonl.is_file():
        fixed_jsonl.unlink()

    if tool == DESIGN_TOOL_BOLTZGEN:
        binder_sequence_file = _resolve_boltzgen_binder_sequence_file(pc, entry)
        if not Path(binder_sequence_file).is_file():
            raise Failure(
                description=f"BoltzGen binder_sequence_file not found: {binder_sequence_file}"
            )
        designed_chain = primary_designed_chain(mpnn.chain_list)
        context.log.info(
            f"[proteinmpnn_parsed] writing fixed scaffold positions from "
            f"{binder_sequence_file}  designed_chain={designed_chain}"
        )
        mapping = write_fixed_positions_jsonl(
            parsed_jsonl,
            fixed_jsonl,
            binder_sequence_file=binder_sequence_file,
            designed_chain=designed_chain,
        )
        fixed_count = sum(len(pos) for chains in mapping.values() for pos in chains.values())
        context.log.info(
            f"Fixed positions JSONL written → {fixed_jsonl}  "
            f"({len(mapping)} structure(s), {fixed_count} fixed binder residues total)"
        )
    else:
        context.log.info(
            f"[proteinmpnn_parsed] tool={tool or 'unknown'} — skipping fixed_positions_jsonl "
            "(BoltzGen-only partial inverse folding)"
        )

    yield Output(
        {
            "partition_key": context.partition_key,
            "parsed_jsonl": str(parsed_jsonl),
            "assigned_jsonl": str(assigned_jsonl),
            "fixed_jsonl": str(fixed_jsonl) if fixed_jsonl.is_file() else "",
            "candidate_count": len(passed_pdbs),
        },
        metadata={
            "parsed_jsonl": MetadataValue.path(str(parsed_jsonl)),
            "assigned_jsonl": MetadataValue.path(str(assigned_jsonl)),
            "fixed_jsonl": (
                MetadataValue.path(str(fixed_jsonl)) if fixed_jsonl.is_file() else ""
            ),
            "pdbs_dir": MetadataValue.path(str(pdbs_dir)),
            "chain_list": mpnn.chain_list,
            "candidate_count": len(passed_pdbs),
            "branch_status": "has_candidates",
            "parsed_size_bytes": parsed_jsonl.stat().st_size if parsed_jsonl.is_file() else 0,
            "fixed_binder_residue_count": fixed_count,
            "binder_sequence_file": binder_sequence_file,
        },
    )


@asset(
    group_name="proteinmpnn",
    partitions_def=design_configs,
    deps=[proteinmpnn_parsed],
    output_required=False,
    description=(
        "Generates ProteinMPNN sequences for one partition and writes FASTA files "
        "to the partition's ``proteinmpnn/seqs`` directory. "
        "Uses global ``proteinmpnn.omit_AAs``, merged with ``proteinmpnn.omit_AA`` "
        "when a scaffold key appears in the partition / design name. "
        "For ``sequence_import`` / ``promera`` partitions, or when "
        "``proteinmpnn_parsed`` left no candidates, materializes "
        "``branch_status=skipped`` / ``no_candidates`` (promera / sequence_import "
        "sequences are imported in ``proteinmpnn_soluprot_filter``)."
    ),
)
def proteinmpnn_sequences(
    context: AssetExecutionContext,
):
    pc = _read_pipeline_config(partition_key=context.partition_key)
    if _is_sequence_import_partition(context, pc):
        context.log.info(
            f"[proteinmpnn_sequences] partition={context.partition_key}  "
            "sequence_import partition; materializing ProteinMPNN as skipped"
        )
        yield _empty_branch_output(
            context,
            reason="sequence_import partition",
            branch_status="skipped",
        )
        return

    entry = _manifest_entry(context, pc)
    if str(entry.get("tool") or "") == DESIGN_TOOL_PROMERA:
        context.log.info(
            f"[proteinmpnn_sequences] partition={context.partition_key}  "
            "promera partition; materializing ProteinMPNN as skipped "
            "(uses its own AbMPNN sequence)"
        )
        yield _empty_branch_output(
            context,
            reason="promera partition uses its own AbMPNN sequences",
            branch_status="skipped",
        )
        return
    mpnn = pc.proteinmpnn
    omit_aas, matched_scaffold, matched_extra = resolve_omit_aas_for_names(
        mpnn.omit_AAs,
        mpnn.omit_AA,
        [
            context.partition_key,
            str(entry.get("design_name") or ""),
            str(entry.get("config_name") or ""),
            str(entry.get("subdir") or ""),
            str(entry.get("scaffold") or ""),
        ],
    )

    mpnn_dir = _partition_root(_run_outputs_dir(pc), context.partition_key, pc.run_id) / "proteinmpnn"
    parsed_jsonl = mpnn_dir / "parsed_pdbs.jsonl"
    if not parsed_jsonl.is_file():
        context.log.warning(
            f"[proteinmpnn_sequences] partition={context.partition_key}  "
            f"no {parsed_jsonl.name}; materializing empty ProteinMPNN sequences"
        )
        yield _empty_branch_output(
            context,
            reason=f"missing {parsed_jsonl}",
            value={"fasta_count": 0},
            fasta_count=0,
        )
        return

    context.log.info(
        f"[proteinmpnn_sequences] partition={context.partition_key}  "
        f"num_seq_per_target={mpnn.num_seq_per_target}  sampling_temp={mpnn.sampling_temp}  "
        f"use_soluble_model={mpnn.use_soluble_model}  omit_AAs={omit_aas}  "
        f"omit_AA_match={matched_scaffold!r}:{matched_extra!r}"
    )
    fixed_jsonl = mpnn_dir / "fixed_positions.jsonl"
    cmd: List[str] = [
        "docker", "run", "--rm", *_docker_user_args(),
        "--runtime=nvidia", *_docker_gpu_args(pc.gpus),
        "-v", f"{mpnn_dir}:/output",
        mpnn.docker_image,
        "--jsonl_path", "/output/parsed_pdbs.jsonl",
        "--chain_id_jsonl", "/output/assigned_chains.jsonl",
        "--out_folder", "/output",
        "--num_seq_per_target", str(mpnn.num_seq_per_target),
        "--omit_AAs", omit_aas,
        "--sampling_temp", mpnn.sampling_temp,
    ]
    if fixed_jsonl.is_file():
        context.log.info(
            f"[proteinmpnn_sequences] using fixed_positions_jsonl={fixed_jsonl}"
        )
        cmd.extend(["--fixed_positions_jsonl", "/output/fixed_positions.jsonl"])
    if mpnn.use_soluble_model:
        cmd.append("--use_soluble_model")
    cmd.extend(mpnn.extra_args)
    _run(context, cmd)

    seq_dir = mpnn_dir / "seqs"
    fa_files = sorted(seq_dir.glob("*.fa")) if seq_dir.is_dir() else []
    context.log.info(f"ProteinMPNN sequences done → {seq_dir}  ({len(fa_files)} FASTA file(s))")
    yield Output(
        {"seq_dir": str(seq_dir), "fasta_count": len(fa_files)},
        metadata={
            "seq_dir": MetadataValue.path(str(seq_dir)),
            "fasta_count": len(fa_files),
            "num_seq_per_target": mpnn.num_seq_per_target,
            "omit_AAs": omit_aas,
            "omit_AA_match": matched_scaffold or "",
            "omit_AA_extra": matched_extra,
            "sampling_temp": mpnn.sampling_temp,
            "use_soluble_model": mpnn.use_soluble_model,
            "fixed_positions_jsonl": (
                MetadataValue.path(str(fixed_jsonl)) if fixed_jsonl.is_file() else ""
            ),
            "sample_files": MetadataValue.text("\n".join(f.name for f in fa_files[:10])),
        },
    )


@asset(
    group_name="proteinmpnn",
    partitions_def=design_configs,
    deps=[proteinmpnn_sequences],
    description=(
        "Run SoluProt on binder sequences in ``proteinmpnn/seqs/`` and write passing "
        "FASTAs to ``proteinmpnn/seqs_filtered/``. Always materializes a filter summary; "
        "when ``soluprot.enabled`` is false, copies ``seqs/`` unchanged. When no "
        "sequences are available or none pass, still materializes "
        "``branch_status=no_candidates`` (downstream Boltz-2 / ESMFold assets then "
        "materialize empty rather than leaving a backfill hanging). For "
        "``sequence_import`` partitions, auto-imports from the manifest FASTA when "
        "``seqs/`` is empty."
    ),
)
def proteinmpnn_soluprot_filter(
    context: AssetExecutionContext,
) -> MaterializeResult:
    pc = _read_pipeline_config(partition_key=context.partition_key)
    sp = pc.soluprot
    bz = pc.boltz2
    proot = _partition_root(_run_outputs_dir(pc), context.partition_key, pc.run_id)
    mpnn_dir = proot / "proteinmpnn"
    seq_dir = mpnn_dir / "seqs"
    filtered_dir = mpnn_dir / "seqs_filtered"
    work_dir = mpnn_dir / "soluprot"
    exts = tuple(bz.fasta_extensions)

    entry = _manifest_entry(context, pc)
    if entry.get("source") == "sequence_import" and not list_mpnn_fasta_files(seq_dir, exts):
        input_fasta = Path(entry["input_fasta"])
        context.log.info(
            f"[proteinmpnn_soluprot_filter] auto-importing sequences from {input_fasta}"
        )
        import_external_fasta_to_mpnn_seqs(input_fasta, seq_dir)
    elif str(entry.get("tool") or "") == DESIGN_TOOL_PROMERA and not list_mpnn_fasta_files(
        seq_dir, exts
    ):
        # ProteinMPNN never ran for promera partitions (proteinmpnn_sequences skips
        # them) — import promera's own AbMPNN CDR-redesigned sequence per passing
        # design instead, matching the sequence_import layout in seqs/.
        pdbs_filtered_dir = proot / "pdbs_filtered"
        designs_dir = _designs_dir(proot, DESIGN_TOOL_PROMERA)
        context.log.info(
            f"[proteinmpnn_soluprot_filter] importing promera's own sequences from "
            f"{pdbs_filtered_dir} (design_index.json in {designs_dir})"
        )
        import_promera_fasta_to_mpnn_seqs(designs_dir, pdbs_filtered_dir, seq_dir)

    if not seq_dir.is_dir() or not list_mpnn_fasta_files(seq_dir, exts):
        context.log.warning(
            f"[proteinmpnn_soluprot_filter] partition={context.partition_key}  "
            f"no sequences in {seq_dir}; materializing empty SoluProt result"
        )
        if filtered_dir.exists():
            shutil.rmtree(filtered_dir)
        filtered_dir.mkdir(parents=True, exist_ok=True)
        work_dir.mkdir(parents=True, exist_ok=True)
        return _empty_branch_result(
            context,
            reason=f"ProteinMPNN sequence directory missing or empty: {seq_dir}",
            enabled=sp.enabled,
            seq_dir=MetadataValue.path(str(seq_dir)),
            filtered_dir=MetadataValue.path(str(filtered_dir)),
            work_dir=MetadataValue.path(str(work_dir)),
            input_sequence_count=0,
            passed_sequence_count=0,
            failed_sequence_count=0,
            output_fasta_count=0,
        )

    context.log.info(
        f"[proteinmpnn_soluprot_filter] partition={context.partition_key}  "
        f"enabled={sp.enabled}  min_soluble_score={sp.min_soluble_score}  "
        f"seq_dir={seq_dir}  output={filtered_dir}"
    )

    if filtered_dir.exists():
        shutil.rmtree(filtered_dir)
    work_dir.mkdir(parents=True, exist_ok=True)

    if sp.enabled:
        input_fa = work_dir / "all_designs.fa"
        preds_csv = work_dir / "preds.csv"
        tmp_dir = work_dir / "tmp"
        if tmp_dir.exists():
            shutil.rmtree(tmp_dir)
        tmp_dir.mkdir(parents=True, exist_ok=True)

        seq_count = write_soluprot_input_fasta(seq_dir, input_fa, exts)
        if seq_count == 0:
            raise Failure(
                description=(
                    f"No MPNN design sequences found in {seq_dir} for partition "
                    f"{context.partition_key!r}."
                )
            )

        context.log.info(f"SoluProt input FASTA → {input_fa}  ({seq_count} sequence(s))")

        cmd: List[str] = [
            "docker", "run", "--rm", *_docker_user_args(),
            "-v", f"{work_dir}:/workspace",
            sp.docker_image,
            "soluprot",
            "--i_fa", "/workspace/all_designs.fa",
            "--o_csv", "/workspace/preds.csv",
            "--tmp_dir", "/workspace/tmp",
        ]
        if sp.no_proc > 1:
            cmd.extend(["--no_proc", str(sp.no_proc)])
        _run(context, cmd)

    payload = filter_partition_soluprot(
        seq_dir,
        filtered_dir,
        work_dir,
        enabled=sp.enabled,
        min_soluble_score=sp.min_soluble_score,
        fasta_extensions=exts,
        fail_if_all_filtered=False,
    )
    summary = payload["summary"]
    has_candidates = summary["passed_sequence_count"] > 0 and summary["output_fasta_count"] > 0

    if not has_candidates:
        context.log.warning(
            f"[proteinmpnn_soluprot_filter] partition={context.partition_key}  "
            f"no sequences passed SoluProt; downstream Boltz-2 / ESMFold assets will "
            f"materialize as no_candidates. See {work_dir / 'soluprot_filter_summary.json'}."
        )

    context.log.info(
        f"SoluProt filter done → {filtered_dir}  "
        f"({summary['passed_sequence_count']}/{summary['input_sequence_count']} sequences passed)"
    )
    return MaterializeResult(
        metadata={
            "partition_key": context.partition_key,
            "enabled": sp.enabled,
            "seq_dir": MetadataValue.path(str(seq_dir)),
            "filtered_dir": MetadataValue.path(str(filtered_dir)),
            "work_dir": MetadataValue.path(str(work_dir)),
            "input_sequence_count": summary["input_sequence_count"],
            "passed_sequence_count": summary["passed_sequence_count"],
            "failed_sequence_count": summary["failed_sequence_count"],
            "output_fasta_count": summary["output_fasta_count"],
            "has_candidates": has_candidates,
            "branch_status": _branch_status(has_candidates),
            "min_soluble_score": sp.min_soluble_score,
            "docker_image": sp.docker_image,
            "summary_path": MetadataValue.path(str(work_dir / "soluprot_filter_summary.json")),
        }
    )


# ---------------------------------------------------------------------------
# boltz2 group  (partitioned; YAML inputs after ProteinMPNN)
# ---------------------------------------------------------------------------

@asset(
    group_name="boltz2",
    partitions_def=design_configs,
    deps=[proteinmpnn_soluprot_filter],
    output_required=False,
    description=(
        "Precomputes Boltz-2 paired and unpaired MSAs after SoluProt filtering. "
        "Unpaired searches are shared globally by sequence checksum; paired searches "
        "are cached by the ordered complex. ESMFold does not depend on this asset. "
        "When ``seqs_filtered/`` is empty, materializes ``branch_status=no_candidates``."
    ),
)
def MSA(context: AssetExecutionContext):
    pc = _read_pipeline_config(partition_key=context.partition_key)
    proot = _partition_root(_run_outputs_dir(pc), context.partition_key, pc.run_id)
    seq_dir = proot / "proteinmpnn" / "seqs_filtered"
    msa_dir = proot / "proteinmpnn" / "boltz2_msas"
    bz = pc.boltz2
    msa_config = pc.msa
    target_path = Path(bz.target_fasta)

    target_raw = read_fasta_sequence_flat(target_path)
    target_segments = split_target_segments(target_raw)
    if not target_segments:
        raise Failure(description=f"Boltz-2 target FASTA has no sequences: {target_path}")

    exts = tuple(e if e.startswith(".") else f".{e}" for e in bz.fasta_extensions)
    if not seq_dir.is_dir():
        context.log.warning(
            f"[MSA] partition={context.partition_key} missing {seq_dir}; "
            "materializing empty Boltz-2 MSA branch"
        )
        yield _empty_branch_output(
            context,
            reason=f"SoluProt filtered sequence directory missing: {seq_dir}",
            value={"complex_count": 0, "chain_count": 0},
            complex_count=0,
            chain_count=0,
        )
        return
    files = sorted(p for p in seq_dir.iterdir() if p.is_file() and p.suffix.lower() in exts)
    if not files:
        context.log.warning(
            f"[MSA] partition={context.partition_key} no FASTAs in "
            f"{seq_dir}; materializing empty Boltz-2 MSA branch"
        )
        yield _empty_branch_output(
            context,
            reason=f"no FASTAs in {seq_dir}",
            value={"complex_count": 0, "chain_count": 0},
            complex_count=0,
            chain_count=0,
            seq_dir=MetadataValue.path(str(seq_dir)),
        )
        return

    complexes: list[ComplexSpec] = []
    for src in files:
        for base, binder, target in iter_exploded_binder_target_mpnn(src, target_raw):
            segments = split_target_segments(target)
            chains = [("A", binder)]
            chains.extend(
                (chr(ord("B") + index), sequence)
                for index, sequence in enumerate(segments)
            )
            complexes.append(ComplexSpec(name=base, chains=tuple(chains)))

    if not complexes:
        context.log.warning(
            f"[MSA] partition={context.partition_key} no binder records in "
            f"{seq_dir}; materializing empty Boltz-2 MSA branch"
        )
        yield _empty_branch_output(
            context,
            reason=f"no binder records in {seq_dir}",
            value={"complex_count": 0, "chain_count": 0},
            complex_count=0,
            chain_count=0,
            seq_dir=MetadataValue.path(str(seq_dir)),
        )
        return

    if msa_dir.exists():
        shutil.rmtree(msa_dir)
    client = MsaServerClient(
        msa_config.server_url,
        timeout_seconds=msa_config.request_timeout_seconds,
        max_retries=msa_config.max_retries,
        poll_interval_seconds=msa_config.poll_interval_seconds,
        retry_backoff_seconds=msa_config.retry_backoff_seconds,
    )
    paired_mode = f"pair{msa_config.pairing_strategy}"
    if msa_config.use_env:
        paired_mode += "-env"
    unpaired_mode = "env" if msa_config.use_env else "all"

    context.log.info(
        f"[MSA] partition={context.partition_key} complexes={len(complexes)} "
        f"unique_chains={len(set(sequence for item in complexes for _, sequence in item.chains))} "
        f"server={msa_config.server_url} cache={DEFAULT_MSA_CACHE_DIR}"
    )
    unique_sequences = list(
        dict.fromkeys(sequence for item in complexes for _, sequence in item.chains)
    )
    msa_total = len(unique_sequences) + len(complexes)

    # tqdm uses \\r redraws; emit each refresh as a full line so Dagster's
    # line-oriented compute log (same path as other stdout) can show progress.
    class _TqdmStdout:
        def write(self, s: str) -> int:
            if not s:
                return 0
            text = s.replace("\r", "\n").rstrip("\n")
            if text:
                print(text, flush=True)
            return len(s)

        def flush(self) -> None:
            sys.stdout.flush()

    with tqdm(
        total=msa_total,
        desc=f"MSA {context.partition_key}",
        unit="msa",
        file=_TqdmStdout(),
        dynamic_ncols=True,
        mininterval=0.5,
        ascii=True,
    ) as pbar:
        manifest = build_boltz_msa_bundle(
            complexes,
            output_dir=msa_dir,
            cache_root=Path(DEFAULT_MSA_CACHE_DIR),
            client=client,
            unpaired_mode=unpaired_mode,
            paired_mode=paired_mode,
            use_env=msa_config.use_env,
            max_paired_seqs=msa_config.max_paired_seqs,
            max_msa_seqs=msa_config.max_msa_seqs,
            on_msa_done=pbar.update,
        )
    manifest_complexes = manifest["complexes"]
    chain_count = sum(len(item["chains"]) for item in manifest_complexes.values())
    yield Output(
        {
            "partition_key": context.partition_key,
            "msa_dir": str(msa_dir),
            "complex_count": len(complexes),
            "chain_count": chain_count,
        },
        metadata={
            "msa_dir": MetadataValue.path(str(msa_dir)),
            "manifest_path": MetadataValue.path(str(msa_dir / "manifest.json")),
            "global_cache_dir": MetadataValue.path(DEFAULT_MSA_CACHE_DIR),
            "server_url": msa_config.server_url,
            "complex_count": len(complexes),
            "chain_count": chain_count,
            "branch_status": "has_candidates",
        },
    )


@asset(
    group_name="boltz2",
    partitions_def=design_configs,
    deps=[MSA],
    output_required=False,
    description=(
        "Writes Boltz-2 ``predict`` input YAMLs to ``proteinmpnn/combined_yamls`` when "
        "the MSA asset produced precomputed per-chain CSVs. When no sequences passed, "
        "materializes ``branch_status=no_candidates``. ``boltz2.target_fasta``, "
        "``boltz2.fasta_extensions``, and ``boltz2.template_yaml`` are read from the "
        "run-scoped config."
    ),
)
def boltz2_input_yamls(context: AssetExecutionContext):
    pc = _read_pipeline_config(partition_key=context.partition_key)
    proot = _partition_root(_run_outputs_dir(pc), context.partition_key, pc.run_id)
    seq_dir = proot / "proteinmpnn" / "seqs_filtered"
    yaml_dir = proot / "proteinmpnn" / "combined_yamls"
    msa_dir = proot / "proteinmpnn" / "boltz2_msas"
    bz = pc.boltz2
    append_path = Path(bz.target_fasta)
    template_path = Path(bz.template_yaml)

    append_seq = read_fasta_sequence_flat(append_path)
    if not append_seq:
        raise Failure(
            description=f"Boltz-2 target FASTA has no sequence residues: {append_path}"
        )
    if not template_path.is_file():
        raise Failure(description=f"Boltz-2 template YAML not found: {template_path}")

    template_text = template_path.read_text(encoding="utf-8")
    exts = tuple(e if e.startswith(".") else f".{e}" for e in bz.fasta_extensions)
    if not seq_dir.is_dir():
        context.log.warning(
            f"[boltz2_input_yamls] partition={context.partition_key}  "
            f"missing {seq_dir}; materializing empty Boltz-2 YAML branch"
        )
        yield _empty_branch_output(
            context,
            reason=f"SoluProt filtered sequence directory missing: {seq_dir}",
            value={"source_fasta_count": 0, "yaml_files_written": 0},
            source_fastas=0,
            yaml_files_written=0,
        )
        return
    files = sorted(p for p in seq_dir.iterdir() if p.is_file() and p.suffix.lower() in exts)
    if not files:
        context.log.warning(
            f"[boltz2_input_yamls] partition={context.partition_key}  "
            f"no FASTAs in {seq_dir}; materializing empty Boltz-2 YAML branch"
        )
        yield _empty_branch_output(
            context,
            reason=f"no FASTAs in {seq_dir}",
            value={"source_fasta_count": 0, "yaml_files_written": 0},
            source_fastas=0,
            yaml_files_written=0,
            seq_dir=MetadataValue.path(str(seq_dir)),
        )
        return

    msa_manifest_path = msa_dir / "manifest.json"
    if not msa_manifest_path.is_file():
        raise Failure(
            description=(
                f"Boltz-2 MSA manifest missing for partition {context.partition_key!r}: "
                f"{msa_manifest_path}. Materialize MSA first."
            )
        )
    msa_manifest = json.loads(msa_manifest_path.read_text(encoding="utf-8"))
    manifest_complexes = msa_manifest.get("complexes")
    if not isinstance(manifest_complexes, dict):
        raise Failure(description=f"Invalid Boltz-2 MSA manifest: {msa_manifest_path}")
    if yaml_dir.exists():
        shutil.rmtree(yaml_dir)
    yaml_dir.mkdir(parents=True, exist_ok=True)
    written = 0
    for src in files:
        n = 0
        for base, binder, target in iter_exploded_binder_target_mpnn(src, append_seq):
            complex_payload = manifest_complexes.get(base)
            if not isinstance(complex_payload, dict) or not isinstance(
                complex_payload.get("chains"), list
            ):
                raise Failure(description=f"MSA manifest has no complex entry for {base!r}.")
            expected_chains = [("A", binder)]
            expected_chains.extend(
                (chr(ord("B") + index), sequence)
                for index, sequence in enumerate(split_target_segments(target))
            )
            chain_payloads = complex_payload["chains"]
            actual = [
                (str(chain.get("id")), str(chain.get("sequence")))
                for chain in chain_payloads
                if isinstance(chain, dict)
            ]
            if actual != expected_chains:
                raise Failure(
                    description=(
                        f"MSA manifest chain mismatch for {base!r}: "
                        f"expected {expected_chains}, got {actual}."
                    )
                )
            msa_paths: dict[str, str] = {}
            for chain in chain_payloads:
                chain_id = str(chain["id"])
                host_path = Path(str(chain["csv_path"]))
                if not host_path.is_file():
                    raise Failure(description=f"MSA CSV missing for {base}/{chain_id}: {host_path}")
                try:
                    relative_path = host_path.relative_to(proot)
                except ValueError as exc:
                    raise Failure(
                        description=f"MSA CSV is outside partition root: {host_path}"
                    ) from exc
                msa_paths[chain_id] = f"/work/{relative_path.as_posix()}"
            write_yaml_for_pair(
                template_text,
                yaml_dir,
                base,
                binder,
                target,
                msa_paths=msa_paths,
            )
            n += 1
        written += n
        context.log.info(f"Boltz-2 YAMLs from {src.name}: {n} file(s)")

    yaml_files = sorted(yaml_dir.glob("*.yaml")) if yaml_dir.is_dir() else []
    yield Output(
        {
            "partition_key": context.partition_key,
            "combined_yamls_dir": str(yaml_dir),
            "source_fasta_count": len(files),
            "yaml_files_written": written,
        },
        metadata={
            "combined_yamls_dir": MetadataValue.path(str(yaml_dir)),
            "source_fastas": len(files),
            "yaml_files_written": written,
            "branch_status": "has_candidates",
            "template_yaml": MetadataValue.path(bz.template_yaml),
            "target_fasta": MetadataValue.path(bz.target_fasta),
            "sample_files": MetadataValue.text("\n".join(f.name for f in yaml_files[:10])),
        },
    )


@asset(
    group_name="boltz2",
    partitions_def=design_configs,
    deps=[boltz2_input_yamls],
    description=(
        "Runs Boltz-2 structure predictions for one partition using input YAMLs from "
        "``proteinmpnn/combined_yamls``, writes results to ``boltz2/``. "
        "Target-chain renumbering is a separate ``boltz2_renumber`` asset. "
        "Parameters are read from the run-scoped ``pipeline_config.yaml``."
    ),
)
def boltz2_predictions(
    context: AssetExecutionContext,
) -> MaterializeResult:
    pc = _read_pipeline_config(partition_key=context.partition_key)
    proot = _partition_root(_run_outputs_dir(pc), context.partition_key, pc.run_id)
    bz = pc.boltz2
    yaml_dir = proot / "proteinmpnn" / "combined_yamls"
    out_dir = proot / "boltz2"
    out_dir.mkdir(parents=True, exist_ok=True)

    yaml_files = sorted(yaml_dir.glob("*.yaml")) if yaml_dir.is_dir() else []
    if not yaml_files:
        context.log.warning(
            f"[boltz2_predictions] partition={context.partition_key}  "
            f"no YAMLs in {yaml_dir}; materializing empty Boltz-2 predictions"
        )
        return _empty_branch_result(
            context,
            reason=f"Boltz-2 YAML input directory missing or empty: {yaml_dir}",
            predictions_dir=MetadataValue.path(str(out_dir)),
            input_yaml_count=0,
            structure_file_count=0,
        )

    chunk_size = bz.query_chunk_size if bz.query_chunk_size > 0 else len(yaml_files)
    context.log.info(
        f"[boltz2_predictions] partition={context.partition_key}  "
        f"yaml_files={len(yaml_files)}  chunk_size={chunk_size}  "
        f"recycling_steps={bz.recycling_steps}  diffusion_samples={bz.diffusion_samples}  "
        "msa_source=precomputed"
    )
    chunk_root = proot / "proteinmpnn" / "combined_yamls_chunks"
    if chunk_root.exists():
        shutil.rmtree(chunk_root)
    chunk_root.mkdir(parents=True, exist_ok=True)


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

        boltz_devices = max(1, bz.devices)
        boltz_gpus = _select_gpu_ids(pc.gpus, boltz_devices)
        cmd = [
            "docker", "run", "--rm", *_docker_user_args(),
            "--runtime=nvidia",
            "--ipc=host",
            *_docker_gpu_args(boltz_gpus),
            "-e",
            "LD_LIBRARY_PATH=/usr/local/lib/python3.11/dist-packages/nvidia/cu13/lib:/usr/local/nvidia/lib:/usr/local/nvidia/lib64",
            "-v", f"{proot}:/work:rw",
            "-v", f"{DEFAULT_BOLTZ2_CACHE_VOLUME}:/cache:rw",
            bz.docker_image,
            "predict", f"/work/proteinmpnn/combined_yamls_chunks/chunk_{chunk_count:04d}",
            "--cache", "/cache",
            "--out_dir", "/work/boltz2",
            "--recycling_steps", str(bz.recycling_steps),
            "--devices", str(boltz_devices),
            "--diffusion_samples", str(bz.diffusion_samples),
            "--num_workers", str(bz.num_workers),
            "--preprocessing-threads", str(bz.preprocessing_threads),
        ]
        if bz.use_potentials:
            cmd.append("--use_potentials")
        cmd.append("--override")
        cmd.append("--no_kernels")

        _run(context, cmd)

    cif_files = list(out_dir.rglob("*.cif"))
    pdb_files = list(out_dir.rglob("*.pdb"))
    json_files = list(out_dir.rglob("*.json"))
    structure_files = cif_files or pdb_files
    context.log.info(
        f"Boltz-2 done → {out_dir}  "
        f"({len(structure_files)} structure file(s), {len(json_files)} JSON file(s))"
    )
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
            "msa_source": "precomputed",
        }
    )


@asset(
    group_name="boltz2",
    partitions_def=design_configs,
    deps=[boltz2_predictions],
    description=(
        "Renumbers Boltz-2 target chains in ``boltz2/`` to match ``boltz2.target_pdb`` "
        "from the run-scoped ``pipeline_config.yaml``, writing ``boltz2_renumbered/``. "
        "Uses ``structure_filters.structure_docker_image``; gated by "
        "``boltz2.renumber_outputs`` (independent of ``structure_filters.enabled``)."
    ),
)
def boltz2_renumber(
    context: AssetExecutionContext,
) -> MaterializeResult:
    pc = _read_pipeline_config(partition_key=context.partition_key)
    proot = _partition_root(_run_outputs_dir(pc), context.partition_key, pc.run_id)
    bz = pc.boltz2
    input_dir = proot / "boltz2"
    renumbered_dir = proot / "boltz2_renumbered"

    if not bz.renumber_outputs:
        context.log.info("[boltz2_renumber] boltz2.renumber_outputs=false — skipping")
        return _empty_branch_result(
            context,
            reason="boltz2.renumber_outputs=false",
            branch_status="skipped",
            predictions_dir=MetadataValue.path(str(input_dir)),
            renumbered_dir=MetadataValue.path(str(renumbered_dir)),
            renumbered_structure_file_count=0,
            structure_docker_image=pc.structure_filters.structure_docker_image,
        )

    structure_files = list(input_dir.rglob("*.cif")) + list(input_dir.rglob("*.pdb"))
    if not structure_files:
        context.log.warning(
            f"[boltz2_renumber] no structures in {input_dir}; materializing empty renumber"
        )
        return _empty_branch_result(
            context,
            reason=f"Boltz-2 predictions directory missing or empty: {input_dir}",
            predictions_dir=MetadataValue.path(str(input_dir)),
            renumbered_dir=MetadataValue.path(str(renumbered_dir)),
            renumbered_structure_file_count=0,
            structure_docker_image=pc.structure_filters.structure_docker_image,
        )

    target_pdb = _pipeline_target_pdb(pc)
    if not Path(bz.target_fasta).is_file():
        raise FileNotFoundError(f"Boltz-2 target FASTA not found: {bz.target_fasta}")

    renumbered_structure_count, mapping_path = _renumber_predicted_structures(
        context,
        pc,
        proot=proot,
        target_pdb=target_pdb,
        target_fasta=bz.target_fasta,
        input_dir=input_dir,
        output_dir=renumbered_dir,
        log_label="boltz2_renumber",
    )

    return MaterializeResult(
        metadata={
            "predictions_dir": MetadataValue.path(str(input_dir)),
            "renumbered_dir": MetadataValue.path(str(renumbered_dir)),
            "renumbered_structure_file_count": renumbered_structure_count,
            "target_pdb": MetadataValue.path(target_pdb),
            "target_fasta": MetadataValue.path(bz.target_fasta),
            "mapping_path": MetadataValue.path(str(mapping_path)) if mapping_path else "",
            "structure_docker_image": pc.structure_filters.structure_docker_image,
        }
    )


# ---------------------------------------------------------------------------
# esmfold group  (partitioned; JSON inputs after SoluProt)
# ---------------------------------------------------------------------------

def _esmfold_target_fasta(pc: PipelineAllToolsConfig) -> str:
    """Shared target epitope FASTA for ESMFold and Boltz-2."""
    return str(pc.boltz2.target_fasta or "").strip()


def _esmfold_fasta_extensions(pc: PipelineAllToolsConfig) -> tuple[str, ...]:
    """Shared FASTA suffixes for ESMFold and Boltz-2."""
    raw = list(pc.boltz2.fasta_extensions or [])
    return tuple(e if e.startswith(".") else f".{e}" for e in raw)


def _esmfold_predict_script() -> Path:
    return (_PROTEINDESIGN / "ESMfold2" / "predict.py").resolve()


def _esmfold_hf_cache_dir() -> Path:
    return Path(DEFAULT_ESMFOLD2_HF_CACHE_DIR).resolve()


@asset(
    group_name="esmfold",
    partitions_def=design_configs,
    deps=[proteinmpnn_soluprot_filter],
    output_required=False,
    description=(
        "Builds ESMFold2 ``StructurePredictionInput`` JSON files in "
        "``proteinmpnn/esmfold_inputs`` when SoluProt filtering left sequences in "
        "``seqs_filtered/``. When no sequences passed, materializes "
        "``branch_status=no_candidates``. "
        "ESMFold shares ``boltz2.target_fasta`` and ``boltz2.fasta_extensions``."
    ),
)
def esmfold_input_jsons(context: AssetExecutionContext):
    pc = _read_pipeline_config(partition_key=context.partition_key)
    proot = _partition_root(_run_outputs_dir(pc), context.partition_key, pc.run_id)
    seq_dir = proot / "proteinmpnn" / "seqs_filtered"
    json_dir = proot / "proteinmpnn" / "esmfold_inputs"
    ef = pc.esmfold
    target_fasta = _esmfold_target_fasta(pc)
    append_path = Path(target_fasta)

    append_seq = read_fasta_sequence_flat(append_path)
    if not append_seq:
        raise Failure(
            description=f"ESMFold target FASTA has no sequence residues: {append_path}"
        )

    exts = _esmfold_fasta_extensions(pc)
    if not seq_dir.is_dir():
        context.log.warning(
            f"[esmfold_input_jsons] partition={context.partition_key}  "
            f"missing {seq_dir}; materializing empty ESMFold input branch"
        )
        yield _empty_branch_output(
            context,
            reason=f"SoluProt filtered sequence directory missing: {seq_dir}",
            value={"source_fasta_count": 0, "files_written": 0},
            source_fastas=0,
            files_written=0,
        )
        return
    files = sorted(p for p in seq_dir.iterdir() if p.is_file() and p.suffix.lower() in exts)
    if not files:
        context.log.warning(
            f"[esmfold_input_jsons] partition={context.partition_key}  "
            f"no FASTAs in {seq_dir}; materializing empty ESMFold input branch"
        )
        yield _empty_branch_output(
            context,
            reason=f"no FASTAs in {seq_dir}",
            value={"source_fasta_count": 0, "files_written": 0},
            source_fastas=0,
            files_written=0,
            seq_dir=MetadataValue.path(str(seq_dir)),
        )
        return

    if json_dir.exists():
        shutil.rmtree(json_dir)
    json_dir.mkdir(parents=True, exist_ok=True)
    written = 0
    for src in files:
        n = 0
        for base, binder, target in iter_exploded_binder_target_mpnn(src, append_seq):
            write_json_for_pair(json_dir, base, binder, target)
            n += 1
        written += n
        context.log.info(f"ESMFold2 JSON inputs from {src.name}: {n} file(s)")

    query_files = sorted(json_dir.glob("*.json"))
    yield Output(
        {
            "partition_key": context.partition_key,
            "esmfold_inputs_dir": str(json_dir),
            "source_fasta_count": len(files),
            "files_written": written,
        },
        metadata={
            "esmfold_inputs_dir": MetadataValue.path(str(json_dir)),
            "source_fastas": len(files),
            "files_written": written,
            "branch_status": "has_candidates",
            "target_fasta": MetadataValue.path(target_fasta),
            "sample_files": MetadataValue.text("\n".join(f.name for f in query_files[:10])),
        },
    )


@asset(
    group_name="esmfold",
    partitions_def=design_configs,
    deps=[esmfold_input_jsons],
    description=(
        "Runs ESMFold2 structure predictions for one partition using JSON inputs from "
        "``proteinmpnn/esmfold_inputs``. Uses ``ESMfold2/predict.py`` to write mmCIF plus "
        "PAE/confidence sidecars under ``esmfold/``. Target-chain renumbering is a "
        "separate ``esmfold_renumber`` asset. "
        "Parameters are read from the run-scoped ``pipeline_config.yaml``."
    ),
)
def esmfold_predictions(
    context: AssetExecutionContext,
) -> MaterializeResult:
    pc = _read_pipeline_config(partition_key=context.partition_key)
    proot = _partition_root(_run_outputs_dir(pc), context.partition_key, pc.run_id)
    ef = pc.esmfold
    json_dir = proot / "proteinmpnn" / "esmfold_inputs"
    out_dir = proot / "esmfold"
    out_dir.mkdir(parents=True, exist_ok=True)

    query_files = sorted(json_dir.glob("*.json"))
    if not query_files:
        context.log.warning(
            f"[esmfold_predictions] partition={context.partition_key}  "
            f"no JSONs in {json_dir}; materializing empty ESMFold predictions"
        )
        return _empty_branch_result(
            context,
            reason=f"ESMFold2 JSON input directory missing or empty: {json_dir}",
            predictions_dir=MetadataValue.path(str(out_dir)),
            input_json_count=0,
            cif_file_count=0,
        )

    chunk_size = ef.query_chunk_size if ef.query_chunk_size > 0 else len(query_files)
    context.log.info(
        f"[esmfold_predictions] partition={context.partition_key}  "
        f"json_files={len(query_files)}  chunk_size={chunk_size}  "
        f"num_loops={ef.num_loops}  num_sampling_steps={ef.num_sampling_steps}  "
        f"model={ef.model}"
    )

    chunk_root = proot / "proteinmpnn" / "esmfold_input_chunks"
    if chunk_root.exists():
        shutil.rmtree(chunk_root)
    chunk_root.mkdir(parents=True, exist_ok=True)

    proot_str = str(proot.resolve())

    predict_script = _esmfold_predict_script()
    esmfold2_dir = predict_script.parent
    esmfold2_dir_str = str(esmfold2_dir)
    hf_cache = _esmfold_hf_cache_dir()
    hf_cache.mkdir(parents=True, exist_ok=True)
    hf_cache_str = str(hf_cache)

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
            os.symlink(os.path.relpath(src, start=chunk_dir), dst)

        context.log.info(
            f"ESMFold2 chunk {chunk_count}: {len(chunk_files)} JSON file(s) "
            f"({i + 1}-{i + len(chunk_files)} of {len(query_files)})"
        )

        esmfold_gpus = _select_gpu_ids(pc.gpus, 1)
        chunk_input = f"{proot_str}/proteinmpnn/esmfold_input_chunks/chunk_{chunk_count:04d}"
        cmd: List[str] = [
            "docker", "run", "--rm", *_docker_user_args(),
            "--runtime=nvidia",
            *_docker_gpu_args(esmfold_gpus),
            "-v", f"{proot_str}:{proot_str}:rw",
            "-v", f"{esmfold2_dir_str}:{esmfold2_dir_str}:ro",
            "-v", f"{hf_cache_str}:/cache/huggingface",
            "-e", "HF_HOME=/cache/huggingface",
            "-e", "TRANSFORMERS_CACHE=/cache/huggingface",
            "-e", "HUGGINGFACE_HUB_CACHE=/cache/huggingface",
            ef.docker_image,
            "python", str(predict_script),
            "-i", chunk_input,
            "-o", f"{proot_str}/esmfold",
            "--model", ef.model,
            "--num-loops", str(ef.num_loops),
            "--num-sampling-steps", str(ef.num_sampling_steps),
            "--num-diffusion-samples", str(ef.num_diffusion_samples),
            "--seed", str(ef.seed),
            "--device", "cuda",
        ]

        _run(context, cmd)


    cif_files = list(out_dir.glob("*.cif"))
    scores_files = list(out_dir.glob("*_scores.json"))
    confidence_files = list(out_dir.glob("confidence_*.json"))
    pae_files = list(out_dir.glob("pae_*.npz"))
    context.log.info(
        f"ESMFold2 done → {out_dir}  ({len(cif_files)} CIF, "
        f"{len(scores_files)} scores JSON, {len(confidence_files)} confidence JSON, "
        f"{len(pae_files)} PAE NPZ)"
    )
    if not cif_files:
        raise RuntimeError(
            f"ESMFold2 completed without producing any mmCIF files in {out_dir}. "
            "Check the ESMFold2 log for errors."
        )

    return MaterializeResult(
        metadata={
            "predictions_dir": MetadataValue.path(str(out_dir)),
            "input_json_count": len(query_files),
            "query_chunk_size": chunk_size,
            "chunk_count": chunk_count,
            "cif_file_count": len(cif_files),
            "scores_json_count": len(scores_files),
            "confidence_json_count": len(confidence_files),
            "pae_npz_count": len(pae_files),
            "predict_script": MetadataValue.path(str(predict_script)),
            "num_loops": ef.num_loops,
            "num_sampling_steps": ef.num_sampling_steps,
            "num_diffusion_samples": ef.num_diffusion_samples,
            "model": ef.model,
            "seed": ef.seed,
            "docker_image": ef.docker_image,
            "hf_cache_dir": MetadataValue.path(hf_cache_str),
            "partition_root": MetadataValue.path(proot_str),
        }
    )


@asset(
    group_name="esmfold",
    partitions_def=design_configs,
    deps=[esmfold_predictions],
    description=(
        "Renumbers ESMFold target chains in ``esmfold/`` to match ``boltz2.target_pdb`` "
        "from the run-scoped ``pipeline_config.yaml``, writing ``esmfold_renumbered/``. "
        "Uses ``structure_filters.structure_docker_image``; gated by "
        "``esmfold.renumber_outputs`` (independent of ``structure_filters.enabled``)."
    ),
)
def esmfold_renumber(
    context: AssetExecutionContext,
) -> MaterializeResult:
    pc = _read_pipeline_config(partition_key=context.partition_key)
    proot = _partition_root(_run_outputs_dir(pc), context.partition_key, pc.run_id)
    ef = pc.esmfold
    input_dir = proot / "esmfold"
    renumbered_dir = proot / "esmfold_renumbered"
    target_fasta = _esmfold_target_fasta(pc)

    if not ef.renumber_outputs:
        context.log.info("[esmfold_renumber] esmfold.renumber_outputs=false — skipping")
        return _empty_branch_result(
            context,
            reason="esmfold.renumber_outputs=false",
            branch_status="skipped",
            predictions_dir=MetadataValue.path(str(input_dir)),
            renumbered_dir=MetadataValue.path(str(renumbered_dir)),
            renumbered_structure_file_count=0,
            structure_docker_image=pc.structure_filters.structure_docker_image,
        )

    cif_files = list(input_dir.glob("*.cif"))
    if not cif_files:
        context.log.warning(
            f"[esmfold_renumber] no CIFs in {input_dir}; materializing empty renumber"
        )
        return _empty_branch_result(
            context,
            reason=f"ESMFold predictions directory missing or empty: {input_dir}",
            predictions_dir=MetadataValue.path(str(input_dir)),
            renumbered_dir=MetadataValue.path(str(renumbered_dir)),
            renumbered_structure_file_count=0,
            structure_docker_image=pc.structure_filters.structure_docker_image,
        )

    target_pdb = _pipeline_target_pdb(pc)
    if not Path(target_fasta).is_file():
        raise FileNotFoundError(f"ESMFold target FASTA not found: {target_fasta}")

    renumbered_structure_count, mapping_path = _renumber_predicted_structures(
        context,
        pc,
        proot=proot,
        target_pdb=target_pdb,
        target_fasta=target_fasta,
        input_dir=input_dir,
        output_dir=renumbered_dir,
        log_label="esmfold_renumber",
    )

    return MaterializeResult(
        metadata={
            "predictions_dir": MetadataValue.path(str(input_dir)),
            "renumbered_dir": MetadataValue.path(str(renumbered_dir)),
            "renumbered_structure_file_count": renumbered_structure_count,
            "target_pdb": MetadataValue.path(target_pdb),
            "target_fasta": MetadataValue.path(target_fasta),
            "mapping_path": MetadataValue.path(str(mapping_path)) if mapping_path else "",
            "structure_docker_image": pc.structure_filters.structure_docker_image,
        }
    )


# ---------------------------------------------------------------------------
# final_scores group  (partitioned)
# ---------------------------------------------------------------------------

def _final_scores_boltz_subdir(proot: Path) -> str:
    if (proot / "boltz2_renumbered").is_dir():
        return "boltz2_renumbered"
    return "boltz2"


def _final_scores_esmfold_subdir(proot: Path) -> str:
    if (proot / "esmfold_renumbered").is_dir():
        return "esmfold_renumbered"
    return "esmfold"


@asset(
    group_name="final_scores",
    partitions_def=design_configs,
    description=(
        "Runs developability metrics for one selected design partition. "
        "Writes ``{partition}/final_scores/metrics.csv`` with parallel ``_boltz`` and "
        "``_esmfold`` metric columns. First screens ``specificity_hotspots`` (B68/B77); "
        "if every Boltz and ESMFold model for an MPNN sequence is 0, skips Rosetta / "
        "IPSAE / RMSD for that sequence and continues. Requires at least one of "
        "``boltz2_predictions`` or ``esmfold_predictions``; prefers ``*_renumbered/`` "
        "when present. Missing predictor columns are filled with placeholders."
    ),
)
def final_scores_metrics(context: AssetExecutionContext) -> MaterializeResult:
    pc = _read_pipeline_config(partition_key=context.partition_key)
    fs = pc.final_scores
    outputs_dir = Path(_run_outputs_dir(pc))
    proot = _partition_root(_run_outputs_dir(pc), context.partition_key, pc.run_id)
    boltz_subdir = _final_scores_boltz_subdir(proot)
    esmfold_subdir = _final_scores_esmfold_subdir(proot)
    has_boltz = (proot / boltz_subdir).is_dir()
    has_esmfold = (proot / esmfold_subdir).is_dir()
    if not has_boltz and not has_esmfold:
        raise Failure(
            description=(
                f"Partition {context.partition_key!r} has neither Boltz nor ESMFold outputs. "
                f"Expected at least one of {proot / boltz_subdir} or "
                f"{proot / esmfold_subdir}. Materialize boltz2_predictions and/or "
                "esmfold_predictions first."
            )
        )
    manifest = _read_manifest(str(outputs_dir))
    if context.partition_key not in manifest:
        raise Failure(
            description=(
                f"Partition {context.partition_key!r} not found in manifest for "
                f"{outputs_dir}."
            )
        )

    batch_script = _PROTEINDESIGN / "filters" / "final_scores" / "run_batch_metrics.py"
    cmd: List[str] = [
        sys.executable,
        str(batch_script),
        str(outputs_dir),
        "--docker-image",
        fs.docker_image,
        "--cpus",
        str(fs.cpus),
        "--boltz-subdir",
        boltz_subdir,
        "--esmfold-subdir",
        esmfold_subdir,
        "--ipsae-script",
        fs.ipsae_script,
        "--dalphaball",
        fs.dalphaball_path,
    ]
    if fs.skip_ipsae:
        cmd.append("--skip-ipsae")
    cmd.extend(["--partition-key", context.partition_key])

    _run(context, cmd)

    metrics_csv = proot / "final_scores" / "metrics.csv"

    return MaterializeResult(
        metadata={
            "outputs_dir": MetadataValue.path(str(outputs_dir)),
            "run_id": pc.run_id,
            "partition_key": context.partition_key,
            "partition_root": MetadataValue.path(str(proot)),
            "docker_image": fs.docker_image,
            "cpus": fs.cpus,
            "boltz_subdir": boltz_subdir,
            "esmfold_subdir": esmfold_subdir,
            "has_boltz_outputs": has_boltz,
            "has_esmfold_outputs": has_esmfold,
            "partition_count_manifest": len(manifest),
            "metrics_csv": MetadataValue.path(str(metrics_csv)),
            "metrics_csv_exists": metrics_csv.is_file(),
        }
    )


@asset(
    group_name="final_scores",
    partitions_def=design_configs,
    deps=[final_scores_metrics],
    description=(
        "Select MPNN sequences from ``final_scores/metrics.csv`` via specificity-hotspot "
        "consensus rules (Rules A/B/C), write Rule A/C metrics to "
        "``filtered_designs/filtered_metrics_rule_ac.csv``, Rule B metrics to "
        "``filtered_designs/filtered_metrics_rule_b.csv`` (plus combined "
        "``filtered_metrics.csv``), and write Rule A/C binder+target AF3 Server jobs to "
        "``filtered_designs/for_AF3.json``."
    ),
)
def filtered_designs_export(context: AssetExecutionContext) -> MaterializeResult:
    pc = _read_pipeline_config(partition_key=context.partition_key)
    fd = pc.filtered_designs
    proot = _partition_root(_run_outputs_dir(pc), context.partition_key, pc.run_id)
    metrics_csv = proot / "final_scores" / "metrics.csv"
    out_dir = proot / "filtered_designs"

    if not fd.enabled:
        context.log.info(
            f"[filtered_designs_export] partition={context.partition_key}  disabled in config"
        )
        return MaterializeResult(
            metadata={
                "partition_key": context.partition_key,
                "enabled": False,
                "output_dir": MetadataValue.path(str(out_dir)),
            }
        )

    if not metrics_csv.is_file():
        raise Failure(
            description=(
                f"Metrics CSV missing for partition {context.partition_key!r}: {metrics_csv}. "
                "Materialize final_scores_metrics first."
            )
        )

    # Repo-root package (filters/) — re-assert path in case a run worker dropped it.
    if str(_PROTEINDESIGN) not in sys.path:
        sys.path.insert(0, str(_PROTEINDESIGN))
    from filters.final_scores.filter_designs import FilterConfig, filter_partition_designs

    context.log.info(
        f"[filtered_designs_export] partition={context.partition_key}  "
        f"metrics={metrics_csv}  "
        f"specificity_hotspots_required={fd.specificity_hotspots_required}  "
        f"min_parent_successes={fd.min_parent_successes}"
    )

    summary = filter_partition_designs(
        proot,
        metrics_csv=metrics_csv,
        output_dir=out_dir,
        config=FilterConfig(
            specificity_hotspots_required=fd.specificity_hotspots_required,
            min_parent_successes=fd.min_parent_successes,
            min_seq_successes_fallback=fd.min_seq_successes_fallback,
            min_good_binder_scores=fd.min_good_binder_scores,
            max_binder_score=fd.max_binder_score,
            rule_b_min_each=fd.rule_b_min_each,
            rule_b_min_minor=fd.rule_b_min_minor,
            rule_b_min_major=fd.rule_b_min_major,
        ),
    )

    if summary["passed_sequences"] == 0:
        context.log.warning(
            f"No designs passed filters for partition {context.partition_key!r}. "
            f"See {summary['summary_path']}."
        )

    return MaterializeResult(
        metadata={
            "partition_key": context.partition_key,
            "partition_root": MetadataValue.path(str(proot)),
            "metrics_csv": MetadataValue.path(str(metrics_csv)),
            "output_dir": MetadataValue.path(str(out_dir)),
            "filtered_metrics_csv": MetadataValue.path(str(summary["filtered_metrics_csv"])),
            "filtered_metrics_rule_ac_csv": MetadataValue.path(
                str(summary["filtered_metrics_rule_ac_csv"])
            ),
            "filtered_metrics_rule_b_csv": MetadataValue.path(
                str(summary["filtered_metrics_rule_b_csv"])
            ),
            "af3_json": MetadataValue.path(str(summary["af3_json"])),
            "af3_sequence_count": summary["af3_sequence_count"],
            "summary_path": MetadataValue.path(str(summary["summary_path"])),
            "input_rows": summary["input_rows"],
            "passed_rows": summary["passed_rows"],
            "passed_rows_rule_ac": summary["passed_rows_rule_ac"],
            "passed_rows_rule_b": summary["passed_rows_rule_b"],
            "passed_sequences": summary["passed_sequences"],
            "passed_sequences_rule_ac": summary["passed_sequences_rule_ac"],
            "passed_sequences_rule_b": summary["passed_sequences_rule_b"],
            "failed_sequences": summary["failed_sequences"],
            "predictors": MetadataValue.text(",".join(summary["predictors"]) or "(none)"),
            "rule_counts": MetadataValue.json(summary["rule_counts"]),
            "partition_meta": MetadataValue.json(summary["partition_meta"]),
        }
    )


# ---------------------------------------------------------------------------
# Glycan–binder clashes (Rule B; parallel to LH off-target)
# ---------------------------------------------------------------------------

@asset(
    group_name="lh_offtarget_b",
    partitions_def=design_configs,
    deps=[filtered_designs_export],
    description=(
        "Align ``hCG_glycans.pdb`` onto each successful Rule B model's beta "
        "(chain B), write superimposed PDBs under "
        "``filtered_designs/glycan_aligned_rule_b/``, and score glycan-atom "
        "clashes with the binder (2.4 Å). Writes "
        "``filtered_designs/glycan_binder_clashes_rule_b.csv``."
    ),
)
def glycan_binder_clashes_rule_b(context: AssetExecutionContext) -> MaterializeResult:
    from dagster_pipeline.glycan_binder_clashes import process_rule_b_partition

    pc = _read_pipeline_config(partition_key=context.partition_key)
    proot = _partition_root(_run_outputs_dir(pc), context.partition_key, pc.run_id)
    metrics_csv = proot / "filtered_designs" / "filtered_metrics_rule_b.csv"
    if not metrics_csv.is_file():
        return _empty_branch_result(
            context,
            reason=f"missing {metrics_csv}",
            branch_status="no_candidates",
        )
    summary = process_rule_b_partition(proot)
    context.log.info(
        f"[glycan_binder_clashes_rule_b] partition={context.partition_key}  "
        f"rows={summary['row_count']}  by_method={summary['by_method']}  "
        f"csv={summary['clash_csv']}"
    )
    return MaterializeResult(
        metadata={
            "partition_key": context.partition_key,
            "clash_csv": MetadataValue.path(summary["clash_csv"]),
            "aligned_dir": MetadataValue.path(summary["aligned_dir"]),
            "row_count": summary["row_count"],
            "by_method": MetadataValue.json(summary["by_method"]),
            "glycans_pdb": MetadataValue.path(summary["glycans_pdb"]),
            "branch_status": _branch_status(summary["row_count"] > 0),
        }
    )


@asset(
    group_name="lh_offtarget_b",
    partitions_def=design_configs,
    deps=[filtered_designs_export],
    description=(
        "Score α-Asn52 glycan–binder clash fraction over MD-cluster GlycoSHIELD "
        "ensembles (``/storage/hCG/glycans/gs_runs``): mean clash over accepted GS "
        "conformers per MD cluster, then population-weighted mean across clusters. "
        "Writes ``filtered_designs/glycan_gs_ensemble_clashes_rule_b.csv``."
    ),
)
def glycan_gs_ensemble_clashes_rule_b(context: AssetExecutionContext) -> MaterializeResult:
    from dagster_pipeline.glycan_gs_ensemble_clashes import process_rule_b_partition

    pc = _read_pipeline_config(partition_key=context.partition_key)
    proot = _partition_root(_run_outputs_dir(pc), context.partition_key, pc.run_id)
    metrics_csv = proot / "filtered_designs" / "filtered_metrics_rule_b.csv"
    if not metrics_csv.is_file():
        return _empty_branch_result(
            context,
            reason=f"missing {metrics_csv}",
            branch_status="no_candidates",
        )
    summary = process_rule_b_partition(proot)
    context.log.info(
        f"[glycan_gs_ensemble_clashes_rule_b] partition={context.partition_key}  "
        f"rows={summary['row_count']}  by_method={summary['by_method']}  "
        f"clusters={summary['n_md_clusters_available']}  csv={summary['clash_csv']}"
    )
    return MaterializeResult(
        metadata={
            "partition_key": context.partition_key,
            "clash_csv": MetadataValue.path(summary["clash_csv"]),
            "row_count": summary["row_count"],
            "by_method": MetadataValue.json(summary["by_method"]),
            "gs_runs_dir": MetadataValue.path(summary["gs_runs_dir"]),
            "n_md_clusters_available": summary["n_md_clusters_available"],
            "branch_status": _branch_status(summary["row_count"] > 0),
        }
    )


# ---------------------------------------------------------------------------
# LH off-target group (Rule B sequences vs LH_alpha_beta.fasta)
# ---------------------------------------------------------------------------

def _lh_enabled(pc: PipelineAllToolsConfig) -> bool:
    return bool(getattr(pc, "lh_offtarget_b", None) and pc.lh_offtarget_b.enabled)


def _lh_target_fasta(pc: PipelineAllToolsConfig) -> str:
    return str(pc.lh_offtarget_b.target_fasta or "").strip()


def _lh_paths(proot: Path, dirname: str = "lh_offtarget_b") -> dict[str, Path]:
    root = proot / dirname
    return {
        "root": root,
        "seqs": root / "seqs",
        "msa": root / "boltz2_msas",
        "yamls": root / "combined_yamls",
        "yaml_chunks": root / "combined_yamls_chunks",
        "boltz2": root / "boltz2",
        "esmfold_inputs": root / "esmfold_inputs",
        "esmfold_chunks": root / "esmfold_input_chunks",
        "esmfold": root / "esmfold",
    }


def _lh_ac_enabled(pc: PipelineAllToolsConfig) -> bool:
    return bool(getattr(pc, "lh_offtarget_ac", None) and pc.lh_offtarget_ac.enabled)


def _lh_ac_target_fasta(pc: PipelineAllToolsConfig) -> str:
    return str(pc.lh_offtarget_ac.target_fasta or "").strip()


def _lh_paths_ac(proot: Path) -> dict[str, Path]:
    return _lh_paths(proot, "lh_offtarget_ac")


@asset(
    group_name="lh_offtarget_b",
    partitions_def=design_configs,
    deps=[filtered_designs_export],
    output_required=False,
    description=(
        "Stage unique Rule B ``mpnn_seq_id`` FASTAs under ``lh_offtarget_b/seqs/`` "
        "for LH off-target Boltz-2 / ESMFold prediction."
    ),
)
def lh_rule_b_inputs(context: AssetExecutionContext):
    pc = _read_pipeline_config(partition_key=context.partition_key)
    proot = _partition_root(_run_outputs_dir(pc), context.partition_key, pc.run_id)
    paths = _lh_paths(proot)
    rule_b_csv = proot / "filtered_designs" / "filtered_metrics_rule_b.csv"
    seqs_filtered = proot / "proteinmpnn" / "seqs_filtered"

    if not _lh_enabled(pc):
        context.log.info("[lh_rule_b_inputs] lh_offtarget_b.enabled=false — skipping")
        yield _empty_branch_output(
            context,
            reason="lh_offtarget_b.enabled=false",
            branch_status="skipped",
            value={"sequence_count": 0},
            sequence_count=0,
        )
        return

    if str(_PROTEINDESIGN) not in sys.path:
        sys.path.insert(0, str(_PROTEINDESIGN))
    from filters.final_scores.lh_binding import stage_rule_b_fastas

    staged = stage_rule_b_fastas(
        rule_b_csv=rule_b_csv,
        seqs_filtered_dir=seqs_filtered,
        output_seqs_dir=paths["seqs"],
    )
    if not staged:
        context.log.warning(
            f"[lh_rule_b_inputs] partition={context.partition_key}  "
            "no Rule B sequences; materializing empty LH branch"
        )
        yield _empty_branch_output(
            context,
            reason="no Rule B sequences in filtered_metrics_rule_b.csv",
            value={"sequence_count": 0},
            sequence_count=0,
            rule_b_csv=MetadataValue.path(str(rule_b_csv)),
            seqs_dir=MetadataValue.path(str(paths["seqs"])),
        )
        return

    context.log.info(
        f"[lh_rule_b_inputs] partition={context.partition_key}  "
        f"staged={len(staged)}  dir={paths['seqs']}"
    )
    yield Output(
        {
            "partition_key": context.partition_key,
            "seqs_dir": str(paths["seqs"]),
            "sequence_count": len(staged),
        },
        metadata={
            "seqs_dir": MetadataValue.path(str(paths["seqs"])),
            "sequence_count": len(staged),
            "rule_b_csv": MetadataValue.path(str(rule_b_csv)),
            "target_fasta": MetadataValue.path(_lh_target_fasta(pc)),
            "branch_status": "has_candidates",
            "sample_ids": MetadataValue.text(
                "\n".join(s.mpnn_seq_id for s in staged[:20])
            ),
        },
    )


@asset(
    group_name="lh_offtarget_b",
    partitions_def=design_configs,
    deps=[lh_rule_b_inputs],
    output_required=False,
    description=(
        "Precompute Boltz-2 MSAs for Rule B binders complexed with "
        "``lh_offtarget_b.target_fasta`` (LH)."
    ),
)
def lh_MSA(context: AssetExecutionContext):
    pc = _read_pipeline_config(partition_key=context.partition_key)
    proot = _partition_root(_run_outputs_dir(pc), context.partition_key, pc.run_id)
    paths = _lh_paths(proot)
    seq_dir = paths["seqs"]
    msa_dir = paths["msa"]
    msa_config = pc.msa
    target_path = Path(_lh_target_fasta(pc))

    if not _lh_enabled(pc):
        yield _empty_branch_output(
            context,
            reason="lh_offtarget_b.enabled=false",
            branch_status="skipped",
            value={"complex_count": 0, "chain_count": 0},
            complex_count=0,
            chain_count=0,
        )
        return

    if not target_path.is_file():
        raise Failure(description=f"LH target FASTA not found: {target_path}")

    target_raw = read_fasta_sequence_flat(target_path)
    if not split_target_segments(target_raw):
        raise Failure(description=f"LH target FASTA has no sequences: {target_path}")

    if str(_PROTEINDESIGN) not in sys.path:
        sys.path.insert(0, str(_PROTEINDESIGN))
    from filters.final_scores.lh_binding import iter_lh_binder_target

    exts = tuple(e if e.startswith(".") else f".{e}" for e in pc.boltz2.fasta_extensions)
    files = (
        sorted(p for p in seq_dir.iterdir() if p.is_file() and p.suffix.lower() in exts)
        if seq_dir.is_dir()
        else []
    )
    if not files:
        yield _empty_branch_output(
            context,
            reason=f"no staged Rule B FASTAs in {seq_dir}",
            value={"complex_count": 0, "chain_count": 0},
            complex_count=0,
            chain_count=0,
        )
        return

    complexes: list[ComplexSpec] = []
    for src in files:
        for base, binder, target in iter_lh_binder_target(src, target_raw):
            segments = split_target_segments(target)
            chains = [("A", binder)]
            chains.extend(
                (chr(ord("B") + index), sequence)
                for index, sequence in enumerate(segments)
            )
            complexes.append(ComplexSpec(name=base, chains=tuple(chains)))

    if not complexes:
        yield _empty_branch_output(
            context,
            reason=f"no binder records in {seq_dir}",
            value={"complex_count": 0, "chain_count": 0},
            complex_count=0,
            chain_count=0,
        )
        return

    if msa_dir.exists():
        shutil.rmtree(msa_dir)
    client = MsaServerClient(
        msa_config.server_url,
        timeout_seconds=msa_config.request_timeout_seconds,
        max_retries=msa_config.max_retries,
        poll_interval_seconds=msa_config.poll_interval_seconds,
        retry_backoff_seconds=msa_config.retry_backoff_seconds,
    )
    paired_mode = f"pair{msa_config.pairing_strategy}"
    if msa_config.use_env:
        paired_mode += "-env"
    unpaired_mode = "env" if msa_config.use_env else "all"

    context.log.info(
        f"[lh_MSA] partition={context.partition_key} complexes={len(complexes)} "
        f"target={target_path} server={msa_config.server_url}"
    )
    unique_sequences = list(
        dict.fromkeys(sequence for item in complexes for _, sequence in item.chains)
    )
    msa_total = len(unique_sequences) + len(complexes)

    class _TqdmStdout:
        def write(self, s: str) -> int:
            if not s:
                return 0
            text = s.replace("\r", "\n").rstrip("\n")
            if text:
                print(text, flush=True)
            return len(s)

        def flush(self) -> None:
            sys.stdout.flush()

    with tqdm(
        total=msa_total,
        desc=f"LH MSA {context.partition_key}",
        unit="msa",
        file=_TqdmStdout(),
        dynamic_ncols=True,
        mininterval=0.5,
        ascii=True,
    ) as pbar:
        manifest = build_boltz_msa_bundle(
            complexes,
            output_dir=msa_dir,
            cache_root=Path(DEFAULT_MSA_CACHE_DIR),
            client=client,
            unpaired_mode=unpaired_mode,
            paired_mode=paired_mode,
            use_env=msa_config.use_env,
            max_paired_seqs=msa_config.max_paired_seqs,
            max_msa_seqs=msa_config.max_msa_seqs,
            on_msa_done=pbar.update,
        )
    manifest_complexes = manifest["complexes"]
    chain_count = sum(len(item["chains"]) for item in manifest_complexes.values())
    yield Output(
        {
            "partition_key": context.partition_key,
            "msa_dir": str(msa_dir),
            "complex_count": len(complexes),
            "chain_count": chain_count,
        },
        metadata={
            "msa_dir": MetadataValue.path(str(msa_dir)),
            "manifest_path": MetadataValue.path(str(msa_dir / "manifest.json")),
            "complex_count": len(complexes),
            "chain_count": chain_count,
            "target_fasta": MetadataValue.path(str(target_path)),
            "branch_status": "has_candidates",
        },
    )


@asset(
    group_name="lh_offtarget_b",
    partitions_def=design_configs,
    deps=[lh_MSA],
    output_required=False,
    description="Write Boltz-2 predict YAMLs for Rule B × LH complexes.",
)
def lh_boltz2_input_yamls(context: AssetExecutionContext):
    pc = _read_pipeline_config(partition_key=context.partition_key)
    proot = _partition_root(_run_outputs_dir(pc), context.partition_key, pc.run_id)
    paths = _lh_paths(proot)
    seq_dir = paths["seqs"]
    yaml_dir = paths["yamls"]
    msa_dir = paths["msa"]
    bz = pc.boltz2
    target_path = Path(_lh_target_fasta(pc))
    template_path = Path(bz.template_yaml)

    if not _lh_enabled(pc):
        yield _empty_branch_output(
            context,
            reason="lh_offtarget_b.enabled=false",
            branch_status="skipped",
            value={"yaml_files_written": 0},
            yaml_files_written=0,
        )
        return

    append_seq = read_fasta_sequence_flat(target_path)
    if not append_seq:
        raise Failure(description=f"LH target FASTA empty: {target_path}")
    if not template_path.is_file():
        raise Failure(description=f"Boltz-2 template YAML not found: {template_path}")
    template_text = template_path.read_text(encoding="utf-8")

    if str(_PROTEINDESIGN) not in sys.path:
        sys.path.insert(0, str(_PROTEINDESIGN))
    from filters.final_scores.lh_binding import iter_lh_binder_target

    exts = tuple(e if e.startswith(".") else f".{e}" for e in bz.fasta_extensions)
    files = (
        sorted(p for p in seq_dir.iterdir() if p.is_file() and p.suffix.lower() in exts)
        if seq_dir.is_dir()
        else []
    )
    if not files:
        yield _empty_branch_output(
            context,
            reason=f"no staged FASTAs in {seq_dir}",
            value={"yaml_files_written": 0},
            yaml_files_written=0,
        )
        return

    msa_manifest_path = msa_dir / "manifest.json"
    if not msa_manifest_path.is_file():
        raise Failure(description=f"LH MSA manifest missing: {msa_manifest_path}")
    msa_manifest = json.loads(msa_manifest_path.read_text(encoding="utf-8"))
    manifest_complexes = msa_manifest.get("complexes")
    if not isinstance(manifest_complexes, dict):
        raise Failure(description=f"Invalid LH MSA manifest: {msa_manifest_path}")

    if yaml_dir.exists():
        shutil.rmtree(yaml_dir)
    yaml_dir.mkdir(parents=True, exist_ok=True)
    written = 0
    for src in files:
        for base, binder, target in iter_lh_binder_target(src, append_seq):
            complex_payload = manifest_complexes.get(base)
            if not isinstance(complex_payload, dict) or not isinstance(
                complex_payload.get("chains"), list
            ):
                raise Failure(description=f"LH MSA manifest has no complex for {base!r}.")
            expected_chains = [("A", binder)]
            expected_chains.extend(
                (chr(ord("B") + index), sequence)
                for index, sequence in enumerate(split_target_segments(target))
            )
            chain_payloads = complex_payload["chains"]
            actual = [
                (str(chain.get("id")), str(chain.get("sequence")))
                for chain in chain_payloads
                if isinstance(chain, dict)
            ]
            if actual != expected_chains:
                raise Failure(
                    description=(
                        f"LH MSA chain mismatch for {base!r}: "
                        f"expected {expected_chains}, got {actual}."
                    )
                )
            msa_paths: dict[str, str] = {}
            for chain in chain_payloads:
                chain_id = str(chain["id"])
                host_path = Path(str(chain["csv_path"]))
                if not host_path.is_file():
                    raise Failure(
                        description=f"LH MSA CSV missing for {base}/{chain_id}: {host_path}"
                    )
                try:
                    relative_path = host_path.relative_to(proot)
                except ValueError as exc:
                    raise Failure(
                        description=f"LH MSA CSV outside partition root: {host_path}"
                    ) from exc
                msa_paths[chain_id] = f"/work/{relative_path.as_posix()}"
            write_yaml_for_pair(
                template_text,
                yaml_dir,
                base,
                binder,
                target,
                msa_paths=msa_paths,
            )
            written += 1

    yaml_files = sorted(yaml_dir.glob("*.yaml"))
    yield Output(
        {
            "partition_key": context.partition_key,
            "combined_yamls_dir": str(yaml_dir),
            "yaml_files_written": written,
        },
        metadata={
            "combined_yamls_dir": MetadataValue.path(str(yaml_dir)),
            "yaml_files_written": written,
            "branch_status": "has_candidates",
            "target_fasta": MetadataValue.path(str(target_path)),
            "sample_files": MetadataValue.text("\n".join(f.name for f in yaml_files[:10])),
        },
    )


@asset(
    group_name="lh_offtarget_b",
    partitions_def=design_configs,
    deps=[lh_boltz2_input_yamls],
    description=(
        "Boltz-2 predict for Rule B × LH into ``lh_offtarget_b/boltz2/`` "
        "(same sampling settings as ``boltz2``; no renumber)."
    ),
)
def lh_boltz2_predictions(context: AssetExecutionContext) -> MaterializeResult:
    pc = _read_pipeline_config(partition_key=context.partition_key)
    proot = _partition_root(_run_outputs_dir(pc), context.partition_key, pc.run_id)
    paths = _lh_paths(proot)
    bz = pc.boltz2
    yaml_dir = paths["yamls"]
    out_dir = paths["boltz2"]
    out_dir.mkdir(parents=True, exist_ok=True)

    if not _lh_enabled(pc):
        return _empty_branch_result(
            context,
            reason="lh_offtarget_b.enabled=false",
            branch_status="skipped",
            predictions_dir=MetadataValue.path(str(out_dir)),
            input_yaml_count=0,
            structure_file_count=0,
        )

    yaml_files = sorted(yaml_dir.glob("*.yaml")) if yaml_dir.is_dir() else []
    if not yaml_files:
        return _empty_branch_result(
            context,
            reason=f"no LH Boltz YAMLs in {yaml_dir}",
            predictions_dir=MetadataValue.path(str(out_dir)),
            input_yaml_count=0,
            structure_file_count=0,
        )

    chunk_size = bz.query_chunk_size if bz.query_chunk_size > 0 else len(yaml_files)
    chunk_root = paths["yaml_chunks"]
    if chunk_root.exists():
        shutil.rmtree(chunk_root)
    chunk_root.mkdir(parents=True, exist_ok=True)

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
            os.symlink(os.path.relpath(src, start=chunk_dir), dst)

        boltz_devices = max(1, bz.devices)
        boltz_gpus = _select_gpu_ids(pc.gpus, boltz_devices)
        cmd = [
            "docker", "run", "--rm", *_docker_user_args(),
            "--runtime=nvidia",
            "--ipc=host",
            *_docker_gpu_args(boltz_gpus),
            "-e",
            "LD_LIBRARY_PATH=/usr/local/lib/python3.11/dist-packages/nvidia/cu13/lib:/usr/local/nvidia/lib:/usr/local/nvidia/lib64",
            "-v", f"{proot}:/work:rw",
            "-v", f"{DEFAULT_BOLTZ2_CACHE_VOLUME}:/cache:rw",
            bz.docker_image,
            "predict",
            f"/work/lh_offtarget_b/combined_yamls_chunks/chunk_{chunk_count:04d}",
            "--cache", "/cache",
            "--out_dir", "/work/lh_offtarget_b/boltz2",
            "--recycling_steps", str(bz.recycling_steps),
            "--devices", str(boltz_devices),
            "--diffusion_samples", str(bz.diffusion_samples),
            "--num_workers", str(bz.num_workers),
            "--preprocessing-threads", str(bz.preprocessing_threads),
        ]
        if bz.use_potentials:
            cmd.append("--use_potentials")
        cmd.extend(["--override", "--no_kernels"])
        _run(context, cmd)

    structure_files = list(out_dir.rglob("*.cif")) or list(out_dir.rglob("*.pdb"))
    if not structure_files:
        raise RuntimeError(
            f"LH Boltz-2 completed without structures in {out_dir}. Check Boltz-2 logs."
        )
    return MaterializeResult(
        metadata={
            "predictions_dir": MetadataValue.path(str(out_dir)),
            "input_yaml_count": len(yaml_files),
            "chunk_count": chunk_count,
            "structure_file_count": len(structure_files),
            "diffusion_samples": bz.diffusion_samples,
            "target_fasta": MetadataValue.path(_lh_target_fasta(pc)),
        }
    )


@asset(
    group_name="lh_offtarget_b",
    partitions_def=design_configs,
    deps=[lh_rule_b_inputs],
    output_required=False,
    description="ESMFold2 JSON inputs for Rule B binders × LH target.",
)
def lh_esmfold_input_jsons(context: AssetExecutionContext):
    pc = _read_pipeline_config(partition_key=context.partition_key)
    proot = _partition_root(_run_outputs_dir(pc), context.partition_key, pc.run_id)
    paths = _lh_paths(proot)
    seq_dir = paths["seqs"]
    json_dir = paths["esmfold_inputs"]
    target_path = Path(_lh_target_fasta(pc))

    if not _lh_enabled(pc):
        yield _empty_branch_output(
            context,
            reason="lh_offtarget_b.enabled=false",
            branch_status="skipped",
            value={"files_written": 0},
            files_written=0,
        )
        return

    append_seq = read_fasta_sequence_flat(target_path)
    if not append_seq:
        raise Failure(description=f"LH target FASTA empty: {target_path}")

    if str(_PROTEINDESIGN) not in sys.path:
        sys.path.insert(0, str(_PROTEINDESIGN))
    from filters.final_scores.lh_binding import iter_lh_binder_target

    exts = tuple(e if e.startswith(".") else f".{e}" for e in pc.boltz2.fasta_extensions)
    files = (
        sorted(p for p in seq_dir.iterdir() if p.is_file() and p.suffix.lower() in exts)
        if seq_dir.is_dir()
        else []
    )
    if not files:
        yield _empty_branch_output(
            context,
            reason=f"no staged FASTAs in {seq_dir}",
            value={"files_written": 0},
            files_written=0,
        )
        return

    if json_dir.exists():
        shutil.rmtree(json_dir)
    json_dir.mkdir(parents=True, exist_ok=True)
    written = 0
    for src in files:
        for base, binder, target in iter_lh_binder_target(src, append_seq):
            write_json_for_pair(json_dir, base, binder, target)
            written += 1

    yield Output(
        {
            "partition_key": context.partition_key,
            "esmfold_inputs_dir": str(json_dir),
            "files_written": written,
        },
        metadata={
            "esmfold_inputs_dir": MetadataValue.path(str(json_dir)),
            "files_written": written,
            "branch_status": "has_candidates",
            "target_fasta": MetadataValue.path(str(target_path)),
        },
    )


@asset(
    group_name="lh_offtarget_b",
    partitions_def=design_configs,
    deps=[lh_esmfold_input_jsons],
    description=(
        "ESMFold2 predict for Rule B × LH into ``lh_offtarget_b/esmfold/`` "
        "(same sampling settings as ``esmfold``; no renumber)."
    ),
)
def lh_esmfold_predictions(context: AssetExecutionContext) -> MaterializeResult:
    pc = _read_pipeline_config(partition_key=context.partition_key)
    proot = _partition_root(_run_outputs_dir(pc), context.partition_key, pc.run_id)
    paths = _lh_paths(proot)
    ef = pc.esmfold
    json_dir = paths["esmfold_inputs"]
    out_dir = paths["esmfold"]
    out_dir.mkdir(parents=True, exist_ok=True)

    if not _lh_enabled(pc):
        return _empty_branch_result(
            context,
            reason="lh_offtarget_b.enabled=false",
            branch_status="skipped",
            predictions_dir=MetadataValue.path(str(out_dir)),
            input_json_count=0,
            cif_file_count=0,
        )

    query_files = sorted(json_dir.glob("*.json")) if json_dir.is_dir() else []
    if not query_files:
        return _empty_branch_result(
            context,
            reason=f"no LH ESMFold JSONs in {json_dir}",
            predictions_dir=MetadataValue.path(str(out_dir)),
            input_json_count=0,
            cif_file_count=0,
        )

    chunk_size = ef.query_chunk_size if ef.query_chunk_size > 0 else len(query_files)
    chunk_root = paths["esmfold_chunks"]
    if chunk_root.exists():
        shutil.rmtree(chunk_root)
    chunk_root.mkdir(parents=True, exist_ok=True)

    proot_str = str(proot.resolve())
    predict_script = _esmfold_predict_script()
    esmfold2_dir_str = str(predict_script.parent)
    hf_cache = _esmfold_hf_cache_dir()
    hf_cache.mkdir(parents=True, exist_ok=True)
    hf_cache_str = str(hf_cache)

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
            os.symlink(os.path.relpath(src, start=chunk_dir), dst)

        esmfold_gpus = _select_gpu_ids(pc.gpus, 1)
        chunk_input = f"{proot_str}/lh_offtarget_b/esmfold_input_chunks/chunk_{chunk_count:04d}"
        cmd: List[str] = [
            "docker", "run", "--rm", *_docker_user_args(),
            "--runtime=nvidia",
            *_docker_gpu_args(esmfold_gpus),
            "-v", f"{proot_str}:{proot_str}:rw",
            "-v", f"{esmfold2_dir_str}:{esmfold2_dir_str}:ro",
            "-v", f"{hf_cache_str}:/cache/huggingface",
            "-e", "HF_HOME=/cache/huggingface",
            "-e", "TRANSFORMERS_CACHE=/cache/huggingface",
            "-e", "HUGGINGFACE_HUB_CACHE=/cache/huggingface",
            ef.docker_image,
            "python", str(predict_script),
            "-i", chunk_input,
            "-o", f"{proot_str}/lh_offtarget_b/esmfold",
            "--model", ef.model,
            "--num-loops", str(ef.num_loops),
            "--num-sampling-steps", str(ef.num_sampling_steps),
            "--num-diffusion-samples", str(ef.num_diffusion_samples),
            "--seed", str(ef.seed),
            "--device", "cuda",
        ]
        _run(context, cmd)

    cif_files = list(out_dir.glob("*.cif")) + list(out_dir.rglob("*_model_*.cif"))
    # de-dupe
    cif_files = sorted({p.resolve() for p in cif_files})
    if not cif_files:
        raise RuntimeError(
            f"LH ESMFold completed without CIF files in {out_dir}. Check ESMFold logs."
        )
    return MaterializeResult(
        metadata={
            "predictions_dir": MetadataValue.path(str(out_dir)),
            "input_json_count": len(query_files),
            "chunk_count": chunk_count,
            "cif_file_count": len(cif_files),
            "num_diffusion_samples": ef.num_diffusion_samples,
            "target_fasta": MetadataValue.path(_lh_target_fasta(pc)),
        }
    )


@asset(
    group_name="lh_offtarget_b",
    partitions_def=design_configs,
    deps=[lh_boltz2_predictions, lh_esmfold_predictions],
    description=(
        "Score LH Boltz-2 / ESMFold predictions with specificity_hotspots (B68/B77) "
        "inside the ``final_scores`` Docker image and write "
        "``filtered_designs/LH_binding.csv``."
    ),
)
def lh_binding_scores(context: AssetExecutionContext) -> MaterializeResult:
    pc = _read_pipeline_config(partition_key=context.partition_key)
    proot = _partition_root(_run_outputs_dir(pc), context.partition_key, pc.run_id)
    out_csv = proot / "filtered_designs" / "LH_binding.csv"
    fs = pc.final_scores

    if not _lh_enabled(pc):
        out_csv.parent.mkdir(parents=True, exist_ok=True)
        out_csv.write_text(
            "mpnn_seq_id,model_index,binder_seq,specificity_hotspots_boltz,"
            "specificity_hotspots_esmfold,binder_aas_contact_B77_boltz,"
            "binder_aas_contact_B77_esmfold,structure_pdb_boltz,structure_pdb_esmfold\n",
            encoding="utf-8",
        )
        return _empty_branch_result(
            context,
            reason="lh_offtarget_b.enabled=false",
            branch_status="skipped",
            lh_binding_csv=MetadataValue.path(str(out_csv)),
            row_count=0,
        )

    # Score inside final_scores Docker (numpy / Biopython / scipy). Host Dagster
    # envs often lack these packages — do not import residue_contacts on the host.
    final_scores_dir = _PROTEINDESIGN / "filters" / "final_scores"
    script = final_scores_dir / "run_lh_binding.py"
    if not script.is_file():
        raise Failure(description=f"LH binding scorer missing: {script}")

    out_csv.parent.mkdir(parents=True, exist_ok=True)
    cmd: List[str] = [
        "docker",
        "run",
        "--rm",
        *_docker_user_args(),
        "-v",
        f"{final_scores_dir.resolve()}:{final_scores_dir.resolve()}",
        "-v",
        f"{proot.resolve()}:{proot.resolve()}",
        fs.docker_image,
        "python",
        str(script.resolve()),
        str(proot.resolve()),
        "--output-csv",
        str(out_csv.resolve()),
    ]
    _run(context, cmd)

    if not out_csv.is_file():
        raise Failure(description=f"LH_binding.csv was not written: {out_csv}")

    # Lightweight host-side summary (stdlib only) for Dagster metadata.
    rows = list(csv.DictReader(out_csv.open(newline="", encoding="utf-8")))
    rule_b_csv = proot / "filtered_designs" / "filtered_metrics_rule_b.csv"
    rule_b_ids: set[str] = set()
    if rule_b_csv.is_file():
        with rule_b_csv.open(newline="", encoding="utf-8") as fh:
            rule_b_ids = {
                str(r.get("mpnn_seq_id") or "").strip()
                for r in csv.DictReader(fh)
                if str(r.get("mpnn_seq_id") or "").strip()
            }
    context.log.info(
        f"[lh_binding_scores] partition={context.partition_key}  "
        f"rows={len(rows)}  csv={out_csv}  docker_image={fs.docker_image}"
    )
    return MaterializeResult(
        metadata={
            "lh_binding_csv": MetadataValue.path(str(out_csv)),
            "row_count": len(rows),
            "rule_b_sequence_count": len(rule_b_ids),
            "boltz_model_count": sum(1 for r in rows if r.get("structure_pdb_boltz")),
            "esmfold_model_count": sum(1 for r in rows if r.get("structure_pdb_esmfold")),
            "docker_image": fs.docker_image,
            "target_fasta": MetadataValue.path(_lh_target_fasta(pc)),
        }
    )


@asset(
    group_name="lh_offtarget_b",
    partitions_def=design_configs,
    deps=[lh_binding_scores, glycan_gs_ensemble_clashes_rule_b, glycan_binder_clashes_rule_b],
    description=(
        "Build a run-level Rule B Marp presentation (``deck_rule_b.md`` + "
        "``deck_rule_b.pdf`` + ``deck_rule_b.pptx``) after LH specificity scoring "
        "and MD/GlycoSHIELD ensemble glycan–binder clash scoring. "
        "Writes ``{run_id}/rule_b/`` using every partition under the run that has "
        "``filtered_metrics_rule_b.csv``."
    ),
)
def rule_b_presentation(context: AssetExecutionContext) -> MaterializeResult:
    pc = _read_pipeline_config(partition_key=context.partition_key)
    run_dir = Path(_run_outputs_dir(pc))
    cfg = pc.rule_b_presentation
    out_dir = run_dir / "rule_b"

    if not cfg.enabled:
        return _empty_branch_result(
            context,
            reason="rule_b_presentation.enabled=false",
            branch_status="skipped",
            output_dir=MetadataValue.path(str(out_dir)),
        )

    run_dirs = [
        child
        for child in sorted(run_dir.iterdir())
        if child.is_dir()
        and (child / "filtered_designs" / "filtered_metrics_rule_b.csv").is_file()
    ]
    if not run_dirs:
        return _empty_branch_result(
            context,
            reason=f"no filtered_metrics_rule_b.csv under {run_dir}",
            branch_status="no_candidates",
            output_dir=MetadataValue.path(str(out_dir)),
        )

    from dagster_pipeline.rule_b_presentation import build_rule_b_presentation

    context.log.info(
        f"[rule_b_presentation] partition={context.partition_key}  "
        f"run_dirs={len(run_dirs)}  out={out_dir}  "
        f"pymol={cfg.pymol}  marp_image={cfg.marp_docker_image}"
    )
    summary = build_rule_b_presentation(
        run_dirs,
        out_dir,
        seed=cfg.seed,
        pymol=Path(cfg.pymol),
        scaffolds_dir=Path(cfg.scaffolds_dir),
        marp_docker_image=cfg.marp_docker_image,
        export_pdf=cfg.export_pdf,
        export_pptx=getattr(cfg, "export_pptx", True),
        export_html=False,
    )
    context.log.info(
        f"[rule_b_presentation] slides={summary['slide_count']}  "
        f"md={summary['deck_md']}  pdf={summary.get('deck_pdf')}  "
        f"pptx={summary.get('deck_pptx')}"
    )
    meta: dict[str, Any] = {
        "output_dir": MetadataValue.path(str(out_dir)),
        "slide_count": summary["slide_count"],
        "partition_count": len(run_dirs),
        "deck_md": MetadataValue.path(str(summary["deck_md"])),
        "assets_dir": MetadataValue.path(str(summary["assets_dir"])),
        "export_pdf": cfg.export_pdf,
        "export_pptx": getattr(cfg, "export_pptx", True),
        "marp_docker_image": cfg.marp_docker_image,
    }
    if summary.get("deck_pdf"):
        meta["deck_pdf"] = MetadataValue.path(str(summary["deck_pdf"]))
    if summary.get("deck_pptx"):
        meta["deck_pptx"] = MetadataValue.path(str(summary["deck_pptx"]))
    if summary["slide_count"] == 0:
        meta["branch_status"] = "no_candidates"
        meta["skip_reason"] = "no Rule B sequences with ≥1 successful Boltz and ESMFold"
    return MaterializeResult(metadata=meta)


@asset(
    group_name="lh_offtarget_b",
    partitions_def=design_configs,
    deps=[rule_b_presentation],
    description=(
        "Zip every Rule B design that appears in the Marp deck (≥1 successful "
        "Boltz and ESMFold on hCG). Writes ``{run_id}/rule_b/successful_designs.zip`` "
        "with per-sequence FASTA, hCG metrics CSV, and one superimposed PDB per "
        "Boltz / ESMFold model (prediction A/B/C + ``hCG_glycans.pdb`` as X/Y)."
    ),
)
def rule_b_successful_designs_zip(context: AssetExecutionContext) -> MaterializeResult:
    from dagster_pipeline.successful_designs_export import build_rule_b_successful_zip

    pc = _read_pipeline_config(partition_key=context.partition_key)
    run_dir = Path(_run_outputs_dir(pc))
    out_zip = run_dir / "rule_b" / "successful_designs.zip"

    run_dirs = [
        child
        for child in sorted(run_dir.iterdir())
        if child.is_dir()
        and (child / "filtered_designs" / "filtered_metrics_rule_b.csv").is_file()
    ]
    if not run_dirs:
        return _empty_branch_result(
            context,
            reason=f"no filtered_metrics_rule_b.csv under {run_dir}",
            branch_status="no_candidates",
            zip_path=MetadataValue.path(str(out_zip)),
        )

    summary = build_rule_b_successful_zip(run_dirs, out_zip)
    context.log.info(
        f"[rule_b_successful_designs_zip] sequences={summary['sequence_count']}  "
        f"partitions={summary['partition_count']}  zip={summary['zip_path'] or out_zip}  "
        f"glycans={summary.get('glycans_pdb')}"
    )
    if summary["sequence_count"] == 0:
        return _empty_branch_result(
            context,
            reason="no Rule B presentation sequences (≥1 successful Boltz and ESMFold)",
            branch_status="no_candidates",
            zip_path=MetadataValue.path(str(out_zip)),
            sequence_count=0,
            partition_count=0,
        )
    return MaterializeResult(
        metadata={
            "zip_path": MetadataValue.path(summary["zip_path"]),
            "sequence_count": summary["sequence_count"],
            "partition_count": summary["partition_count"],
            "glycans_pdb": MetadataValue.path(summary["glycans_pdb"]),
            "track": "rule_b",
        }
    )


# ---------------------------------------------------------------------------
# colabfold group  (partitioned)  [commented out — not registered in all_assets]
# ---------------------------------------------------------------------------

@asset(
    group_name="colabfold",
    partitions_def=design_configs,
    deps=[proteinmpnn_soluprot_filter],
    output_required=False,
    description=(
        "Builds ColabFold input FASTAs when SoluProt filtering left sequences in "
        "``seqs_filtered/``. When no sequences passed, materializes "
        "``branch_status=no_candidates``."
    ),
)
def colabfold_input_fastas(context: AssetExecutionContext):
    pc = _read_pipeline_config(partition_key=context.partition_key)
    proot = _partition_root(_run_outputs_dir(pc), context.partition_key, pc.run_id)
    seq_dir = proot / "proteinmpnn" / "seqs_filtered"
    combined_dir = proot / "proteinmpnn" / "combined_seqs"
    bz = pc.boltz2
    append_path = Path(bz.target_fasta)

    append_seq = read_fasta_sequence_flat(append_path)
    if not append_seq:
        raise Failure(
            description=f"ColabFold target FASTA has no sequence residues: {append_path}"
        )

    exts = tuple(e if e.startswith(".") else f".{e}" for e in bz.fasta_extensions)
    if not seq_dir.is_dir():
        context.log.warning(
            f"[colabfold_input_fastas] partition={context.partition_key}  "
            f"missing {seq_dir}; materializing empty ColabFold input branch"
        )
        yield _empty_branch_output(
            context,
            reason=f"SoluProt filtered sequence directory missing: {seq_dir}",
            value={"source_fasta_count": 0, "files_written": 0},
            source_fastas=0,
            files_written=0,
        )
        return
    files = sorted(p for p in seq_dir.iterdir() if p.is_file() and p.suffix.lower() in exts)
    if not files:
        context.log.warning(
            f"[colabfold_input_fastas] partition={context.partition_key}  "
            f"no FASTAs in {seq_dir}; materializing empty ColabFold input branch"
        )
        yield _empty_branch_output(
            context,
            reason=f"no FASTAs in {seq_dir}",
            value={"source_fasta_count": 0, "files_written": 0},
            source_fastas=0,
            files_written=0,
            seq_dir=MetadataValue.path(str(seq_dir)),
        )
        return

    combined_dir.mkdir(parents=True, exist_ok=True)
    written = 0
    for src in files:
        n = explode_fasta_records_with_append(src, combined_dir, append_seq)
        written += n
        context.log.info(f"Merged {src.name} into {n} single-record FASTA(s)")

    yield Output(
        {
            "partition_key": context.partition_key,
            "combined_dir": str(combined_dir),
            "source_fasta_count": len(files),
            "files_written": written,
        },
        metadata={
            "combined_dir": MetadataValue.path(str(combined_dir)),
            "source_fastas": len(files),
            "files_written": written,
            "branch_status": "has_candidates",
            "target_fasta": MetadataValue.path(bz.target_fasta),
            "file_list": MetadataValue.text("\n".join(f.name for f in files)),
        },
    )


@asset(
    group_name="colabfold",
    partitions_def=design_configs,
    deps=[colabfold_input_fastas],
    description=(
        "Runs ColabFold predictions for one partition using merged FASTAs from "
        "``proteinmpnn/combined_seqs`` and writes results to ``colabfold/``. "
        "All parameters are read from the run-scoped ``pipeline_config.yaml``."
    ),
)
def colabfold_predictions(
    context: AssetExecutionContext,
) -> MaterializeResult:
    pc = _read_pipeline_config(partition_key=context.partition_key)
    proot = _partition_root(_run_outputs_dir(pc), context.partition_key, pc.run_id)
    cf = pc.colabfold
    out_dir = proot / "colabfold"
    out_dir.mkdir(parents=True, exist_ok=True)
    combined_dir = proot / "proteinmpnn" / "combined_seqs"
    query_files = sorted(p for p in combined_dir.iterdir() if p.is_file()) if combined_dir.is_dir() else []
    if not query_files:
        context.log.warning(
            f"[colabfold_predictions] partition={context.partition_key}  "
            f"no queries in {combined_dir}; materializing empty ColabFold predictions"
        )
        return _empty_branch_result(
            context,
            reason=f"ColabFold query directory missing or empty: {combined_dir}",
            predictions_dir=MetadataValue.path(str(out_dir)),
            query_count=0,
            pdb_file_count=0,
        )

    patch = _colabfold_alphafold_patch_prefix() if cf.apply_alphafold_numpy_patch else ""

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
                "docker", "run", "--rm", *_docker_user_args(),
                "--runtime=nvidia", *_docker_gpu_args(pc.gpus),
                "-v", f"{cf.colabfold_cache_host}:/cache:rw",
                "-v", f"{proot}:/work:rw",
                cf.colabfold_image,
                "bash", "-c", inner_cmd,
            ],
        )

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
# Exported list consumed by definitions.py
# ---------------------------------------------------------------------------

all_assets = [
    pipeline_config,
    register_sequence_partitions,
    import_binder_sequences,
    rfdiffusion_generation,
    boltzgen_generation,
    promera_generation,
    design_structure_filter,
    proteinmpnn_parsed,
    proteinmpnn_sequences,
    proteinmpnn_soluprot_filter,
    MSA,
    boltz2_input_yamls,
    boltz2_predictions,
    boltz2_renumber,
    esmfold_input_jsons,
    esmfold_predictions,
    esmfold_renumber,
    final_scores_metrics,
    filtered_designs_export,
    glycan_binder_clashes_rule_b,
    glycan_gs_ensemble_clashes_rule_b,
    lh_rule_b_inputs,
    lh_MSA,
    lh_boltz2_input_yamls,
    lh_boltz2_predictions,
    lh_esmfold_input_jsons,
    lh_esmfold_predictions,
    lh_binding_scores,
    rule_b_presentation,
    rule_b_successful_designs_zip,
    # colabfold_input_fastas,
    # colabfold_predictions,
]

# Imported only after every shared helper and base asset above is defined.
# rule_ac_track deliberately does not import this module at import time.
from dagster_pipeline.rule_ac_track import RULE_AC_ASSETS  # noqa: E402

all_assets.extend(RULE_AC_ASSETS)
