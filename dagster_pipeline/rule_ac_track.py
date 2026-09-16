"""Dagster assets for the Rule A/C AF3 gate and LH off-target track.

This module deliberately has no module-level dependency on ``assets.py``.  It
can therefore be imported by ``assets.py`` after its base helpers are defined
without creating an import cycle.
"""

import csv
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Iterator

import yaml
from dagster import (
    AssetExecutionContext,
    DynamicPartitionsDefinition,
    Failure,
    MaterializeResult,
    MetadataValue,
    Output,
    asset,
)

from dagster_pipeline.resources import (
    DEFAULT_BOLTZ2_CACHE_VOLUME,
    DEFAULT_ESMFOLD2_HF_CACHE_DIR,
    DEFAULT_MSA_CACHE_DIR,
    DEFAULT_OUTPUTS_ROOT,
)

# Repo root that contains the ``filters/`` and ``utils/`` packages. Prefer the
# path relative to this file; fall back to the known install location so Dagster
# workers still resolve imports when the package is loaded from elsewhere.
_PROTEINDESIGN = Path(__file__).resolve().parents[2]
_PROTEINDESIGN_FALLBACK = Path("/storage/proteindesign")


def _ensure_proteindesign_on_path() -> Path:
    """Make ``filters`` / ``utils`` importable (same pattern as assets.py)."""
    root = _PROTEINDESIGN if (_PROTEINDESIGN / "filters").is_dir() else _PROTEINDESIGN_FALLBACK
    root_s = str(root)
    if root_s not in sys.path:
        sys.path.insert(0, root_s)
    return root


_ensure_proteindesign_on_path()

from utils.boltz2_msas import ComplexSpec, MsaServerClient, build_boltz_msa_bundle
from utils.merge_fasta import read_fasta_sequence_flat
from utils.write_boltz2_yamls import split_target_segments, write_yaml_for_pair
from utils.write_esmfold2_jsons import write_json_for_pair

# Dagster identifies dynamic partition definitions by name.  Defining it here
# avoids importing the definition object from assets.py while preserving the
# shared ``design_configs`` key space.
design_configs = DynamicPartitionsDefinition(name="design_configs")


def _as_namespace(value: Any) -> Any:
    if isinstance(value, dict):
        return SimpleNamespace(**{str(k): _as_namespace(v) for k, v in value.items()})
    if isinstance(value, list):
        return [_as_namespace(item) for item in value]
    return value


def _deep_merge_defaults(base: dict[str, Any], overrides: dict[str, Any]) -> dict[str, Any]:
    """Fill missing keys from ``base`` into ``overrides`` (run config wins on conflicts)."""
    merged = dict(base)
    for key, value in overrides.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _deep_merge_defaults(merged[key], value)
        else:
            merged[key] = value
    return merged


def _default_ac_sections() -> dict[str, Any]:
    """Defaults for sections added after older runs were materialized."""
    from dagster_pipeline.resources import (
        LhAcOfftargetConfig,
        LhOfftargetConfig,
        RuleAcPresentationConfig,
    )

    return {
        "lh_offtarget_b": LhOfftargetConfig().model_dump(),
        "lh_offtarget_ac": LhAcOfftargetConfig().model_dump(),
        "rule_ac_presentation": RuleAcPresentationConfig().model_dump(),
    }


def _read_pipeline_config(*, partition_key: str) -> SimpleNamespace:
    """Load the run-scoped YAML without importing the composite assets config.

    Older run configs predate ``lh_offtarget_ac`` / ``rule_ac_presentation``; merge
    those (and any other missing template keys) so SimpleNamespace access matches
    ``PipelineAllToolsConfig`` defaults used by assets.py.
    """
    run_id, separator, _ = partition_key.partition("__")
    if not separator:
        raise Failure(description=f"Invalid design_configs partition key: {partition_key!r}")
    root = _ensure_proteindesign_on_path()
    template = root / "dagster_pipeline" / "pipeline_config.yaml"
    default_root = DEFAULT_OUTPUTS_ROOT
    template_data: dict[str, Any] = {}
    if template.is_file():
        loaded = yaml.safe_load(template.read_text(encoding="utf-8")) or {}
        if isinstance(loaded, dict):
            template_data = loaded
            default_root = str(template_data.get("outputs_root") or default_root)
    config_path = Path(default_root) / run_id / "pipeline_config.yaml"
    if not config_path.is_file():
        raise FileNotFoundError(
            f"Pipeline config not found at {config_path}. Materialize pipeline_config first."
        )
    data = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise Failure(description=f"Pipeline config is not a YAML mapping: {config_path}")
    # Older runs used ``lh_offtarget:``; prefer ``lh_offtarget_b:``.
    if "lh_offtarget_b" not in data and isinstance(data.get("lh_offtarget"), dict):
        data["lh_offtarget_b"] = data.pop("lh_offtarget")
    else:
        data.pop("lh_offtarget", None)
    # Template first, then hard-coded AC defaults (in case template is also stale),
    # then run overrides.
    defaults = _deep_merge_defaults(_default_ac_sections(), template_data)
    return _as_namespace(_deep_merge_defaults(defaults, data))


def _run_outputs_dir(pc: SimpleNamespace) -> Path:
    run_id = str(getattr(pc, "run_id", "") or "").strip()
    if not run_id:
        raise Failure(description="Run-scoped pipeline config has no run_id.")
    return Path(str(getattr(pc, "outputs_root", DEFAULT_OUTPUTS_ROOT))) / run_id


def _partition_root(outputs_dir: Path, partition_key: str, run_id: str) -> Path:
    prefix = f"{run_id}__"
    return outputs_dir / (partition_key[len(prefix) :] if partition_key.startswith(prefix) else partition_key)


def _empty_branch_output(
    context: AssetExecutionContext,
    *,
    reason: str,
    branch_status: str = "no_candidates",
    value: dict[str, Any] | None = None,
    **extra_metadata: Any,
) -> Output:
    return Output(
        {"partition_key": context.partition_key, "has_candidates": False, **(value or {})},
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
    **extra_metadata: Any,
) -> MaterializeResult:
    return MaterializeResult(
        metadata={
            "partition_key": context.partition_key,
            "has_candidates": False,
            "branch_status": branch_status,
            "skip_reason": reason,
            **extra_metadata,
        }
    )


def _run(context: AssetExecutionContext, cmd: list[str], cwd: Path | None = None) -> None:
    context.log.info("$ " + " ".join(cmd))
    proc = subprocess.Popen(
        cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, cwd=cwd
    )
    assert proc.stdout is not None
    for line in proc.stdout:
        if text := line.rstrip():
            context.log.info(text)
    proc.wait()
    if proc.returncode:
        raise RuntimeError(f"Command exited with code {proc.returncode}: {cmd[0]} ...")


def _docker_user_args() -> list[str]:
    import grp
    import pwd

    uid, gid = os.getuid(), os.getgid()
    try:
        name = pwd.getpwuid(uid).pw_name
    except KeyError:
        name = f"u{uid}"
    try:
        group = grp.getgrgid(gid).gr_name
    except KeyError:
        group = f"g{gid}"
    identity = Path(f"/tmp/dagster_docker_id_{uid}_{gid}")
    identity.mkdir(parents=True, exist_ok=True)
    passwd, group_file = identity / "passwd", identity / "group"
    passwd.write_text(
        f"root:x:0:0:root:/root:/bin/sh\n{name}:x:{uid}:{gid}::/tmp:/bin/sh\n",
        encoding="utf-8",
    )
    group_file.write_text(f"root:x:0:\n{group}:x:{gid}:\n", encoding="utf-8")
    return [
        "--user", f"{uid}:{gid}", "-v", f"{passwd}:/etc/passwd:ro", "-v",
        f"{group_file}:/etc/group:ro", "-e", "HOME=/tmp", "-e", f"USER={name}",
    ]


def _docker_gpu_args(gpus: str) -> list[str]:
    value = str(gpus).strip()
    if not value:
        return []
    if value == "all" or value.startswith(("device=", "count=", '"device=', '"count=')):
        return ["--gpus", value]
    return ["--gpus", f'"device={value}"']


def _select_gpu_ids(gpus: str, count: int) -> str:
    value = str(gpus).strip().strip('"')
    if not value or value == "all" or value.startswith("count="):
        return value or "all"
    if value.startswith("device="):
        value = value[len("device=") :].strip('"')
    parts = [part.strip() for part in value.split(",") if part.strip()]
    return ",".join(parts[: max(1, count)]) if parts else value


def _paths(proot: Path) -> dict[str, Path]:
    root = proot / "lh_offtarget_ac"
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


def _enabled(pc: SimpleNamespace) -> bool:
    return bool(getattr(getattr(pc, "lh_offtarget_ac", None), "enabled", False))


def _target_fasta(pc: SimpleNamespace) -> str:
    return str(getattr(getattr(pc, "lh_offtarget_ac", None), "target_fasta", "") or "").strip()


def _fasta_files(path: Path, extensions: list[str]) -> list[Path]:
    allowed = tuple(ext if ext.startswith(".") else f".{ext}" for ext in extensions)
    return sorted(p for p in path.iterdir() if p.is_file() and p.suffix.lower() in allowed) if path.is_dir() else []


@asset(group_name="lh_offtarget_ac", partitions_def=design_configs, deps=["filtered_designs_export"], output_required=False)
def af3_rule_ac_ready(context: AssetExecutionContext) -> Iterator[Output]:
    """Gate Rule A/C candidates on a complete AF3 upload."""
    _ensure_proteindesign_on_path()
    from filters.final_scores.af3_rule_ac import check_af3_completeness

    pc = _read_pipeline_config(partition_key=context.partition_key)
    if not _enabled(pc):
        yield _empty_branch_output(
            context,
            reason="lh_offtarget_ac.enabled=false",
            branch_status="skipped",
            value={"job_count": 0},
            job_count=0,
        )
        return
    proot = _partition_root(_run_outputs_dir(pc), context.partition_key, pc.run_id)
    status = check_af3_completeness(proot)
    if status.status == "missing":
        yield _empty_branch_output(
            context, reason=f"AF3 directory not uploaded: {proot / 'AF3'}",
            branch_status="skipped", value={"job_count": len(status.jobs)}, job_count=len(status.jobs),
        )
        return
    if status.status == "incomplete":
        raise Failure(
            description=f"AF3 is incomplete under {status.af3_dir}; missing folds: "
            + ", ".join(status.missing_stems)
        )
    yield Output(
        {"partition_key": context.partition_key, "job_count": len(status.jobs)},
        metadata={
            "job_count": len(status.jobs),
            "af3_dir": MetadataValue.path(str(status.af3_dir)),
            "branch_status": "has_candidates" if status.jobs else "no_candidates",
        },
    )


@asset(group_name="lh_offtarget_ac", partitions_def=design_configs, deps=["af3_rule_ac_ready"])
def af3_rule_ac_scores(context: AssetExecutionContext) -> MaterializeResult:
    _ensure_proteindesign_on_path()
    from filters.final_scores.af3_rule_ac import (
        DEFAULT_REFERENCE_PDB,
        DEFAULT_TARGET_FASTA,
        check_af3_completeness,
        score_af3_partition,
    )

    pc = _read_pipeline_config(partition_key=context.partition_key)
    proot = _partition_root(_run_outputs_dir(pc), context.partition_key, pc.run_id)
    if not _enabled(pc):
        return _empty_branch_result(context, reason="lh_offtarget_ac.enabled=false", branch_status="skipped")
    status = check_af3_completeness(proot)
    if status.status == "missing":
        return _empty_branch_result(
            context,
            reason=f"AF3 directory not uploaded: {proot / 'AF3'}",
            branch_status="skipped",
        )
    if status.status == "incomplete":
        raise Failure(
            description=f"AF3 is incomplete under {status.af3_dir}; missing folds: "
            + ", ".join(status.missing_stems)
        )
    cfg = pc.lh_offtarget_ac
    # AF3 complexes are vs hCG (same as for_AF3 export), not LH.
    summary = score_af3_partition(
        proot,
        docker_image=getattr(pc.structure_filters, "structure_docker_image", "structure_tools"),
        hotspots_dir=cfg.hotspots_dir,
        reference_pdb=DEFAULT_REFERENCE_PDB,
        target_fasta=DEFAULT_TARGET_FASTA,
    )
    return MaterializeResult(
        metadata={
            "job_count": summary["job_count"],
            "model_row_count": summary["model_row_count"],
            "successful_sequence_count": summary["successful_sequence_count"],
            "hotspots_file": MetadataValue.path(summary["hotspots_file"]),
            "af3_metrics_csv": MetadataValue.path(summary["af3_metrics_csv"]),
            "af3_successful_sequences_csv": MetadataValue.path(summary["af3_successful_sequences_csv"]),
        }
    )


@asset(group_name="lh_offtarget_ac", partitions_def=design_configs, deps=["af3_rule_ac_scores"], output_required=False)
def af3_rule_ac_successes(context: AssetExecutionContext) -> Iterator[Output]:
    pc = _read_pipeline_config(partition_key=context.partition_key)
    proot = _partition_root(_run_outputs_dir(pc), context.partition_key, pc.run_id)
    success_csv = proot / "filtered_designs" / "AF3_successful_sequences.csv"
    rows = list(csv.DictReader(success_csv.open(encoding="utf-8", newline=""))) if success_csv.is_file() else []
    if not rows:
        yield _empty_branch_output(
            context, reason="no AF3-successful Rule A/C sequences",
            value={"successful_sequence_count": 0},
            successful_sequence_count=0, success_csv=MetadataValue.path(str(success_csv)),
        )
        return
    yield Output(
        {"partition_key": context.partition_key, "successful_sequence_count": len(rows)},
        metadata={
            "successful_sequence_count": len(rows),
            "success_csv": MetadataValue.path(str(success_csv)),
            "branch_status": "has_candidates",
        },
    )


@asset(
    group_name="lh_offtarget_ac",
    partitions_def=design_configs,
    deps=["af3_rule_ac_successes"],
    description=(
        "Score α-Asn52 glycan–binder clash fraction over MD-cluster GlycoSHIELD "
        "ensembles for AF3-successful Rule A/C sequences: mean clash over accepted "
        "GS conformers per MD cluster, then population-weighted mean across clusters. "
        "Writes ``filtered_designs/glycan_gs_ensemble_clashes_rule_ac.csv``."
    ),
)
def glycan_gs_ensemble_clashes_rule_ac(context: AssetExecutionContext) -> MaterializeResult:
    from dagster_pipeline.glycan_gs_ensemble_clashes import process_rule_ac_partition

    pc = _read_pipeline_config(partition_key=context.partition_key)
    proot = _partition_root(_run_outputs_dir(pc), context.partition_key, pc.run_id)
    if not _enabled(pc):
        return _empty_branch_result(
            context, reason="lh_offtarget_ac.enabled=false", branch_status="skipped"
        )
    success_csv = proot / "filtered_designs" / "AF3_successful_sequences.csv"
    if not success_csv.is_file():
        return _empty_branch_result(
            context,
            reason=f"missing {success_csv}",
            branch_status="no_candidates",
        )
    rows = list(csv.DictReader(success_csv.open(encoding="utf-8", newline="")))
    if not rows:
        return _empty_branch_result(
            context,
            reason="no AF3-successful Rule A/C sequences",
            branch_status="no_candidates",
        )
    summary = process_rule_ac_partition(proot)
    return MaterializeResult(
        metadata={
            "partition_key": context.partition_key,
            "clash_csv": MetadataValue.path(summary["clash_csv"]),
            "row_count": summary["row_count"],
            "by_method": MetadataValue.json(summary["by_method"]),
            "gs_runs_dir": MetadataValue.path(summary["gs_runs_dir"]),
            "n_md_clusters_available": summary["n_md_clusters_available"],
            "branch_status": "has_candidates" if summary["row_count"] > 0 else "no_candidates",
        }
    )


@asset(
    group_name="lh_offtarget_ac",
    partitions_def=design_configs,
    deps=["af3_rule_ac_successes"],
    description=(
        "Align ``hCG_glycans.pdb`` onto successful AF3 / Boltz / ESMFold models "
        "for AF3-successful Rule A/C sequences. Writes "
        "``filtered_designs/glycan_binder_clashes_rule_ac.csv`` and "
        "``filtered_designs/glycan_aligned_rule_ac/``."
    ),
)
def glycan_binder_clashes_rule_ac(context: AssetExecutionContext) -> MaterializeResult:
    from dagster_pipeline.glycan_binder_clashes import process_rule_ac_partition

    pc = _read_pipeline_config(partition_key=context.partition_key)
    proot = _partition_root(_run_outputs_dir(pc), context.partition_key, pc.run_id)
    if not _enabled(pc):
        return _empty_branch_result(
            context, reason="lh_offtarget_ac.enabled=false", branch_status="skipped"
        )
    success_csv = proot / "filtered_designs" / "AF3_successful_sequences.csv"
    if not success_csv.is_file():
        return _empty_branch_result(
            context,
            reason=f"missing {success_csv}",
            branch_status="no_candidates",
        )
    rows = list(csv.DictReader(success_csv.open(encoding="utf-8", newline="")))
    if not rows:
        return _empty_branch_result(
            context,
            reason="no AF3-successful Rule A/C sequences",
            branch_status="no_candidates",
        )
    summary = process_rule_ac_partition(proot)
    return MaterializeResult(
        metadata={
            "partition_key": context.partition_key,
            "clash_csv": MetadataValue.path(summary["clash_csv"]),
            "aligned_dir": MetadataValue.path(summary["aligned_dir"]),
            "row_count": summary["row_count"],
            "by_method": MetadataValue.json(summary["by_method"]),
            "glycans_pdb": MetadataValue.path(summary["glycans_pdb"]),
            "branch_status": "has_candidates" if summary["row_count"] > 0 else "no_candidates",
        }
    )


@asset(group_name="lh_offtarget_ac", partitions_def=design_configs, deps=["af3_rule_ac_successes"], output_required=False)
def lh_ac_inputs(context: AssetExecutionContext) -> Iterator[Output]:
    _ensure_proteindesign_on_path()
    from filters.final_scores.lh_binding import stage_candidate_fastas

    pc = _read_pipeline_config(partition_key=context.partition_key)
    proot = _partition_root(_run_outputs_dir(pc), context.partition_key, pc.run_id)
    paths = _paths(proot)
    candidates = proot / "filtered_designs" / "AF3_successful_sequences.csv"
    if not _enabled(pc):
        yield _empty_branch_output(context, reason="lh_offtarget_ac.enabled=false", branch_status="skipped", value={"sequence_count": 0})
        return
    staged = stage_candidate_fastas(
        candidates_csv=candidates,
        seqs_filtered_dir=proot / "proteinmpnn" / "seqs_filtered",
        output_seqs_dir=paths["seqs"],
    )
    if not staged:
        yield _empty_branch_output(context, reason=f"no candidates in {candidates}", value={"sequence_count": 0})
        return
    yield Output(
        {"partition_key": context.partition_key, "sequence_count": len(staged), "seqs_dir": str(paths["seqs"])},
        metadata={"sequence_count": len(staged), "seqs_dir": MetadataValue.path(str(paths["seqs"])), "branch_status": "has_candidates"},
    )


@asset(group_name="lh_offtarget_ac", partitions_def=design_configs, deps=["lh_ac_inputs"], output_required=False)
def lh_ac_MSA(context: AssetExecutionContext) -> Iterator[Output]:
    _ensure_proteindesign_on_path()
    from filters.final_scores.lh_binding import iter_lh_binder_target

    pc = _read_pipeline_config(partition_key=context.partition_key)
    proot = _partition_root(_run_outputs_dir(pc), context.partition_key, pc.run_id)
    paths, target_path = _paths(proot), Path(_target_fasta(pc))
    if not _enabled(pc):
        yield _empty_branch_output(context, reason="lh_offtarget_ac.enabled=false", branch_status="skipped", value={"complex_count": 0})
        return
    if not target_path.is_file():
        raise Failure(description=f"LH target FASTA not found: {target_path}")
    target = read_fasta_sequence_flat(target_path)
    if not target:
        raise Failure(description=f"LH target FASTA is empty: {target_path}")
    files = _fasta_files(paths["seqs"], list(pc.boltz2.fasta_extensions))
    if not files:
        yield _empty_branch_output(context, reason=f"no staged Rule A/C FASTAs in {paths['seqs']}", value={"complex_count": 0})
        return
    complexes = [
        ComplexSpec(name=base, chains=tuple([("A", binder)] + [(chr(ord("B") + i), seq) for i, seq in enumerate(split_target_segments(target))]))
        for src in files for base, binder, _ in iter_lh_binder_target(src, target)
    ]
    if not complexes:
        yield _empty_branch_output(context, reason="no LH complexes to prepare", value={"complex_count": 0})
        return
    if paths["msa"].exists():
        shutil.rmtree(paths["msa"])
    msa = pc.msa
    client = MsaServerClient(msa.server_url, timeout_seconds=msa.request_timeout_seconds, max_retries=msa.max_retries, poll_interval_seconds=msa.poll_interval_seconds, retry_backoff_seconds=msa.retry_backoff_seconds)
    manifest = build_boltz_msa_bundle(
        complexes, output_dir=paths["msa"], cache_root=Path(DEFAULT_MSA_CACHE_DIR), client=client,
        unpaired_mode="env" if msa.use_env else "all",
        paired_mode=f"pair{msa.pairing_strategy}" + ("-env" if msa.use_env else ""),
        use_env=msa.use_env, max_paired_seqs=msa.max_paired_seqs, max_msa_seqs=msa.max_msa_seqs,
    )
    yield Output(
        {"partition_key": context.partition_key, "complex_count": len(complexes)},
        metadata={"complex_count": len(complexes), "chain_count": sum(len(x["chains"]) for x in manifest["complexes"].values()), "msa_dir": MetadataValue.path(str(paths["msa"])), "branch_status": "has_candidates"},
    )


@asset(group_name="lh_offtarget_ac", partitions_def=design_configs, deps=["lh_ac_MSA"], output_required=False)
def lh_ac_boltz2_input_yamls(context: AssetExecutionContext) -> Iterator[Output]:
    _ensure_proteindesign_on_path()
    from filters.final_scores.lh_binding import iter_lh_binder_target

    pc = _read_pipeline_config(partition_key=context.partition_key)
    proot = _partition_root(_run_outputs_dir(pc), context.partition_key, pc.run_id)
    paths, bz = _paths(proot), pc.boltz2
    if not _enabled(pc):
        yield _empty_branch_output(context, reason="lh_offtarget_ac.enabled=false", branch_status="skipped", value={"yaml_files_written": 0})
        return
    files = _fasta_files(paths["seqs"], list(bz.fasta_extensions))
    manifest_path = paths["msa"] / "manifest.json"
    if not files:
        yield _empty_branch_output(context, reason="no staged Rule A/C FASTAs", value={"yaml_files_written": 0})
        return
    if not manifest_path.is_file():
        raise Failure(description=f"LH Rule A/C MSA manifest missing: {manifest_path}")
    target, template = read_fasta_sequence_flat(Path(_target_fasta(pc))), Path(bz.template_yaml)
    if not target or not template.is_file():
        raise Failure(description="LH target FASTA is empty or Boltz-2 template is missing.")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8")).get("complexes", {})
    if paths["yamls"].exists():
        shutil.rmtree(paths["yamls"])
    paths["yamls"].mkdir(parents=True)
    written = 0
    for src in files:
        for base, binder, target_seq in iter_lh_binder_target(src, target):
            payload = manifest.get(base)
            if not isinstance(payload, dict):
                raise Failure(description=f"LH MSA manifest has no complex for {base!r}.")
            msa_paths = {}
            for chain in payload.get("chains", []):
                host_path = Path(chain["csv_path"])
                if not host_path.is_file():
                    raise Failure(description=f"LH MSA CSV missing: {host_path}")
                msa_paths[str(chain["id"])] = f"/work/{host_path.relative_to(proot).as_posix()}"
            write_yaml_for_pair(template.read_text(encoding="utf-8"), paths["yamls"], base, binder, target_seq, msa_paths=msa_paths)
            written += 1
    yield Output({"partition_key": context.partition_key, "yaml_files_written": written}, metadata={"yaml_files_written": written, "combined_yamls_dir": MetadataValue.path(str(paths["yamls"])), "branch_status": "has_candidates"})


@asset(group_name="lh_offtarget_ac", partitions_def=design_configs, deps=["lh_ac_boltz2_input_yamls"])
def lh_ac_boltz2_predictions(context: AssetExecutionContext) -> MaterializeResult:
    pc = _read_pipeline_config(partition_key=context.partition_key)
    proot = _partition_root(_run_outputs_dir(pc), context.partition_key, pc.run_id)
    paths, bz = _paths(proot), pc.boltz2
    paths["boltz2"].mkdir(parents=True, exist_ok=True)
    yaml_files = sorted(paths["yamls"].glob("*.yaml")) if paths["yamls"].is_dir() else []
    if not _enabled(pc) or not yaml_files:
        return _empty_branch_result(context, reason="lh_offtarget_ac disabled or no Boltz YAMLs", predictions_dir=MetadataValue.path(str(paths["boltz2"])), input_yaml_count=0)
    chunk_root = paths["yaml_chunks"]
    if chunk_root.exists():
        shutil.rmtree(chunk_root)
    chunk_root.mkdir(parents=True)
    chunk_size, chunks = max(1, bz.query_chunk_size), 0
    for start in range(0, len(yaml_files), chunk_size):
        chunks += 1
        chunk = chunk_root / f"chunk_{chunks:04d}"
        chunk.mkdir()
        for src in yaml_files[start : start + chunk_size]:
            os.symlink(os.path.relpath(src, chunk), chunk / src.name)
        devices = max(1, bz.devices)
        cmd = [
            "docker", "run", "--rm", *_docker_user_args(), "--runtime=nvidia", "--ipc=host",
            *_docker_gpu_args(_select_gpu_ids(pc.gpus, devices)),
            "-e", "LD_LIBRARY_PATH=/usr/local/lib/python3.11/dist-packages/nvidia/cu13/lib:/usr/local/nvidia/lib:/usr/local/nvidia/lib64",
            "-v", f"{proot}:/work:rw", "-v", f"{DEFAULT_BOLTZ2_CACHE_VOLUME}:/cache:rw",
            bz.docker_image, "predict", f"/work/lh_offtarget_ac/combined_yamls_chunks/chunk_{chunks:04d}",
            "--cache", "/cache", "--out_dir", "/work/lh_offtarget_ac/boltz2",
            "--recycling_steps", str(bz.recycling_steps), "--devices", str(devices),
            "--diffusion_samples", str(bz.diffusion_samples), "--num_workers", str(bz.num_workers),
            "--preprocessing-threads", str(bz.preprocessing_threads), "--override", "--no_kernels",
        ]
        if bz.use_potentials:
            cmd.append("--use_potentials")
        _run(context, cmd)
    count = len(list(paths["boltz2"].rglob("*.cif"))) + len(list(paths["boltz2"].rglob("*.pdb")))
    if not count:
        raise Failure(description=f"LH Rule A/C Boltz-2 produced no structures: {paths['boltz2']}")
    return MaterializeResult(metadata={"predictions_dir": MetadataValue.path(str(paths["boltz2"])), "input_yaml_count": len(yaml_files), "chunk_count": chunks, "structure_file_count": count})


@asset(group_name="lh_offtarget_ac", partitions_def=design_configs, deps=["lh_ac_inputs"], output_required=False)
def lh_ac_esmfold_input_jsons(context: AssetExecutionContext) -> Iterator[Output]:
    _ensure_proteindesign_on_path()
    from filters.final_scores.lh_binding import iter_lh_binder_target

    pc = _read_pipeline_config(partition_key=context.partition_key)
    proot = _partition_root(_run_outputs_dir(pc), context.partition_key, pc.run_id)
    paths = _paths(proot)
    if not _enabled(pc):
        yield _empty_branch_output(context, reason="lh_offtarget_ac.enabled=false", branch_status="skipped", value={"files_written": 0})
        return
    files, target = _fasta_files(paths["seqs"], list(pc.boltz2.fasta_extensions)), read_fasta_sequence_flat(Path(_target_fasta(pc)))
    if not files or not target:
        yield _empty_branch_output(context, reason="no Rule A/C FASTAs or LH target sequence", value={"files_written": 0})
        return
    if paths["esmfold_inputs"].exists():
        shutil.rmtree(paths["esmfold_inputs"])
    paths["esmfold_inputs"].mkdir(parents=True)
    written = 0
    for src in files:
        for base, binder, target_seq in iter_lh_binder_target(src, target):
            write_json_for_pair(paths["esmfold_inputs"], base, binder, target_seq)
            written += 1
    yield Output({"partition_key": context.partition_key, "files_written": written}, metadata={"files_written": written, "esmfold_inputs_dir": MetadataValue.path(str(paths["esmfold_inputs"])), "branch_status": "has_candidates"})


@asset(group_name="lh_offtarget_ac", partitions_def=design_configs, deps=["lh_ac_esmfold_input_jsons"])
def lh_ac_esmfold_predictions(context: AssetExecutionContext) -> MaterializeResult:
    pc = _read_pipeline_config(partition_key=context.partition_key)
    proot = _partition_root(_run_outputs_dir(pc), context.partition_key, pc.run_id)
    paths, ef = _paths(proot), pc.esmfold
    paths["esmfold"].mkdir(parents=True, exist_ok=True)
    queries = sorted(paths["esmfold_inputs"].glob("*.json")) if paths["esmfold_inputs"].is_dir() else []
    if not _enabled(pc) or not queries:
        return _empty_branch_result(context, reason="lh_offtarget_ac disabled or no ESMFold inputs", predictions_dir=MetadataValue.path(str(paths["esmfold"])), input_json_count=0)
    chunks = paths["esmfold_chunks"]
    if chunks.exists():
        shutil.rmtree(chunks)
    chunks.mkdir(parents=True)
    script = (_ensure_proteindesign_on_path() / "ESMfold2" / "predict.py").resolve()
    cache = Path(DEFAULT_ESMFOLD2_HF_CACHE_DIR).resolve()
    cache.mkdir(parents=True, exist_ok=True)
    chunk_size, chunk_count = max(1, ef.query_chunk_size), 0
    for start in range(0, len(queries), chunk_size):
        chunk_count += 1
        chunk = chunks / f"chunk_{chunk_count:04d}"
        chunk.mkdir()
        for src in queries[start : start + chunk_size]:
            os.symlink(os.path.relpath(src, chunk), chunk / src.name)
        proot_s = str(proot.resolve())
        cmd = [
            "docker", "run", "--rm", *_docker_user_args(), "--runtime=nvidia",
            *_docker_gpu_args(_select_gpu_ids(pc.gpus, 1)), "-v", f"{proot_s}:{proot_s}:rw",
            "-v", f"{script.parent}:{script.parent}:ro", "-v", f"{cache}:/cache/huggingface",
            "-e", "HF_HOME=/cache/huggingface", "-e", "TRANSFORMERS_CACHE=/cache/huggingface",
            "-e", "HUGGINGFACE_HUB_CACHE=/cache/huggingface", ef.docker_image, "python", str(script),
            "-i", f"{proot_s}/lh_offtarget_ac/esmfold_input_chunks/chunk_{chunk_count:04d}",
            "-o", f"{proot_s}/lh_offtarget_ac/esmfold", "--model", ef.model,
            "--num-loops", str(ef.num_loops), "--num-sampling-steps", str(ef.num_sampling_steps),
            "--num-diffusion-samples", str(ef.num_diffusion_samples), "--seed", str(ef.seed), "--device", "cuda",
        ]
        _run(context, cmd)
    count = len(list(paths["esmfold"].rglob("*.cif")))
    if not count:
        raise Failure(description=f"LH Rule A/C ESMFold produced no CIF structures: {paths['esmfold']}")
    return MaterializeResult(metadata={"predictions_dir": MetadataValue.path(str(paths["esmfold"])), "input_json_count": len(queries), "chunk_count": chunk_count, "cif_file_count": count})


@asset(group_name="lh_offtarget_ac", partitions_def=design_configs, deps=["lh_ac_boltz2_predictions", "lh_ac_esmfold_predictions"])
def lh_ac_binding_scores(context: AssetExecutionContext) -> MaterializeResult:
    _ensure_proteindesign_on_path()
    from filters.final_scores.lh_binding import write_lh_binding_csv

    pc = _read_pipeline_config(partition_key=context.partition_key)
    proot = _partition_root(_run_outputs_dir(pc), context.partition_key, pc.run_id)
    output = proot / "filtered_designs" / "LH_binding_rule_ac.csv"
    if not _enabled(pc):
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(
            "mpnn_seq_id,model_index,binder_seq,specificity_hotspots_boltz,specificity_hotspots_esmfold,binder_aas_contact_B77_boltz,binder_aas_contact_B77_esmfold,structure_pdb_boltz,structure_pdb_esmfold\n",
            encoding="utf-8",
        )
        return _empty_branch_result(context, reason="lh_offtarget_ac.enabled=false", branch_status="skipped", lh_binding_csv=MetadataValue.path(str(output)))
    final_scores = _ensure_proteindesign_on_path() / "filters" / "final_scores"
    script = final_scores / "run_lh_binding.py"
    if not script.is_file():
        raise Failure(description=f"LH binding scorer missing: {script}")
    output.parent.mkdir(parents=True, exist_ok=True)
    _run(context, [
        "docker", "run", "--rm", *_docker_user_args(), "-v", f"{final_scores.resolve()}:{final_scores.resolve()}",
        "-v", f"{proot.resolve()}:{proot.resolve()}", pc.final_scores.docker_image, "python", str(script.resolve()),
        str(proot.resolve()), "--output-csv", str(output.resolve()), "--candidates-csv",
        str((proot / "filtered_designs" / "AF3_successful_sequences.csv").resolve()),
        "--boltz-subdir", "lh_offtarget_ac/boltz2", "--esmfold-subdir", "lh_offtarget_ac/esmfold",
    ])
    if not output.is_file():
        raise Failure(description=f"LH_binding_rule_ac.csv was not written: {output}")
    rows = list(csv.DictReader(output.open(encoding="utf-8", newline="")))
    return MaterializeResult(metadata={"lh_binding_csv": MetadataValue.path(str(output)), "row_count": len(rows), "boltz_model_count": sum(bool(row.get("structure_pdb_boltz")) for row in rows), "esmfold_model_count": sum(bool(row.get("structure_pdb_esmfold")) for row in rows)})


@asset(
    group_name="lh_offtarget_ac",
    partitions_def=design_configs,
    deps=["lh_ac_binding_scores", "glycan_gs_ensemble_clashes_rule_ac", "glycan_binder_clashes_rule_ac"],
)
def rule_ac_presentation(context: AssetExecutionContext) -> MaterializeResult:
    from dagster_pipeline.rule_ac_presentation import build_rule_ac_presentation

    pc = _read_pipeline_config(partition_key=context.partition_key)
    run_dir = _run_outputs_dir(pc)
    cfg, out_dir = pc.rule_ac_presentation, run_dir / "rule_ac"
    if not cfg.enabled:
        return _empty_branch_result(context, reason="rule_ac_presentation.enabled=false", branch_status="skipped", output_dir=MetadataValue.path(str(out_dir)))
    run_dirs = [p for p in sorted(run_dir.iterdir()) if p.is_dir() and (p / "filtered_designs" / "AF3_successful_sequences.csv").is_file()]
    if not run_dirs:
        return _empty_branch_result(context, reason=f"no AF3_successful_sequences.csv under {run_dir}", output_dir=MetadataValue.path(str(out_dir)))
    summary = build_rule_ac_presentation(run_dirs, out_dir, seed=cfg.seed, pymol=Path(cfg.pymol), scaffolds_dir=Path(cfg.scaffolds_dir), marp_docker_image=cfg.marp_docker_image, export_pdf=cfg.export_pdf, export_pptx=getattr(cfg, "export_pptx", True))
    meta: dict[str, Any] = {"output_dir": MetadataValue.path(str(out_dir)), "slide_count": summary["slide_count"], "partition_count": len(run_dirs), "deck_md": MetadataValue.path(str(summary["deck_md"])), "assets_dir": MetadataValue.path(str(summary["assets_dir"]))}
    if summary.get("deck_pdf"):
        meta["deck_pdf"] = MetadataValue.path(str(summary["deck_pdf"]))
    if summary.get("deck_pptx"):
        meta["deck_pptx"] = MetadataValue.path(str(summary["deck_pptx"]))
    return MaterializeResult(metadata=meta)


@asset(
    group_name="lh_offtarget_ac",
    partitions_def=design_configs,
    deps=["rule_ac_presentation"],
    description=(
        "Zip every AF3-successful Rule A/C design that appears in the Marp deck. "
        "Writes ``{run_id}/rule_ac/successful_designs.zip`` with per-sequence FASTA, "
        "hCG metrics CSV, AF3_metrics.csv, and one superimposed PDB per "
        "Boltz / ESMFold / AF3 model (prediction A/B/C + ``hCG_glycans.pdb`` as X/Y)."
    ),
)
def rule_ac_successful_designs_zip(context: AssetExecutionContext) -> MaterializeResult:
    from dagster_pipeline.successful_designs_export import build_rule_ac_successful_zip

    pc = _read_pipeline_config(partition_key=context.partition_key)
    run_dir = _run_outputs_dir(pc)
    out_zip = run_dir / "rule_ac" / "successful_designs.zip"

    run_dirs = [
        child
        for child in sorted(run_dir.iterdir())
        if child.is_dir()
        and (child / "filtered_designs" / "AF3_successful_sequences.csv").is_file()
    ]
    if not run_dirs:
        return _empty_branch_result(
            context,
            reason=f"no AF3_successful_sequences.csv under {run_dir}",
            branch_status="no_candidates",
            zip_path=MetadataValue.path(str(out_zip)),
        )

    summary = build_rule_ac_successful_zip(run_dirs, out_zip)
    context.log.info(
        f"[rule_ac_successful_designs_zip] sequences={summary['sequence_count']}  "
        f"partitions={summary['partition_count']}  zip={summary['zip_path'] or out_zip}  "
        f"glycans={summary.get('glycans_pdb')}"
    )
    if summary["sequence_count"] == 0:
        return _empty_branch_result(
            context,
            reason="no AF3-successful Rule A/C presentation sequences",
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
            "track": "rule_ac",
        }
    )


RULE_AC_ASSETS = [
    af3_rule_ac_ready, af3_rule_ac_scores, af3_rule_ac_successes,
    glycan_binder_clashes_rule_ac, glycan_gs_ensemble_clashes_rule_ac, lh_ac_inputs,
    lh_ac_MSA, lh_ac_boltz2_input_yamls, lh_ac_boltz2_predictions,
    lh_ac_esmfold_input_jsons, lh_ac_esmfold_predictions, lh_ac_binding_scores,
    rule_ac_presentation, rule_ac_successful_designs_zip,
]
