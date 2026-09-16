"""MD-cluster GlycoSHIELD ensemble glycan–binder clash scoring.

For each successful design model:

1. For every MD cluster with a precomputed GlycoSHIELD clash-filter cache
   (``gs_runs/md_cluster_*/ensemble_clash_cache.npz``):
   - Superimpose MD beta Cα onto design beta Cα (by residue number).
   - Transform every accepted GS sugar conformer into the design frame.
   - Score clash fraction (2.4 Å heavy-atom) vs the binder (chain A).
   - Average over conformers within the cluster.
2. Take the population-weighted mean across MD clusters
   (weights = ``#st`` from GROMACS ``cluster.log`` / ``gs_runs_summary.tsv``).

Caches are built offline from GlycoSHIELD ``A_52.pdb`` / ``A_52.xtc`` so the
Dagster env only needs NumPy / SciPy / BioPython.
"""

from __future__ import annotations

import csv
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import numpy as np
from scipy.spatial import cKDTree

from dagster_pipeline.glycan_binder_clashes import (
    ALIGN_MAX_CA,
    BINDER_CHAIN,
    CLASH_CUTOFF_A,
    _heavy_coords,
    _is_successful_row,
    _model_index,
    _read_metrics,
)
from dagster_pipeline.successful_designs_export import (
    ALIGN_CHAIN_MODEL,
    RULE_AC_AF3_METRICS,
    RULE_AC_METRICS,
    RULE_AC_SUCCESS,
    RULE_B_METRICS,
    _load_structure,
    _structure_path,
)

DEFAULT_GS_RUNS_DIR = Path("/storage/hCG/glycans/gs_runs")
CACHE_NAME = "ensemble_clash_cache.npz"

ENSEMBLE_CSV_FIELDNAMES = [
    "mpnn_seq_id",
    "method",
    "model_index",
    "structure_path",
    "clash_fraction",
    "n_md_clusters",
    "n_gs_frames_total",
    "mean_align_rmsd",
    "per_cluster_mean_clash",
    "gs_runs_dir",
]

RULE_B_ENSEMBLE_CSV = (
    Path("filtered_designs") / "glycan_gs_ensemble_clashes_rule_b.csv"
)
RULE_AC_ENSEMBLE_CSV = (
    Path("filtered_designs") / "glycan_gs_ensemble_clashes_rule_ac.csv"
)


@dataclass(frozen=True)
class ClusterCache:
    md_cluster: int
    md_n_structures: int
    beta_ca: np.ndarray  # (n_ca, 3) float
    beta_resids: np.ndarray  # (n_ca,) int
    sugar_heavy: np.ndarray  # (n_frames, n_atoms, 3) float


def _kabsch_rmsd_transform(
    P: np.ndarray, Q: np.ndarray
) -> tuple[np.ndarray, np.ndarray, float]:
    """R, t, rmsd with ``P @ R + t ≈ Q`` (both (N,3))."""
    P = np.asarray(P, dtype=np.float64)
    Q = np.asarray(Q, dtype=np.float64)
    Pc = P - P.mean(axis=0)
    Qc = Q - Q.mean(axis=0)
    # Map P → Q (same convention as Bio.PDB Superimposer: coord @ rot + tran).
    H = Qc.T @ Pc
    U, _S, Vt = np.linalg.svd(H)
    d = np.linalg.det(Vt.T @ U.T)
    sign = 1.0 if d >= 0.0 else -1.0
    R = Vt.T @ np.diag([1.0, 1.0, sign]) @ U.T
    t = Q.mean(axis=0) - P.mean(axis=0) @ R
    aligned = Pc @ R
    rmsd = float(np.sqrt(((aligned - Qc) ** 2).sum() / len(P)))
    return R, t, rmsd


def load_cluster_caches(gs_runs_dir: Path) -> list[ClusterCache]:
    """Load all ``ensemble_clash_cache.npz`` under ``gs_runs_dir``."""
    gs_runs_dir = Path(gs_runs_dir)
    caches: list[ClusterCache] = []
    for path in sorted(gs_runs_dir.glob("md_cluster_*/" + CACHE_NAME)):
        data = np.load(path)
        caches.append(
            ClusterCache(
                md_cluster=int(data["md_cluster"]),
                md_n_structures=int(data["md_n_structures"]),
                beta_ca=np.asarray(data["beta_ca"], dtype=np.float64),
                beta_resids=np.asarray(data["beta_resids"], dtype=np.int32),
                sugar_heavy=np.asarray(data["sugar_heavy"], dtype=np.float64),
            )
        )
    if not caches:
        raise FileNotFoundError(
            f"No {CACHE_NAME} found under {gs_runs_dir}. "
            "Build caches from GlycoSHIELD A_52.pdb/xtc first."
        )
    caches.sort(key=lambda c: c.md_cluster)
    return caches


def _design_beta_ca_by_resid(model_path: Path) -> dict[int, np.ndarray]:
    """Map residue number → Cα coord for design beta (chain B)."""
    model = _load_structure(Path(model_path))
    if ALIGN_CHAIN_MODEL not in model[0]:
        raise ValueError(f"Missing chain {ALIGN_CHAIN_MODEL} in {model_path}")
    out: dict[int, np.ndarray] = {}
    for res in model[0][ALIGN_CHAIN_MODEL]:
        if "CA" not in res:
            continue
        out[int(res.id[1])] = np.asarray(res["CA"].coord, dtype=np.float64)
    return out


def _binder_coords(model_path: Path) -> np.ndarray:
    model = _load_structure(Path(model_path))
    if BINDER_CHAIN not in model[0]:
        raise ValueError(f"Missing binder chain {BINDER_CHAIN} in {model_path}")
    return _heavy_coords(model[0][BINDER_CHAIN].get_atoms())


def _paired_coords(
    design_ca: dict[int, np.ndarray],
    md_ca: np.ndarray,
    md_resids: np.ndarray,
    *,
    max_pairs: int = ALIGN_MAX_CA,
) -> tuple[np.ndarray, np.ndarray]:
    """Paired (design, MD) Cα arrays ordered by residue number."""
    design_pts = []
    md_pts = []
    for resid, coord in zip(md_resids.tolist(), md_ca):
        if resid not in design_ca:
            continue
        design_pts.append(design_ca[resid])
        md_pts.append(coord)
        if len(design_pts) >= max_pairs:
            break
    if len(design_pts) < 10:
        raise ValueError(f"Too few CA pairs for alignment ({len(design_pts)})")
    return np.asarray(design_pts, dtype=np.float64), np.asarray(md_pts, dtype=np.float64)


def cluster_mean_clash_fraction(
    cache: ClusterCache,
    *,
    design_ca: dict[int, np.ndarray],
    binder_tree: cKDTree,
    cutoff: float = CLASH_CUTOFF_A,
    frame_stride: int = 1,
) -> tuple[float, float, int]:
    """Return (mean_clash_fraction, align_rmsd, n_frames_used) for one MD cluster."""
    design_pts, md_pts = _paired_coords(
        design_ca, cache.beta_ca, cache.beta_resids
    )
    # Align MD → design: P=MD, Q=design
    R, t, rmsd = _kabsch_rmsd_transform(md_pts, design_pts)

    sugar = cache.sugar_heavy[:: max(1, frame_stride)]
    n_frames, n_atoms, _ = sugar.shape
    if n_frames == 0 or n_atoms == 0:
        return 0.0, rmsd, 0

    # (n_frames, n_atoms, 3)
    aligned = sugar @ R + t
    flat = aligned.reshape(-1, 3)
    dists, _ = binder_tree.query(flat, k=1)
    dists = dists.reshape(n_frames, n_atoms)
    frac = (dists < cutoff).mean(axis=1)
    return float(frac.mean()), rmsd, int(n_frames)


def population_weighted_clash(
    caches: list[ClusterCache],
    *,
    model_path: Path,
    cutoff: float = CLASH_CUTOFF_A,
    frame_stride: int = 1,
) -> dict[str, object]:
    """Weighted-mean clash fraction across MD clusters for one design model."""
    design_ca = _design_beta_ca_by_resid(model_path)
    binder = _binder_coords(model_path)
    if binder.size == 0:
        return {
            "clash_fraction": 0.0,
            "n_md_clusters": 0,
            "n_gs_frames_total": 0,
            "mean_align_rmsd": 0.0,
            "per_cluster_mean_clash": "",
        }
    binder_tree = cKDTree(binder)

    means: list[tuple[int, float, int, float]] = []  # cid, mean, n_frames, rmsd
    for cache in caches:
        if cache.sugar_heavy.shape[0] == 0:
            continue
        mean_cf, rmsd, n_fr = cluster_mean_clash_fraction(
            cache,
            design_ca=design_ca,
            binder_tree=binder_tree,
            cutoff=cutoff,
            frame_stride=frame_stride,
        )
        means.append((cache.md_cluster, mean_cf, n_fr, rmsd))

    if not means:
        return {
            "clash_fraction": 0.0,
            "n_md_clusters": 0,
            "n_gs_frames_total": 0,
            "mean_align_rmsd": 0.0,
            "per_cluster_mean_clash": "",
        }

    # Weights from MD populations; only clusters that contributed frames.
    pop_by_id = {c.md_cluster: c.md_n_structures for c in caches}
    weight_sum = 0.0
    weighted = 0.0
    for cid, mean_cf, _n_fr, _rmsd in means:
        w = float(pop_by_id.get(cid, 0))
        if w <= 0:
            continue
        weighted += mean_cf * w
        weight_sum += w
    clash_fraction = (weighted / weight_sum) if weight_sum > 0 else 0.0

    return {
        "clash_fraction": clash_fraction,
        "n_md_clusters": len(means),
        "n_gs_frames_total": sum(n for _c, _m, n, _r in means),
        "mean_align_rmsd": float(np.mean([r for _c, _m, _n, r in means])),
        "per_cluster_mean_clash": ",".join(
            f"{cid}:{mean_cf:.6f}" for cid, mean_cf, _n, _r in means
        ),
    }


def process_model_ensemble(
    row: dict[str, str],
    *,
    method: str,
    caches: list[ClusterCache],
    gs_runs_dir: Path,
    cutoff: float = CLASH_CUTOFF_A,
    frame_stride: int = 1,
) -> Optional[dict[str, str]]:
    src = _structure_path(row, method)
    if src is None or not src.is_file():
        return None
    sid = (row.get("mpnn_seq_id") or "").strip()
    if not sid:
        return None
    mi = _model_index(row)
    try:
        stats = population_weighted_clash(
            caches,
            model_path=src,
            cutoff=cutoff,
            frame_stride=frame_stride,
        )
    except Exception as exc:  # noqa: BLE001
        print(f"[warn] GS ensemble clash failed for {method} {src}: {exc}")
        return None

    return {
        "mpnn_seq_id": sid,
        "method": method,
        "model_index": mi,
        "structure_path": str(src),
        "clash_fraction": f"{float(stats['clash_fraction']):.6f}",
        "n_md_clusters": str(stats["n_md_clusters"]),
        "n_gs_frames_total": str(stats["n_gs_frames_total"]),
        "mean_align_rmsd": f"{float(stats['mean_align_rmsd']):.4f}",
        "per_cluster_mean_clash": str(stats["per_cluster_mean_clash"]),
        "gs_runs_dir": str(Path(gs_runs_dir).resolve()),
    }


def _write_csv(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=ENSEMBLE_CSV_FIELDNAMES)
        writer.writeheader()
        for row in rows:
            writer.writerow({k: row.get(k, "") for k in ENSEMBLE_CSV_FIELDNAMES})


def _process_ensemble_methods(
    *,
    metrics_by_method: dict[str, list[dict[str, str]]],
    out_csv: Path,
    gs_runs_dir: Path,
    track: str,
    cutoff: float = CLASH_CUTOFF_A,
    frame_stride: int = 1,
) -> dict:
    caches = load_cluster_caches(gs_runs_dir)
    rows: list[dict[str, str]] = []
    for method, method_rows in metrics_by_method.items():
        for row in method_rows:
            result = process_model_ensemble(
                row,
                method=method,
                caches=caches,
                gs_runs_dir=gs_runs_dir,
                cutoff=cutoff,
                frame_stride=frame_stride,
            )
            if result is not None:
                rows.append(result)
    _write_csv(out_csv, rows)
    return {
        "track": track,
        "clash_csv": str(out_csv),
        "row_count": len(rows),
        "gs_runs_dir": str(Path(gs_runs_dir).resolve()),
        "n_md_clusters_available": len(caches),
        "by_method": {
            method: sum(1 for r in rows if r["method"] == method)
            for method in metrics_by_method
        },
    }


def process_rule_b_partition(
    partition_root: Path,
    *,
    gs_runs_dir: Path = DEFAULT_GS_RUNS_DIR,
    cutoff: float = CLASH_CUTOFF_A,
    frame_stride: int = 1,
) -> dict:
    partition_root = Path(partition_root)
    metrics = _read_metrics(partition_root / RULE_B_METRICS)
    return _process_ensemble_methods(
        metrics_by_method={
            "boltz": [r for r in metrics if _is_successful_row(r, "boltz")],
            "esmfold": [r for r in metrics if _is_successful_row(r, "esmfold")],
        },
        out_csv=partition_root / RULE_B_ENSEMBLE_CSV,
        gs_runs_dir=Path(gs_runs_dir),
        track="rule_b",
        cutoff=cutoff,
        frame_stride=frame_stride,
    )


def process_rule_ac_partition(
    partition_root: Path,
    *,
    gs_runs_dir: Path = DEFAULT_GS_RUNS_DIR,
    cutoff: float = CLASH_CUTOFF_A,
    frame_stride: int = 1,
) -> dict:
    partition_root = Path(partition_root)
    success_ids = {
        (r.get("mpnn_seq_id") or "").strip()
        for r in _read_metrics(partition_root / RULE_AC_SUCCESS)
        if (r.get("mpnn_seq_id") or "").strip()
    }
    af3_rows = [
        r
        for r in _read_metrics(partition_root / RULE_AC_AF3_METRICS)
        if (r.get("mpnn_seq_id") or "").strip() in success_ids
        and _is_successful_row(r, "af3")
    ]
    hcg_rows = [
        r
        for r in _read_metrics(partition_root / RULE_AC_METRICS)
        if (r.get("mpnn_seq_id") or "").strip() in success_ids
    ]
    return _process_ensemble_methods(
        metrics_by_method={
            "af3": af3_rows,
            "boltz": [r for r in hcg_rows if _is_successful_row(r, "boltz")],
            "esmfold": [r for r in hcg_rows if _is_successful_row(r, "esmfold")],
        },
        out_csv=partition_root / RULE_AC_ENSEMBLE_CSV,
        gs_runs_dir=Path(gs_runs_dir),
        track="rule_ac",
        cutoff=cutoff,
        frame_stride=frame_stride,
    )


def mean_ensemble_clash_fraction(
    clash_rows: list[dict[str, str]],
    *,
    mpnn_seq_id: str,
    method: str,
) -> Optional[float]:
    """Mean ensemble ``clash_fraction`` over models for one sequence + method."""
    vals: list[float] = []
    for row in clash_rows:
        if (row.get("mpnn_seq_id") or "").strip() != mpnn_seq_id:
            continue
        if (row.get("method") or "").strip() != method:
            continue
        try:
            vals.append(float(row.get("clash_fraction") or ""))
        except ValueError:
            continue
    if not vals:
        return None
    return sum(vals) / len(vals)
