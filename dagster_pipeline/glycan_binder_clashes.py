"""Align ``hCG_glycans.pdb`` onto design beta and score glycan–binder clashes.

For each successful model (``specificity_hotspots_* == 2``):

1. Fix design chain-B Cα; move all glycans atoms into the design frame.
2. Write a combined PDB (design A/B/C + glycans A→X, B→Y).
3. Score only the α-subunit Asn52 N-glycan: fraction of that glycan's heavy
   atoms within 2.4 Å of binder (chain A) heavy atoms.

Used by Rule B / Rule A/C Dagster assets and reused by successful-designs zips.
"""

from __future__ import annotations

import copy
import csv
import shutil
from collections import defaultdict
from io import StringIO
from pathlib import Path
from typing import Optional

import numpy as np
from scipy.spatial import cKDTree

from dagster_pipeline.successful_designs_export import (
    ALIGN_CHAIN_GLYCAN,
    ALIGN_CHAIN_MODEL,
    DEFAULT_GLYCAN_PDB,
    GLYCAN_CHAIN_MAP,
    RULE_AC_AF3_METRICS,
    RULE_AC_METRICS,
    RULE_AC_SUCCESS,
    RULE_B_METRICS,
    SPEC_REQUIRED,
    _load_structure,
    _remap_pdb_chains,
    _strip_pdb_trailer,
    _structure_path,
)

CLASH_CUTOFF_A = 2.4
BINDER_CHAIN = "A"
# α-Asn52 N-glycan in ``hCG_glycans.pdb`` (chain A = alpha; NLN 52 + sugars).
ASN52_ALPHA_CHAIN = "A"
ASN52_ALPHA_RESSEQ = 52
ASN52_ANCHOR_RESNAMES = frozenset({"NLN", "ASN"})
# C-terminal gaps in hCG_glycans.pdb desync residue pairing; N-terminal ~100 Cα
# pairs give ~1 Å RMSD and cover the B68/B77 epitope region.
ALIGN_MAX_CA = 100

# Standard polymer residues (+ common PDB variants) — everything else is glycan.
_PROTEIN_RESNAMES = frozenset(
    {
        "ALA",
        "ARG",
        "ASN",
        "ASP",
        "CYS",
        "CYX",
        "GLN",
        "GLU",
        "GLY",
        "HIS",
        "HIE",
        "HID",
        "HIP",
        "ILE",
        "LEU",
        "LYS",
        "MET",
        "PHE",
        "PRO",
        "SER",
        "THR",
        "TRP",
        "TYR",
        "VAL",
    }
)
# NLN is glycosylated Asn — keep its CA for beta alignment (matches model ASN).
_ALIGN_RESNAMES = _PROTEIN_RESNAMES | {"NLN"}

CLASH_CSV_FIELDNAMES = [
    "mpnn_seq_id",
    "method",
    "model_index",
    "structure_path",
    "rmsd",
    "n_glycan_atoms",
    "n_clash_glycan_atoms",
    "clash_fraction",
    "glycans_clashing",
    "superimposed_pdb",
]

RULE_B_CLASH_CSV = Path("filtered_designs") / "glycan_binder_clashes_rule_b.csv"
RULE_AC_CLASH_CSV = Path("filtered_designs") / "glycan_binder_clashes_rule_ac.csv"
RULE_B_ALIGNED_DIR = Path("filtered_designs") / "glycan_aligned_rule_b"
RULE_AC_ALIGNED_DIR = Path("filtered_designs") / "glycan_aligned_rule_ac"


def is_glycan_resname(resname: str) -> bool:
    return resname.strip().upper() not in _PROTEIN_RESNAMES


def _protein_ca_atoms(chain) -> list:
    """Cα atoms for alignment: standard polymer + NLN (skip sugar residues)."""
    out = []
    for res in chain:
        resname = res.get_resname().strip().upper()
        if resname not in _ALIGN_RESNAMES:
            continue
        if "CA" in res:
            out.append(res["CA"])
    return out


def _norm_aa(resname: str) -> str:
    rn = resname.strip().upper()
    if rn == "NLN":
        return "ASN"
    if rn == "CYX":
        return "CYS"
    if rn in {"HIE", "HID", "HIP"}:
        return "HIS"
    return rn


def _paired_ca_atoms(fixed_chain, moving_chain) -> tuple[list, list]:
    """Pair design vs glycans beta Cα by walking both chains in order.

    Skips sugar residues. Advances only the side whose next residue does not
    match when a gap appears (glycans PDB omits some C-terminal residues).
    """
    fixed_res = [
        res
        for res in fixed_chain
        if res.get_resname().strip().upper() in _ALIGN_RESNAMES and "CA" in res
    ]
    moving_res = [
        res
        for res in moving_chain
        if res.get_resname().strip().upper() in _ALIGN_RESNAMES and "CA" in res
    ]
    fixed_ca: list = []
    moving_ca: list = []
    i = j = 0
    while i < len(fixed_res) and j < len(moving_res):
        fi = _norm_aa(fixed_res[i].get_resname())
        mj = _norm_aa(moving_res[j].get_resname())
        if fi == mj:
            fixed_ca.append(fixed_res[i]["CA"])
            moving_ca.append(moving_res[j]["CA"])
            i += 1
            j += 1
            continue
        # Gap in one chain: advance the side that unlocks a match within a short window.
        matched = False
        for di, dj in ((1, 0), (0, 1), (1, 1), (2, 0), (0, 2), (2, 1), (1, 2)):
            ni, nj = i + di, j + dj
            if ni >= len(fixed_res) or nj >= len(moving_res):
                continue
            if _norm_aa(fixed_res[ni].get_resname()) == _norm_aa(
                moving_res[nj].get_resname()
            ):
                i, j = ni, nj
                matched = True
                break
        if not matched:
            break
    return fixed_ca, moving_ca


def _glycan_moving_cache(glycans_path: Path) -> tuple[object, object]:
    """Return ``(glycans_structure, glycans_structure)`` for deepcopy source."""
    glycans = _load_structure(glycans_path)
    model0 = glycans[0]
    if ALIGN_CHAIN_GLYCAN not in model0:
        raise ValueError(
            f"Glycans PDB missing align chain {ALIGN_CHAIN_GLYCAN!r}: {glycans_path}"
        )
    if not _protein_ca_atoms(model0[ALIGN_CHAIN_GLYCAN]):
        raise ValueError(f"No CA atoms on chain {ALIGN_CHAIN_GLYCAN} in {glycans_path}")
    return glycans, glycans


def superimpose_glycans_onto_model(
    model_path: Path,
    *,
    glycans_path: Path = DEFAULT_GLYCAN_PDB,
    glycans_cache: Optional[tuple[object, object]] = None,
) -> tuple[str, float, object]:
    """Align glycans onto design beta; return combined PDB text, RMSD, aligned glycans struct.

    Contents (design coordinate frame)::

        A/B/C — original prediction (binder / beta / alpha), unmoved
        X/Y   — ``hCG_glycans.pdb`` transformed onto design beta (A→X, B→Y)
    """
    from Bio.PDB import PDBIO
    from Bio.PDB.Superimposer import Superimposer

    if glycans_cache is None:
        glycans_cache = _glycan_moving_cache(Path(glycans_path))
    _glycans_src, _ = glycans_cache

    model = _load_structure(Path(model_path))
    model0 = model[0]
    if ALIGN_CHAIN_MODEL not in model0:
        raise ValueError(
            f"Model missing align chain {ALIGN_CHAIN_MODEL!r}: {model_path}"
        )
    if BINDER_CHAIN not in model0:
        raise ValueError(f"Model missing binder chain {BINDER_CHAIN!r}: {model_path}")

    glycans = copy.deepcopy(_glycans_src)
    fixed, moving = _paired_ca_atoms(model0[ALIGN_CHAIN_MODEL], glycans[0][ALIGN_CHAIN_GLYCAN])
    n = min(len(fixed), ALIGN_MAX_CA)
    if n < 10:
        raise ValueError(
            f"Too few CA pairs to align ({n}): for {model_path}"
        )

    sup = Superimposer()
    # Fixed = design beta; moving = glycans beta.
    sup.set_atoms(fixed[:n], moving[:n])
    sup.apply(list(glycans.get_atoms()))
    rms = float(sup.rms)

    io_pdb = PDBIO()
    io_pdb.set_structure(model)
    model_buf = StringIO()
    io_pdb.save(model_buf)
    model_body, serial = _remap_pdb_chains(
        model_buf.getvalue(),
        chain_map={},
        serial_start=0,
    )

    io_gly = PDBIO()
    io_gly.set_structure(glycans)
    gly_buf = StringIO()
    io_gly.save(gly_buf)
    gly_body_raw, _ = _strip_pdb_trailer(gly_buf.getvalue())
    gly_body, _ = _remap_pdb_chains(
        gly_body_raw,
        chain_map=GLYCAN_CHAIN_MAP,
        serial_start=serial,
    )

    combined = model_body
    if not combined.endswith("\n"):
        combined += "\n"
    combined += gly_body
    if not combined.rstrip().endswith("END"):
        combined += "END\n"
    return combined, rms, glycans


def _heavy_coords(atoms) -> np.ndarray:
    coords = [a.coord for a in atoms if getattr(a, "element", "C") != "H"]
    if not coords:
        return np.empty((0, 3), dtype=float)
    return np.asarray(coords, dtype=float)


def _glycan_residue_atoms(glycans_struct) -> list[tuple[str, str, int, object]]:
    """Return ``(cluster_key_seed, resname, resseq, atom)`` for glycan heavy atoms.

    ``cluster_key_seed`` is ``{orig_chain}:{resseq}:{resname}`` before remapping.
    """
    out: list[tuple[str, str, int, object]] = []
    for chain in glycans_struct[0]:
        for res in chain:
            resname = res.get_resname().strip()
            if not is_glycan_resname(resname):
                continue
            resseq = int(res.id[1])
            seed = f"{chain.id}:{resseq}:{resname}"
            for atom in res:
                if atom.element == "H":
                    continue
                out.append((seed, resname, resseq, atom))
    return out


def _cluster_glycan_residues(
    residue_atoms: list[tuple[str, str, int, object]],
    *,
    link_cutoff: float = 2.0,
) -> dict[str, str]:
    """Map residue seed → glycan cluster id via spatial proximity of any atoms."""
    by_seed: dict[str, list[np.ndarray]] = defaultdict(list)
    for seed, _rn, _rs, atom in residue_atoms:
        by_seed[seed].append(np.asarray(atom.coord, dtype=float))
    seeds = sorted(by_seed.keys())
    if not seeds:
        return {}
    parent = {s: s for s in seeds}

    def find(x: str) -> str:
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    def union(a: str, b: str) -> None:
        ra, rb = find(a), find(b)
        if ra != rb:
            parent[rb] = ra

    # Link residues whose any-atom min distance is below cutoff.
    seed_list = seeds
    all_coords: list[np.ndarray] = []
    all_seed_idx: list[int] = []
    for i, s in enumerate(seed_list):
        for c in by_seed[s]:
            all_coords.append(c)
            all_seed_idx.append(i)
    if all_coords:
        tree = cKDTree(np.stack(all_coords))
        for i, j in tree.query_pairs(link_cutoff):
            union(seed_list[all_seed_idx[i]], seed_list[all_seed_idx[j]])

    # Stable cluster labels g0, g1, … ordered by min resseq then chain.
    roots: dict[str, list[str]] = defaultdict(list)
    for s in seeds:
        roots[find(s)].append(s)

    def root_sort_key(items: list[str]) -> tuple:
        parsed = []
        for s in items:
            chain, resseq_s, resname = s.split(":", 2)
            parsed.append((chain, int(resseq_s), resname))
        return min(parsed)

    ordered_roots = sorted(roots.keys(), key=lambda r: root_sort_key(roots[r]))
    seed_to_cluster: dict[str, str] = {}
    for gi, root in enumerate(ordered_roots):
        label = f"g{gi}"
        for s in roots[root]:
            seed_to_cluster[s] = label
    return seed_to_cluster


def _asn52_alpha_anchor_seed(
    residue_atoms: list[tuple[str, str, int, object]],
) -> Optional[str]:
    """Seed for α-Asn52 (``A:52:NLN`` / ``ASN``) among glycan residue atoms."""
    for seed, rn, rs, _atom in residue_atoms:
        chain = seed.split(":", 1)[0]
        if (
            chain == ASN52_ALPHA_CHAIN
            and rs == ASN52_ALPHA_RESSEQ
            and rn.strip().upper() in ASN52_ANCHOR_RESNAMES
        ):
            return seed
    return None


def _asn52_alpha_cluster_label(
    residue_atoms: list[tuple[str, str, int, object]],
    seed_to_cluster: dict[str, str],
    glycans_struct,
) -> Optional[str]:
    """Cluster id for the α-Asn52 N-glycan (anchor residue or nearest sugar)."""
    anchor = _asn52_alpha_anchor_seed(residue_atoms)
    if anchor is not None:
        return seed_to_cluster.get(anchor)

    # Fallback: ND2 of A:52 → nearest glycan heavy atom's cluster.
    nd2 = None
    model0 = glycans_struct[0]
    if ASN52_ALPHA_CHAIN in model0:
        for res in model0[ASN52_ALPHA_CHAIN]:
            if int(res.id[1]) != ASN52_ALPHA_RESSEQ:
                continue
            if res.get_resname().strip().upper() not in ASN52_ANCHOR_RESNAMES:
                continue
            if "ND2" in res:
                nd2 = np.asarray(res["ND2"].coord, dtype=float)
            break
    if nd2 is None or not residue_atoms:
        return None
    best_seed = None
    best_d = float("inf")
    for seed, _rn, _rs, atom in residue_atoms:
        d = float(np.linalg.norm(np.asarray(atom.coord, dtype=float) - nd2))
        if d < best_d:
            best_d = d
            best_seed = seed
    return seed_to_cluster.get(best_seed) if best_seed else None


def glycan_binder_clash_stats(
    model_path: Path,
    aligned_glycans_struct,
    *,
    binder_chain: str = BINDER_CHAIN,
    cutoff: float = CLASH_CUTOFF_A,
) -> dict[str, object]:
    """Clash stats for α-Asn52 glycan vs binder in ``model_path`` (same frame).

    ``clash_fraction`` is the per-glycan share: clashing heavy atoms / all heavy
    atoms on the α-Asn52 N-glycan only (other glycans ignored).
    """
    model = _load_structure(Path(model_path))
    if binder_chain not in model[0]:
        raise ValueError(f"Binder chain {binder_chain!r} missing in {model_path}")
    binder_coords = _heavy_coords(model[0][binder_chain].get_atoms())
    residue_atoms = _glycan_residue_atoms(aligned_glycans_struct)
    if not residue_atoms:
        return {
            "n_glycan_atoms": 0,
            "n_clash_glycan_atoms": 0,
            "clash_fraction": 0.0,
            "glycans_clashing": "",
        }

    seed_to_cluster = _cluster_glycan_residues(residue_atoms)
    target = _asn52_alpha_cluster_label(
        residue_atoms, seed_to_cluster, aligned_glycans_struct
    )
    if target is None:
        return {
            "n_glycan_atoms": 0,
            "n_clash_glycan_atoms": 0,
            "clash_fraction": 0.0,
            "glycans_clashing": "",
        }

    # The anchor NLN/ASN identifies the glycan but is a protein residue, so do
    # not include its atoms in the glycan clash numerator or denominator.
    selected = [
        (seed, rn, rs, atom)
        for seed, rn, rs, atom in residue_atoms
        if seed_to_cluster.get(seed) == target
        and not (
            seed.split(":", 1)[0] == ASN52_ALPHA_CHAIN
            and rs == ASN52_ALPHA_RESSEQ
            and rn.strip().upper() in ASN52_ANCHOR_RESNAMES
        )
    ]
    n_glycan = len(selected)
    if n_glycan == 0:
        return {
            "n_glycan_atoms": 0,
            "n_clash_glycan_atoms": 0,
            "clash_fraction": 0.0,
            "glycans_clashing": "",
        }
    if binder_coords.size == 0:
        return {
            "n_glycan_atoms": n_glycan,
            "n_clash_glycan_atoms": 0,
            "clash_fraction": 0.0,
            "glycans_clashing": f"Asn52_alpha:0/{n_glycan}",
        }

    gly_coords = np.asarray([a.coord for *_, a in selected], dtype=float)
    binder_tree = cKDTree(binder_coords)
    dists, _ = binder_tree.query(gly_coords, k=1)
    n_clash = int(np.count_nonzero(dists < cutoff))
    return {
        "n_glycan_atoms": n_glycan,
        "n_clash_glycan_atoms": n_clash,
        "clash_fraction": (n_clash / n_glycan) if n_glycan else 0.0,
        "glycans_clashing": f"Asn52_alpha:{n_clash}/{n_glycan}",
    }


def _model_index(row: dict[str, str]) -> str:
    return (row.get("model_index") or "").strip()


def _is_successful_row(row: dict[str, str], method: str) -> bool:
    if method == "af3":
        return (row.get("specificity_hotspots") or "").strip() == SPEC_REQUIRED
    return (row.get(f"specificity_hotspots_{method}") or "").strip() == SPEC_REQUIRED


def _aligned_pdb_name(method: str, src: Path, model_index: str) -> str:
    stem = src.stem
    for suffix in ("_superimposed", "_with_glycans"):
        if stem.endswith(suffix):
            stem = stem[: -len(suffix)]
            break
    idx = model_index if model_index else "x"
    return f"{method}_{stem}_m{idx}_superimposed.pdb"


def process_model(
    row: dict[str, str],
    *,
    method: str,
    out_dir: Path,
    glycans_path: Path,
    glycans_cache: tuple[object, object],
) -> Optional[dict[str, str]]:
    """Align + clash one successful model; write superimposed PDB; return CSV row."""
    src = _structure_path(row, method)
    if src is None or not src.is_file():
        return None
    sid = (row.get("mpnn_seq_id") or "").strip()
    if not sid:
        return None
    mi = _model_index(row)
    try:
        pdb_text, rms, aligned_gly = superimpose_glycans_onto_model(
            src,
            glycans_path=glycans_path,
            glycans_cache=glycans_cache,
        )
        stats = glycan_binder_clash_stats(src, aligned_gly)
    except Exception as exc:  # noqa: BLE001
        print(f"[warn] glycan clash failed for {method} {src}: {exc}")
        return None

    out_dir.mkdir(parents=True, exist_ok=True)
    out_pdb = out_dir / _aligned_pdb_name(method, src, mi)
    out_pdb.write_text(pdb_text, encoding="utf-8")
    return {
        "mpnn_seq_id": sid,
        "method": method,
        "model_index": mi,
        "structure_path": str(src),
        "rmsd": f"{rms:.4f}",
        "n_glycan_atoms": str(stats["n_glycan_atoms"]),
        "n_clash_glycan_atoms": str(stats["n_clash_glycan_atoms"]),
        "clash_fraction": f"{float(stats['clash_fraction']):.6f}",
        "glycans_clashing": str(stats["glycans_clashing"]),
        "superimposed_pdb": str(out_pdb),
    }


def _write_clash_csv(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=CLASH_CSV_FIELDNAMES)
        writer.writeheader()
        for row in rows:
            writer.writerow({k: row.get(k, "") for k in CLASH_CSV_FIELDNAMES})


def _reset_aligned_dir(path: Path) -> None:
    if path.is_dir():
        shutil.rmtree(path)
    path.mkdir(parents=True, exist_ok=True)


def process_rule_b_partition(
    partition_root: Path,
    *,
    glycans_pdb: Path = DEFAULT_GLYCAN_PDB,
) -> dict:
    """Score successful Boltz/ESMFold models from Rule B filtered metrics."""
    partition_root = Path(partition_root)
    metrics = _read_metrics(partition_root / RULE_B_METRICS)
    out_csv = partition_root / RULE_B_CLASH_CSV
    out_dir = partition_root / RULE_B_ALIGNED_DIR
    return _process_methods(
        metrics_by_method={
            "boltz": [r for r in metrics if _is_successful_row(r, "boltz")],
            "esmfold": [r for r in metrics if _is_successful_row(r, "esmfold")],
        },
        out_csv=out_csv,
        out_dir=out_dir,
        glycans_pdb=Path(glycans_pdb),
        track="rule_b",
    )


def process_rule_ac_partition(
    partition_root: Path,
    *,
    glycans_pdb: Path = DEFAULT_GLYCAN_PDB,
) -> dict:
    """Score successful AF3 + Boltz/ESMFold for AF3-successful sequences."""
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
    out_csv = partition_root / RULE_AC_CLASH_CSV
    out_dir = partition_root / RULE_AC_ALIGNED_DIR
    return _process_methods(
        metrics_by_method={
            "af3": af3_rows,
            "boltz": [r for r in hcg_rows if _is_successful_row(r, "boltz")],
            "esmfold": [r for r in hcg_rows if _is_successful_row(r, "esmfold")],
        },
        out_csv=out_csv,
        out_dir=out_dir,
        glycans_pdb=Path(glycans_pdb),
        track="rule_ac",
    )


def _read_metrics(path: Path) -> list[dict[str, str]]:
    if not path.is_file():
        return []
    with path.open(newline="", encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def _process_methods(
    *,
    metrics_by_method: dict[str, list[dict[str, str]]],
    out_csv: Path,
    out_dir: Path,
    glycans_pdb: Path,
    track: str,
) -> dict:
    if not glycans_pdb.is_file():
        raise FileNotFoundError(f"Glycans PDB not found: {glycans_pdb}")
    glycans_cache = _glycan_moving_cache(glycans_pdb)
    _reset_aligned_dir(out_dir)
    rows: list[dict[str, str]] = []
    for method, method_rows in metrics_by_method.items():
        for row in method_rows:
            result = process_model(
                row,
                method=method,
                out_dir=out_dir,
                glycans_path=glycans_pdb,
                glycans_cache=glycans_cache,
            )
            if result is not None:
                rows.append(result)
    _write_clash_csv(out_csv, rows)
    return {
        "track": track,
        "clash_csv": str(out_csv),
        "aligned_dir": str(out_dir),
        "row_count": len(rows),
        "glycans_pdb": str(glycans_pdb.resolve()),
        "by_method": {
            method: sum(1 for r in rows if r["method"] == method)
            for method in metrics_by_method
        },
    }


def load_clash_rows(partition_or_csv: Path) -> list[dict[str, str]]:
    """Load clash CSV from a path or from ``filtered_designs/`` under a partition."""
    path = Path(partition_or_csv)
    if path.is_dir():
        for rel in (RULE_B_CLASH_CSV, RULE_AC_CLASH_CSV):
            cand = path / rel
            if cand.is_file():
                path = cand
                break
    if not path.is_file():
        return []
    with path.open(newline="", encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def mean_clash_fraction(
    clash_rows: list[dict[str, str]],
    *,
    mpnn_seq_id: str,
    method: str,
) -> Optional[float]:
    """Mean α-Asn52 ``clash_fraction`` over successful models for one sequence + method."""
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


def lookup_precomputed_superimposed(
    clash_rows: list[dict[str, str]],
    *,
    structure_path: Path,
    method: str,
    model_index: str = "",
) -> Optional[Path]:
    """Return precomputed superimposed PDB path if present in clash CSV."""
    src = Path(structure_path)
    src_str = str(src)
    src_name = src.name
    mi = (model_index or "").strip()
    for row in clash_rows:
        if (row.get("method") or "").strip() != method:
            continue
        row_mi = (row.get("model_index") or "").strip()
        if mi and row_mi and row_mi != mi:
            continue
        row_src = (row.get("structure_path") or "").strip()
        if not row_src:
            continue
        if row_src != src_str and Path(row_src).name != src_name:
            continue
        pdb = Path((row.get("superimposed_pdb") or "").strip())
        if pdb.is_file():
            return pdb
    return None
