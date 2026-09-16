"""Export zip archives of presentation designs after Rule B / Rule A/C decks.

A sequence is exported when it would appear on the Marp deck for that track:

* **Rule B** — ``filtered_metrics_rule_b.csv`` sequences with ≥1 successful
  Boltz **and** ≥1 successful ESMFold model (``specificity_hotspots_* == 2``
  with an on-disk structure), matching ``rule_b_presentation``.
* **Rule A/C** — every ``mpnn_seq_id`` in ``AF3_successful_sequences.csv``,
  matching ``rule_ac_presentation``.

Each model is packed as **one** PDB with the prediction and ``hCG_glycans.pdb``
superimposed in the design frame (glycans beta Cα → design beta Cα):
  - chains A/B/C — prediction (binder / beta / alpha), unmoved
  - chains X/Y   — glycosylated target aligned onto design beta (A→X, B→Y)

Prefer precomputed files from ``glycan_aligned_rule_{b,ac}/`` when present.

Zip layout (per sequence)::

    {partition}/{mpnn_seq_id}/
        sequence.fasta
        metrics.csv
        boltz/*_superimposed.pdb
        esmfold/*_superimposed.pdb
        af3/*_superimposed.pdb      # Rule A/C only
        AF3_metrics.csv             # Rule A/C only

Plus a root ``manifest.csv`` listing every exported sequence.
"""

from __future__ import annotations

import csv
import tempfile
import zipfile
from collections import defaultdict
from io import StringIO
from pathlib import Path
from typing import Optional

SPEC_REQUIRED = "2"

DEFAULT_GLYCAN_PDB = Path("/storage/hCG/hCG_glycans.pdb")
ALIGN_CHAIN_MODEL = "B"
ALIGN_CHAIN_GLYCAN = "B"
# Glycans chains remapped so they can sit beside prediction A/B/C in one PDB.
GLYCAN_CHAIN_MAP = {"A": "X", "B": "Y"}

RULE_B_METRICS = Path("filtered_designs") / "filtered_metrics_rule_b.csv"
RULE_B_LH = Path("filtered_designs") / "LH_binding.csv"
RULE_AC_METRICS = Path("filtered_designs") / "filtered_metrics_rule_ac.csv"
RULE_AC_LH = Path("filtered_designs") / "LH_binding_rule_ac.csv"
RULE_AC_SUCCESS = Path("filtered_designs") / "AF3_successful_sequences.csv"
RULE_AC_AF3_METRICS = Path("filtered_designs") / "AF3_metrics.csv"


def _read_csv(path: Path) -> list[dict[str, str]]:
    if not path.is_file():
        return []
    with path.open(newline="", encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def _spec_value(raw: Optional[str]) -> Optional[str]:
    text = (raw or "").strip()
    if not text or text.startswith("ERROR"):
        return None
    return text


def _has_successful_model(rows: list[dict[str, str]], method: str) -> bool:
    """True if ≥1 row has specificity==2 and an on-disk structure (deck gate)."""
    col = f"specificity_hotspots_{method}"
    for row in rows:
        if (row.get(col) or "").strip() != SPEC_REQUIRED:
            continue
        if _structure_path(row, method) is not None:
            return True
    return False


def presentation_rule_b_seq_ids(metrics_rows: list[dict[str, str]]) -> set[str]:
    """Rule B deck sequences: ≥1 successful Boltz and ≥1 successful ESMFold."""
    by_seq: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in metrics_rows:
        sid = (row.get("mpnn_seq_id") or "").strip()
        if sid:
            by_seq[sid].append(row)
    return {
        sid
        for sid, rows in by_seq.items()
        if _has_successful_model(rows, "boltz") and _has_successful_model(rows, "esmfold")
    }


def presentation_rule_ac_seq_ids(success_rows: list[dict[str, str]]) -> set[str]:
    """Rule A/C deck sequences: unique ids from AF3_successful_sequences.csv."""
    return {
        (row.get("mpnn_seq_id") or "").strip()
        for row in success_rows
        if (row.get("mpnn_seq_id") or "").strip()
    }


def lh_zero_specificity_seq_ids(lh_rows: list[dict[str, str]]) -> set[str]:
    """Return ``mpnn_seq_id``s with scored Boltz+ESMFold LH models and 0× specificity 2.

    Kept for ad-hoc analysis; zip export no longer filters on this.
    """
    by_seq: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in lh_rows:
        sid = (row.get("mpnn_seq_id") or "").strip()
        if sid:
            by_seq[sid].append(row)

    out: set[str] = set()
    for sid, rows in by_seq.items():
        scored_b = scored_e = ok_b = ok_e = 0
        for row in rows:
            b = _spec_value(row.get("specificity_hotspots_boltz"))
            e = _spec_value(row.get("specificity_hotspots_esmfold"))
            if b is not None:
                scored_b += 1
                if b == SPEC_REQUIRED:
                    ok_b += 1
            if e is not None:
                scored_e += 1
                if e == SPEC_REQUIRED:
                    ok_e += 1
        if scored_b > 0 and scored_e > 0 and ok_b == 0 and ok_e == 0:
            out.add(sid)
    return out


def _first_existing(row: dict[str, str], *keys: str) -> Optional[Path]:
    """Return the first existing file path among ``row[keys]``."""
    for key in keys:
        raw = (row.get(key) or "").strip()
        if not raw:
            continue
        path = Path(raw)
        if path.is_file():
            return path
    return None


def _structure_path(row: dict[str, str], method: str) -> Optional[Path]:
    """One structure file per model: prefer PDB, else CIF."""
    if method == "af3":
        return _first_existing(row, "structure_pdb", "structure_cif")
    return _first_existing(row, f"structure_pdb_{method}", f"structure_cif_{method}")


def _binder_seq(rows: list[dict[str, str]]) -> str:
    for key in ("binder_seq", "binder_seq_boltz", "binder_seq_esmfold"):
        for row in rows:
            seq = (row.get(key) or "").strip()
            if seq:
                return seq
    return ""


def _load_structure(path: Path):
    """Load PDB or mmCIF via Biopython."""
    from Bio.PDB import MMCIFParser, PDBParser

    path = Path(path)
    suffix = path.suffix.lower()
    if suffix in {".cif", ".mmcif"}:
        return MMCIFParser(QUIET=True).get_structure(path.stem, str(path))
    return PDBParser(QUIET=True).get_structure(path.stem, str(path))


def _ca_atoms(chain) -> list:
    return [res["CA"] for res in chain if "CA" in res]


def _glycan_ca_cache(glycans_path: Path) -> tuple[object, object]:
    """Return ``(glycans_structure, glycans_structure)`` for align cache."""
    from dagster_pipeline.glycan_binder_clashes import _glycan_moving_cache

    return _glycan_moving_cache(Path(glycans_path))


def _strip_pdb_trailer(pdb_text: str) -> tuple[str, int]:
    """Drop END/CONECT/MASTER trailers; return body text and max atom serial."""
    body: list[str] = []
    max_serial = 0
    for line in pdb_text.splitlines(True):
        rec = line[:6].strip()
        if rec in {"END", "CONECT", "MASTER", "ENDMDL"}:
            continue
        if line.startswith(("ATOM", "HETATM")):
            try:
                max_serial = max(max_serial, int(line[6:11]))
            except ValueError:
                pass
        body.append(line)
    return "".join(body), max_serial


def _remap_pdb_chains(
    pdb_text: str,
    *,
    chain_map: dict[str, str],
    serial_start: int = 0,
) -> tuple[str, int]:
    """Remap chain IDs and renumber atom serials. Returns (text, last_serial)."""
    out: list[str] = []
    serial = serial_start
    for line in pdb_text.splitlines(True):
        if line.startswith("END"):
            continue
        if line.startswith(("ATOM", "HETATM")):
            serial += 1
            old = line[21] if len(line) > 21 else " "
            new = chain_map.get(old, old)
            line = f"{line[:6]}{serial:5d}{line[11:21]}{new}{line[22:]}"
        elif line.startswith("TER"):
            serial += 1
            if len(line) > 21:
                old = line[21]
                new = chain_map.get(old, old)
                resname = line[17:20] if len(line) > 20 else "   "
                resseq = line[22:26] if len(line) > 26 else "    "
                line = f"TER   {serial:5d}      {resname}{new}{resseq}\n"
            else:
                line = f"TER   {serial:5d}\n"
        out.append(line if line.endswith("\n") else line + "\n")
    return "".join(out), serial


def superimpose_model_with_glycans_pdb(
    model_path: Path,
    *,
    glycans_path: Path = DEFAULT_GLYCAN_PDB,
    glycans_cache: Optional[tuple[object, object]] = None,
) -> tuple[str, float]:
    """Align glycans onto design beta; return one PDB with both structures + RMSD.

    Contents (design coordinate frame)::

        A/B/C — prediction (binder / beta / alpha), unmoved
        X/Y   — ``hCG_glycans.pdb`` transformed onto design beta (A→X, B→Y)
    """
    from dagster_pipeline.glycan_binder_clashes import superimpose_glycans_onto_model

    pdb_text, rms, _aligned = superimpose_glycans_onto_model(
        model_path,
        glycans_path=glycans_path,
        glycans_cache=glycans_cache,
    )
    return pdb_text, rms


# Back-compat alias used by earlier revisions / notebooks.
align_model_onto_glycans_pdb = superimpose_model_with_glycans_pdb


def _superimposed_arcname(method: str, src: Path, prefix: str) -> str:
    stem = src.stem
    for suffix in ("_superimposed", "_with_glycans"):
        if stem.endswith(suffix):
            stem = stem[: -len(suffix)]
            break
    return f"{prefix}/{method}/{stem}_superimposed.pdb"


def _pack_predictor_models(
    zf: zipfile.ZipFile,
    *,
    rows: list[dict[str, str]],
    method: str,
    prefix: str,
    seen: set[str],
    glycans_path: Path,
    glycans_cache: tuple[object, object],
    tmp_dir: Path,
    clash_rows: Optional[list[dict[str, str]]] = None,
    successful_only: bool = True,
) -> int:
    """Pack one superimposed PDB (prediction + glycans) per model row."""
    from dagster_pipeline.glycan_binder_clashes import lookup_precomputed_superimposed

    count = 0
    for row in rows:
        if successful_only:
            if method == "af3":
                if (row.get("specificity_hotspots") or "").strip() != SPEC_REQUIRED:
                    continue
            elif (row.get(f"specificity_hotspots_{method}") or "").strip() != SPEC_REQUIRED:
                continue
        src = _structure_path(row, method)
        if src is None or not src.is_file():
            continue
        arc = _superimposed_arcname(method, src, prefix)
        if arc in seen:
            continue
        out_path = tmp_dir / f"{method}_{src.stem}_superimposed.pdb"
        precomputed = None
        if clash_rows:
            precomputed = lookup_precomputed_superimposed(
                clash_rows,
                structure_path=src,
                method=method,
                model_index=(row.get("model_index") or "").strip(),
            )
        if precomputed is not None:
            out_path.write_text(precomputed.read_text(encoding="utf-8"), encoding="utf-8")
        else:
            try:
                pdb_text, _rms = superimpose_model_with_glycans_pdb(
                    src,
                    glycans_path=glycans_path,
                    glycans_cache=glycans_cache,
                )
            except Exception as exc:  # noqa: BLE001 — keep packing other models
                print(f"[warn] glycans superimpose failed for {src}: {exc}")
                continue
            print(f"[warn] no precomputed glycan PDB for {src}; aligned on the fly")
            out_path.write_text(pdb_text, encoding="utf-8")
        zf.write(out_path, arc)
        seen.add(arc)
        count += 1
    return count


def _pack_sequence(
    zf: zipfile.ZipFile,
    *,
    partition_name: str,
    mpnn_seq_id: str,
    metrics_rows: list[dict[str, str]],
    metrics_fieldnames: list[str],
    af3_rows: Optional[list[dict[str, str]]] = None,
    af3_fieldnames: Optional[list[str]] = None,
    include_af3: bool = False,
    glycans_path: Path = DEFAULT_GLYCAN_PDB,
    glycans_cache: Optional[tuple[object, object]] = None,
    tmp_dir: Optional[Path] = None,
    clash_rows: Optional[list[dict[str, str]]] = None,
) -> dict[str, object]:
    """Write one sequence's files into the zip; return manifest fields."""
    prefix = f"{partition_name}/{mpnn_seq_id}"
    seen: set[str] = set()
    binder = _binder_seq(metrics_rows)
    if not binder and af3_rows:
        binder = _binder_seq(af3_rows)

    fasta_arc = f"{prefix}/sequence.fasta"
    fasta_body = f">{mpnn_seq_id}\n{binder}\n" if binder else f">{mpnn_seq_id}\n"
    zf.writestr(fasta_arc, fasta_body)
    seen.add(fasta_arc)

    if metrics_rows and metrics_fieldnames:
        buf = StringIO()
        writer = csv.DictWriter(buf, fieldnames=metrics_fieldnames, extrasaction="ignore")
        writer.writeheader()
        for row in metrics_rows:
            writer.writerow({k: row.get(k, "") for k in metrics_fieldnames})
        zf.writestr(f"{prefix}/metrics.csv", buf.getvalue())

    if glycans_cache is None:
        glycans_cache = _glycan_ca_cache(Path(glycans_path))
    if tmp_dir is None:
        raise ValueError("tmp_dir is required for glycans-aligned packing")

    n_boltz = _pack_predictor_models(
        zf,
        rows=metrics_rows,
        method="boltz",
        prefix=prefix,
        seen=seen,
        glycans_path=Path(glycans_path),
        glycans_cache=glycans_cache,
        tmp_dir=tmp_dir,
        clash_rows=clash_rows,
    )
    n_esm = _pack_predictor_models(
        zf,
        rows=metrics_rows,
        method="esmfold",
        prefix=prefix,
        seen=seen,
        glycans_path=Path(glycans_path),
        glycans_cache=glycans_cache,
        tmp_dir=tmp_dir,
        clash_rows=clash_rows,
    )
    n_af3 = 0
    if include_af3 and af3_rows is not None:
        if af3_fieldnames:
            buf = StringIO()
            writer = csv.DictWriter(buf, fieldnames=af3_fieldnames, extrasaction="ignore")
            writer.writeheader()
            for row in af3_rows:
                writer.writerow({k: row.get(k, "") for k in af3_fieldnames})
            zf.writestr(f"{prefix}/AF3_metrics.csv", buf.getvalue())
        n_af3 = _pack_predictor_models(
            zf,
            rows=af3_rows,
            method="af3",
            prefix=prefix,
            seen=seen,
            glycans_path=Path(glycans_path),
            glycans_cache=glycans_cache,
            tmp_dir=tmp_dir,
            clash_rows=clash_rows,
        )

    return {
        "partition": partition_name,
        "mpnn_seq_id": mpnn_seq_id,
        "binder_seq_len": len(binder),
        "n_metrics_rows": len(metrics_rows),
        "n_boltz_models": n_boltz,
        "n_esmfold_models": n_esm,
        "n_af3_models": n_af3,
    }


def build_rule_b_successful_zip(
    run_dirs: list[Path],
    zip_path: Path,
    *,
    glycans_pdb: Path = DEFAULT_GLYCAN_PDB,
) -> dict:
    """Build Rule B zip of presentation sequences under ``zip_path``."""
    return _build_track_zip(
        run_dirs,
        zip_path,
        track="rule_b",
        metrics_rel=RULE_B_METRICS,
        candidate_rel=RULE_B_METRICS,
        include_af3=False,
        glycans_pdb=Path(glycans_pdb),
    )


def build_rule_ac_successful_zip(
    run_dirs: list[Path],
    zip_path: Path,
    *,
    glycans_pdb: Path = DEFAULT_GLYCAN_PDB,
) -> dict:
    """Build Rule A/C zip of presentation (AF3-successful) sequences under ``zip_path``."""
    return _build_track_zip(
        run_dirs,
        zip_path,
        track="rule_ac",
        metrics_rel=RULE_AC_METRICS,
        candidate_rel=RULE_AC_SUCCESS,
        include_af3=True,
        glycans_pdb=Path(glycans_pdb),
    )


def _build_track_zip(
    run_dirs: list[Path],
    zip_path: Path,
    *,
    track: str,
    metrics_rel: Path,
    candidate_rel: Path,
    include_af3: bool,
    glycans_pdb: Path,
) -> dict:
    zip_path = Path(zip_path).resolve()
    zip_path.parent.mkdir(parents=True, exist_ok=True)
    if zip_path.is_file():
        zip_path.unlink()

    from dagster_pipeline.glycan_binder_clashes import (
        RULE_AC_CLASH_CSV,
        RULE_B_CLASH_CSV,
        load_clash_rows,
    )

    glycans_pdb = Path(glycans_pdb)
    if not glycans_pdb.is_file():
        raise FileNotFoundError(f"Glycans target PDB not found: {glycans_pdb}")
    glycans_cache = _glycan_ca_cache(glycans_pdb)
    clash_rel = RULE_AC_CLASH_CSV if include_af3 else RULE_B_CLASH_CSV

    manifest_rows: list[dict[str, object]] = []
    partitions_with_exports = 0

    with tempfile.TemporaryDirectory(prefix="succ_glycans_") as tmp:
        tmp_dir = Path(tmp)
        with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
            for run_dir in (Path(p).resolve() for p in run_dirs):
                metrics_rows = _read_csv(run_dir / metrics_rel)
                candidate_rows = _read_csv(run_dir / candidate_rel)
                if not candidate_rows:
                    continue
                # Rule B metrics == candidates; Rule AC needs AF3 success CSV +
                # metrics when packing Boltz/ESMFold (metrics may be empty).
                if not include_af3 and not metrics_rows:
                    continue

                if include_af3:
                    export_ids = presentation_rule_ac_seq_ids(candidate_rows)
                else:
                    export_ids = presentation_rule_b_seq_ids(metrics_rows)
                if not export_ids:
                    continue

                clash_rows = load_clash_rows(run_dir / clash_rel)

                metrics_by_id: dict[str, list[dict[str, str]]] = defaultdict(list)
                for row in metrics_rows:
                    sid = (row.get("mpnn_seq_id") or "").strip()
                    if sid in export_ids:
                        metrics_by_id[sid].append(row)
                metrics_fields = list(metrics_rows[0].keys()) if metrics_rows else []

                af3_by_id: dict[str, list[dict[str, str]]] = defaultdict(list)
                af3_fields: list[str] = []
                if include_af3:
                    af3_rows = _read_csv(run_dir / RULE_AC_AF3_METRICS)
                    if af3_rows:
                        af3_fields = list(af3_rows[0].keys())
                        for row in af3_rows:
                            sid = (row.get("mpnn_seq_id") or "").strip()
                            if sid in export_ids:
                                af3_by_id[sid].append(row)

                partitions_with_exports += 1
                for sid in sorted(export_ids):
                    seq_metrics = metrics_by_id.get(sid, [])
                    if not seq_metrics and not include_af3:
                        continue
                    if not seq_metrics:
                        cand = next(
                            (
                                r
                                for r in candidate_rows
                                if (r.get("mpnn_seq_id") or "").strip() == sid
                            ),
                            None,
                        )
                        if cand is not None:
                            seq_metrics = [cand]
                            if not metrics_fields:
                                metrics_fields = list(cand.keys())

                    info = _pack_sequence(
                        zf,
                        partition_name=run_dir.name,
                        mpnn_seq_id=sid,
                        metrics_rows=seq_metrics,
                        metrics_fieldnames=metrics_fields,
                        af3_rows=af3_by_id.get(sid, []),
                        af3_fieldnames=af3_fields,
                        include_af3=include_af3,
                        glycans_path=glycans_pdb,
                        glycans_cache=glycans_cache,
                        tmp_dir=tmp_dir,
                        clash_rows=clash_rows,
                    )
                    info["track"] = track
                    manifest_rows.append(info)

            if manifest_rows:
                fields = [
                    "track",
                    "partition",
                    "mpnn_seq_id",
                    "binder_seq_len",
                    "n_metrics_rows",
                    "n_boltz_models",
                    "n_esmfold_models",
                    "n_af3_models",
                ]
                buf = StringIO()
                writer = csv.DictWriter(buf, fieldnames=fields, extrasaction="ignore")
                writer.writeheader()
                for row in manifest_rows:
                    writer.writerow(row)
                zf.writestr("manifest.csv", buf.getvalue())
                zf.writestr(
                    "glycans_target.txt",
                    f"Superimposed with: {glycans_pdb.resolve()}\n"
                    f"Align: glycans chain {ALIGN_CHAIN_GLYCAN} CA → "
                    f"model chain {ALIGN_CHAIN_MODEL} CA (sequence order)\n"
                    f"Chains A/B/C = prediction; "
                    f"X/Y = glycans aligned onto design beta (A→X, B→Y)\n"
                    f"Sequences match the Rule {'A/C' if include_af3 else 'B'} presentation deck.\n",
                )

    if not manifest_rows and zip_path.is_file():
        zip_path.unlink()

    return {
        "zip_path": str(zip_path) if manifest_rows else "",
        "sequence_count": len(manifest_rows),
        "partition_count": partitions_with_exports,
        "track": track,
        "glycans_pdb": str(glycans_pdb.resolve()),
        "manifest_rows": manifest_rows,
    }
