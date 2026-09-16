"""Build a Rule A/C AF3 → LH off-target Marp presentation.

One slide per AF3-successful ``mpnn_seq_id``. Columns:
  - AF3 always (from ``AF3_metrics.csv`` specificity==2 models)
  - Boltz2 only if that sequence has ≥1 hCG Boltz model at specificity==2
  - ESMFold only if that sequence has ≥1 hCG ESMFold model at specificity==2

LH specificity counts come from ``LH_binding_rule_ac.csv`` (same line as Rule B).
"""

from __future__ import annotations

import csv
import random
from pathlib import Path
from typing import Optional

from dagster_pipeline.rule_b_presentation import (
    DEFAULT_MARP_DOCKER_IMAGE,
    DEFAULT_PYMOL,
    DEFAULT_SCAFFOLDS_DIR,
    MODELS_PER_SEQ,
    avg_metrics,
    binder_aas_contact_b77,
    designed_residues_1based,
    detect_binder_chain,
    export_marp_pdf,
    export_marp_pptx,
    filter_successful,
    fmt,
    fmt_aas,
    lh_spec_counts_for_seq,
    load_design_fragment,
    parse_run_folder,
    render_pymol,
    resi_list_to_pymol,
)


def _rows(path: Path) -> list[dict[str, str]]:
    if not path.is_file():
        return []
    with path.open(encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def _mean(rows: list[dict[str, str]], column: str) -> Optional[float]:
    values: list[float] = []
    for row in rows:
        try:
            values.append(float(row.get(column) or ""))
        except ValueError:
            pass
    return sum(values) / len(values) if values else None


def _render(
    row: dict[str, str],
    *,
    method: str,
    assets_dir: Path,
    stem: str,
    pymol: Path,
    binder_seq: str,
    designed_resi_sel: str,
    ref_pdb: Optional[Path],
    view: Optional[str],
) -> tuple[Optional[Path], Optional[Path], Optional[str]]:
    """Return (png, new_ref_pdb, new_view)."""
    path_key = "structure_pdb" if method == "af3" else f"structure_pdb_{method}"
    structure = Path(row.get(path_key) or "")
    if not structure.is_file() or not binder_seq:
        return None, ref_pdb, view
    output = assets_dir / f"{stem}_{method}.png"
    try:
        used = render_pymol(
            structure,
            output,
            pymol,
            binder_chain=detect_binder_chain(structure, binder_seq),
            designed_resi_sel=designed_resi_sel,
            ref_pdb=ref_pdb,
            view=view,
        )
    except Exception as exc:  # Deck remains useful when a model cannot render.
        print(f"[warn] could not render {structure}: {exc}")
        return None, ref_pdb, view
    new_ref = structure if ref_pdb is None else ref_pdb
    new_view = used if view is None else view
    return output, new_ref, new_view


def _hcg_method_line(
    label: str,
    ok_rows: list[dict],
    method: str,
    *,
    mean_glycan_clash: Optional[float] = None,
) -> str:
    metrics = avg_metrics(ok_rows, method) if ok_rows else {
        "iptm": None,
        "iptm_beta": None,
        "iptm_alpha": None,
        "ipsae_beta": None,
        "ipsae_alpha": None,
        "binder_score": None,
        "interface_dg": None,
    }
    shown = (ok_rows[0].get("model_index") or "?") if ok_rows else "?"
    return (
        f"**{label}** n={len(ok_rows)}/{MODELS_PER_SEQ} · shown m{shown} · "
        f"mean binder score {fmt(metrics['binder_score'], 1)} · "
        f"mean global ipTM {fmt(metrics['iptm'])} · "
        f"mean ipTM beta {fmt(metrics['iptm_beta'])} · "
        f"mean ipTM alpha {fmt(metrics['iptm_alpha'])} · "
        f"mean ipSAE beta {fmt(metrics['ipsae_beta'])} · "
        f"mean ipSAE alpha {fmt(metrics['ipsae_alpha'])} · "
        f"mean interface_dG {fmt(metrics['interface_dg'], 1)} · "
        f"mean glycan–binder clash {fmt(mean_glycan_clash)}"
    )


def _slide(
    *,
    title: str,
    sequence_id: str,
    af3_rows: list[dict[str, str]],
    boltz_ok: list[dict],
    esm_ok: list[dict],
    lh_line: str,
    b77_line: str,
    images: list[tuple[str, Optional[Path]]],
    assets_dir: Path,
    mean_glycan_clash_af3: Optional[float] = None,
    mean_glycan_clash_boltz: Optional[float] = None,
    mean_glycan_clash_esm: Optional[float] = None,
) -> str:
    columns = []
    for label, image in images:
        content = (
            f"![]({(Path(assets_dir.name) / image.name).as_posix()})"
            if image
            else "_structure unavailable_"
        )
        columns.append(f"<div>\n\n**{label}**\n\n{content}\n\n</div>")
    af3_iptm = _mean(af3_rows, "iptm")
    lines = [
        f"# {title} · `{sequence_id}`",
        "",
        f"**AF3** successful models: {len(af3_rows)} · mean global ipTM {fmt(af3_iptm)} · "
        f"mean glycan–binder clash {fmt(mean_glycan_clash_af3)}",
        "",
    ]
    if boltz_ok:
        lines.extend(
            [
                _hcg_method_line(
                    "Boltz2",
                    boltz_ok,
                    "boltz",
                    mean_glycan_clash=mean_glycan_clash_boltz,
                ),
                "",
            ]
        )
    if esm_ok:
        lines.extend(
            [
                _hcg_method_line(
                    "ESMFold",
                    esm_ok,
                    "esmfold",
                    mean_glycan_clash=mean_glycan_clash_esm,
                ),
                "",
            ]
        )
    lines.extend(
        [
            lh_line,
            "",
            b77_line,
            "",
            f'<div class="columns cols-{len(columns)}">',
            "".join(columns),
            "</div>",
        ]
    )
    return "---\n\n" + "\n".join(lines) + "\n"


def build_rule_ac_presentation(
    run_dirs: list[Path],
    out_dir: Path,
    *,
    seed: int = 0,
    pymol: Path = DEFAULT_PYMOL,
    scaffolds_dir: Path = DEFAULT_SCAFFOLDS_DIR,
    marp_docker_image: str = DEFAULT_MARP_DOCKER_IMAGE,
    export_pdf: bool = True,
    export_pptx: bool = True,
) -> dict:
    """Write one slide per AF3-successful Rule A/C sequence."""
    pymol = Path(pymol)
    if not pymol.is_file():
        raise FileNotFoundError(f"PyMOL not found: {pymol}")
    out_dir = Path(out_dir).resolve()
    assets_dir = out_dir / "assets"
    out_dir.mkdir(parents=True, exist_ok=True)
    assets_dir.mkdir(parents=True, exist_ok=True)
    rng = random.Random(seed)
    slides: list[str] = []
    ref_pdb: Optional[Path] = None
    view: Optional[str] = None

    for run_dir in (Path(path).resolve() for path in run_dirs):
        meta = parse_run_folder(run_dir)
        filtered = run_dir / "filtered_designs"
        successes = _rows(filtered / "AF3_successful_sequences.csv")
        af3_metrics = _rows(filtered / "AF3_metrics.csv")
        hcg_metrics = _rows(filtered / "filtered_metrics_rule_ac.csv")
        lh_path = filtered / "LH_binding_rule_ac.csv"
        lh_rows = _rows(lh_path) if lh_path.is_file() else None
        from dagster_pipeline.glycan_gs_ensemble_clashes import (
            RULE_AC_ENSEMBLE_CSV,
            mean_ensemble_clash_fraction,
        )
        from dagster_pipeline.glycan_binder_clashes import load_clash_rows

        clash_rows = load_clash_rows(run_dir / RULE_AC_ENSEMBLE_CSV)
        fragment = load_design_fragment(run_dir, meta, Path(scaffolds_dir))

        success_by_id: dict[str, dict[str, str]] = {}
        for row in successes:
            sid = (row.get("mpnn_seq_id") or "").strip()
            if sid:
                success_by_id.setdefault(sid, row)

        for success in success_by_id.values():
            sid = (success.get("mpnn_seq_id") or "").strip()
            binder = (success.get("binder_seq") or "").strip()
            if not sid:
                continue

            af3_rows = [
                row
                for row in af3_metrics
                if row.get("mpnn_seq_id") == sid and row.get("specificity_hotspots") == "2"
            ]
            if not af3_rows:
                af3_rows = [{"structure_pdb": "", "iptm": ""}]

            seq_hcg = [row for row in hcg_metrics if row.get("mpnn_seq_id") == sid]
            boltz_ok = filter_successful(seq_hcg, "boltz")
            esm_ok = filter_successful(seq_hcg, "esmfold")
            # Plan: show Boltz/ESMFold only when that predictor has ≥1 specificity-2 model.
            # Prefer PDB-backed rows from filter_successful; fall back to any specificity-2
            # count so the column gate matches metrics even if a path is missing.
            show_boltz = bool(boltz_ok) or any(
                (r.get("specificity_hotspots_boltz") or "").strip() == "2" for r in seq_hcg
            )
            show_esm = bool(esm_ok) or any(
                (r.get("specificity_hotspots_esmfold") or "").strip() == "2" for r in seq_hcg
            )

            designed = designed_residues_1based(fragment, binder) if binder else []
            sel = resi_list_to_pymol(designed) if designed else "none"
            stem = f"{meta.binder}_{meta.set_name.replace(' ', '')}_{sid}"

            images: list[tuple[str, Optional[Path]]] = []
            af3_row = rng.choice(af3_rows)
            af3_png, ref_pdb, view = _render(
                af3_row,
                method="af3",
                assets_dir=assets_dir,
                stem=stem,
                pymol=pymol,
                binder_seq=binder,
                designed_resi_sel=sel,
                ref_pdb=ref_pdb,
                view=view,
            )
            images.append(("AF3", af3_png))

            boltz_row = rng.choice(boltz_ok) if boltz_ok else None
            esm_row = rng.choice(esm_ok) if esm_ok else None
            if show_boltz and boltz_row is not None:
                boltz_png, ref_pdb, view = _render(
                    boltz_row,
                    method="boltz",
                    assets_dir=assets_dir,
                    stem=stem,
                    pymol=pymol,
                    binder_seq=binder or (boltz_row.get("binder_seq") or ""),
                    designed_resi_sel=sel,
                    ref_pdb=ref_pdb,
                    view=view,
                )
                images.append(("Boltz2", boltz_png))
            elif show_boltz:
                images.append(("Boltz2", None))

            if show_esm and esm_row is not None:
                esm_png, ref_pdb, view = _render(
                    esm_row,
                    method="esmfold",
                    assets_dir=assets_dir,
                    stem=stem,
                    pymol=pymol,
                    binder_seq=binder or (esm_row.get("binder_seq") or ""),
                    designed_resi_sel=sel,
                    ref_pdb=ref_pdb,
                    view=view,
                )
                images.append(("ESMFold", esm_png))
            elif show_esm:
                images.append(("ESMFold", None))

            lh = lh_spec_counts_for_seq(lh_rows, sid)
            af3_pdb = Path(af3_row.get("structure_pdb") or "")
            hcg_b77_af3 = binder_aas_contact_b77(af3_pdb) if af3_pdb.is_file() else ""
            hcg_b77_boltz = (
                binder_aas_contact_b77(Path(boltz_row["structure_pdb_boltz"]))
                if boltz_row
                else ""
            )
            hcg_b77_esm = (
                binder_aas_contact_b77(Path(esm_row["structure_pdb_esmfold"]))
                if esm_row
                else ""
            )
            b77_line = (
                f"**B77 binder AAs** AF3: {fmt_aas(hcg_b77_af3 or None)} · "
                f"hCG Boltz2: {fmt_aas(hcg_b77_boltz or None)} · "
                f"hCG ESMFold: {fmt_aas(hcg_b77_esm or None)} · "
                f"LH Boltz2: {fmt_aas(lh.b77_aas_boltz)} · "
                f"LH ESMFold: {fmt_aas(lh.b77_aas_esm)}"
            )

            slides.append(
                _slide(
                    title=f"{meta.binder} · {meta.hotspots} · {meta.set_name}",
                    sequence_id=sid,
                    af3_rows=af3_rows,
                    boltz_ok=boltz_ok if show_boltz else [],
                    esm_ok=esm_ok if show_esm else [],
                    lh_line=lh.fmt_line(),
                    b77_line=b77_line,
                    images=images,
                    assets_dir=assets_dir,
                    mean_glycan_clash_af3=mean_ensemble_clash_fraction(
                        clash_rows, mpnn_seq_id=sid, method="af3"
                    ),
                    mean_glycan_clash_boltz=mean_ensemble_clash_fraction(
                        clash_rows, mpnn_seq_id=sid, method="boltz"
                    ),
                    mean_glycan_clash_esm=mean_ensemble_clash_fraction(
                        clash_rows, mpnn_seq_id=sid, method="esmfold"
                    ),
                )
            )

    header = """---
marp: true
paginate: true
size: 16:9
theme: default
style: |
  section { font-size: 20px; padding: 34px 42px; }
  h1 { font-size: 27px; margin-bottom: 0.2em; }
  p { margin: 0.12em 0; font-size: 14px; }
  .columns { display: grid; gap: 0.75rem; align-items: start; margin-top: 0.35em; }
  .cols-1 { grid-template-columns: 1fr; }
  .cols-2 { grid-template-columns: 1fr 1fr; }
  .cols-3 { grid-template-columns: 1fr 1fr 1fr; }
  .columns img { max-width: 100%; max-height: 360px; object-fit: contain; display: block; }
---

# hCG Rule A/C AF3 → LH off-target

One slide per AF3-successful design sequence. AF3 is always shown. Boltz2 /
ESMFold columns appear only when that hCG predictor has ≥1 specificity-2 model.

**Global ipTM**: whole-complex inter-chain model score. **ipTM / ipSAE beta & alpha**:
binder–target chain-pair (IPSAE `max`; B=β, C=α). **interface_dG**: Rosetta interface
ΔG on Boltz/ESMFold lines.

Mean α-Asn52 glycan–binder clash: population-weighted over MD clusters of the mean
clash fraction across GlycoSHIELD-accepted conformers (2.4 Å; from
`glycan_gs_ensemble_clashes_rule_ac.csv`).
"""
    deck = out_dir / "deck_rule_ac.md"
    deck.write_text("\n".join([header, *slides]) + "\n", encoding="utf-8")
    pdf = export_marp_pdf(deck, docker_image=marp_docker_image) if export_pdf and slides else None
    pptx = (
        export_marp_pptx(deck, docker_image=marp_docker_image)
        if export_pptx and slides
        else None
    )
    return {
        "slide_count": len(slides),
        "deck_md": str(deck),
        "deck_pdf": str(pdf) if pdf else None,
        "deck_pptx": str(pptx) if pptx else None,
        "assets_dir": str(assets_dir),
        "run_dirs": [str(Path(path).resolve()) for path in run_dirs],
    }
