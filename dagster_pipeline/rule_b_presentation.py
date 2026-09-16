#!/home/kb/miniforge3/envs/bio-ds/bin/python
"""Build a Marp deck (+ PDF) from BoltzGen rule-B filtered design runs.

For each run folder, reads filtered_designs/filtered_metrics_rule_b.csv and
emits **one slide per design–sequence** (`mpnn_seq_id`, e.g. design_11__r8)
that has ≥1 successful Boltz2 and ≥1 successful ESMFold model
(`specificity_hotspots_* == 2`). Metrics (n, mean binder score, mean global
ipTM, mean binder–target ipTM / ipSAE for beta and alpha, mean interface_dG)
are averaged only over that sequence’s successful models (n ≤ 5).

Also reads ``filtered_designs/LH_binding.csv`` when present and prints LH
off-target specificity-2 counts per sequence (e.g. ``3/5 boltz2 · 0/5 esmfold``);
shows ``pending`` until that CSV has usable (non-ERROR) scores.

Always writes ``deck_rule_b.md`` and, by default, ``deck_rule_b.pdf`` +
``deck_rule_b.pptx`` via Marp Docker
(``marpteam/marp-cli``) or a host ``marp`` binary. PPTX uses one rendered image per slide.

Binder designed/masked regions come from the BoltzGen design-fragment string.

Examples:
  python -m dagster_pipeline.rule_b_presentation \\
    /storage/hCG/designs/runs/2026-07-27_162122/boltzgen__affitin__*_set1 \\
    -o /storage/hCG/presentations/rule_b_deck \\
    --seed 0
"""

from __future__ import annotations

import argparse
import csv
import os
import random
import re
import shutil
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Optional

RULE_B_REL = Path("filtered_designs") / "filtered_metrics_rule_b.csv"
LH_BINDING_REL = Path("filtered_designs") / "LH_binding.csv"
DEFAULT_PYMOL = Path("/home/kb/miniforge3/envs/bio-ds/bin/pymol")
DEFAULT_SCAFFOLDS_DIR = Path("/storage/hCG/scaffolds")
DEFAULT_MARP_DOCKER_IMAGE = "marpteam/marp-cli:v4.1.2"
FOCUS_RESI = "65-80"
HOT_RESI = "68+77"
ZOOM_BUFFER = 28
MODELS_PER_SEQ = 5
FRAGMENT_TOKEN_RE = re.compile(r"(\d+\.\.\d+|\d+|[A-Z]+)")
AA3_TO_1 = {
    "ALA": "A", "CYS": "C", "ASP": "D", "GLU": "E", "PHE": "F", "GLY": "G",
    "HIS": "H", "ILE": "I", "LYS": "K", "LEU": "L", "MET": "M", "ASN": "N",
    "PRO": "P", "GLN": "Q", "ARG": "R", "SER": "S", "THR": "T", "VAL": "V",
    "TRP": "W", "TYR": "Y", "MSE": "M",
}


@dataclass
class RunMeta:
    folder: Path
    binder: str
    binder_token: str  # e.g. affibody_2B89 / affitin
    hotspots: str
    set_name: str


@dataclass
class LhSpecCounts:
    """LH off-target specificity==2 counts from LH_binding.csv (per sequence)."""

    n_ok_boltz: Optional[int]  # None → CSV missing / sequence not scored yet
    n_total_boltz: Optional[int]
    n_ok_esm: Optional[int]
    n_total_esm: Optional[int]
    b77_aas_boltz: Optional[str]  # e.g. "Asp,Arg"; None → pending
    b77_aas_esm: Optional[str]

    def fmt_line(self) -> str:
        def one(label: str, n_ok: Optional[int], n_tot: Optional[int]) -> str:
            if n_ok is None or n_tot is None:
                return f"{label} pending"
            tot = n_tot if n_tot > 0 else MODELS_PER_SEQ
            return f"{n_ok}/{tot} {label}"

        return (
            f"**LH** specificity 2: {one('boltz2', self.n_ok_boltz, self.n_total_boltz)} · "
            f"{one('esmfold', self.n_ok_esm, self.n_total_esm)}"
        )


@dataclass
class SlideData:
    meta: RunMeta
    mpnn_seq_id: str
    n_ok_boltz: int
    n_ok_esm: int
    avg_iptm_boltz: Optional[float]
    avg_iptm_beta_boltz: Optional[float]
    avg_iptm_alpha_boltz: Optional[float]
    avg_ipsae_beta_boltz: Optional[float]
    avg_ipsae_alpha_boltz: Optional[float]
    avg_binder_score_boltz: Optional[float]
    avg_interface_dg_boltz: Optional[float]
    avg_glycan_clash_boltz: Optional[float]
    avg_iptm_esm: Optional[float]
    avg_iptm_beta_esm: Optional[float]
    avg_iptm_alpha_esm: Optional[float]
    avg_ipsae_beta_esm: Optional[float]
    avg_ipsae_alpha_esm: Optional[float]
    avg_binder_score_esm: Optional[float]
    avg_interface_dg_esm: Optional[float]
    avg_glycan_clash_esm: Optional[float]
    lh: LhSpecCounts
    hcg_b77_aas_boltz: str
    hcg_b77_aas_esm: str
    boltz_row: dict
    esm_row: dict
    boltz_png: Path
    esm_png: Path


def parse_run_folder(folder: Path) -> RunMeta:
    """Parse boltzgen__<binder>__...__<hotspots>__..._setN folder names."""
    name = folder.name
    parts = name.split("__")
    if len(parts) < 5:
        raise ValueError(f"Unexpected run folder name (need >=5 __ parts): {name}")

    binder_token = parts[1]  # e.g. affibody_2B89, affitin
    binder = binder_token.split("_")[0]

    hotspots = parts[3].replace("_", " ")  # e.g. both_loops -> both loops

    set_m = re.search(r"_set(\d+)$", name)
    if not set_m:
        set_m = re.search(r"set(\d+)$", parts[-1])
    if not set_m:
        raise ValueError(f"Could not parse set number from: {name}")
    set_name = f"set {int(set_m.group(1))}"

    return RunMeta(
        folder=folder,
        binder=binder,
        binder_token=binder_token,
        hotspots=hotspots,
        set_name=set_name,
    )


def parse_fragment_tokens(fragment: str) -> list[tuple]:
    """Parse BoltzGen design-fragment string into fixed / design tokens."""
    toks: list[tuple] = []
    for m in FRAGMENT_TOKEN_RE.finditer(fragment.strip()):
        t = m.group(1)
        if t[0].isdigit():
            if ".." in t:
                a, b = map(int, t.split(".."))
                toks.append(("design", a, b))
            else:
                n = int(t)
                toks.append(("design", n, n))
        else:
            toks.append(("fixed", t))
    return toks


def designed_residues_1based(fragment: str, binder_seq: str) -> list[int]:
    """Map variable-length design slots onto a designed binder sequence.

    BoltzGen fragment notation: fixed AAs interleaved with N or N..M design
    slots. Build a regex and take capture-group spans as designed residues.
    """
    toks = parse_fragment_tokens(fragment)
    if not toks:
        raise ValueError(f"Empty design fragment: {fragment!r}")

    parts: list[str] = []
    design_groups: list[int] = []
    gi = 0
    for tok in toks:
        if tok[0] == "fixed":
            parts.append(re.escape(tok[1]))
        else:
            lo, hi = tok[1], tok[2]
            gi += 1
            parts.append(f"(.{{{lo},{hi}}})")
            design_groups.append(gi)

    pat = re.compile("^" + "".join(parts) + "$")
    m = pat.match(binder_seq)
    if not m:
        raise ValueError(
            f"Design fragment does not match binder sequence.\n"
            f"  fragment: {fragment}\n"
            f"  sequence: {binder_seq}\n"
            f"  pattern:  {pat.pattern}"
        )

    resi: list[int] = []
    for gi in design_groups:
        start, end = m.start(gi), m.end(gi)
        resi.extend(range(start + 1, end + 1))
    return resi


def load_design_fragment(run_dir: Path, meta: RunMeta, scaffolds_dir: Path) -> str:
    """Load BoltzGen design-fragment string for this run.

    Preference order:
      1) design_configs_manifest.json → binder_sequence_file / design_spec_yaml
      2) scaffolds/<binder_token>*design_fragments*.txt
      3) scaffolds/<binder>*design_fragments*.txt
    """
    manifest = run_dir.parent / "design_configs_manifest.json"
    if manifest.is_file():
        import json

        data = json.loads(manifest.read_text())
        # keys look like: <run_id>__<subdir>
        hit = None
        for k, v in data.items():
            if v.get("subdir") == run_dir.name or k.endswith("__" + run_dir.name):
                hit = v
                break
        if hit:
            seq_file = hit.get("binder_sequence_file")
            if seq_file and Path(seq_file).is_file():
                return Path(seq_file).read_text().strip().splitlines()[0].strip()
            yaml_path = hit.get("design_spec_yaml")
            if yaml_path and Path(yaml_path).is_file():
                text = Path(yaml_path).read_text()
                # protein binder entity sequence (contains digits / ..)
                m = re.search(
                    r"sequence:\s*([A-Z0-9.]+)",
                    text,
                )
                if m and re.search(r"\d", m.group(1)):
                    return m.group(1)

    candidates = [
        *sorted(scaffolds_dir.glob(f"{meta.binder_token}*design_fragments*.txt")),
        *sorted(scaffolds_dir.glob(f"{meta.binder}*design_fragments*.txt")),
    ]
    for c in candidates:
        line = c.read_text().strip().splitlines()[0].strip()
        if line and re.search(r"\d", line):
            return line
    raise FileNotFoundError(
        f"No design-fragment file found for {meta.binder_token} "
        f"(looked in manifest and {scaffolds_dir})"
    )


def pdb_chain_sequences(pdb: Path) -> dict[str, str]:
    """CA-trace one-letter sequences per chain (residue order)."""
    seqs: dict[str, list[str]] = {}
    last_resi: dict[str, Optional[int]] = {}
    with pdb.open() as fh:
        for line in fh:
            if not line.startswith("ATOM"):
                continue
            if line[12:16].strip() != "CA":
                continue
            ch = line[21]
            resi = int(line[22:26])
            resn = line[17:20].strip()
            if last_resi.get(ch) == resi:
                continue
            last_resi[ch] = resi
            seqs.setdefault(ch, []).append(AA3_TO_1.get(resn, "X"))
    return {ch: "".join(aa) for ch, aa in seqs.items()}


def detect_binder_chain(pdb: Path, binder_seq: str) -> str:
    seqs = pdb_chain_sequences(pdb)
    for ch, seq in seqs.items():
        if seq == binder_seq:
            return ch
    # fallback: longest non-B/C if unique prefix match
    for ch, seq in seqs.items():
        if binder_seq in seq or seq in binder_seq:
            return ch
    raise ValueError(
        f"Could not find binder chain matching sequence in {pdb} "
        f"(chains={ {c: len(s) for c, s in seqs.items()} })"
    )


def resi_list_to_pymol(resi: list[int]) -> str:
    """Compress 1-based residue ids to PyMOL resi selection (e.g. 7-9+21-22+24)."""
    if not resi:
        return "none"
    resi = sorted(set(resi))
    ranges: list[str] = []
    start = prev = resi[0]
    for r in resi[1:]:
        if r == prev + 1:
            prev = r
            continue
        ranges.append(f"{start}-{prev}" if start != prev else f"{start}")
        start = prev = r
    ranges.append(f"{start}-{prev}" if start != prev else f"{start}")
    return "+".join(ranges)


def resolve_run_dirs(patterns: Iterable[str]) -> list[Path]:
    """Expand CLI paths/globs to existing directories (stable unique order)."""
    out: list[Path] = []
    seen: set[Path] = set()
    for pattern in patterns:
        text = str(pattern)
        matches = (
            sorted(Path().glob(text)) if any(c in text for c in "*?[") else []
        )
        candidates = matches or [Path(text)]
        for path in candidates:
            resolved = path.expanduser().resolve()
            if not resolved.is_dir():
                raise FileNotFoundError(f"Not a directory: {resolved}")
            if resolved in seen:
                continue
            seen.add(resolved)
            out.append(resolved)
    return out


def load_rule_b_rows(run_dir: Path) -> list[dict]:
    csv_path = run_dir / RULE_B_REL
    if not csv_path.is_file():
        raise FileNotFoundError(f"Missing {csv_path}")
    with csv_path.open(newline="") as fh:
        return list(csv.DictReader(fh))


def load_lh_binding_rows(run_dir: Path) -> Optional[list[dict]]:
    """Return LH_binding.csv rows, or None if the file is not written yet."""
    csv_path = run_dir / LH_BINDING_REL
    if not csv_path.is_file():
        return None
    with csv_path.open(newline="") as fh:
        return list(csv.DictReader(fh))


def lh_spec_counts_for_seq(
    lh_rows: Optional[list[dict]],
    mpnn_seq_id: str,
) -> LhSpecCounts:
    """Count models with specificity_hotspots_* == 2 for one design–sequence."""
    if lh_rows is None:
        return LhSpecCounts(None, None, None, None, None, None)
    seq_rows = [
        r for r in lh_rows if (r.get("mpnn_seq_id") or "").strip() == mpnn_seq_id
    ]
    if not seq_rows:
        # CSV exists but this sequence not scored yet (partial run)
        return LhSpecCounts(None, None, None, None, None, None)

    def count(method: str) -> tuple[Optional[int], Optional[int]]:
        col = f"specificity_hotspots_{method}"
        scored = 0
        ok = 0
        for r in seq_rows:
            raw = (r.get(col) or "").strip()
            if raw == "" or raw.startswith("ERROR"):
                continue
            scored += 1
            if raw == "2":
                ok += 1
        # No usable numeric scores (missing / all ERROR) → pending
        if scored == 0:
            return None, None
        return ok, scored

    def union_b77_aas(method: str) -> Optional[str]:
        spec_col = f"specificity_hotspots_{method}"
        aa_col = f"binder_aas_contact_B77_{method}"
        has_score = False
        seen: list[str] = []
        for r in seq_rows:
            spec = (r.get(spec_col) or "").strip()
            if spec == "" or spec.startswith("ERROR"):
                continue
            has_score = True
            raw = (r.get(aa_col) or "").strip()
            if not raw or raw.startswith("ERROR"):
                continue
            for aa in raw.split(","):
                aa = aa.strip()
                if aa and aa not in seen:
                    seen.append(aa)
        if not has_score:
            return None
        return ",".join(seen)

    n_ok_b, n_tot_b = count("boltz")
    n_ok_e, n_tot_e = count("esmfold")
    return LhSpecCounts(
        n_ok_b,
        n_tot_b,
        n_ok_e,
        n_tot_e,
        union_b77_aas("boltz"),
        union_b77_aas("esmfold"),
    )


_RESIDUE_CONTACTS_DIR = Path("/storage/proteindesign/filters/final_scores")


def binder_aas_contact_b77(pdb: Path) -> str:
    """Binder AAs contacting target B77 (4 Å heavy atoms); empty if none/unavailable."""
    if str(_RESIDUE_CONTACTS_DIR) not in sys.path:
        sys.path.insert(0, str(_RESIDUE_CONTACTS_DIR))
    try:
        from residue_contacts import binder_aas_contacting_target

        return binder_aas_contacting_target(pdb, "B77")
    except Exception as exc:  # noqa: BLE001
        print(f"[warn] B77 contacts failed for {pdb}: {exc}", file=sys.stderr)
        return ""


def _f(row: dict, key: str) -> Optional[float]:
    raw = (row.get(key) or "").strip()
    if raw == "":
        return None
    try:
        return float(raw)
    except ValueError:
        return None


def _mean_col(rows: list[dict], key: str) -> Optional[float]:
    vals = [v for r in rows if (v := _f(r, key)) is not None]
    if not vals:
        return None
    return sum(vals) / len(vals)


def filter_successful(rows: list[dict], method: str) -> list[dict]:
    """method: 'boltz' or 'esmfold'. Successful = specificity_hotspots_* == 2."""
    col = f"specificity_hotspots_{method}"
    pdb_col = f"structure_pdb_{method}"
    out = []
    for r in rows:
        if (r.get(col) or "").strip() != "2":
            continue
        pdb = (r.get(pdb_col) or "").strip()
        if not pdb or not Path(pdb).is_file():
            continue
        out.append(r)
    return out


def avg_metrics(rows: list[dict], method: str) -> dict[str, Optional[float]]:
    """Per-method means over successful rows of one design–sequence.

    Chain B = hCG beta subunit, chain C = hCG alpha subunit.
    Per-chain ipTM uses ipTM_af-B / ipTM_af-C columns.
    """
    return {
        "iptm": _mean_col(rows, f"iptm_{method}"),
        "iptm_beta": _mean_col(rows, f"ipTM_af-B_{method}"),
        "iptm_alpha": _mean_col(rows, f"ipTM_af-C_{method}"),
        "ipsae_beta": _mean_col(rows, f"ipSAE-B_{method}"),
        "ipsae_alpha": _mean_col(rows, f"ipSAE-C_{method}"),
        "binder_score": _mean_col(rows, f"binder_score_{method}"),
        "interface_dg": _mean_col(rows, f"interface_dG_{method}"),
    }


def render_pymol(
    pdb: Path,
    out_png: Path,
    pymol: Path,
    binder_chain: str,
    designed_resi_sel: str,
    *,
    ref_pdb: Optional[Path] = None,
    view: Optional[str] = None,
    width: int = 1000,
    height: int = 750,
) -> str:
    """Render a snapshot. Returns the PyMOL view string used (repr of get_view()).

    If ref_pdb is set, chain B CA atoms are aligned to that reference first.
    If view is set, that camera is applied; otherwise orient+zoom on the epitope
    focus and the resulting view is returned for reuse on later slides.
    """
    out_png.parent.mkdir(parents=True, exist_ok=True)
    view_out = out_png.with_suffix(".view.txt")

    align_block = ""
    if ref_pdb is not None:
        align_block = f"""
load {ref_pdb.as_posix()}, ref
hide everything, ref
align design and chain B and name CA, ref and chain B and name CA
delete ref
"""

    if view is not None:
        camera_py = f"cmd.set_view({view})"
    else:
        camera_py = f'cmd.orient("focus"); cmd.zoom("focus", buffer={ZOOM_BUFFER})'

    script = f"""
load {pdb.as_posix()}, design
{align_block}
hide everything
show cartoon
color gray70, all
color palecyan, chain {binder_chain}
select designed, chain {binder_chain} and resi {designed_resi_sel}
color hotpink, designed
color slate, chain B
select focus, chain B and resi {FOCUS_RESI}
select hot, chain B and resi {HOT_RESI}
color marine, focus
color orange, hot
show sticks, hot and not hydro
bg_color white
set ray_opaque_background, 1
set antialias, 2
set ray_shadows, 0
python
{camera_py}
open({view_out.as_posix()!r}, "w").write(repr(cmd.get_view()))
python end
ray {width},{height}
png {out_png.as_posix()}, dpi=150
"""
    cmd = [str(pymol), "-cq", "-d", script]
    proc = subprocess.run(cmd, capture_output=True, text=True)
    if proc.returncode != 0 or not out_png.is_file():
        raise RuntimeError(
            f"PyMOL failed for {pdb}\nstdout:\n{proc.stdout}\nstderr:\n{proc.stderr}"
        )
    if not view_out.is_file():
        raise RuntimeError(f"PyMOL did not write view file for {pdb}\n{proc.stdout}\n{proc.stderr}")
    used_view = view_out.read_text().strip()
    view_out.unlink()
    return used_view


def fmt(x: Optional[float], digits: int = 3) -> str:
    if x is None:
        return "n/a"
    return f"{x:.{digits}f}"


def fmt_aas(raw: Optional[str]) -> str:
    if raw is None:
        return "pending"
    return raw or "(none)"


def slide_markdown(slide: SlideData, assets_rel: Path) -> str:
    m = slide.meta
    boltz_img = (assets_rel / slide.boltz_png.name).as_posix()
    esm_img = (assets_rel / slide.esm_png.name).as_posix()
    boltz_mi = slide.boltz_row.get("model_index", "?")
    esm_mi = slide.esm_row.get("model_index", "?")
    return f"""---

# {m.binder} · {m.hotspots} · {m.set_name} · `{slide.mpnn_seq_id}`

**Boltz2** n={slide.n_ok_boltz}/{MODELS_PER_SEQ} · shown m{boltz_mi} · mean binder score {fmt(slide.avg_binder_score_boltz, 1)} · mean global ipTM {fmt(slide.avg_iptm_boltz)} · mean ipTM beta {fmt(slide.avg_iptm_beta_boltz)} · mean ipTM alpha {fmt(slide.avg_iptm_alpha_boltz)} · mean ipSAE beta {fmt(slide.avg_ipsae_beta_boltz)} · mean ipSAE alpha {fmt(slide.avg_ipsae_alpha_boltz)} · mean interface_dG {fmt(slide.avg_interface_dg_boltz, 1)} · mean glycan–binder clash {fmt(slide.avg_glycan_clash_boltz)}

**ESMFold** n={slide.n_ok_esm}/{MODELS_PER_SEQ} · shown m{esm_mi} · mean binder score {fmt(slide.avg_binder_score_esm, 1)} · mean global ipTM {fmt(slide.avg_iptm_esm)} · mean ipTM beta {fmt(slide.avg_iptm_beta_esm)} · mean ipTM alpha {fmt(slide.avg_iptm_alpha_esm)} · mean ipSAE beta {fmt(slide.avg_ipsae_beta_esm)} · mean ipSAE alpha {fmt(slide.avg_ipsae_alpha_esm)} · mean interface_dG {fmt(slide.avg_interface_dg_esm, 1)} · mean glycan–binder clash {fmt(slide.avg_glycan_clash_esm)}

{slide.lh.fmt_line()}

**B77 binder AAs** hCG Boltz2: {fmt_aas(slide.hcg_b77_aas_boltz)} · hCG ESMFold: {fmt_aas(slide.hcg_b77_aas_esm)} · LH Boltz2: {fmt_aas(slide.lh.b77_aas_boltz)} · LH ESMFold: {fmt_aas(slide.lh.b77_aas_esm)}

<div class="columns">
<div>

**Boltz2** model {boltz_mi}

![]({boltz_img})

</div>
<div>

**ESMFold** model {esm_mi}

![]({esm_img})

</div>
</div>
"""


def write_deck(slides: list[SlideData], out_md: Path, assets_dir: Path) -> None:
    header = """---
marp: true
paginate: true
size: 16:9
theme: default
style: |
  section {
    font-size: 22px;
    padding: 36px 44px;
  }
  h1 {
    font-size: 28px;
    margin-bottom: 0.2em;
  }
  p {
    margin: 0.12em 0;
    font-size: 14px;
  }
  .columns {
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 0.75rem;
    align-items: start;
    margin-top: 0.35em;
  }
  .columns > div {
    min-width: 0;
  }
  .columns img {
    max-width: 100%;
    max-height: 380px;
    width: auto;
    height: auto;
    object-fit: contain;
    display: block;
  }
---

# hCG binder designs (rule B)

One slide per **design–sequence** (`design_N__rM`).

Successful models: `specificity_hotspots_* == 2` (max 5 models per sequence).

Metrics (n, mean binder score, mean **global** ipTM, mean binder–target ipTM /
ipSAE for **beta** and **alpha**, mean interface_dG, mean glycan–binder clash
fraction) are averaged **only over that sequence’s** successful models.

**Global ipTM**: model whole-complex inter-chain ipTM (all chain pairs, including
β–α and glycans/ligands). **Not** a binder–target-only score.

**ipTM / ipSAE beta & alpha**: binder–target chain-pair scores (IPSAE `max` of
both directions). Chain mapping: beta = B, alpha = C. Per-chain ipTM = `ipTM_af`.

**interface_dG**: Rosetta interface ΔG (binder vs target).

**LH** line: off-target specificity 2 counts from `filtered_designs/LH_binding.csv`
(binder × LH α/β). Shows e.g. `3/5 boltz2 · 0/5 esmfold`, or `pending` until
that CSV exists.

**Glycan–binder clash**: population-weighted mean over MD Asn52 clusters of the
mean α-Asn52 N-glycan clash fraction vs binder across GlycoSHIELD-accepted
conformers (2.4 Å heavy atoms; beta C-terminus resid≥121 excluded from GS clash
filtering; `glycan_gs_ensemble_clashes_rule_b.csv`).

**B77 binder AAs**: binder amino acids contacting target residue B77 (4 Å heavy
atoms). hCG values are from the **shown** Boltz2 / ESMFold models; LH values are
the union across LH models for that sequence (from `LH_binding.csv`).

Structure view: β chain B, residues 65–80 (68 & 77 highlighted).

Binder: framework pale cyan; **designed / masked** regions hot pink.
"""
    parts = [header]
    assets_rel = Path(assets_dir.name)
    for s in slides:
        parts.append(slide_markdown(s, assets_rel))
    out_md.write_text("\n".join(parts) + "\n", encoding="utf-8")


def build_slides_for_run(
    run_dir: Path,
    assets_dir: Path,
    rng: random.Random,
    pymol: Path,
    scaffolds_dir: Path,
    *,
    ref_pdb: Optional[Path] = None,
    view: Optional[str] = None,
) -> tuple[list[SlideData], Optional[Path], Optional[str]]:
    """Build one slide per design–sequence with ≥1 successful Boltz and ESMFold."""
    meta = parse_run_folder(run_dir)
    rows = load_rule_b_rows(run_dir)
    if not rows:
        print(f"[skip] empty rule_b CSV: {run_dir.name}", file=sys.stderr)
        return [], ref_pdb, view

    lh_rows = load_lh_binding_rows(run_dir)
    if lh_rows is None:
        print(f"[lh] {run_dir.name}: LH_binding.csv not ready → pending", file=sys.stderr)
    else:
        print(f"[lh] {run_dir.name}: {len(lh_rows)} LH_binding row(s)", file=sys.stderr)

    from dagster_pipeline.glycan_gs_ensemble_clashes import (
        RULE_B_ENSEMBLE_CSV,
        mean_ensemble_clash_fraction,
    )
    from dagster_pipeline.glycan_binder_clashes import load_clash_rows

    clash_rows = load_clash_rows(run_dir / RULE_B_ENSEMBLE_CSV)
    if clash_rows:
        print(f"[glycan] {run_dir.name}: {len(clash_rows)} ensemble clash row(s)", file=sys.stderr)
    else:
        print(f"[glycan] {run_dir.name}: no ensemble clash CSV → n/a", file=sys.stderr)

    fragment = load_design_fragment(run_dir, meta, scaffolds_dir)
    print(f"[mask] {meta.binder_token}: fragment={fragment[:60]}{'…' if len(fragment)>60 else ''}")

    # Group all rows by design–sequence id
    by_seq: dict[str, list[dict]] = {}
    for r in rows:
        sid = (r.get("mpnn_seq_id") or "").strip()
        if not sid:
            continue
        by_seq.setdefault(sid, []).append(r)

    slides: list[SlideData] = []

    for sid in sorted(by_seq.keys()):
        seq_rows = by_seq[sid]
        ok_b = filter_successful(seq_rows, "boltz")
        ok_e = filter_successful(seq_rows, "esmfold")
        if not ok_b or not ok_e:
            print(
                f"[skip] {run_dir.name} / {sid}: need ≥1 successful Boltz and ESMFold "
                f"(ok_b={len(ok_b)}, ok_e={len(ok_e)})",
                file=sys.stderr,
            )
            continue

        mb = avg_metrics(ok_b, "boltz")
        me = avg_metrics(ok_e, "esmfold")
        boltz_row = rng.choice(ok_b)
        esm_row = rng.choice(ok_e)
        lh = lh_spec_counts_for_seq(lh_rows, sid)

        stem = (
            f"{meta.binder}_{meta.hotspots.replace(' ', '_')}_"
            f"{meta.set_name.replace(' ', '')}_{sid}"
        )
        boltz_png = assets_dir / f"{stem}_boltz.png"
        esm_png = assets_dir / f"{stem}_esmfold.png"

        def _render(row: dict, method: str, out_png: Path, seq_id: str = sid) -> None:
            nonlocal ref_pdb, view
            binder_seq = (row.get("binder_seq") or row.get(f"binder_seq_{method}") or "").strip()
            if not binder_seq:
                raise ValueError(f"Missing binder_seq in row for {method}")
            designed = designed_residues_1based(fragment, binder_seq)
            sel = resi_list_to_pymol(designed)
            pdb = Path(row[f"structure_pdb_{method}"])
            chain = detect_binder_chain(pdb, binder_seq)
            print(
                f"[render] {run_dir.name}: {seq_id} {method} m{row['model_index']} "
                f"chain={chain} designed_n={len(designed)} resi={sel}"
            )
            used = render_pymol(
                pdb,
                out_png,
                pymol,
                binder_chain=chain,
                designed_resi_sel=sel,
                ref_pdb=ref_pdb,
                view=view,
            )
            if ref_pdb is None:
                ref_pdb = pdb
            if view is None:
                view = used

        _render(boltz_row, "boltz", boltz_png)
        _render(esm_row, "esmfold", esm_png)

        hcg_b77_boltz = binder_aas_contact_b77(Path(boltz_row["structure_pdb_boltz"]))
        hcg_b77_esm = binder_aas_contact_b77(Path(esm_row["structure_pdb_esmfold"]))

        slides.append(
            SlideData(
                meta=meta,
                mpnn_seq_id=sid,
                n_ok_boltz=len(ok_b),
                n_ok_esm=len(ok_e),
                avg_iptm_boltz=mb["iptm"],
                avg_iptm_beta_boltz=mb["iptm_beta"],
                avg_iptm_alpha_boltz=mb["iptm_alpha"],
                avg_ipsae_beta_boltz=mb["ipsae_beta"],
                avg_ipsae_alpha_boltz=mb["ipsae_alpha"],
                avg_binder_score_boltz=mb["binder_score"],
                avg_interface_dg_boltz=mb["interface_dg"],
                avg_glycan_clash_boltz=mean_ensemble_clash_fraction(
                    clash_rows, mpnn_seq_id=sid, method="boltz"
                ),
                avg_iptm_esm=me["iptm"],
                avg_iptm_beta_esm=me["iptm_beta"],
                avg_iptm_alpha_esm=me["iptm_alpha"],
                avg_ipsae_beta_esm=me["ipsae_beta"],
                avg_ipsae_alpha_esm=me["ipsae_alpha"],
                avg_binder_score_esm=me["binder_score"],
                avg_interface_dg_esm=me["interface_dg"],
                avg_glycan_clash_esm=mean_ensemble_clash_fraction(
                    clash_rows, mpnn_seq_id=sid, method="esmfold"
                ),
                lh=lh,
                hcg_b77_aas_boltz=hcg_b77_boltz,
                hcg_b77_aas_esm=hcg_b77_esm,
                boltz_row=boltz_row,
                esm_row=esm_row,
                boltz_png=boltz_png,
                esm_png=esm_png,
            )
        )

    return slides, ref_pdb, view

def maybe_export_html(md: Path, marp_bin: Optional[str]) -> None:
    if not marp_bin:
        return
    html = md.with_suffix(".html")
    cmd = [marp_bin, str(md), "-o", str(html), "--allow-local-files"]
    print(f"[marp] {' '.join(cmd)}")
    proc = subprocess.run(cmd, capture_output=True, text=True)
    if proc.returncode != 0:
        print(proc.stderr or proc.stdout, file=sys.stderr)
        raise RuntimeError("marp-cli HTML export failed")
    print(f"[marp] wrote {html}")


def _marp_docker_user_env() -> tuple[list[str], dict[str, str]]:
    """Host-UID bind-mount identity for ``marpteam/marp-cli`` (gosu MARP_USER)."""
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
    passwd_path.write_text(
        f"root:x:0:0:root:/root:/bin/sh\n"
        f"{name}:x:{uid}:{gid}::/tmp:/bin/sh\n"
        f"marp:x:{uid}:{gid}::/home/marp:/bin/sh\n",
        encoding="utf-8",
    )
    group_path.write_text(
        f"root:x:0:\n{gname}:x:{gid}:\nmarp:x:{gid}:\n",
        encoding="utf-8",
    )
    args = [
        "-v",
        f"{passwd_path}:/etc/passwd:ro",
        "-v",
        f"{group_path}:/etc/group:ro",
    ]
    env = {
        "MARP_USER": f"{uid}:{gid}",
        "HOME": "/tmp",
        "USER": name,
    }
    return args, env


def _export_marp(
    md: Path,
    *,
    fmt: str,
    out: Optional[Path] = None,
    docker_image: str = DEFAULT_MARP_DOCKER_IMAGE,
    marp_bin: Optional[str] = None,
) -> Path:
    """Export Marp markdown via host marp-cli or Marp Docker (``fmt``: ``pdf`` or ``pptx``)."""
    if fmt not in {"pdf", "pptx"}:
        raise ValueError(f"Unsupported Marp export format: {fmt!r}")
    md = md.resolve()
    out = (out or md.with_suffix(f".{fmt}")).resolve()
    out_dir = md.parent
    flag = f"--{fmt}"

    host_marp = marp_bin or shutil.which("marp")
    if host_marp:
        cmd = [
            host_marp,
            str(md),
            flag,
            "--allow-local-files",
            "-o",
            str(out),
        ]
        print(f"[marp] {' '.join(cmd)}")
        proc = subprocess.run(cmd, capture_output=True, text=True)
        if proc.returncode != 0:
            print(proc.stderr or proc.stdout, file=sys.stderr)
            raise RuntimeError(f"marp-cli {fmt.upper()} export failed")
        if not out.is_file():
            raise RuntimeError(f"marp-cli did not write {fmt.upper()}: {out}")
        print(f"[marp] wrote {out}")
        return out

    if not docker_image:
        raise RuntimeError(
            f"No host marp-cli and marp_docker_image is empty; cannot export {fmt.upper()}"
        )

    user_args, env = _marp_docker_user_env()
    cmd = [
        "docker",
        "run",
        "--rm",
        *user_args,
        "-e",
        f"MARP_USER={env['MARP_USER']}",
        "-e",
        f"HOME={env['HOME']}",
        "-e",
        f"USER={env['USER']}",
        "-v",
        f"{out_dir}:{out_dir}",
        "-w",
        str(out_dir),
        docker_image,
        md.name,
        flag,
        "--allow-local-files",
        "-o",
        out.name,
    ]
    print(f"[marp-docker] {' '.join(cmd)}")
    proc = subprocess.run(cmd, capture_output=True, text=True)
    if proc.returncode != 0:
        print(proc.stderr or proc.stdout, file=sys.stderr)
        raise RuntimeError(
            f"Marp Docker {fmt.upper()} export failed (image={docker_image}): "
            f"{(proc.stderr or proc.stdout or '')[:500]}"
        )
    if not out.is_file():
        raise RuntimeError(f"Marp Docker did not write {fmt.upper()}: {out}")
    print(f"[marp-docker] wrote {out}")
    return out


def export_marp_pdf(
    md: Path,
    *,
    pdf: Optional[Path] = None,
    docker_image: str = DEFAULT_MARP_DOCKER_IMAGE,
    marp_bin: Optional[str] = None,
) -> Path:
    """Export Marp markdown → PDF via host marp-cli or Marp Docker image."""
    return _export_marp(
        md, fmt="pdf", out=pdf, docker_image=docker_image, marp_bin=marp_bin
    )


def export_marp_pptx(
    md: Path,
    *,
    pptx: Optional[Path] = None,
    docker_image: str = DEFAULT_MARP_DOCKER_IMAGE,
    marp_bin: Optional[str] = None,
) -> Path:
    """Export Marp markdown → PPTX (one rendered image per slide)."""
    return _export_marp(
        md, fmt="pptx", out=pptx, docker_image=docker_image, marp_bin=marp_bin
    )


def build_rule_b_presentation(
    run_dirs: list[Path],
    out_dir: Path,
    *,
    seed: Optional[int] = 0,
    pymol: Path = DEFAULT_PYMOL,
    scaffolds_dir: Path = DEFAULT_SCAFFOLDS_DIR,
    marp_docker_image: str = DEFAULT_MARP_DOCKER_IMAGE,
    marp_bin: Optional[str] = None,
    export_pdf: bool = True,
    export_pptx: bool = True,
    export_html: bool = False,
) -> dict:
    """Build Marp ``deck_rule_b.md`` (+ PDF / PPTX) for one or more partition run folders."""
    if not Path(pymol).is_file():
        raise FileNotFoundError(f"PyMOL not found: {pymol}")

    resolved_runs = [Path(p).expanduser().resolve() for p in run_dirs]
    missing = [str(p) for p in resolved_runs if not p.is_dir()]
    if missing:
        raise FileNotFoundError(f"Run directories not found: {missing}")

    out_dir = Path(out_dir).expanduser().resolve()
    assets_dir = out_dir / "assets"
    out_dir.mkdir(parents=True, exist_ok=True)
    assets_dir.mkdir(parents=True, exist_ok=True)

    rng = random.Random(seed)
    slides: list[SlideData] = []
    ref_pdb: Optional[Path] = None
    view: Optional[str] = None
    scaffolds_dir = Path(scaffolds_dir).expanduser().resolve()

    for run_dir in resolved_runs:
        new_slides, ref_pdb, view = build_slides_for_run(
            run_dir,
            assets_dir,
            rng,
            Path(pymol),
            scaffolds_dir,
            ref_pdb=ref_pdb,
            view=view,
        )
        slides.extend(new_slides)

    md_path = out_dir / "deck_rule_b.md"
    pdf_path: Optional[Path] = None
    pptx_path: Optional[Path] = None
    html_path: Optional[Path] = None

    if not slides:
        # Keep an empty title-only deck so downstream paths stay stable.
        write_deck([], md_path, assets_dir)
        return {
            "slide_count": 0,
            "deck_md": str(md_path),
            "deck_pdf": None,
            "deck_pptx": None,
            "deck_html": None,
            "assets_dir": str(assets_dir),
            "run_dirs": [str(p) for p in resolved_runs],
        }

    write_deck(slides, md_path, assets_dir)
    print(f"[ok] {len(slides)} slide(s) → {md_path}")

    if export_html:
        host_marp = marp_bin or shutil.which("marp")
        if host_marp:
            maybe_export_html(md_path, host_marp)
            html_path = md_path.with_suffix(".html")
        else:
            print("[warn] HTML export skipped (no host marp-cli)", file=sys.stderr)

    if export_pdf:
        pdf_path = export_marp_pdf(
            md_path,
            docker_image=marp_docker_image,
            marp_bin=marp_bin,
        )
    if export_pptx:
        pptx_path = export_marp_pptx(
            md_path,
            docker_image=marp_docker_image,
            marp_bin=marp_bin,
        )

    return {
        "slide_count": len(slides),
        "deck_md": str(md_path),
        "deck_pdf": str(pdf_path) if pdf_path else None,
        "deck_pptx": str(pptx_path) if pptx_path else None,
        "deck_html": str(html_path) if html_path and html_path.is_file() else None,
        "assets_dir": str(assets_dir),
        "run_dirs": [str(p) for p in resolved_runs],
    }


def main(argv: Optional[list[str]] = None) -> int:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    ap.add_argument(
        "runs",
        nargs="+",
        help="One or more BoltzGen run folders (…/boltzgen__…_setN)",
    )
    ap.add_argument(
        "-o",
        "--output",
        type=Path,
        default=Path("rule_b_marp_deck"),
        help="Output directory for deck_rule_b.md + PDF/PPTX + assets/ (default: ./rule_b_marp_deck)",
    )
    ap.add_argument("--seed", type=int, default=0, help="RNG seed for model picks")
    ap.add_argument(
        "--pymol",
        type=Path,
        default=DEFAULT_PYMOL,
        help=f"PyMOL binary (default: {DEFAULT_PYMOL})",
    )
    ap.add_argument(
        "--scaffolds-dir",
        type=Path,
        default=DEFAULT_SCAFFOLDS_DIR,
        help=f"Scaffold design-fragment directory (default: {DEFAULT_SCAFFOLDS_DIR})",
    )
    ap.add_argument(
        "--marp",
        default=None,
        help="Optional path to host marp-cli (else Docker image is used for PDF)",
    )
    ap.add_argument(
        "--marp-docker-image",
        default=DEFAULT_MARP_DOCKER_IMAGE,
        help=f"Marp Docker image for PDF export (default: {DEFAULT_MARP_DOCKER_IMAGE})",
    )
    ap.add_argument(
        "--no-pdf",
        action="store_true",
        help="Skip PDF export (markdown + PNGs only)",
    )
    ap.add_argument(
        "--no-pptx",
        action="store_true",
        help="Skip PPTX export",
    )
    ap.add_argument(
        "--html",
        action="store_true",
        help="Also export HTML when host marp-cli is available",
    )
    args = ap.parse_args(argv)

    run_dirs = resolve_run_dirs(args.runs)
    if not run_dirs:
        print("No run directories given", file=sys.stderr)
        return 1

    try:
        summary = build_rule_b_presentation(
            run_dirs,
            args.output,
            seed=args.seed,
            pymol=args.pymol,
            scaffolds_dir=args.scaffolds_dir,
            marp_docker_image=args.marp_docker_image,
            marp_bin=args.marp,
            export_pdf=not args.no_pdf,
            export_pptx=not args.no_pptx,
            export_html=args.html,
        )
    except Exception as exc:  # noqa: BLE001
        print(f"[error] {exc}", file=sys.stderr)
        return 1

    if summary["slide_count"] == 0:
        print(
            "No slides generated (no runs with successful Boltz+ESMFold).",
            file=sys.stderr,
        )
        return 2

    print(
        f"[ok] slides={summary['slide_count']}  "
        f"md={summary['deck_md']}  pdf={summary.get('deck_pdf')}  "
        f"pptx={summary.get('deck_pptx')}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
