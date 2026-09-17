# Protein design Dagster pipeline

Asset graph for **design → structure filter → ProteinMPNN → SoluProt → MSA → Boltz-2 / ESMFold → scores → Rule A/B/C follow-up**.

Runtime config is written per run to `{outputs_root}/{run_id}/pipeline_config.yaml`. The file in this directory is only the **Launchpad template**.

![Global asset lineage](Global_Asset_Lineage_1.svg)

## Jobs

| Job | What it does |
|---|---|
| `generate_configs` | Persist tool params, generate YAMLs for enabled design tools, register partitions. Run this first for a design campaign. |
| `design_pipeline` | Design backends → structure filter → ProteinMPNN → SoluProt → shared MSA → Boltz-2 + ESMFold (and renumber). Select partition(s) in Launchpad. |
| `register_sequences` | Add external-sequence partitions under a run (`{run_id}__seq__{fasta_stem}`). Template: [`register_sequences_config.yaml`](register_sequences_config.yaml). |
| `sequence_pipeline` | Import FASTAs → SoluProt → MSA → Boltz-2 + ESMFold for `sequence_import` partitions. Run `register_sequences` first. |
| `final_scores` | Developability metrics, Rule A/B/C export, glycan clashes, LH off-target, Marp decks, presentation zips. Materialize after predictions (at least one of Boltz-2 / ESMFold). |

ColabFold assets exist in code but are **not** registered in `design_pipeline`.

## Install

```bash
mamba env create -f environment.yml        # first time
mamba activate proteindesign-dagster
pip install -e /storage/proteindesign/dagster_pipeline
```

## Start UI (SSH tunnel)

**On the server:**

```bash
mamba activate proteindesign-dagster
cd /storage/proteindesign/dagster_pipeline
dagster dev -h 0.0.0.0 -p 3000 -m dagster_pipeline.definitions
```

**On your laptop:**

```bash
ssh -L 3000:127.0.0.1:3000 USER@SERVER
```

Then open **http://127.0.0.1:3000**.

## Running the pipeline

1. Edit [`pipeline_config.yaml`](pipeline_config.yaml) (enable tools, designs, GPUs, etc.).
2. Launchpad → **`generate_configs`** → Launch (empty `run_id` creates a timestamped folder under `outputs_root`).
3. Launchpad → **`design_pipeline`** → select partition(s) → Launch.
4. After Boltz-2 and/or ESMFold finish: Launchpad → **`final_scores`** → same partition(s).

**Partial re-run** (e.g. changed ProteinMPNN params): Asset Catalog → select `proteinmpnn_sequences` and everything downstream → **Materialize selected**. Downstream assets read the **run-scoped** `{outputs_root}/{run_id}/pipeline_config.yaml`, not the repo template.

**External sequences:** Launchpad → `register_sequences` (set `add_to_run_id` + `input_path`) → `sequence_pipeline` on the new `{run_id}__seq__*` keys → then `final_scores` if needed.

### Partitions

Design partitions are tool-tagged:

```text
{run_id}__{rfdiffusion|boltzgen|promera}__{design_name}__{target}__{hotspot_or_spec}__{yaml_stem}
```

Imported sequences:

```text
{run_id}__seq__{fasta_stem}
```

Run layout (default `outputs_root`: `/storage/hCG/designs/runs`):

```text
{outputs_root}/{run_id}/
  pipeline_config.yaml
  design_configs_manifest.json
  design_configs/{rfdiffusion|boltzgen|promera}/...
  {partition_suffix}/
    designs/{tool}/design_*.pdb    # BoltzGen also keeps raw/*.cif
    pdbs_filtered/                 # after design_structure_filter → ProteinMPNN
    proteinmpnn/
    boltz2/  boltz2_renumbered/
    esmfold/ esmfold_renumbered/
    final_scores/metrics.csv
    filtered_designs/
    lh_offtarget_b/                # Rule B LH
    lh_offtarget_ac/               # Rule A/C LH
  rule_b/                          # Marp deck for Rule B (run-level)
  rule_ac/                         # Marp deck for Rule A/C (run-level)
```

### Empty filters

When the design structure filter or SoluProt leaves no candidates, downstream assets still **materialize** with `has_candidates=false` / `branch_status=no_candidates` (or `skipped` for the wrong tool / `sequence_import`). Expensive work does not run, but backfills complete instead of hanging on Dagster `STEP_SKIPPED`.

| `branch_status` | Meaning |
|---|---|
| `has_candidates` | Normal work ran |
| `no_candidates` | Filter left nothing; empty success |
| `skipped` | Asset does not apply to this partition type |

## Launchpad config (what you change often)

Top-level keys live on the `pipeline_config` asset (`generate_configs`). Defaults below match the repo template.

| Scope | Fields | Notes |
|---|---|---|
| Top level | `gpus`, `run_id`, `outputs_root` | `gpus` is shared (e.g. `1,2`). Empty `run_id` → timestamped run dir. |
| `rfdiffusion` | `enabled`, `designs_config`, `extra_hydra_args` | Per-design hotspots, target PDB, scaffold list, inline or file template YAML. |
| `boltzgen` | `enabled`, `num_designs`, `designs_config` | Per-scaffold `binder_sequence_file`, hotspots, antihotspots. |
| `promera` | `enabled`, `designs_config` | `task_config_yaml` template + `target_fasta` / `target_pdb` / `hotspots_txts_dir`. Optional `hotspot_files` subset (`[]` = all `.txt`). |
| `structure_filters` | `enabled`, `structure_docker_image` | `enabled` only gates `design_structure_filter`. Renumber still uses the image when `*_renumber_outputs` is true. |
| `proteinmpnn` | `num_seq_per_target`, `sampling_temp`, `omit_AAs`, `omit_AA`, `use_soluble_model` | Per-scaffold `omit_AA` (e.g. `affimer: C`) merged when the name matches. BoltzGen can pass `--fixed_positions_jsonl`. |
| `soluprot` | `enabled`, `min_soluble_score`, `no_proc` | After MPNN; passing seqs go to `proteinmpnn/seqs_filtered/`. |
| `msa` | `server_url`, pairing / retry settings | Required for Boltz-2 (and LH Boltz-2). ESMFold is MSA-free. |
| `boltz2` | `target_fasta`, `target_pdb`, `diffusion_samples`, `renumber_outputs` | Target FASTA segments split on `:` become chains B, C, … |
| `esmfold` | `num_loops`, `num_diffusion_samples`, `renumber_outputs` | Target FASTA shared with Boltz-2. |
| `final_scores` | `cpus`, `ipsae_script`, `skip_ipsae` | Hotspots/antihotspots usually come from the design YAML; sequence-import writes them via `register_sequences`. |
| `filtered_designs` | Rule A/B/C thresholds | See [Final scores](#final-scores-and-rules-abc). |
| `lh_offtarget_b` / `lh_offtarget_ac` | `enabled`, `target_fasta` | LH alpha:beta FASTA instead of hCG. |
| `rule_b_presentation` / `rule_ac_presentation` | `pymol`, `marp_docker_image`, `export_pdf` / `export_pptx` | Host PyMOL + Marp CLI image. |

## Design backends

Enable any combination:

| Key | Partitions | Output |
|---|---|---|
| `rfdiffusion.enabled` | `tool=rfdiffusion` | Scaffold-guided backbones |
| `boltzgen.enabled` | `tool=boltzgen` | Design-only (`boltzgen run … --steps design`); pipeline ProteinMPNN replaces inverse folding |
| `promera.enabled` | `tool=promera` | `python -m promera` Design task; PDBs normalized to `designs/promera/design_N.pdb` (A=binder, B=target) |

Shared **`design_structure_filter`**: optional binder–target CA contacts (`antihotspots.enabled` + `structure_filters.enabled`), then 100% hotspot coverage and a capped antihotspot contact fraction. Passing PDBs go to `pdbs_filtered/` for ProteinMPNN. Binder–alpha contacts are reported only.

BoltzGen extra Docker flags: `--shm-size`, `--group-add` (gpuusers), `LD_LIBRARY_PATH` for CUDA 13 NVRTC, `--use_kernels false` by default. Set `HF_TOKEN` in the environment or repo `.env` for HuggingFace downloads.

Promera mounts `promera.weights_host`, `tinyprot_cache_host`, and `ligandmpnn_dir`. `target_pdb` is for the structure filter only (independent of Promera’s target JSON).

## SoluProt (`proteinmpnn_soluprot_filter`)

After `proteinmpnn_sequences`, SoluProt scores all binder sequences (one combined FASTA per partition) and writes passers to `proteinmpnn/seqs_filtered/`. Boltz-2 and ESMFold read from that directory.

| Key | Default | Meaning |
|-----|---------|---------|
| `enabled` | `true` | Set `false` to pass `seqs/` through unchanged |
| `min_soluble_score` | `0.5` | Keep sequences with SoluProt probability ≥ this |
| `no_proc` | `1` | Parallel SoluProt workers |
| `fail_if_all_filtered` | `false` | Ignored by Dagster; empty result → downstream `no_candidates` |

TMHMM is always on (`no_tmhmm` in old YAML is ignored). Build: [`filters/soluprot/build.sh`](../filters/soluprot/build.sh).

## MSA (Boltz-2 only)

The `MSA` asset talks to a ColabFold-style MSA server (`msa.server_url`, default `http://127.0.0.1:18080/api`). The server must be reachable from the Dagster host. ESMFold does not use this step.

## Final scores and Rules A/B/C

Materialize the **`final_scores`** job after predictions. Metrics: [`filters/final_scores/README.md`](../filters/final_scores/README.md).

1. **`final_scores_metrics`** — `{partition}/final_scores/metrics.csv` with `_boltz` / `_esmfold` columns (PyRosetta interface, IPSAE, design-vs-prediction RMSD). Prefers `*_renumbered/`. Sequences with `specificity_hotspots=0` on every model skip expensive Rosetta/IPSAE/RMSD.
2. **`filtered_designs_export`** — consensus on `specificity_hotspots_* == 2` (configurable):
   - **Rule A** — a parent design has ≥ `min_parent_successes` (default 5) successful models under one predictor. AF3 JSON keeps **one best sequence per parent**.
   - **Rule B** — same MPNN sequence succeeds on **both** predictors: (≥2 Boltz and ≥2 ESMFold) **or** (4+ on one and 1+ on the other).
   - **Rule C** — sequence not already selected; ≥ `min_seq_successes_fallback` (4) successes on one predictor and ≥ `min_good_binder_scores` (2) with `binder_score <= max_binder_score` (0).
   - Writes `filtered_metrics_rule_ac.csv`, `filtered_metrics_rule_b.csv`, combined `filtered_metrics.csv`, and `for_AF3.json`.
3. **Rule B track** — stationary glycan superimpose (`hCG_glycans.pdb` onto design β), GlycoSHIELD ensemble clashes (`/storage/hCG/glycans/gs_runs`), LH off-target under `lh_offtarget_b/`, Marp `deck_rule_b`, presentation zip.
4. **Rule A/C track** — waits for complete AF3 uploads under the partition `AF3/` folder, scores AF3 specificity, then the same glycan / LH / Marp / zip path under `lh_offtarget_ac/` and `{run_id}/rule_ac/`. Partitions without `AF3/` **soft-skip** this track.

Glycan clash: α-Asn52 N-glycan heavy atoms within 2.4 Å of binder (chain A). Ensemble scores are population-weighted across MD clusters.

## Docker images

The pipeline runs tools with `docker run`. Names come from the run-scoped `pipeline_config.yaml`. GPU steps need the [NVIDIA Container Toolkit](https://docs.nvidia.com/datacenter/cloud-native/container-toolkit/install-guide.html) (`--runtime=nvidia`).

| Image | Used by | Config |
|---|---|---|
| `rfdiffusion` | `rfdiffusion_generation` | `rfdiffusion.docker_image` |
| `boltzgen` | `boltzgen_generation` | `boltzgen.docker_image` |
| `promera` | `promera_generation` | `promera.docker_image` |
| `rosettacommons/proteinmpnn` | `proteinmpnn_*` | `proteinmpnn.docker_image` |
| `soluprot` | `proteinmpnn_soluprot_filter` | `soluprot.docker_image` |
| `boltz2` | `boltz2_predictions` (+ LH) | `boltz2.docker_image` |
| `esmfold2` | `esmfold_predictions` (+ LH) | `esmfold.docker_image` |
| `structure_tools` | `design_structure_filter`, `boltz2_renumber`, `esmfold_renumber` | `structure_filters.structure_docker_image` |
| `final_scores` | `final_scores_metrics` | `final_scores.docker_image` |
| `marpteam/marp-cli:v4.1.2` | Rule B / A/C PDF+PPTX | `rule_*_presentation.marp_docker_image` |
| `alpine` | internal `chown` / `chmod` after container writes | hard-coded in `assets.py` |

`aggregation_filters` is **not** used by the current Dagster jobs. RMSD lives in `final_scores`. Manual NetSolP / protein-sol / Aggrescan3D usage is at the end of this file.

### Pull from a registry

**ProteinMPNN** — official [RosettaCommons image](https://hub.docker.com/r/rosettacommons/proteinmpnn) (`linux/amd64` only). Sequence generation needs a GPU; parse/assign do not.

```bash
docker pull rosettacommons/proteinmpnn:latest
docker pull marpteam/marp-cli:v4.1.2
docker pull alpine
```

### Build local images

**RFdiffusion** — [`RFdiffusion/docker/Dockerfile`](../RFdiffusion/docker/Dockerfile):

```bash
cd /storage/proteindesign/RFdiffusion
docker build -f docker/Dockerfile -t rfdiffusion .
```

**BoltzGen** — [`boltzgen/Dockerfile`](../boltzgen/Dockerfile):

```bash
cd /storage/proteindesign/boltzgen
docker build -t boltzgen .
```

Weights download to `boltzgen.cache_dir` (default `/storage/proteindesign/boltzgen/cache`) on first run. Export `HF_TOKEN` (or put it in the repo `.env`).

**Promera** — [`promera/Dockerfile`](../promera/Dockerfile):

```bash
cd /storage/proteindesign/promera
docker build -t promera .
```

Mounts (not baked in): `promera.weights_host` (default `/storage/proteindesign/promera/weights`), `tinyprot_cache_host`, `ligandmpnn_dir`. See [`promera/README.md`](../promera/README.md).

**Boltz-2** — [`boltz2/Dockerfile`](../boltz2/Dockerfile):

```bash
cd /storage/proteindesign/boltz2
docker build -t boltz2 .
```

Named volume `boltz2-cache` (`boltz2.cache_volume`) is created on first use.

**ESMFold2:**

```bash
cd /storage/proteindesign/ESMfold2
docker build -t esmfold2 .
```

HuggingFace cache: `esmfold.hf_cache_dir` (default `/storage/proteindesign/ESMfold2/hf_cache`). First run downloads `biohub/ESMFold2`. See [ESMfold2/README.md](../ESMfold2/README.md). Path-preserving binds: partition root is mounted at its host path; `ESMfold2/predict.py` is mounted read-only.

**structure_tools** — [`filters/structure/README.md`](../filters/structure/README.md):

```bash
docker build -f structure/Dockerfile -t structure_tools /storage/proteindesign/filters
```

**final_scores** — [`filters/final_scores/README.md`](../filters/final_scores/README.md):

```bash
docker build -f filters/final_scores/Dockerfile -t final_scores /storage/proteindesign
```

**soluprot:** [`filters/soluprot/build.sh`](../filters/soluprot/build.sh).

### Host paths (not images)

- **RFdiffusion models** — `rfdiffusion.rfdiffusion_models_host` (default `/storage/proteindesign/RFdiffusion/models`)
- **ESMFold2 HuggingFace cache** — `esmfold.hf_cache_dir`
- **BoltzGen cache** — under the boltzgen repo `cache/` directory
- **Promera weights / tinyprot cache / LigandMPNN** — `promera.*`
- **IPSAE script** — `final_scores.ipsae_script` (default `/storage/shaburova/antibodies/IPSAE/ipsae.py`)
- **Glycans** — `/storage/hCG/hCG_glycans.pdb` and GlycoSHIELD caches under `/storage/hCG/glycans/gs_runs`
- **PyMOL** — `rule_*_presentation.pymol` on the host

### Verify images

```bash
docker images --format '{{.Repository}}:{{.Tag}}' \
  | grep -E '^(rfdiffusion|boltzgen|promera|boltz2|esmfold2|structure_tools|final_scores|soluprot|rosettacommons/proteinmpnn|marpteam/marp-cli|alpine)'
```

### Load / save (offline hosts)

There is no checked-in image tarball. Copy with `docker save` / `docker load`; tags must match `pipeline_config.yaml`.

---

## Standalone aggregation image (`aggregation_filters`)

Not part of the Dagster jobs. Image: [`filters/aggregation/Dockerfile`](../filters/aggregation/Dockerfile). Details: [`filters/aggregation/README.md`](../filters/aggregation/README.md).

Place extracted NetSolP models in the build context:

```text
/storage/proteindesign/filters/aggregation/models/*.onnx
/storage/proteindesign/filters/aggregation/models/*.pkl
```

```bash
docker build -f aggregation/Dockerfile -t aggregation_filters /storage/proteindesign/filters
```

The image ships **NetSolP**, **protein-sol**, and **Aggrescan3D**. SoluProt is a separate image.

```bash
export AGGREGATION_FILTERS_IMAGE=aggregation_filters
export WORK=/storage/proteindesign/my_filter_run
mkdir -p "$WORK"
```

```bash
docker run --rm -v "$WORK:/workspace" "$AGGREGATION_FILTERS_IMAGE" <command> ...
```

| Tool | Input | Multiple designs in one run? | In-tool parallelism | Scale out |
|------|--------|-------------------------------|---------------------|-----------|
| NetSolP | FASTA | Yes — many `>seq` records in one file | `--NUM_THREADS` (PyTorch) | Split FASTA + multiple `docker run`, or raise threads |
| protein-sol | FASTA | Yes — many sequences in one file | No (sequential Perl) | Split FASTA or one container per file |
| SoluProt | FASTA | Yes — many sequences in one file | `--no_proc N` (TMHMM + USEARCH) | Split FASTA or multiple containers |
| Aggrescan3D | PDB | No — one structure per invocation | No | One `docker run` per PDB |

**NetSolP** (`netsolp`):

```bash
docker run --rm -v "$WORK:/workspace" "$AGGREGATION_FILTERS_IMAGE" \
  netsolp \
    --FASTA_PATH /workspace/sequences.fasta \
    --OUTPUT_PATH /workspace/netsolp_preds.csv \
    --MODEL_TYPE Distilled \
    --PREDICTION_TYPE S \
    --NUM_THREADS 8
```

- `--PREDICTION_TYPE`: `S` (solubility), `U` (usability), `SU` (both).
- `--MODEL_TYPE`: `Distilled` (fast), `ESM12`, `ESM1b`, or `Both`.

**protein-sol** — output is under the tool install dir inside the image; copy it out:

```bash
docker run --rm -v "$WORK:/workspace" "$AGGREGATION_FILTERS_IMAGE" bash -c \
  'protein-sol /workspace/sequences.fasta && cp /opt/tools/protein-sol/protein-sol-sequence-prediction-software/seq_prediction.txt /workspace/protein_sol_seq_prediction.txt'
```

**SoluProt** (separate image):

```bash
export SOLUPROT_IMAGE=soluprot
mkdir -p "$WORK/soluprot_tmp"

docker run --rm -v "$WORK:/workspace" "$SOLUPROT_IMAGE" \
  soluprot \
    --i_fa /workspace/sequences.fasta \
    --o_csv /workspace/soluprot_preds.csv \
    --tmp_dir /workspace/soluprot_tmp \
    --no_proc 4
```

**Aggrescan3D** (`aggrescan`):

```bash
mkdir -p "$WORK/aggrescan_out"

docker run --rm -v "$WORK:/workspace" "$AGGREGATION_FILTERS_IMAGE" \
  aggrescan \
    -i /workspace/model.pdb \
    -w /workspace/aggrescan_out \
    -v 4 \
    -O
```

- `-O` — overwrite an existing work directory.
- `-C A` — restrict to chain `A` (optional).
- Outputs: `output.pdb` (B-factors = scores), `A3D.csv`, `A.png` / `B.png` per chain.

```bash
find "$WORK/pdbs" -name '*.pdb' | parallel -j 4 \
  'docker run --rm -v "$WORK:/workspace" '"$AGGREGATION_FILTERS_IMAGE"' \
    aggrescan -i /workspace/pdbs/{/} -w /workspace/aggrescan_{/%.pdb} -v 2 -O'
```

FoldX, CABS-flex, and dynamic modes are not installed.

**Smoke test:**

```bash
docker run --rm "$AGGREGATION_FILTERS_IMAGE" \
  python -c "import Bio; import pyrosetta; print('BioPython:', Bio.__version__); print('PyRosetta OK')"
```
