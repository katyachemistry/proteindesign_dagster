# Protein design Dagster pipeline

## Install

```bash
mamba env create -f environment.yml        # first time
mamba activate proteindesign-dagster
pip install -e /storage/proteindesign/dagster_pipeline
```

## Docker images

The pipeline runs tools via `docker run`. Image names come from
[`pipeline_config.yaml`](pipeline_config.yaml) (written by the `pipeline_config`
asset). GPU steps need the [NVIDIA Container Toolkit](https://docs.nvidia.com/datacenter/cloud-native/container-toolkit/install-guide.html)
(`--runtime=nvidia`).

| Image | Used by | Default in config |
|---|---|---|
| `rfdiffusion` | `rfdiffusion_generation` | `rfdiffusion.docker_image` |
| `boltzgen` | `boltzgen_generation` | `boltzgen.docker_image` |
| `rosettacommons/proteinmpnn` | `proteinmpnn_*` | `proteinmpnn.docker_image` |
| `boltz2` | `boltz2_predictions` | `boltz2.docker_image` |
| `esmfold2` | `esmfold_predictions` | `esmfold.docker_image` |
| `structure_tools` | `design_structure_filter`, renumbering | `structure_filters.structure_docker_image` |
| `aggregation_filters` | NetSolP, protein-sol, Aggrescan3D; Dagster RMSD reuses image for Python only | `filters.docker_image` (RMSD script: host `filters.scripts_host_dir`, not in image) |
| `soluprot` | SoluProt sequence solubility (USEARCH + TMHMM) | `soluprot.docker_image` (filter after ProteinMPNN) |
| `alpine` | internal `chown` / `chmod` after container writes | hard-coded in `assets.py` |

#### SoluProt filter (`proteinmpnn_soluprot_filter`)

After `proteinmpnn_sequences`, the pipeline runs SoluProt on all binder sequences
(one combined FASTA per partition), then writes passing sequences to
``proteinmpnn/seqs_filtered/``. Boltz-2 and ESMFold read from that directory.

Configure in ``pipeline_config.yaml`` under ``soluprot``:

| Key | Default | Meaning |
|-----|---------|---------|
| `enabled` | `true` | Set `false` to passthrough ``seqs/`` unchanged |
| `min_soluble_score` | `0.5` | Keep sequences with SoluProt probability ≥ this |
| `no_tmhmm` | `false` | Use full TMHMM model (recommended with `soluprot` image) |
| `no_proc` | `1` | Parallel SoluProt workers |
| `fail_if_all_filtered` | `false` | Legacy flag; Dagster always records an empty SoluProt result and skips Boltz-2 / ESMFold instead of failing the partition |

Build the image: [`filters/soluprot/build.sh`](../filters/soluprot/build.sh).

**Empty filter results:** When the design structure filter or SoluProt step leaves
no candidates, the partition still succeeds. Status assets
(``design_structure_filter``, ``proteinmpnn_soluprot_filter``) always materialize
with ``has_candidates`` / ``branch_status`` metadata; conditional assets
(``proteinmpnn_parsed``, ``boltz2_input_yamls``, ``esmfold_input_jsons``) are skipped and expensive downstream
steps do not run.

### Design backends (RFdiffusion / BoltzGen)

Enable either or both under ``pipeline_config.yaml``:

| Key | Meaning |
|-----|---------|
| `rfdiffusion.enabled` | Generate RFdiffusion partitions (`tool=rfdiffusion`) |
| `boltzgen.enabled` | Generate BoltzGen design-only partitions (`tool=boltzgen`) |

Partition keys are tool-tagged:

```text
{run_id}__{rfdiffusion|boltzgen}__{design_name}__{target}__{hotspot_or_spec}__{yaml_stem}
```

Run layout (default ``outputs_root``: ``/storage/hCG/designs/runs``):

```text
{outputs_root}/{run_id}/
  design_configs/rfdiffusion/...
  design_configs/boltzgen/...
  {partition_suffix}/
    designs/{tool}/design_*.pdb   # BoltzGen also keeps raw/*.cif
    pdbs_filtered/                # after design_structure_filter → ProteinMPNN
```

BoltzGen runs ``boltzgen run … --steps design`` only (pipeline ProteinMPNN replaces
inverse folding). Set ``HF_TOKEN`` in the environment or repo ``.env`` for HuggingFace
weight downloads. The asset also sets ``--shm-size``, ``--group-add`` (gpuusers),
``LD_LIBRARY_PATH`` for CUDA 13 NVRTC, and ``--use_kernels false`` by default.

### Pull from a registry

**ProteinMPNN** — not built in this repo. The pipeline uses the official RosettaCommons
image ([Docker Hub](https://hub.docker.com/r/rosettacommons/proteinmpnn)) for all three
`proteinmpnn_*` assets (parse, assign chains, generate sequences). Default tag is
`latest` (`proteinmpnn.docker_image` in config).

```bash
docker pull rosettacommons/proteinmpnn:latest
```

The image is `linux/amd64` only (not ARM). Sequence generation needs a GPU
(`--runtime=nvidia`); the parse/assign steps do not.

**ESMFold2** and **alpine**:

```bash
cd /storage/proteindesign/ESMfold2
docker build -t esmfold2 .
docker pull alpine
```

The HuggingFace model cache is mounted from ``esmfold.hf_cache_dir`` (default
``/storage/proteindesign/ESMfold2/hf_cache``). The first run downloads
``biohub/ESMFold2`` into that directory. See [ESMfold2/README.md](../ESMfold2/README.md).

**Mount paths:** ESMFold2 uses path-preserving binds (same as design backends): the partition
root is mounted at its host path, e.g.
``/storage/hCG/designs/runs/{run_id}/{partition}/`` → mmCIF + sidecars in ``…/esmfold/``.
``ESMfold2/predict.py`` is mounted read-only from the host.

Use the ESMFold2 tag pinned in `esmfold.docker_image` — do not substitute a
different tag unless you update `pipeline_config.yaml`.

### Build local images

Three images are built from this repo (not published to a registry):

**RFdiffusion** — Dockerfile at [`/storage/proteindesign/RFdiffusion/docker/Dockerfile`](../RFdiffusion/docker/Dockerfile):

```bash
cd /storage/proteindesign/RFdiffusion
docker build -f docker/Dockerfile -t rfdiffusion .
```

**BoltzGen** — Dockerfile at [`/storage/proteindesign/boltzgen/Dockerfile`](../boltzgen/Dockerfile):

```bash
cd /storage/proteindesign/boltzgen
docker build -t boltzgen .
```

Weights download to ``boltzgen.cache_dir`` (default ``/storage/proteindesign/boltzgen/cache``)
on first run. Export ``HF_TOKEN`` (or put it in the repo ``.env``) to avoid unauthenticated
HuggingFace rate limits.

**Boltz-2** — Dockerfile at [`/storage/proteindesign/boltz2/Dockerfile`](../boltz2/Dockerfile):

```bash
cd /storage/proteindesign/boltz2
docker build -t boltz2 .
```

The pipeline uses a named Docker volume `boltz2-cache` (see `boltz2.cache_volume` in
config). Docker creates it on first use; no manual step is required.

**aggregation_filters** — Dockerfile at [`/storage/proteindesign/filters/aggregation/Dockerfile`](../filters/aggregation/Dockerfile).
See [`/storage/proteindesign/filters/aggregation/README.md`](../filters/aggregation/README.md) for details.

(Place extracted NetSolP model files in the build context:

```text
/storage/proteindesign/filters/aggregation/models/*.onnx
/storage/proteindesign/filters/aggregation/models/*.pkl
```
)

Build from `filters/` (tag must match `filters.docker_image` in `pipeline_config.yaml`):

```bash
docker build -f aggregation/Dockerfile -t aggregation_filters /storage/proteindesign/filters
```

#### Using aggregation tools manually (`aggregation_filters`)

The image ships **NetSolP**, **protein-sol**, and **Aggrescan3D** only.
SoluProt is a separate image — see [`/storage/proteindesign/filters/soluprot/README.md`](../filters/soluprot/README.md).

All examples mount a host directory at `/workspace` inside the container. Adjust
paths as needed.

```bash
export AGGREGATION_FILTERS_IMAGE=aggregation_filters
export WORK=/storage/proteindesign/my_filter_run   # your inputs + outputs
mkdir -p "$WORK"
```

General pattern:

```bash
docker run --rm -v "$WORK:/workspace" "$AGGREGATION_FILTERS_IMAGE" <command> ...
```

**Batching vs parallelization**

| Tool | Input | Multiple designs in one run? | In-tool parallelism | Scale out |
|------|--------|-------------------------------|---------------------|-----------|
| NetSolP | FASTA | Yes — many `>seq` records in one file | `--NUM_THREADS` (PyTorch) | Split FASTA + multiple `docker run`, or raise threads |
| protein-sol | FASTA | Yes — many sequences in one file | No (sequential Perl) | Split FASTA or one container per file |
| SoluProt | FASTA | Yes — many sequences in one file | `--no_proc N` (TMHMM + USEARCH) | Split FASTA or multiple containers |
| Aggrescan3D | PDB | No — one structure per invocation | No | One `docker run` per PDB (e.g. GNU parallel, Dagster) |

---

**NetSolP** (`netsolp`) — sequence solubility / usability (requires models baked into the image).

```bash
# Single FASTA, many sequences; CSV output
docker run --rm -v "$WORK:/workspace" "$AGGREGATION_FILTERS_IMAGE" \
  netsolp \
    --FASTA_PATH /workspace/sequences.fasta \
    --OUTPUT_PATH /workspace/netsolp_preds.csv \
    --MODEL_TYPE Distilled \
    --PREDICTION_TYPE S

# Faster on CPU: more PyTorch threads (default is low)
docker run --rm -v "$WORK:/workspace" "$AGGREGATION_FILTERS_IMAGE" \
  netsolp \
    --FASTA_PATH /workspace/sequences.fasta \
    --OUTPUT_PATH /workspace/netsolp_preds.csv \
    --MODEL_TYPE Distilled \
    --PREDICTION_TYPE S \
    --NUM_THREADS 8
```

- `--PREDICTION_TYPE`: `S` (solubility), `U` (usability), `SU` (both).
- `--MODEL_TYPE`: `Distilled` (fast), `ESM12`, `ESM1b`, or `Both` (slowest, averages).

---

**protein-sol** (`protein-sol`) — sequence solubility (Manchester server model).

```bash
docker run --rm -v "$WORK:/workspace" "$AGGREGATION_FILTERS_IMAGE" \
  protein-sol /workspace/sequences.fasta
```

Output is written under the tool install dir inside the image, not next to your
input. Copy results out after the run:

```bash
docker run --rm -v "$WORK:/workspace" "$AGGREGATION_FILTERS_IMAGE" bash -c \
  'protein-sol /workspace/sequences.fasta && cp /opt/tools/protein-sol/protein-sol-sequence-prediction-software/seq_prediction.txt /workspace/protein_sol_seq_prediction.txt'
```

The wrapper copies your FASTA to a temp file so the original on the host is not
rewritten. One FASTA can contain many sequences; they are processed in one batch
with no multi-core option inside the tool.

---

**SoluProt** (`soluprot`) — sequence solubility with TMHMM. Separate image; build with
[`filters/soluprot/build.sh`](../filters/soluprot/build.sh).

```bash
export SOLUPROT_IMAGE=soluprot
mkdir -p "$WORK/soluprot_tmp"

docker run --rm -v "$WORK:/workspace" "$SOLUPROT_IMAGE" \
  soluprot \
    --i_fa /workspace/sequences.fasta \
    --o_csv /workspace/soluprot_preds.csv \
    --tmp_dir /workspace/soluprot_tmp

# Optional: parallel workers
docker run --rm -v "$WORK:/workspace" "$SOLUPROT_IMAGE" \
  soluprot \
    --i_fa /workspace/sequences.fasta \
    --o_csv /workspace/soluprot_preds.csv \
    --tmp_dir /workspace/soluprot_tmp \
    --no_proc 4
```

TMHMM is enabled by default. Pass `--no_tmhmm` only if you want the slightly less
accurate model without transmembrane features.

---

**Aggrescan3D** (`aggrescan`) — structure aggregation propensity (PDB in, scores + plots out).

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

One PDB per command. To score many structures in parallel on the host:

```bash
find "$WORK/pdbs" -name '*.pdb' | parallel -j 4 \
  'docker run --rm -v "$WORK:/workspace" '"$AGGREGATION_FILTERS_IMAGE"' \
    aggrescan -i /workspace/pdbs/{/} -w /workspace/aggrescan_{/%.pdb} -v 2 -O'
```

(Requires [GNU parallel](https://www.gnu.org/software/parallel/); adjust `-j` to CPU count.)

Example with a repo test structure:

```bash
docker run --rm -v /storage/proteindesign:/workspace "$AGGREGATION_FILTERS_IMAGE" \
  aggrescan -i /workspace/5I0Z.pdb -w /workspace/aggrescan_5I0Z -v 4 -O
```

FoldX, CABS-flex, and dynamic modes are not installed in this image.

---

**Smoke test** (BioPython + PyRosetta in the image):

```bash
docker run --rm "$AGGREGATION_FILTERS_IMAGE" \
  python -c "import Bio; import pyrosetta; print('BioPython:', Bio.__version__); print('PyRosetta OK')"
```

### Host paths mounted at run time

These are not Docker images, but the pipeline expects them to exist:

- **RFdiffusion models** — `rfdiffusion.rfdiffusion_models_host` (default
  `/storage/proteindesign/RFdiffusion/models`)
- **ESMFold2 HuggingFace cache** — ``esmfold.hf_cache_dir`` (default
  ``/storage/proteindesign/ESMfold2/hf_cache``)
- **RMSD script (host only)** — `filters.scripts_host_dir` (default
  `/storage/proteindesign/filters/rmsd`; not baked into `aggregation_filters`)

#### Backbone RMSD (Dagster only — not in the image)
 TODO

### Verify images are present

```bash
docker images --format '{{.Repository}}:{{.Tag}}' \
  | grep -E '^(rfdiffusion|boltz2|esmfold2|aggregation_filters|rosettacommons/proteinmpnn|alpine)'
```

### Load / save (offline or air-gapped hosts)

There is no checked-in image tarball in this repo. To copy images from a machine
that already has them:

```bash
# On the source machine (example: rfdiffusion)
docker save rfdiffusion:latest | gzip > rfdiffusion.tar.gz

# On the target machine
docker load < rfdiffusion.tar.gz
```

Repeat for `boltz2`, `aggregation_filters`, `rosettacommons/proteinmpnn:latest`,
`esmfold2`, and `alpine`.
After loading, image names/tags must match those in `pipeline_config.yaml`.

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

Then open **http://127.0.0.1:3000** in a browser.

## Running the pipeline

1. **Generate configs** (first time, or after changing designs/hotspots): Launchpad →
   `generate_configs` job → Launch
2. **Full run**: Launchpad → `design_pipeline` job → select partition(s) → Launch
3. **Partial re-run** (e.g. changed ProteinMPNN params): Asset Catalog → select
   `proteinmpnn_sequences` and everything downstream → **Materialize selected**
   (update config in the dialog if needed)

## Launchpad config (key params you'll change often)

| Scope | Config field | Default |
|---|---|---|
| Resource `paths.epitopes_hotspots` | `designs_config` | `designs_1`, `designs_2` |
| `rfdiffusion_backbones` | `outputs_dir`, `gpus`, `extra_hydra_args` | `/storage/hCG/rfdiffusion/outputs`, `1,2`, `[]` |
| `proteinmpnn_parsed` | `docker_image` | `rosettacommons/proteinmpnn` |
| `proteinmpnn_parsed` | `docker_image`, `chain_list` | `rosettacommons/proteinmpnn`, `"A"`; BoltzGen also writes `fixed_positions.jsonl` from `binder_sequence_file` |
| `proteinmpnn_sequences` | `docker_image`, `num_seq_per_target`, `sampling_temp`, `use_soluble_model`, `omit_AA` | `rosettacommons/proteinmpnn`, 10 / 0.1 / true; per-scaffold `omit_AA` (e.g. `affimer: C`) merged into `omit_AAs` when the name matches; passes `--fixed_positions_jsonl` when present |
| `esmfold_input_jsons` | `target_fasta` (defaults to `boltz2.target_fasta`) | loops_epitope_target.fasta |
| `esmfold_predictions` | `docker_image`, `num_loops`, `query_chunk_size`, `gpus` | esmfold2 / 20 / 10 |

Named design inputs are in `paths.epitopes_hotspots.designs_config`. Partition
outputs and `design_configs_manifest.json` are under
`rfdiffusion_backbones.config.outputs_dir`. Asset-specific mounts live with each
asset config (`rfdiffusion_models_host` in `rfdiffusion_backbones`, and
`esmfold.hf_cache_dir` for the ESMFold2 HuggingFace model cache).
