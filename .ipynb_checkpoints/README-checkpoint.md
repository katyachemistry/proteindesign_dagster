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
| `rfdiffusion` | `rfdiffusion_backbones` | `rfdiffusion.docker_image` |
| `rosettacommons/proteinmpnn` | `proteinmpnn_*` | `proteinmpnn.docker_image` |
| `boltz2` | `boltz2_predictions` | `boltz2.docker_image` |
| `ghcr.io/sokrypton/colabfold:1.6.0-cuda12` | `colabfold_predictions` | `colabfold.colabfold_image` |
| `aggregation_filters` | solubility/aggregation CLIs; Dagster RMSD reuses image for Python only | `filters.docker_image` (RMSD script: host `filters.scripts_host_dir`, not in image) |
| `alpine` | internal `chown` / `chmod` after container writes | hard-coded in `assets.py` |

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

**ColabFold** and **alpine**:

```bash
docker pull ghcr.io/sokrypton/colabfold:1.6.0-cuda12
docker pull alpine
```

Use the ColabFold tag pinned in `colabfold.colabfold_image` — do not substitute a
different tag unless you update `pipeline_config.yaml`.

### Build local images

Three images are built from this repo (not published to a registry):

**RFdiffusion** — Dockerfile at [`/storage/proteindesign/RFdiffusion/docker/Dockerfile`](../RFdiffusion/docker/Dockerfile):

```bash
cd /storage/proteindesign/RFdiffusion
docker build -f docker/Dockerfile -t rfdiffusion .
```

**Boltz-2** — Dockerfile at [`/storage/proteindesign/boltz2/Dockerfile`](../boltz2/Dockerfile):

```bash
cd /storage/proteindesign/boltz2
docker build -t boltz2 .
```

The pipeline uses a named Docker volume `boltz2-cache` (see `boltz2.cache_volume` in
config). Docker creates it on first use; no manual step is required.

**aggregation_filters** — Dockerfile at [`/storage/proteindesign/filters/aggregation/Dockerfile`](../filters/aggregation/Dockerfile).
See [`/storage/proteindesign/filters/aggregation/README.md`](../filters/aggregation/README.md) for details.

(Place the NetSolP model tarball in the build context (FILE IS ALREADY THERE):

```text
/storage/proteindesign/filters/aggregation/netsolp-1.0.ALL.tar.gz
```
)

Build from `filters/` (tag must match `filters.docker_image` in `pipeline_config.yaml`):

```bash
docker build -f aggregation/Dockerfile -t aggregation_filters /storage/proteindesign/filters
```

If the tarball is missing, either copy it into `filters/aggregation/` or download during build:

```bash
docker build -f aggregation/Dockerfile \
  --build-arg INSTALL_NETSOLP_FULL=1 \
  -t aggregation_filters \
  /storage/proteindesign/filters
```

#### Using aggregation tools manually (`aggregation_filters`)

The image ships **NetSolP**, **protein-sol**, **SoluProt**, and **Aggrescan3D** only.

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
| SoluProt | FASTA | Yes — many sequences in one file | `--no_proc N` (limited without USEARCH/TMHMM) | Split FASTA or multiple containers |
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

**SoluProt** (`soluprot`) — sequence solubility.

```bash
mkdir -p "$WORK/soluprot_tmp"

docker run --rm -v "$WORK:/workspace" "$AGGREGATION_FILTERS_IMAGE" \
  soluprot \
    --i_fa /workspace/sequences.fasta \
    --o_csv /workspace/soluprot_preds.csv \
    --tmp_dir /workspace/soluprot_tmp \
    --no_tmhmm

# Optional: process multiple sequences in parallel (USEARCH/TMHMM not installed in image)
docker run --rm -v "$WORK:/workspace" "$AGGREGATION_FILTERS_IMAGE" \
  soluprot \
    --i_fa /workspace/sequences.fasta \
    --o_csv /workspace/soluprot_preds.csv \
    --tmp_dir /workspace/soluprot_tmp \
    --no_tmhmm \
    --no_proc 4
```

Use `--no_tmhmm` unless you add TMHMM to the image. One multi-record FASTA per run;
increase `--no_proc` only when it helps (often marginal without USEARCH).

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
- **ColabFold cache** — `colabfold.colabfold_cache_host` (default
  `/storage/proteindesign/ColabFold_cache`; populated on first ColabFold run)
- **RMSD script (host only)** — `filters.scripts_host_dir` (default
  `/storage/proteindesign/filters/rmsd`; not baked into `aggregation_filters`)

#### Backbone RMSD (Dagster only — not in the image)
 TODO

### Verify images are present

```bash
docker images --format '{{.Repository}}:{{.Tag}}' \
  | grep -E '^(rfdiffusion|boltz2|aggregation_filters|rosettacommons/proteinmpnn|ghcr\.io/sokrypton/colabfold|alpine)'
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
and the ColabFold/alpine images above.
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
| `proteinmpnn_assigned_chains` | `docker_image`, `chain_list` | `rosettacommons/proteinmpnn`, `"A"` |
| `proteinmpnn_sequences` | `docker_image`, `num_seq_per_target`, `sampling_temp`, `use_soluble_model` | `rosettacommons/proteinmpnn`, 8 / 0.1 / false |
| `colabfold_input_fastas` | `target_epitope_fasta` | loops_epitope_target.fasta |
| `colabfold_predictions` | `model_type`, `input_relative`, `gpus` | af2_multimer_v3 |

Named design inputs are in `paths.epitopes_hotspots.designs_config`. Partition
outputs and `design_configs_manifest.json` are under
`rfdiffusion_backbones.config.outputs_dir`. Asset-specific mounts live with each
asset config (`rfdiffusion_models_host` in `rfdiffusion_backbones`, and
`colabfold_cache_host` in `colabfold_predictions`).
