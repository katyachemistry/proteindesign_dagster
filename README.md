# Protein design Dagster pipeline

Asset graph:

```
configs            rfdiffusion         proteinmpnn                          colabfold
──────────────     ───────────         ───────────────────────────────      ──────────────────────────
rfdiffusion_yamls_loops ─┐
                     ├─► rfdiffusion_backbones ─► proteinmpnn_parsed
rfdiffusion_yamls_knot ──┘                              └─► proteinmpnn_assigned_chains
                                                          └─► proteinmpnn_sequences
                                                                └─► colabfold_input_fastas
                                                                      └─► colabfold_predictions
```

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
| `proteindesign-filters` | `filter_chain_backbone_rmsd` | `filters.docker_image` |
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

**Filters** — Dockerfile at [`/storage/proteindesign/filters/Dockerfile`](../filters/Dockerfile).
See [`/storage/proteindesign/filters/README.md`](../filters/README.md) for optional
build args:

```bash
docker build -t proteindesign-filters /storage/proteindesign/filters
```

### Host paths mounted at run time

These are not Docker images, but the pipeline expects them to exist:

- **RFdiffusion models** — `rfdiffusion.rfdiffusion_models_host` (default
  `/storage/proteindesign/RFdiffusion/models`)
- **ColabFold cache** — `colabfold.colabfold_cache_host` (default
  `/storage/proteindesign/ColabFold_cache`; populated on first ColabFold run)

### Verify images are present

```bash
docker images --format '{{.Repository}}:{{.Tag}}' \
  | grep -E '^(rfdiffusion|boltz2|proteindesign-filters|rosettacommons/proteinmpnn|ghcr\.io/sokrypton/colabfold|alpine)'
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

Repeat for `boltz2`, `proteindesign-filters`, `rosettacommons/proteinmpnn:latest`,
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
| `proteinmpnn_sequences` | `docker_image`, `num_seq_per_target`, `sampling_temp`, `use_soluble_model` | `rosettacommons/proteinmpnn`, 8 / 0.1 / true |
| `colabfold_input_fastas` | `target_epitope_fasta` | loops_epitope_target.fasta |
| `colabfold_predictions` | `model_type`, `input_relative`, `gpus` | af2_multimer_v3 |

Named design inputs are in `paths.epitopes_hotspots.designs_config`. Partition
outputs and `design_configs_manifest.json` are under
`rfdiffusion_backbones.config.outputs_dir`. Asset-specific mounts live with each
asset config (`rfdiffusion_models_host` in `rfdiffusion_backbones`, and
`colabfold_cache_host` in `colabfold_predictions`).
