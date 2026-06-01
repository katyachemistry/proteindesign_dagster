"""
Dagster definitions for the RFdiffusion → ProteinMPNN → ColabFold asset graph
(including Boltz-2 input YAML generation after ProteinMPNN, and optional filter
metrics after folding).

Two jobs
--------
generate_configs       – materialize rfdiffusion_yamls (generates YAML files and
                         registers design_configs partitions).  Run this first.

design_pipeline        – run one or more partitions through the full
                         RFdiffusion → ProteinMPNN → Boltz-2 YAMLs + ColabFold chain.
                         Select partition(s) in the Launchpad.

SSH tunnel (server UI on your laptop)
--------------------------------------
  Server:  dagster dev -h 0.0.0.0 -p 3000 -m dagster_pipeline.definitions
  Laptop:  ssh -L 3000:127.0.0.1:3000 USER@SERVER
  Browser: http://127.0.0.1:3000
"""

from dagster import Definitions, define_asset_job

from dagster_pipeline.assets import all_assets, design_configs
from dagster_pipeline.resources import PipelinePathsResource

generate_configs_job = define_asset_job(
    name="generate_configs",
    selection=["pipeline_config", "rfdiffusion_yamls"],
    description=(
        "Save tool parameters to pipeline_config.yaml and generate all "
        "(design × hotspot_file) YAML configs, registering them as partitions. "
        "Configure everything here — hotspot specs under 'paths' resource, "
        "tool params under 'pipeline_config' op config — then run design_pipeline."
    ),
)

design_pipeline_job = define_asset_job(
    name="design_pipeline",
    selection=[
        "rfdiffusion_backbones",
        "proteinmpnn_parsed",
        "proteinmpnn_assigned_chains",
        "proteinmpnn_sequences",
        "boltz2_input_yamls",
        "boltz2_predictions",
        "colabfold_input_fastas",
        "colabfold_predictions",
        "filter_chain_backbone_rmsd",
    ],
    partitions_def=design_configs,
    description=(
        "Run the full design pipeline (plus optional pairwise RMSD filters) for one or more partitions. "
        "Select partition(s) in the Launchpad. "
        "Run generate_configs first if partitions are not yet populated."
    ),
)

defs = Definitions(
    assets=all_assets,
    resources={"paths": PipelinePathsResource()},
    jobs=[generate_configs_job, design_pipeline_job],
)
