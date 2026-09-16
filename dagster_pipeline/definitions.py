"""
Dagster definitions for design backends (RFdiffusion / BoltzGen) → ProteinMPNN →
Boltz-2 / ESMFold.

Jobs
----
generate_configs       – materialize pipeline_config (persists tool parameters,
                         generates configs for enabled design tools, registers
                         design_configs partitions). Run this first.

design_pipeline        – run one or more partitions through
                         design → structure filter → ProteinMPNN → SoluProt →
                         shared-MSA Boltz-2 + ESMFold. Select partitions in Launchpad.

SSH tunnel (server UI on your laptop)
--------------------------------------
  Server:  dagster dev -h 0.0.0.0 -p 3000 -m dagster_pipeline.definitions
  Laptop:  ssh -L 3000:127.0.0.1:3000 USER@SERVER
  Browser: http://127.0.0.1:3000
"""

from dagster import Definitions, define_asset_job

from dagster_pipeline.assets import all_assets, design_configs
from dagster_pipeline.resources import (
    default_generate_configs_run_config,
    default_register_sequence_run_config,
)

generate_configs_job = define_asset_job(
    name="generate_configs",
    selection=["pipeline_config"],
    config=default_generate_configs_run_config(),
    description=(
        "Save tool parameters to {outputs_root}/{run_id}/pipeline_config.yaml (new timestamped "
        "run dir each launch unless run_id is set), generate configs for enabled design tools "
        "under {outputs_root}/{run_id}/design_configs/{rfdiffusion|boltzgen|promera}/, and "
        "register them as partitions. Enable tools via rfdiffusion.enabled / boltzgen.enabled / "
        "promera.enabled. Launchpad pre-fills from dagster_pipeline/pipeline_config.yaml "
        "(template only)."
    ),
)

design_pipeline_job = define_asset_job(
    name="design_pipeline",
    selection=[
        "rfdiffusion_generation",
        "boltzgen_generation",
        "promera_generation",
        "design_structure_filter",
        "proteinmpnn_parsed",
        "proteinmpnn_sequences",
        "proteinmpnn_soluprot_filter",
        "MSA",
        "boltz2_input_yamls",
        "boltz2_predictions",
        "boltz2_renumber",
        "esmfold_input_jsons",
        "esmfold_predictions",
        "esmfold_renumber",
        # "colabfold_input_fastas",
        # "colabfold_predictions",
    ],
    partitions_def=design_configs,
    description=(
        "Run the full design pipeline for one or more partitions "
        "(RFdiffusion and/or BoltzGen and/or Promera → structure filter → ProteinMPNN → "
        "SoluProt → precomputed MSAs → Boltz-2 + renumber, alongside MSA-free ESMFold + "
        "renumber). Partitions are "
        "tool-tagged ({run_id}__{rfdiffusion|boltzgen|promera}__…). "
        "Partitions with zero designs after a filter step succeed: downstream "
        "assets still materialize with branch_status=no_candidates (not failed, "
        "and not a Dagster skip that would hang asset backfills). Select "
        "partition(s) in the Launchpad. "
        "Run generate_configs first if partitions are not yet populated."
    ),
)

final_scores_job = define_asset_job(
    name="final_scores",
    selection=[
        "final_scores_metrics",
        "filtered_designs_export",
        "glycan_binder_clashes_rule_b",
        "glycan_gs_ensemble_clashes_rule_b",
        "lh_rule_b_inputs",
        "lh_MSA",
        "lh_boltz2_input_yamls",
        "lh_boltz2_predictions",
        "lh_esmfold_input_jsons",
        "lh_esmfold_predictions",
        "lh_binding_scores",
        "rule_b_presentation",
        "rule_b_successful_designs_zip",
        # Rule A/C track (parallel to Rule B; soft-skips partitions without AF3/)
        "af3_rule_ac_ready",
        "af3_rule_ac_scores",
        "af3_rule_ac_successes",
        "glycan_binder_clashes_rule_ac",
        "glycan_gs_ensemble_clashes_rule_ac",
        "lh_ac_inputs",
        "lh_ac_MSA",
        "lh_ac_boltz2_input_yamls",
        "lh_ac_boltz2_predictions",
        "lh_ac_esmfold_input_jsons",
        "lh_ac_esmfold_predictions",
        "lh_ac_binding_scores",
        "rule_ac_presentation",
        "rule_ac_successful_designs_zip",
    ],
    partitions_def=design_configs,
    description=(
        "Score selected design partition(s), export Rule A/B/C filtered designs; "
        "for Rule B successes run stationary glycan superimpose (zip), MD/GlycoSHIELD "
        "ensemble glycan–binder clashes + LH off-target under ``lh_offtarget_b/`` + "
        "Rule B Marp deck + presentation zip; "
        "for Rule A/C gate on complete AF3 uploads, score AF3 specificity, run "
        "stationary glycan superimpose + ensemble clashes + LH under ``lh_offtarget_ac/``, "
        "build ``{run_id}/rule_ac/`` (``deck_rule_ac.md`` + ``deck_rule_ac.pdf``), then zip "
        "presentation designs. Partitions without ``AF3/`` soft-skip the A/C track. "
        "Select partition(s) in the Launchpad. Materialize after "
        "``boltz2_predictions`` / ``boltz2_renumber`` and/or "
        "``esmfold_predictions`` / ``esmfold_renumber`` have finished for every "
        "selected partition (at least one predictor required)."
    ),
)

register_sequences_job = define_asset_job(
    name="register_sequences",
    selection=["register_sequence_partitions"],
    config=default_register_sequence_run_config(),
    description=(
        "Add external-sequence partitions under an existing run "
        "(``add_to_run_id``). One Launchpad partition per FASTA file in "
        "``input_path``; keys look like ``{run_id}__seq__{fasta_stem}``. "
        "Configure hotspots/antihotspots in ``register_sequences_config.yaml``."
    ),
)

sequence_pipeline_job = define_asset_job(
    name="sequence_pipeline",
    selection=[
        "import_binder_sequences",
        "proteinmpnn_sequences",
        "proteinmpnn_soluprot_filter",
        "MSA",
        "boltz2_input_yamls",
        "boltz2_predictions",
        "boltz2_renumber",
        "esmfold_input_jsons",
        "esmfold_predictions",
        "esmfold_renumber",
    ],
    partitions_def=design_configs,
    description=(
        "Run import → SoluProt → precomputed-MSA Boltz-2 + ESMFold (and renumber) for "
        "``sequence_import`` partitions. "
        "Run ``register_sequences`` first (required upstream for import), then select "
        "the new ``{run_id}__seq__*`` partition(s) in the Launchpad."
    ),
)

defs = Definitions(
    assets=all_assets,
    jobs=[
        generate_configs_job,
        register_sequences_job,
        design_pipeline_job,
        sequence_pipeline_job,
        final_scores_job,
    ],
)
