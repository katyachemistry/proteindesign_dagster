from pathlib import Path
from typing import Any, Dict, List, Literal, Optional

from dagster import Config, ConfigurableResource  # ConfigurableResource kept for PipelinePathsResource
from pydantic import BaseModel, Field, field_validator, model_validator

DEFAULT_OUTPUTS_DIR = "/storage/hCG/rfdiffusion/outputs"

# Path where pipeline_config asset writes/reads all tool parameters.
PIPELINE_CONFIG_PATH = Path(__file__).parents[1] / "pipeline_config.yaml"

_DEFAULT_TEMPLATE_PATH = Path(__file__).parent / "default_rfdiffusion_template.yaml"
DEFAULT_SCAFFOLD_GUIDED_TEMPLATE_YAML = _DEFAULT_TEMPLATE_PATH.read_text(encoding="utf-8")

_DEFAULT_BOLTZ2_TEMPLATE_PATH = Path(__file__).parent / "default_boltz2_template.yaml"


class DesignSpec(BaseModel):
    """One named RFdiffusion design input bundle.

    Provide either *template_rfdiffusion_yaml* (path) **or**
    *template_rfdiffusion_yaml_inline* (full YAML text from Launchpad).
    When using inline text, set *naming_prefix* to the config type prefix used for
    output file naming (e.g. ``scaffold_guided``).
    """

    hotspots_txts_dir: str
    target_pdb: str
    # Final directory where generated RFdiffusion YAMLs are written.
    output_base_dir: str
    template_rfdiffusion_yaml: Optional[str] = None
    template_rfdiffusion_yaml_inline: Optional[str] = None
    naming_prefix: str = "scaffold_guided"

    @model_validator(mode="after")
    def _template_path_xor_inline(self) -> "DesignSpec":
        has_path = bool(self.template_rfdiffusion_yaml and self.template_rfdiffusion_yaml.strip())
        has_inline = bool(
            self.template_rfdiffusion_yaml_inline and self.template_rfdiffusion_yaml_inline.strip()
        )
        if has_path == has_inline:
            raise ValueError(
                "Set exactly one of template_rfdiffusion_yaml (file path) or "
                "template_rfdiffusion_yaml_inline (embedded YAML string)."
            )
        return self


class EpitopesHotspotsConfig(Config):
    """Named RFdiffusion design inputs. Add any number of entries."""

    designs_config: Dict[str, Any] = Field(
        default={
            "designs_1": {
                "hotspots_txts_dir": "/storage/hCG/hotspots/loops/",
                "template_rfdiffusion_yaml_inline": DEFAULT_SCAFFOLD_GUIDED_TEMPLATE_YAML,
                "naming_prefix": "scaffold_guided",
                "target_pdb": "/storage/hCG/truncated_renumbered_loops.pdb",
                "output_base_dir": "/storage/hCG/rfdiffusion/configs/scaffold_guided/loops",
            }
        },
        validate_default=True,
    )

    @field_validator("designs_config", mode="before")
    @classmethod
    def _coerce_design_specs(cls, v: Any) -> Any:
        """Coerce each entry (plain dict or DesignSpec) into a DesignSpec."""
        if not isinstance(v, dict):
            return v
        return {
            name: DesignSpec.model_validate(spec) if isinstance(spec, dict) else spec
            for name, spec in v.items()
        }


class PipelinePathsResource(ConfigurableResource):
    """Single pipeline resource split by concern for Launchpad clarity."""

    epitopes_hotspots: EpitopesHotspotsConfig = Field(default_factory=EpitopesHotspotsConfig)


# ---------------------------------------------------------------------------
# Centralised tool-parameters resource
# ---------------------------------------------------------------------------


class RFDiffusionToolConfig(Config):
    """Container/runtime parameters for the RFdiffusion backbone generation step."""

    docker_image: str = "rfdiffusion"
    gpus: str = "1,2"
    outputs_dir: str = DEFAULT_OUTPUTS_DIR
    rfdiffusion_models_host: str = "/storage/proteindesign/RFdiffusion/models"
    model_directory_path: str = "/storage/proteindesign/RFdiffusion/models"
    # Additional Hydra overrides appended verbatim, e.g. ["inference.num_designs=5"]
    extra_hydra_args: List[str] = Field(default_factory=list)


class ProteinMPNNToolConfig(Config):
    """Container/runtime parameters for all three ProteinMPNN steps."""

    docker_image: str = "rosettacommons/proteinmpnn"
    gpus: str = "1,2"
    chain_list: str = "A"
    num_seq_per_target: int = 5
    fixed_positions_jsonl: str = ""
    omit_AAs: str = "X"
    sampling_temp: str = "0.1"
    use_soluble_model: bool = True
    extra_args: List[str] = Field(default_factory=list)


class Boltz2ToolConfig(Config):
    """Parameters for all Boltz-2 steps: YAML input preparation and prediction."""

    # --- boltz2_input_yamls (YAML preparation step) ---
    # Path to a small YAML stub: ``version:`` is read; sequences are built by
    # ``write_boltz2_yamls`` (A = binder; B, C, … = epitope segments split on ':').
    template_yaml: str = str(_DEFAULT_BOLTZ2_TEMPLATE_PATH.resolve())

    # --- boltz2_predictions (inference step) ---
    docker_image: str = "boltz2"
    gpus: str = "1,2"
    cache_volume: str = "boltz2-cache"
    use_msa_server: bool = True
    use_potentials: bool = False
    override: bool = False
    recycling_steps: int = 3
    devices: int = 1
    diffusion_samples: int = 1
    num_workers: int = 0
    no_kernels: bool = True
    query_chunk_size: int = 10


class FiltersToolConfig(Config):
    """Docker image and arguments for structure filter scripts (post-folding)."""

    docker_image: str = "proteindesign-filters"
    align_chain: str = "B"
    rmsd_chains: str = "A,B"


class ColabFoldToolConfig(Config):
    """Parameters for ColabFold input preparation and prediction steps."""

    # --- colabfold_input_fastas (merge-FASTA step) ---
    target_epitope_fasta: str = "/storage/hCG/loops_epitope_target.fasta"
    fasta_extensions: List[str] = Field(default_factory=lambda: [".fa", ".fasta", ".faa"])

    # --- colabfold_predictions ---
    gpus: str = "1,2"
    model_type: str = "alphafold2_multimer_v3"
    # Multimer MSA pairing: ``unpaired``, ``paired``, or ``unpaired_paired`` (ColabFold default).
    pair_mode: Literal["unpaired", "paired", "unpaired_paired"] = "unpaired_paired"
    num_models: int = 5
    num_recycle: int = 3
    num_relax: int = 0
    query_chunk_size: int = 10
    colabfold_image: str = "ghcr.io/sokrypton/colabfold:1.6.0-cuda12"
    colabfold_cache_host: str = "/storage/proteindesign/ColabFold_cache"
    apply_alphafold_numpy_patch: bool = True
    extra_colabfold_args: List[str] = Field(default_factory=list)


