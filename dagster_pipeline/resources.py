from pathlib import Path
from typing import Any, Dict, List, Literal, Optional

import yaml
from dagster import Config
from pydantic import BaseModel, Field, field_validator, model_validator

DEFAULT_OUTPUTS_ROOT = "/storage/hCG/designs/runs"
# Legacy default kept for docs/scripts that still reference a single outputs folder.
DEFAULT_OUTPUTS_DIR = "/storage/hCG/designs/outputs"

_PROTEINDESIGN_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_ESMFOLD2_HF_CACHE_DIR = str(_PROTEINDESIGN_ROOT / "ESMfold2" / "hf_cache")
DEFAULT_MSA_CACHE_DIR = str(_PROTEINDESIGN_ROOT / "msa_cache")
DEFAULT_BOLTZ2_CACHE_VOLUME = "boltz2-cache"
DEFAULT_BOLTZGEN_SHM_SIZE = "16g"
DEFAULT_BOLTZGEN_GPU_GROUP_ID = 1021

# Launchpad template only; runtime config lives under {outputs_root}/{run_id}/pipeline_config.yaml.
PIPELINE_CONFIG_TEMPLATE_PATH = Path(__file__).parents[1] / "pipeline_config.yaml"
REGISTER_SEQUENCES_CONFIG_TEMPLATE_PATH = (
    Path(__file__).parents[1] / "register_sequences_config.yaml"
)
PIPELINE_CONFIG_PATH = PIPELINE_CONFIG_TEMPLATE_PATH  # backward-compatible alias

DEFAULT_ANTIHOTSPOT_RES = ["B80-B87", "B61-B66", "B11-B16", "B30-B33"]
DEFAULT_HOTSPOTS_TXTS_DIR = "/storage/hCG/hotspots/both_loops/"
DEFAULT_RFDIFFUSION_TARGET_PDB = "/storage/hCG/glycosylated_renumbered_loops_B_chain.pdb"
DEFAULT_BOLTZGEN_TARGET_PDB = "/storage/hCG/hCG_no_glycans.pdb"
DEFAULT_TARGET_FASTA = "/storage/hCG/loops_epitope_target_alpha_beta.fasta"
DEFAULT_PREDICTION_TARGET_PDB = DEFAULT_BOLTZGEN_TARGET_PDB
DEFAULT_SCAFFOLDS_ROOT_DIR = "/storage/hCG/scaffold_ss_adj_backup"
DEFAULT_SCAFFOLDS = ["affibody", "affimer", "affitin", "monobody", "nanobody"]
DEFAULT_FASTA_EXTENSIONS = [".fa", ".fasta", ".faa"]

_DEFAULT_TEMPLATE_PATH = Path(__file__).parent / "default_rfdiffusion_template.yaml"
DEFAULT_SCAFFOLD_GUIDED_TEMPLATE_YAML = _DEFAULT_TEMPLATE_PATH.read_text(encoding="utf-8")

_DEFAULT_BOLTZ2_TEMPLATE_PATH = Path(__file__).parent / "default_boltz2_template.yaml"
_DEFAULT_BOLTZGEN_DESIGN_SPEC_PATH = Path(__file__).parent / "default_boltzgen_design_spec.yaml"
DEFAULT_BOLTZGEN_DESIGN_SPEC = str(_DEFAULT_BOLTZGEN_DESIGN_SPEC_PATH.resolve())
DEFAULT_BOLTZGEN_CACHE_DIR = str(_PROTEINDESIGN_ROOT / "boltzgen" / "cache")

DEFAULT_INSERTIONS_FOR_SCAFFOLDS: dict[str, dict[str, bool | int]] = {
    "affitin": {
        "mask_loops": True,
        "sampled_insertion": 2,
        "sampled_N": 1,
        "sampled_C": 1,
    },
    "affibody": {
        "mask_loops": False,
        "sampled_insertion": 0,
        "sampled_N": 0,
        "sampled_C": 0,
    },
    "affimer": {
        "mask_loops": True,
        "sampled_insertion": 5,
        "sampled_N": 3,
        "sampled_C": 3,
    },
    "monobody": {
        "mask_loops": True,
        "sampled_insertion": 2,
        "sampled_N": 1,
        "sampled_C": 1,
    },
    "nanobody": {
        "mask_loops": True,
        "sampled_insertion": 2,
        "sampled_N": 1,
        "sampled_C": 1,
    },
}


class ScaffoldInsertionSettings(BaseModel):
    """Per-scaffold RFdiffusion loop masking and length sampling overrides."""

    mask_loops: bool
    sampled_insertion: int = Field(default=0, ge=0)
    sampled_N: int = Field(default=0, ge=0)
    sampled_C: int = Field(default=0, ge=0)

    @model_validator(mode="after")
    def _insertions_require_mask_loops(self) -> "ScaffoldInsertionSettings":
        if not self.mask_loops and (
            self.sampled_insertion or self.sampled_N or self.sampled_C
        ):
            raise ValueError(
                "RFdiffusion requires sampled_insertion, sampled_N, and sampled_C to be 0 "
                "when mask_loops is false."
            )
        return self


class DesignSpec(BaseModel):
    """One named RFdiffusion design input bundle.

    Provide either *template_rfdiffusion_yaml* (path) **or**
    *template_rfdiffusion_yaml_inline* (full YAML text from Launchpad).
    When using inline text, set *naming_prefix* to the config type prefix used for
    output file naming (e.g. ``scaffold_guided``).

    For scaffold-guided designs, set *scaffolds_root_dir* to the parent directory
    containing one subfolder per scaffold (each with ``*_ss.pt`` / ``*_adj.pt``),
    and list the scaffold folder names to use in *scaffolds*. Optional per-scaffold
    loop/insertion settings live in *insertions_for_scaffolds* (others keep template defaults).
    """

    hotspots_txts_dir: str
    target_pdb: str
    template_rfdiffusion_yaml: Optional[str] = None
    template_rfdiffusion_yaml_inline: Optional[str] = None
    naming_prefix: str = "scaffold_guided"
    scaffolds_root_dir: Optional[str] = None
    scaffolds: List[str] = Field(default_factory=list)
    insertions_for_scaffolds: Dict[str, ScaffoldInsertionSettings] = Field(
        default_factory=dict,
        description=(
            "Optional per-scaffold overrides for scaffoldguided.mask_loops and "
            "sampled_insertion / sampled_N / sampled_C. Scaffolds not listed keep "
            "values from the RFdiffusion template YAML. Entries for scaffolds not in "
            "``scaffolds`` are kept as presets and ignored until that scaffold is enabled."
        ),
    )

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
        if self.scaffolds and not self.scaffolds_root_dir:
            raise ValueError(
                "scaffolds_root_dir is required when scaffolds are specified."
            )
        return self


class BoltzGenAntihotspotsSettings(BaseModel):
    """Hotspot / antihotspot contact filter settings for BoltzGen partitions."""

    enabled: bool = True
    contact_distance: float = 10.0
    # Fraction of configured antihotspot residues allowed to contact (0–1).
    # Applied only when every hotspot is contacted.
    max_antihotspot_contact_fraction: float = Field(default=0.05, ge=0.0, le=1.0)
    res: List[str] = Field(default_factory=lambda: list(DEFAULT_ANTIHOTSPOT_RES))


class BoltzGenDesignSpec(BaseModel):
    """One named BoltzGen design-only input bundle (design step → shared structure filter)."""

    design_spec_yaml: str = DEFAULT_BOLTZGEN_DESIGN_SPEC
    target_pdb: str = DEFAULT_BOLTZGEN_TARGET_PDB
    hotspots_txts_dir: str = DEFAULT_HOTSPOTS_TXTS_DIR
    # BoltzGen design-fragment sequence file (``.txt``; letters = fixed AA, N..M = designed).
    # Injected as protein entity with id ``binder_chain`` when generating YAMLs.
    binder_sequence_file: Optional[str] = None
    antihotspots: BoltzGenAntihotspotsSettings = Field(
        default_factory=BoltzGenAntihotspotsSettings
    )
    protocol: str = "protein-anything"
    binder_chain: str = "C"
    # Keep source beta chain B as normalized chain B so hotspot labels remain valid.
    target_chains: List[str] = Field(default_factory=lambda: ["B", "A"])
    # YAML / partition naming stem; defaults to the designs_config entry name when unset.
    naming_prefix: Optional[str] = None


class RFDiffusionToolConfig(Config):
    """Container/runtime parameters for the RFdiffusion backbone generation step."""

    enabled: bool = False
    docker_image: str = "rfdiffusion"
    rfdiffusion_models_host: str = "/storage/proteindesign/RFdiffusion/models"
    model_directory_path: str = "/storage/proteindesign/RFdiffusion/models"
    # Additional Hydra overrides appended verbatim, e.g. ["inference.num_designs=5"]
    extra_hydra_args: List[str] = Field(default_factory=list)
    # Named design input bundles (hotspots, template, target PDB, scaffold set).
    designs_config: Dict[str, Any] = Field(
        default={
            "designs_1": {
                "hotspots_txts_dir": DEFAULT_HOTSPOTS_TXTS_DIR,
                "template_rfdiffusion_yaml_inline": DEFAULT_SCAFFOLD_GUIDED_TEMPLATE_YAML,
                "naming_prefix": "scaffold_guided",
                "target_pdb": DEFAULT_RFDIFFUSION_TARGET_PDB,
                "scaffolds_root_dir": DEFAULT_SCAFFOLDS_ROOT_DIR,
                "scaffolds": list(DEFAULT_SCAFFOLDS),
                "insertions_for_scaffolds": DEFAULT_INSERTIONS_FOR_SCAFFOLDS,
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
        out: dict[str, Any] = {}
        for name, spec in v.items():
            if not isinstance(spec, dict):
                out[name] = spec
                continue
            spec = dict(spec)
            insertions = spec.get("insertions_for_scaffolds")
            if isinstance(insertions, dict):
                spec["insertions_for_scaffolds"] = {
                    scaffold: ScaffoldInsertionSettings.model_validate(settings)
                    if isinstance(settings, dict)
                    else settings
                    for scaffold, settings in insertions.items()
                }
            out[name] = DesignSpec.model_validate(spec)
        return out


class BoltzGenToolConfig(Config):
    """Container/runtime parameters for BoltzGen design-only backbone generation."""

    enabled: bool = True
    docker_image: str = "boltzgen"
    # Shared across all scaffolds / hotspot partitions.
    num_designs: int = Field(default=20, ge=1)
    extra_args: List[str] = Field(default_factory=list)
    designs_config: Dict[str, Any] = Field(
        default={
            "affimer_4N6T": {
                "design_spec_yaml": DEFAULT_BOLTZGEN_DESIGN_SPEC,
                "binder_sequence_file": (
                    "/storage/hCG/scaffolds/affimer_4N6T_with_Nterm_design_fragments.txt"
                ),
                "target_pdb": DEFAULT_BOLTZGEN_TARGET_PDB,
                "hotspots_txts_dir": DEFAULT_HOTSPOTS_TXTS_DIR,
            },
            "affibody_2B89": {
                "design_spec_yaml": DEFAULT_BOLTZGEN_DESIGN_SPEC,
                "binder_sequence_file": (
                    "/storage/hCG/scaffolds/affibody_2B89_design_fragments.txt"
                ),
                "target_pdb": DEFAULT_BOLTZGEN_TARGET_PDB,
                "hotspots_txts_dir": DEFAULT_HOTSPOTS_TXTS_DIR,
            },
            "affitin": {
                "design_spec_yaml": DEFAULT_BOLTZGEN_DESIGN_SPEC,
                "binder_sequence_file": (
                    "/storage/hCG/scaffolds/affitin_design_fragments.txt"
                ),
                "target_pdb": DEFAULT_BOLTZGEN_TARGET_PDB,
                "hotspots_txts_dir": DEFAULT_HOTSPOTS_TXTS_DIR,
            },
            "monobody_1TTG": {
                "design_spec_yaml": DEFAULT_BOLTZGEN_DESIGN_SPEC,
                "binder_sequence_file": (
                    "/storage/hCG/scaffolds/monobody_1TTG_design_fragments.txt"
                ),
                "target_pdb": DEFAULT_BOLTZGEN_TARGET_PDB,
                "hotspots_txts_dir": DEFAULT_HOTSPOTS_TXTS_DIR,
            },
            "nanobody_1I3V": {
                "design_spec_yaml": DEFAULT_BOLTZGEN_DESIGN_SPEC,
                "binder_sequence_file": (
                    "/storage/hCG/scaffolds/nanobody_1I3V_design_fragments.txt"
                ),
                "target_pdb": DEFAULT_BOLTZGEN_TARGET_PDB,
                "hotspots_txts_dir": DEFAULT_HOTSPOTS_TXTS_DIR,
            },
        },
        validate_default=True,
    )

    @field_validator("designs_config", mode="before")
    @classmethod
    def _coerce_boltzgen_design_specs(cls, v: Any) -> Any:
        if not isinstance(v, dict):
            return v
        out: dict[str, Any] = {}
        for name, spec in v.items():
            if isinstance(spec, dict):
                # num_designs lives on BoltzGenToolConfig, not per scaffold.
                cleaned = {k: val for k, val in spec.items() if k != "num_designs"}
                out[name] = BoltzGenDesignSpec.model_validate(cleaned)
            else:
                out[name] = spec
        return out

class PromeraToolConfig(Config):
    """Container/runtime parameters for Promera VHH backbone generation."""

    enabled: bool = False
    docker_image: str = "promera"
    weights_host: str = "/storage/proteindesign/promera/weights"
    weights_file: str = "promera_2606.ckpt"
    tinyprot_cache_host: str = "/storage/proteindesign/promera/tinyprot_cache"
    ligandmpnn_dir: str = "/storage/proteindesign/promera/LigandMPNN"
    shared_group_gid: int = 1024  # proteindesign — matches docker_user_args.sh usage

    # Each entry = one full promera task_config YAML (test_script.yaml-style) +
    # the dagster-side hotspot/antihotspot definition used for design_structure_filter
    # (independent of whatever promera itself was conditioned on internally).
    designs_config: Dict[str, "PromeraDesignSpec"] = Field(default_factory=dict)

class PromeraDesignSpec(BaseModel):
    task_config_yaml: str          # promera's own full YAML (test_script.yaml-style)
    target_pdb: str                # reference PDB for design_structure_filter
    target_pdb_chain: str = "B"
    target_res_min: int = 1
    target_res_max: int = 102      # promera's own target-chain length for this campaign
    hotspot_res: List[str] = Field(default_factory=list)         # e.g. ["B21","B22","B76","B77"]
    antihotspots: BoltzGenAntihotspotsSettings = Field(default_factory=BoltzGenAntihotspotsSettings)

class StructureFiltersToolConfig(Config):
    """Docker image for structure-based filter and renumbering scripts."""

    enabled: bool = True
    structure_docker_image: str = "structure_tools"

    @model_validator(mode="before")
    @classmethod
    def _coerce_structure_filters_aliases(cls, data: Any) -> Any:
        """Accept legacy ``on`` and YAML 1.1 boolean key ``True`` (from unquoted ``on:``)."""
        if not isinstance(data, dict):
            return data
        data = dict(data)
        if True in data:
            data.setdefault("enabled", data.pop(True))
        if "on" in data and "enabled" not in data:
            data["enabled"] = data.pop("on")
        return data


def _normalize_structure_filters_yaml(data: dict[str, Any]) -> None:
    """Fix structure_filters keys after YAML 1.1 parses unquoted ``on:`` as ``True:``."""
    section = data.get("structure_filters")
    if not isinstance(section, dict):
        return
    if True in section:
        section.setdefault("enabled", section.pop(True))
    if "on" in section and "enabled" not in section:
        section["enabled"] = section.pop("on")


# Default per-scaffold ProteinMPNN omit letters (merged into global omit_AAs).
DEFAULT_OMIT_AA_BY_SCAFFOLD: Dict[str, str] = {
    "affimer": "C",
    "affibody": "C",
    "affitin": "C",
    "monobody": "C",
    "nanobody": "C",
}


class ProteinMPNNToolConfig(Config):
    """Container/runtime parameters for all three ProteinMPNN steps."""

    docker_image: str = "rosettacommons/proteinmpnn"
    chain_list: str = "A"
    num_seq_per_target: int = 10
    # Global omit letters always applied (ProteinMPNN ``--omit_AAs`` base).
    omit_AAs: str = "X"
    # Per-scaffold extra omit letters. If a key appears in the partition / design
    # name (e.g. ``affimer`` in ``...__affimer_4N6T__...``), those letters are
    # merged into ``omit_AAs`` (longest matching key wins).
    # Use a concrete ``default=`` (not default_factory) so Dagster Launchpad
    # serializes the map into the config YAML instead of omitting it.
    omit_AA: Dict[str, str] = Field(
        default={
            "affimer": "C",
            "affibody": "C",
            "affitin": "C",
            "monobody": "C",
            "nanobody": "C",
        },
        description=(
            "Map scaffold name substring → extra amino acids to omit during inverse "
            "folding. Matched against partition key / design_name / config_name."
        ),
    )
    sampling_temp: str = "0.1"
    use_soluble_model: bool = True
    extra_args: List[str] = Field(default_factory=list)


class SoluProtToolConfig(Config):
    """SoluProt sequence solubility filter applied after ProteinMPNN.

    TMHMM is always enabled (``--no_tmhmm`` is not exposed).
    """

    docker_image: str = "soluprot"
    enabled: bool = True
    min_soluble_score: float = 0.5
    no_proc: int = 1
    fail_if_all_filtered: bool = False  # ignored by Dagster; kept for CLI/scripts

    @model_validator(mode="before")
    @classmethod
    def _drop_legacy_no_tmhmm(cls, data: Any) -> Any:
        """Accept and ignore legacy ``no_tmhmm`` from older pipeline_config.yaml files."""
        if isinstance(data, dict):
            data = dict(data)
            data.pop("no_tmhmm", None)
        return data


class MSAToolConfig(Config):
    """Shared precomputed MSA settings used only by the Boltz-2 branch."""

    server_url: str = "http://127.0.0.1:18080/api"
    pairing_strategy: Literal["greedy", "complete"] = "greedy"
    use_env: bool = True
    # SSH tunnels to zebra drop occasionally; prefer long timeouts + many retries.
    request_timeout_seconds: float = Field(default=120.0, gt=0)
    max_retries: int = Field(default=20, ge=0)
    poll_interval_seconds: float = Field(default=5.0, gt=0)
    retry_backoff_seconds: float = Field(default=2.0, gt=0)
    max_paired_seqs: int = Field(default=8192, ge=1)
    max_msa_seqs: int = Field(default=16384, ge=1)

    @model_validator(mode="after")
    def _validate_msa_limits(self) -> "MSAToolConfig":
        if self.max_paired_seqs > self.max_msa_seqs:
            raise ValueError("msa.max_paired_seqs cannot exceed msa.max_msa_seqs")
        if not self.server_url.strip():
            raise ValueError("msa.server_url cannot be empty")
        return self


class Boltz2ToolConfig(Config):
    """Parameters for all Boltz-2 steps: YAML input preparation and prediction."""

    # --- boltz2_input_yamls (YAML preparation step) ---
    # Path to a small YAML stub: ``version:`` is read; sequences are built by
    # ``write_boltz2_yamls`` (A = binder; B, C, … = epitope segments split on ':').
    template_yaml: str = str(_DEFAULT_BOLTZ2_TEMPLATE_PATH.resolve())
    target_fasta: str = DEFAULT_TARGET_FASTA
    target_pdb: str = DEFAULT_PREDICTION_TARGET_PDB
    fasta_extensions: List[str] = Field(default_factory=lambda: list(DEFAULT_FASTA_EXTENSIONS))

    # --- boltz2_predictions (inference step) ---
    docker_image: str = "boltz2"
    use_potentials: bool = False
    override: bool = False
    recycling_steps: int = 3
    diffusion_samples: int = 5
    num_workers: int = 0
    preprocessing_threads: int = 2
    # Lightning ``--devices`` for predict. Default 1 avoids NCCL deadlocks when
    # ``gpus`` lists multiple IDs for RFdiffusion but Docker multi-GPU Boltz hangs.
    devices: int = 1
    query_chunk_size: int = 10

class ESMFoldToolConfig(Config):
    """Parameters for ESMFold2 structure prediction (Biohub ``esmfold2`` Docker image).

    Outputs live under each partition directory, e.g.
    ``{outputs_root}/{run_id}/{partition}/esmfold/`` (mmCIF + confidence / PAE sidecars).

    Target FASTA and FASTA extensions are shared with ``boltz2``. The prediction
    script and HuggingFace cache use fixed repository defaults.
    """

    docker_image: str = "esmfold2"
    model: str = "biohub/ESMFold2"
    num_loops: int = 20
    num_sampling_steps: int = 100
    num_diffusion_samples: int = 5
    seed: int = 0
    query_chunk_size: int = 10
    renumber_outputs: bool = True


class FinalScoresToolConfig(Config):
    """Post-prediction developability metrics (PyRosetta, IPSAE, RMSD) for Boltz-2 and ESMFold."""

    docker_image: str = "final_scores"
    cpus: int = 4
    dalphaball_path: str = "/usr/local/lib/final_scores/DAlphaBall.gcc"
    ipsae_script: str = "/storage/shaburova/antibodies/IPSAE/ipsae.py"
    skip_ipsae: bool = False
    hotspots: List[str] = Field(
        default=[],
        description="Hotspot residues for coverage metrics (reference PDB numbering).",
    )
    antihotspots: List[str] = Field(
        default=[],
        description="Antihotspot residues for coverage metrics (reference PDB numbering).",
    )


class FilteredDesignsToolConfig(Config):
    """Post-metrics developability filters (Filter_analysis.ipynb / pyrosetta_thresholds.json)."""

    enabled: bool = True
    thresholds_json: str = str(
        _PROTEINDESIGN_ROOT / "zebra_developability" / "pyrosetta_thresholds.json"
    )
    predictor: Literal["auto", "boltz", "esmfold", "legacy"] = Field(
        default="auto",
        description="auto = ESMFold columns when present, else Boltz, else legacy flat CSV.",
    )
    min_hotspot_contact_fraction: float = 0.75
    max_binder_seq_len: int = 125
    min_interface_hbonds: int = 1
    skip_dg_threshold: bool = True


class RegisterSequencePartitionsConfig(Config):
    """Register external-sequence partitions under an existing (or new) run directory."""

    add_to_run_id: str = Field(
        default="",
        description="Run id folder under outputs_root where sequence partitions are added.",
    )
    input_path: str = Field(
        default="",
        description=(
            "Directory of input FASTA files (one partition per file; "
            "each file may contain multiple sequences)."
        ),
    )
    target_fasta: str = Field(
        default=DEFAULT_TARGET_FASTA,
        description="Boltz-2 target FASTA path written into the run-scoped pipeline_config.yaml.",
    )
    hotspots: List[str] = Field(
        default=[],
        description=(
            "Hotspot residues for final_scores coverage (reference PDB numbering), "
            "e.g. [B20, B21, B22, B77]. Written to final_scores.hotspots in pipeline_config.yaml."
        ),
    )
    antihotspots: List[str] = Field(
        default=[],
        description=(
            "Antihotspot residues for final_scores coverage (reference PDB numbering), "
            "e.g. [B80-B87, B61-B66, B11-B16, B30-B33]. Written to final_scores.antihotspots in pipeline_config.yaml."
        ),
    )
    outputs_root: str = Field(
        default=DEFAULT_OUTPUTS_ROOT,
        description="Root directory containing run_id subfolders.",
    )


class ColabFoldToolConfig(Config):
    """Parameters for ColabFold input preparation and prediction steps."""

    model_type: str = "alphafold2_multimer_v3"
    pair_mode: Literal["unpaired", "paired", "unpaired_paired"] = "unpaired_paired"
    num_models: int = 5
    num_recycle: int = 3
    num_relax: int = 0
    query_chunk_size: int = 10
    colabfold_image: str = "ghcr.io/sokrypton/colabfold:1.6.0-cuda12"
    colabfold_cache_host: str = "/storage/proteindesign/ColabFold_cache"
    apply_alphafold_numpy_patch: bool = True
    extra_colabfold_args: List[str] = Field(default_factory=list)


def load_pipeline_config_dict() -> dict[str, Any]:
    """Load Launchpad template ``pipeline_config.yaml`` when present."""
    if not PIPELINE_CONFIG_TEMPLATE_PATH.is_file():
        return {}
    data = yaml.safe_load(PIPELINE_CONFIG_TEMPLATE_PATH.read_text(encoding="utf-8"))
    if isinstance(data, dict):
        _normalize_structure_filters_yaml(data)
    return data if isinstance(data, dict) else {}


def load_register_sequences_config_dict() -> dict[str, Any]:
    """Load Launchpad template ``register_sequences_config.yaml`` when present."""
    if not REGISTER_SEQUENCES_CONFIG_TEMPLATE_PATH.is_file():
        return {}
    data = yaml.safe_load(
        REGISTER_SEQUENCES_CONFIG_TEMPLATE_PATH.read_text(encoding="utf-8")
    )
    return data if isinstance(data, dict) else {}


def default_register_sequence_run_config() -> dict[str, Any]:
    """Launchpad defaults for ``register_sequence_partitions``."""
    data = load_register_sequences_config_dict() or load_pipeline_config_dict()
    boltz2 = data.get("boltz2") if isinstance(data.get("boltz2"), dict) else {}
    final_scores = data.get("final_scores") if isinstance(data.get("final_scores"), dict) else {}
    hotspots = data.get("hotspots")
    if hotspots is None:
        hotspots = final_scores.get("hotspots", [])
    antihotspots = data.get("antihotspots")
    if antihotspots is None:
        antihotspots = final_scores.get("antihotspots", [])
    return {
        "ops": {
            "register_sequence_partitions": {
                "config": {
                    "add_to_run_id": str(data.get("add_to_run_id") or data.get("run_id") or ""),
                    "input_path": str(data.get("input_path") or ""),
                    "target_fasta": str(
                        data.get("target_fasta") or boltz2.get("target_fasta") or ""
                    ),
                    "hotspots": list(hotspots or []),
                    "antihotspots": list(antihotspots or []),
                    "outputs_root": str(data.get("outputs_root") or DEFAULT_OUTPUTS_ROOT),
                }
            }
        }
    }


def default_generate_configs_run_config() -> dict[str, Any]:
    """Launchpad defaults: pre-fill ``generate_configs`` from ``pipeline_config.yaml``."""
    data = load_pipeline_config_dict()
    if not data:
        return {}
    return {"ops": {"pipeline_config": {"config": data}}}
