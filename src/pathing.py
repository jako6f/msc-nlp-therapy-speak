from pathlib import Path
from typing import Dict


def interim_base(config: Dict) -> Path:
    return Path(config.get("paths", {}).get("interim_base", "data/interim"))


def stage1_base(config: Dict) -> Path:
    return Path(
        config.get("paths", {}).get("stage1_base", str(interim_base(config) / "stage1_pilot-dev"))
    )


def stage1_stage_dir(config: Dict, stage: str) -> Path:
    if stage.startswith("stage1"):
        return stage1_base(config) / stage
    return interim_base(config) / stage


def stage1_output_dir(config: Dict, default_stage: str = "stage1b") -> Path:
    project_out_dir = config.get("project", {}).get("out_dir")
    if project_out_dir:
        return Path(project_out_dir)

    stage = config.get("run_context", {}).get("stage", default_stage)
    return stage1_stage_dir(config, stage)


def stage1d_warc_dir(config: Dict) -> Path:
    configured = config.get("stage1d", {}).get("warc_out_dir")
    if configured:
        return Path(configured)
    return stage1_stage_dir(config, "stage1d") / "warc"


def stage1d_filter_en_dedup_dir(config: Dict) -> Path:
    configured = config.get("stage1d", {}).get("filter_en_dedup_out_dir")
    if configured:
        return Path(configured)
    return stage1_stage_dir(config, "stage1d") / "filter_en_dedup"


def stage1d_url_export_dir(config: Dict) -> Path:
    configured = config.get("stage1d", {}).get("url_export_dir")
    if configured:
        return Path(configured)
    return stage1_stage_dir(config, "stage1d") / "url_exports"


def stage1d_pointer_cache_dir(config: Dict) -> Path:
    configured = config.get("stage1d", {}).get("pointer_cache_dir")
    if configured:
        return Path(configured)
    return stage1_stage_dir(config, "stage1d") / "pointer_cache"


def stage1e_warc_dir(config: Dict) -> Path:
    configured = config.get("stage1e", {}).get("warc_out_dir")
    if configured:
        return Path(configured)
    return stage1_stage_dir(config, "stage1e") / "warc"


def stage1e_filter_en_dedup_dir(config: Dict) -> Path:
    configured = config.get("stage1e", {}).get("filter_en_dedup_out_dir")
    if configured:
        return Path(configured)
    return stage1_stage_dir(config, "stage1e") / "filter_en_dedup"


def stage1e_document_quality_dir(config: Dict) -> Path:
    configured = config.get("stage1e", {}).get("document_quality_out_dir")
    if configured:
        return Path(configured)
    return stage1_stage_dir(config, "stage1e") / "document_quality"


def stage1e_metrics_dir(config: Dict) -> Path:
    configured = config.get("stage1e", {}).get("metrics_out_dir")
    if configured:
        return Path(configured)
    return stage1_stage_dir(config, "stage1e") / "metrics"


def stage1e_url_export_dir(config: Dict) -> Path:
    configured = config.get("stage1e", {}).get("url_export_dir")
    if configured:
        return Path(configured)
    return stage1_stage_dir(config, "stage1e") / "url_exports"


def stage1e_pointer_cache_dir(config: Dict) -> Path:
    configured = config.get("stage1e", {}).get("pointer_cache_dir")
    if configured:
        return Path(configured)
    return stage1_stage_dir(config, "stage1e") / "pointer_cache"
