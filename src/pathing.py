from pathlib import Path
from typing import Dict


def interim_base(config: Dict) -> Path:
    return Path(config.get("paths", {}).get("interim_base", "data/interim"))


def processed_base(config: Dict) -> Path:
    return Path(config.get("paths", {}).get("processed_base", "data/processed"))


def collection_config(config: Dict) -> Dict:
    return config.get("collection", {})


def collection_interim_dir(config: Dict) -> Path:
    configured = collection_config(config).get("interim_dir")
    return Path(configured) if configured else interim_base(config) / "collection"


def collection_processed_dir(config: Dict) -> Path:
    configured = collection_config(config).get("processed_dir")
    return Path(configured) if configured else processed_base(config)


def collection_track_working_dir(
    config: Dict,
    *,
    track: str,
    year: int | str,
    batch: int | str = 1,
) -> Path:
    track = str(track).strip()
    year = str(year).strip()
    batch_int = int(batch)
    if track == "trend":
        return collection_interim_dir(config) / "trend_working" / year
    return (
        collection_interim_dir(config)
        / "corpus_working"
        / year
        / f"batch_{batch_int:03d}"
    )


def collection_url_export_dir(
    config: Dict, *, track: str, year: int | str, batch: int | str = 1
) -> Path:
    return collection_track_working_dir(config, track=track, year=year, batch=batch) / "url_exports"


def collection_pointer_cache_dir(
    config: Dict, *, track: str, year: int | str, batch: int | str = 1
) -> Path:
    return (
        collection_track_working_dir(config, track=track, year=year, batch=batch)
        / "pointer_cache"
    )


def collection_warc_dir(
    config: Dict, *, track: str, year: int | str, batch: int | str = 1
) -> Path:
    return collection_track_working_dir(config, track=track, year=year, batch=batch) / "warc"


def collection_quality_dir(
    config: Dict, *, track: str, year: int | str, batch: int | str = 1
) -> Path:
    return collection_track_working_dir(config, track=track, year=year, batch=batch) / "quality"


def collection_metrics_dir(
    config: Dict, *, track: str, year: int | str, batch: int | str = 1
) -> Path:
    return collection_track_working_dir(config, track=track, year=year, batch=batch) / "metrics"


def processed_trend_dir(config: Dict) -> Path:
    return collection_processed_dir(config) / "trend"


def processed_corpus_dir(config: Dict) -> Path:
    return collection_processed_dir(config) / "corpus"


def processed_manifest_dir(config: Dict) -> Path:
    return collection_processed_dir(config) / "manifests"


def stage1_base(config: Dict) -> Path:
    return Path(
        config.get("paths", {}).get("stage1_base", str(interim_base(config) / "pilot-dev"))
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
