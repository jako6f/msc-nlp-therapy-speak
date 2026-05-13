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


def working_output_dir(config: Dict, default_name: str = "wet_scan") -> Path:
    project_out_dir = config.get("project", {}).get("out_dir")
    if project_out_dir:
        return Path(project_out_dir)
    return collection_interim_dir(config) / default_name


def collection_stage_config(config: Dict) -> Dict:
    return config.get("collection_stage", {})


def warc_output_dir(config: Dict) -> Path:
    configured = collection_stage_config(config).get("warc_out_dir")
    if configured:
        return Path(configured)
    return collection_interim_dir(config) / "warc"


def document_quality_output_dir(config: Dict) -> Path:
    configured = collection_stage_config(config).get("document_quality_out_dir")
    if configured:
        return Path(configured)
    return collection_interim_dir(config) / "document_quality"


def metrics_output_dir(config: Dict) -> Path:
    configured = collection_stage_config(config).get("metrics_out_dir")
    if configured:
        return Path(configured)
    return collection_interim_dir(config) / "metrics"


def url_export_output_dir(config: Dict) -> Path:
    configured = collection_stage_config(config).get("url_export_dir")
    if configured:
        return Path(configured)
    return collection_interim_dir(config) / "url_exports"


def pointer_cache_output_dir(config: Dict) -> Path:
    configured = collection_stage_config(config).get("pointer_cache_dir")
    if configured:
        return Path(configured)
    return collection_interim_dir(config) / "pointer_cache"
