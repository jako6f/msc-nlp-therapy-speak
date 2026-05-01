import math

from src.analysis.summary_schema import with_warc_metric_defaults
from src.data_sources.commoncrawl.cc_scan import _warc_validation_placeholder_rows


def test_scan_summary_includes_warc_placeholders():
    metrics = {
        row[0]: row[1]
        for row in _warc_validation_placeholder_rows()
    }
    assert "validated_hits_warc" in metrics
    assert metrics["validated_hits_warc"] is None
    assert "validated_hits_warc_per_10k" in metrics
    assert metrics["validated_hits_warc_per_10k"] is None
    assert "warc_validation_attempted" in metrics
    assert metrics["warc_validation_attempted"] is False
    assert "warc_validation_notes" in metrics


def test_legacy_summary_gets_warc_defaults():
    legacy = {
        "docs_scanned": "1000",
        "validated_hits_wet": "10",
    }
    normalized = with_warc_metric_defaults(legacy)
    assert normalized["validated_hits_warc"] is None
    assert normalized["validated_hits_warc_per_10k"] is None
    assert normalized["warc_validation_attempted"] is False
    assert normalized["warc_validation_notes"] == ""


def test_nan_warc_metrics_are_normalized_to_none():
    summary = {
        "validated_hits_warc": math.nan,
        "validated_hits_warc_per_10k": math.nan,
    }
    normalized = with_warc_metric_defaults(summary)
    assert normalized["validated_hits_warc"] is None
    assert normalized["validated_hits_warc_per_10k"] is None
