#!/usr/bin/env python3
"""Run reproducible, schema-validated Codex frame annotation batches."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import subprocess
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd
from sklearn.metrics import f1_score

PROJECT_ROOT = Path(__file__).resolve().parents[2]
CLASSIFICATION_DIR = PROJECT_ROOT / "data/interim/lsc/classification"
LLM_DIR = CLASSIFICATION_DIR / "llm_annotation"
CODEX_DIR = LLM_DIR / "codex"
CODEX_PILOT_DIR = CODEX_DIR / "pilot"
CODEX_PRODUCTION_DIR = CODEX_DIR / "production"
HUMAN_DIR = CLASSIFICATION_DIR / "human_annotation"
PROMPT_DIR = PROJECT_ROOT / "notebooks/01_classification/prompts"

CODEBOOK_PATH = PROJECT_ROOT / "notebooks/01_classification/codebooks/codebook_v4.md"
PROMPT_PATH = PROMPT_DIR / "annotator_v4.md"
SCHEMA_PATH = PROMPT_DIR / "annotator_output_schema.json"
SKILL_PATH = PROJECT_ROOT / ".agents/skills/frame-annotation/SKILL.md"

PRODUCTION_BATCH_DIR = CODEX_PRODUCTION_DIR / "inputs"
PILOT_BATCH_DIR = CODEX_PILOT_DIR / "inputs"
PRODUCTION_RAW_DIR = CODEX_PRODUCTION_DIR / "outputs"
PILOT_RAW_ROOT = CODEX_PILOT_DIR / "outputs"
RUN_ROOT = CODEX_DIR
PARSED_DIR = CODEX_PRODUCTION_DIR
PILOT_EVAL_DIR = CODEX_PILOT_DIR / "evaluation"

PILOT_SOURCE = HUMAN_DIR / "frame_pilot_annotation.xlsx"
VALIDATION_SOURCE = HUMAN_DIR / "frame_validation_annotation_blank.xlsx"
FULL_CONTEXT_POOL = CLASSIFICATION_DIR / "frame_target_context_pool.csv"
PRODUCTION_POOL = CODEX_PRODUCTION_DIR / "training_pool.csv"
PRODUCTION_POOL_WITH_IDS = CODEX_PRODUCTION_DIR / "training_pool_with_annotation_ids.csv"
PRODUCTION_BATCH_MANIFEST = CODEX_PRODUCTION_DIR / "manifest.csv"
PRODUCTION_EXTENSION_MANIFEST = CODEX_PRODUCTION_DIR / "extension_manifest.csv"
MODEL = "gpt-5.5"
PROMPT_VERSION = "annotator_v4"
CODEBOOK_VERSION = "v0.4"
PRODUCTION_BATCH_SIZE = 75
CONFIDENCE_VALUES = {"high", "medium", "low"}
OUTPUT_FIELDS = [
    "annotation_id",
    "substantive_target_discourse",
    "clinical_frame_present",
    "lived_experience_frame_present",
    "confidence",
]


def utc_now() -> str:
    return datetime.now(UTC).isoformat()


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def read_jsonl(path: Path) -> list[dict[str, Any]]:
    rows = []
    with path.open(encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, start=1):
            if not line.strip():
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError as error:
                raise ValueError(
                    f"Invalid JSON in {path.name} line {line_number}: {error}"
                ) from error
            if not isinstance(row, dict):
                raise ValueError(f"Expected an object in {path.name} line {line_number}.")
            rows.append(row)
    return rows


def write_jsonl(rows: list[dict[str, Any]], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False, separators=(",", ":")) + "\n")


def annotation_input_rows(frame: pd.DataFrame) -> list[dict[str, str]]:
    return [
        {
            "annotation_id": str(row.annotation_id),
            "context_id": str(row.context_id),
            "target": str(row.analysis_unit),
            "raw_form": str(row.raw_form),
            "passage": str(row.target_sentence_plus_adjacent),
        }
        for row in frame.itertuples(index=False)
    ]


def batch_dir(dataset: str) -> Path:
    return PILOT_BATCH_DIR if dataset == "pilot" else PRODUCTION_BATCH_DIR


def raw_output_dir(dataset: str, reasoning: str) -> Path:
    return PILOT_RAW_ROOT / reasoning if dataset == "pilot" else PRODUCTION_RAW_DIR


def batch_paths(dataset: str, selected_batch: str | None = None) -> list[Path]:
    paths = sorted(batch_dir(dataset).glob("*.jsonl"))
    if selected_batch:
        selected_name = (
            selected_batch if selected_batch.endswith(".jsonl") else f"{selected_batch}.jsonl"
        )
        paths = [
            path for path in paths if path.name == selected_name or path.stem == selected_batch
        ]
        if not paths:
            raise FileNotFoundError(f"Batch not found for {dataset}: {selected_batch}")
    if not paths:
        raise FileNotFoundError(f"No {dataset} batches found in {batch_dir(dataset)}")
    return paths


def numeric_suffix(value: str, prefix: str) -> int:
    match = re.fullmatch(rf"{re.escape(prefix)}(\d+)", value)
    if not match:
        raise ValueError(f"Expected {prefix}<digits>, found {value!r}.")
    return int(match.group(1))


def validate_input_batch(path: Path) -> list[dict[str, Any]]:
    rows = read_jsonl(path)
    required = {"annotation_id", "context_id", "target", "raw_form", "passage"}
    ids = []
    for index, row in enumerate(rows, start=1):
        missing = required - set(row)
        extra = set(row) - required
        if missing or extra:
            raise ValueError(
                f"{path.name} row {index}: missing={sorted(missing)}, extra={sorted(extra)}"
            )
        if not all(isinstance(row[key], str) for key in required):
            raise ValueError(f"{path.name} row {index}: every input field must be a string.")
        ids.append(row["annotation_id"])
    if len(ids) != len(set(ids)):
        raise ValueError(f"{path.name}: duplicate annotation IDs.")
    return rows


def validate_annotation_rows(
    annotations: Any,
    expected_rows: list[dict[str, Any]],
    source_name: str,
) -> list[dict[str, Any]]:
    if not isinstance(annotations, list):
        raise ValueError(f"{source_name}: annotations must be an array.")
    if len(annotations) != len(expected_rows):
        raise ValueError(
            f"{source_name}: expected {len(expected_rows)} rows, found {len(annotations)}."
        )

    expected_ids = [row["annotation_id"] for row in expected_rows]
    actual_ids = []
    for index, row in enumerate(annotations, start=1):
        if not isinstance(row, dict):
            raise ValueError(f"{source_name} row {index}: expected an object.")
        if set(row) != set(OUTPUT_FIELDS):
            raise ValueError(
                f"{source_name} row {index}: required fields are exactly "
                f"{OUTPUT_FIELDS}; found {sorted(row)}."
            )
        annotation_id = row["annotation_id"]
        if not isinstance(annotation_id, str):
            raise ValueError(f"{source_name} row {index}: annotation_id must be a string.")
        actual_ids.append(annotation_id)

        substantive = row["substantive_target_discourse"]
        clinical = row["clinical_frame_present"]
        lived = row["lived_experience_frame_present"]
        confidence = row["confidence"]
        if type(substantive) is not bool:
            raise ValueError(f"{annotation_id}: substantive_target_discourse must be boolean.")
        if clinical is not None and type(clinical) is not bool:
            raise ValueError(f"{annotation_id}: clinical_frame_present must be boolean or null.")
        if lived is not None and type(lived) is not bool:
            raise ValueError(
                f"{annotation_id}: lived_experience_frame_present must be boolean or null."
            )
        if confidence not in CONFIDENCE_VALUES:
            raise ValueError(f"{annotation_id}: invalid confidence {confidence!r}.")
        if substantive and (clinical is None or lived is None):
            raise ValueError(
                f"{annotation_id}: substantive rows require boolean clinical and lived labels."
            )
        if not substantive and (clinical is not None or lived is not None):
            raise ValueError(
                f"{annotation_id}: non-substantive rows require null clinical and lived labels."
            )

    if actual_ids != expected_ids:
        missing = sorted(set(expected_ids) - set(actual_ids))
        extra = sorted(set(actual_ids) - set(expected_ids))
        raise ValueError(
            f"{source_name}: IDs or order differ from input; "
            f"missing={missing[:10]}, extra={extra[:10]}."
        )
    return annotations


def validate_final_response(final_path: Path, input_path: Path) -> list[dict[str, Any]]:
    expected_rows = validate_input_batch(input_path)
    try:
        response = json.loads(final_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as error:
        raise ValueError(f"Invalid final-response JSON in {final_path}: {error}") from error
    if not isinstance(response, dict) or set(response) != {"annotations"}:
        raise ValueError(
            f"{final_path.name}: final response must contain only the annotations array."
        )
    return validate_annotation_rows(response["annotations"], expected_rows, final_path.name)


def canonical_path(dataset: str, reasoning: str, input_path: Path) -> Path:
    return raw_output_dir(dataset, reasoning) / input_path.name


def run_dir(dataset: str, reasoning: str, input_path: Path) -> Path:
    return RUN_ROOT / dataset / "runs" / reasoning / input_path.stem


def task_prompt(input_path: Path) -> str:
    relative_input = input_path.relative_to(PROJECT_ROOT)
    input_rows = validate_input_batch(input_path)
    return (
        "Use $frame-annotation to annotate exactly one batch.\n\n"
        f"Input batch: `{relative_input}`\n"
        f"Expected rows: {len(input_rows)}\n"
        f"Input SHA-256: `{sha256(input_path)}`\n\n"
        "Read the repo-local skill, the complete codebook v4, and annotator_v4 prompt. "
        "Read no other annotation batches. Return only the schema-constrained final object."
    )


def codex_command(reasoning: str, final_path: Path, prompt: str) -> list[str]:
    return [
        "codex",
        "--ask-for-approval",
        "never",
        "exec",
        "--ephemeral",
        "--sandbox",
        "read-only",
        "--model",
        MODEL,
        "-c",
        f'model_reasoning_effort="{reasoning}"',
        "-c",
        'web_search="disabled"',
        "--output-schema",
        str(SCHEMA_PATH),
        "--json",
        "--output-last-message",
        str(final_path),
        prompt,
    ]


def extract_token_usage(events_path: Path) -> dict[str, Any]:
    if not events_path.exists():
        return {}
    last_usage: dict[str, Any] = {}
    with events_path.open(encoding="utf-8") as handle:
        for line in handle:
            try:
                event = json.loads(line)
            except json.JSONDecodeError:
                continue
            payload = event.get("payload", {})
            if payload.get("type") == "token_count":
                last_usage = payload.get("info", {}).get("total_token_usage", {})
            usage = event.get("usage")
            if isinstance(usage, dict):
                last_usage = usage
    return last_usage


def write_metadata(
    path: Path,
    *,
    dataset: str,
    reasoning: str,
    input_path: Path,
    command: list[str],
    return_code: int,
    attempt: int,
    events_path: Path,
    status: str,
    error: str = "",
) -> None:
    metadata = {
        "dataset": dataset,
        "batch_name": input_path.name,
        "input_rows": len(validate_input_batch(input_path)),
        "input_sha256": sha256(input_path),
        "codebook_version": CODEBOOK_VERSION,
        "codebook_sha256": sha256(CODEBOOK_PATH),
        "prompt_version": PROMPT_VERSION,
        "prompt_sha256": sha256(PROMPT_PATH),
        "schema_sha256": sha256(SCHEMA_PATH),
        "skill_sha256": sha256(SKILL_PATH),
        "model": MODEL,
        "reasoning_effort": reasoning,
        "codex_cli_version": codex_version(),
        "command": command,
        "attempt": attempt,
        "return_code": return_code,
        "status": status,
        "error": error,
        "token_usage": extract_token_usage(events_path),
        "completed_at_utc": utc_now(),
    }
    path.write_text(json.dumps(metadata, indent=2), encoding="utf-8")
    refresh_run_manifest()


def refresh_run_manifest() -> None:
    rows = []
    for path in sorted(RUN_ROOT.glob("*/runs/*/*/attempt_*_metadata.json")):
        metadata = json.loads(path.read_text(encoding="utf-8"))
        token_usage = metadata.pop("token_usage", {})
        metadata.pop("command", None)
        rows.append(
            {
                **metadata,
                **{f"token_{key}": value for key, value in token_usage.items()},
                "metadata_path": str(path.relative_to(PROJECT_ROOT)),
            }
        )
    if rows:
        pd.DataFrame(rows).to_csv(CODEX_DIR / "run_manifest.csv", index=False)


def codex_version() -> str:
    result = subprocess.run(
        ["codex", "--version"], cwd=PROJECT_ROOT, capture_output=True, text=True, check=False
    )
    return (result.stdout or result.stderr).strip()


def validated_canonical_exists(dataset: str, reasoning: str, input_path: Path) -> bool:
    path = canonical_path(dataset, reasoning, input_path)
    if not path.exists():
        return False
    try:
        validate_annotation_rows(read_jsonl(path), validate_input_batch(input_path), path.name)
    except ValueError:
        return False
    expected_contract = {
        "input_sha256": sha256(input_path),
        "codebook_version": CODEBOOK_VERSION,
        "codebook_sha256": sha256(CODEBOOK_PATH),
        "prompt_version": PROMPT_VERSION,
        "prompt_sha256": sha256(PROMPT_PATH),
        "schema_sha256": sha256(SCHEMA_PATH),
        "skill_sha256": sha256(SKILL_PATH),
        "model": MODEL,
        "reasoning_effort": reasoning,
    }
    metadata_paths = sorted(run_dir(dataset, reasoning, input_path).glob("attempt_*_metadata.json"))
    for metadata_path in reversed(metadata_paths):
        metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
        if metadata.get("status") != "valid":
            continue
        return all(metadata.get(key) == value for key, value in expected_contract.items())
    return False


def run_batch(
    dataset: str, reasoning: str, input_path: Path, max_attempts: int, resume: bool
) -> str:
    canonical = canonical_path(dataset, reasoning, input_path)
    if resume and validated_canonical_exists(dataset, reasoning, input_path):
        return "skipped valid"

    destination = run_dir(dataset, reasoning, input_path)
    destination.mkdir(parents=True, exist_ok=True)
    prompt = task_prompt(input_path)
    last_error = ""

    existing_attempts = max(
        (
            int(path.name.split("_")[1])
            for path in destination.glob("attempt_*_*")
            if path.name.split("_")[1].isdigit()
        ),
        default=0,
    )
    for attempt in range(existing_attempts + 1, existing_attempts + max_attempts + 1):
        attempt_name = f"attempt_{attempt:02d}"
        final_path = destination / f"{attempt_name}_final.json"
        events_path = destination / f"{attempt_name}_events.jsonl"
        stderr_path = destination / f"{attempt_name}_stderr.log"
        metadata_path = destination / f"{attempt_name}_metadata.json"
        command = codex_command(reasoning, final_path, prompt)

        with (
            events_path.open("w", encoding="utf-8") as events_handle,
            stderr_path.open("w", encoding="utf-8") as stderr_handle,
        ):
            result = subprocess.run(
                command,
                cwd=PROJECT_ROOT,
                stdout=events_handle,
                stderr=stderr_handle,
                text=True,
                check=False,
            )

        try:
            if result.returncode != 0:
                raise RuntimeError(f"codex exec exited with status {result.returncode}")
            annotations = validate_final_response(final_path, input_path)
            write_jsonl(annotations, canonical)
            write_metadata(
                metadata_path,
                dataset=dataset,
                reasoning=reasoning,
                input_path=input_path,
                command=command,
                return_code=result.returncode,
                attempt=attempt,
                events_path=events_path,
                status="valid",
            )
            return f"valid attempt {attempt}"
        except (OSError, RuntimeError, ValueError) as error:
            last_error = str(error)
            write_metadata(
                metadata_path,
                dataset=dataset,
                reasoning=reasoning,
                input_path=input_path,
                command=command,
                return_code=result.returncode,
                attempt=attempt,
                events_path=events_path,
                status="invalid",
                error=last_error,
            )

    raise RuntimeError(f"{input_path.name} failed after {max_attempts} attempts: {last_error}")


def prepare_pilot(source: Path, batch_size: int) -> None:
    if not source.exists():
        raise FileNotFoundError(f"Pilot workbook not found: {source}")
    frame = pd.read_excel(
        source, sheet_name="annotations" if source.name.endswith("_completed.xlsx") else 0
    )
    required = {
        "annotation_id",
        "context_id",
        "analysis_unit",
        "raw_form",
        "target_sentence_plus_adjacent",
    }
    missing = required - set(frame.columns)
    if missing:
        raise ValueError(f"Pilot workbook is missing columns: {sorted(missing)}")
    if frame["annotation_id"].duplicated().any():
        raise ValueError("Pilot workbook contains duplicate annotation IDs.")

    PILOT_BATCH_DIR.mkdir(parents=True, exist_ok=True)
    manifest_rows = []
    for batch_index, start in enumerate(range(0, len(frame), batch_size), start=1):
        batch = frame.iloc[start : start + batch_size]
        path = PILOT_BATCH_DIR / f"pilot_batch_{batch_index:03d}.jsonl"
        rows = annotation_input_rows(batch)
        write_jsonl(rows, path)
        manifest_rows.append(
            {
                "batch_name": path.name,
                "rows": len(rows),
                "start_row": start,
                "end_row": start + len(rows) - 1,
                "sha256": sha256(path),
            }
        )
    pd.DataFrame(manifest_rows).to_csv(CODEX_PILOT_DIR / "manifest.csv", index=False)
    print(f"Wrote {len(manifest_rows)} pilot batches with {len(frame)} rows.")


def stratified_sample(frame: pd.DataFrame, rows: int, seed: int) -> pd.DataFrame:
    group_columns = ["analysis_unit", "year_band"]
    groups = list(frame.groupby(group_columns, observed=True, sort=True))
    if rows > len(frame):
        raise ValueError(f"Requested {rows} rows but only {len(frame)} eligible rows remain.")

    base, remainder = divmod(rows, len(groups))
    sampled_parts = []
    for index, (_, group) in enumerate(groups):
        group_rows = min(len(group), base + (index < remainder))
        sampled_parts.append(group.sample(n=group_rows, random_state=seed + index))

    sampled = pd.concat(sampled_parts, ignore_index=True)
    shortfall = rows - len(sampled)
    if shortfall:
        remaining = frame.loc[~frame["context_id"].isin(sampled["context_id"])]
        sampled = pd.concat(
            [sampled, remaining.sample(n=shortfall, random_state=seed + 999)],
            ignore_index=True,
        )
    return sampled.sample(frac=1, random_state=seed + 1000).reset_index(drop=True)


def validate_production_inputs() -> tuple[pd.DataFrame, pd.DataFrame]:
    required_paths = [PRODUCTION_POOL, PRODUCTION_POOL_WITH_IDS, PRODUCTION_BATCH_MANIFEST]
    missing_paths = [path for path in required_paths if not path.exists()]
    if missing_paths:
        raise FileNotFoundError(f"Missing production input files: {missing_paths}")

    pool = pd.read_csv(PRODUCTION_POOL)
    pool_with_ids = pd.read_csv(PRODUCTION_POOL_WITH_IDS)
    manifest = pd.read_csv(PRODUCTION_BATCH_MANIFEST)
    if len(pool) != len(pool_with_ids):
        raise ValueError("Production pool files contain different row counts.")
    if pool["context_id"].astype(str).tolist() != pool_with_ids["context_id"].astype(str).tolist():
        raise ValueError("Production pool context order differs from the ID-assigned pool.")
    if pool_with_ids["annotation_id"].duplicated().any():
        raise ValueError("Production pool contains duplicate annotation IDs.")
    if pool_with_ids["context_id"].duplicated().any():
        raise ValueError("Production pool contains duplicate context IDs.")

    input_paths = batch_paths("production")
    expected_names = manifest["batch_name"].astype(str).tolist()
    if [path.name for path in input_paths] != expected_names:
        raise ValueError("Production batch files differ from the batch manifest.")

    batch_rows = []
    for path, manifest_row in zip(input_paths, manifest.itertuples(index=False)):
        if sha256(path) != manifest_row.sha256:
            raise ValueError(f"Batch checksum differs from manifest: {path.name}")
        batch_rows.extend(validate_input_batch(path))
    if [row["annotation_id"] for row in batch_rows] != pool_with_ids["annotation_id"].tolist():
        raise ValueError("Production batch annotation IDs/order differ from the production pool.")
    batch_context_ids = [row["context_id"] for row in batch_rows]
    if batch_context_ids != pool_with_ids["context_id"].astype(str).tolist():
        raise ValueError("Production batch context IDs/order differ from the production pool.")
    return pool, pool_with_ids


def prepare_production_extension(
    extension_name: str,
    additional_rows: int,
    batch_size: int,
    seed: int,
    reasoning: str,
) -> None:
    if not re.fullmatch(r"[a-z0-9][a-z0-9_-]*", extension_name):
        raise ValueError(
            "Extension name must use lowercase letters, digits, underscores, or hyphens."
        )
    if additional_rows < 1 or batch_size < 1:
        raise ValueError("Additional rows and batch size must be positive.")

    pool, pool_with_ids = validate_production_inputs()
    for input_path in batch_paths("production"):
        if not validated_canonical_exists("production", reasoning, input_path):
            raise ValueError(
                "Existing production annotation is incomplete, invalid, or stale; "
                f"cannot extend before validating {input_path.name}."
            )

    if PRODUCTION_EXTENSION_MANIFEST.exists():
        extension_manifest = pd.read_csv(PRODUCTION_EXTENSION_MANIFEST)
        if extension_name in extension_manifest["extension_name"].astype(str).tolist():
            raise ValueError(f"Production extension already exists: {extension_name}")
    else:
        extension_manifest = pd.DataFrame()

    full_pool = pd.read_csv(FULL_CONTEXT_POOL)
    protected_context_ids = set(pool_with_ids["context_id"].astype(str))
    for path in [PILOT_SOURCE, VALIDATION_SOURCE]:
        protected = pd.read_excel(path, usecols=["context_id"])
        protected_context_ids.update(protected["context_id"].astype(str))
    eligible = full_pool.loc[
        ~full_pool["context_id"].astype(str).isin(protected_context_ids)
    ].copy()
    extension = stratified_sample(eligible, additional_rows, seed)

    last_annotation_number = max(
        numeric_suffix(value, "llm_train_") for value in pool_with_ids["annotation_id"]
    )
    extension["annotation_id"] = [
        f"llm_train_{number:05d}"
        for number in range(
            last_annotation_number + 1,
            last_annotation_number + additional_rows + 1,
        )
    ]
    extension["prompt_version"] = PROMPT_VERSION
    if "annotation_tranche" not in pool_with_ids:
        pool_with_ids["annotation_tranche"] = "initial_2000"
    extension["annotation_tranche"] = extension_name

    existing_manifest = pd.read_csv(PRODUCTION_BATCH_MANIFEST)
    last_batch_number = max(
        numeric_suffix(value.removesuffix(".jsonl"), "annotator_batch_")
        for value in existing_manifest["batch_name"]
    )
    start_row = len(pool_with_ids)
    new_manifest_rows = []
    new_batch_paths = []
    for offset, start in enumerate(range(0, len(extension), batch_size), start=1):
        batch = extension.iloc[start : start + batch_size]
        batch_number = last_batch_number + offset
        path = PRODUCTION_BATCH_DIR / f"annotator_batch_{batch_number:03d}.jsonl"
        if path.exists():
            raise FileExistsError(f"Refusing to overwrite existing batch: {path}")
        write_jsonl(annotation_input_rows(batch), path)
        new_batch_paths.append(path)
        new_manifest_rows.append(
            {
                "batch_name": path.name,
                "rows": len(batch),
                "start_row": start_row + start,
                "end_row": start_row + start + len(batch) - 1,
                "sha256": sha256(path),
                "prompt_version": PROMPT_VERSION,
                "codebook_version": CODEBOOK_VERSION,
            }
        )

    expanded_pool = pd.concat([pool, extension[pool.columns]], ignore_index=True)
    expanded_pool_with_ids = pd.concat(
        [pool_with_ids, extension[pool_with_ids.columns]], ignore_index=True
    )
    expanded_manifest = pd.concat(
        [existing_manifest, pd.DataFrame(new_manifest_rows)], ignore_index=True
    )
    expanded_pool.to_csv(PRODUCTION_POOL, index=False)
    expanded_pool_with_ids.to_csv(PRODUCTION_POOL_WITH_IDS, index=False)
    expanded_manifest.to_csv(PRODUCTION_BATCH_MANIFEST, index=False)

    extension_record = pd.DataFrame(
        [
            {
                "extension_name": extension_name,
                "prepared_at_utc": utc_now(),
                "rows": additional_rows,
                "seed": seed,
                "batch_size": batch_size,
                "annotation_id_start": extension["annotation_id"].iloc[0],
                "annotation_id_end": extension["annotation_id"].iloc[-1],
                "batch_start": new_batch_paths[0].name,
                "batch_end": new_batch_paths[-1].name,
                "production_rows_after": len(expanded_pool_with_ids),
                "prompt_version": PROMPT_VERSION,
                "codebook_version": CODEBOOK_VERSION,
                "reasoning_effort": reasoning,
            }
        ]
    )
    pd.concat([extension_manifest, extension_record], ignore_index=True).to_csv(
        PRODUCTION_EXTENSION_MANIFEST, index=False
    )
    validate_production_inputs()
    print(
        f"Prepared extension {extension_name}: {additional_rows} rows in "
        f"{len(new_batch_paths)} append-only batches "
        f"({new_batch_paths[0].stem} to {new_batch_paths[-1].stem})."
    )
    print(f"Production pool now contains {len(expanded_pool_with_ids)} rows.")


def validate_dataset(dataset: str, reasoning: str, selected_batch: str | None) -> None:
    valid = 0
    missing = []
    for input_path in batch_paths(dataset, selected_batch):
        canonical = canonical_path(dataset, reasoning, input_path)
        if not validated_canonical_exists(dataset, reasoning, input_path):
            print(f"missing, invalid, or stale: {canonical.relative_to(PROJECT_ROOT)}")
            missing.append(canonical)
            continue
        validate_annotation_rows(
            read_jsonl(canonical), validate_input_batch(input_path), canonical.name
        )
        valid += 1
        print(f"valid: {canonical.relative_to(PROJECT_ROOT)}")
    print(f"Validated {valid} batch output(s).")
    if missing:
        raise FileNotFoundError(f"Missing {len(missing)} expected canonical output(s).")


def combine_dataset(dataset: str, reasoning: str) -> Path:
    combined = []
    for input_path in batch_paths(dataset):
        canonical = canonical_path(dataset, reasoning, input_path)
        if not validated_canonical_exists(dataset, reasoning, input_path):
            raise ValueError(f"Missing, invalid, or stale canonical output: {canonical}")
        rows = validate_annotation_rows(
            read_jsonl(canonical), validate_input_batch(input_path), canonical.name
        )
        for row in rows:
            combined.append({**row, "source_file": input_path.name})

    if dataset == "production":
        output_path = PARSED_DIR / "labels.csv"
    else:
        output_path = PILOT_EVAL_DIR / f"pilot_codex_labels_{reasoning}.csv"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(combined).to_csv(output_path, index=False)
    print(f"Wrote {len(combined)} combined rows: {output_path.relative_to(PROJECT_ROOT)}")
    return output_path


def normalise_human_bool(value: Any) -> bool | None:
    if pd.isna(value) or str(value).strip().lower() in {"", "na", "n/a", "none", "nan"}:
        return None
    return str(value).strip().lower() in {"true", "t", "yes", "y", "1"}


def bool_label(value: bool | None) -> str:
    if value is None:
        return "na"
    return "true" if value else "false"


def derive_frame(substantive: bool, clinical: bool | None, lived: bool | None) -> str:
    if not substantive:
        return "non_substantive_or_insufficient"
    if clinical and lived:
        return "mixed"
    if clinical:
        return "clinical_only"
    if lived:
        return "lived_only"
    return "substantive_other"


def archive_completed_review(path: Path) -> Path | None:
    if not path.exists():
        return None

    previous = pd.read_csv(path)
    adjudication_columns = [
        "adjudicated_substantive_target_discourse",
        "adjudicated_clinical_frame_present",
        "adjudicated_lived_experience_frame_present",
        "adjudication_note",
    ]
    if not set(adjudication_columns).issubset(previous.columns):
        return None

    completed = (
        previous[adjudication_columns]
        .fillna("")
        .astype(str)
        .apply(lambda column: column.str.strip().ne(""))
        .any(axis=None)
    )
    if not completed:
        return None

    archive = path.with_name(f"{path.stem}_adjudicated_{sha256(path)[:12]}{path.suffix}")
    if not archive.exists():
        archive.write_bytes(path.read_bytes())
    print(f"Archived completed adjudication review: {archive.relative_to(PROJECT_ROOT)}")
    return archive


def evaluate_pilot(reasonings: list[str], source: Path) -> None:
    human = pd.read_excel(source)
    for column in [
        "substantive_target_discourse",
        "clinical_frame_present",
        "lived_experience_frame_present",
    ]:
        human[column] = human[column].map(normalise_human_bool)
    human["human_derived_frame"] = [
        derive_frame(substantive, clinical, lived)
        for substantive, clinical, lived in zip(
            human["substantive_target_discourse"],
            human["clinical_frame_present"],
            human["lived_experience_frame_present"],
        )
    ]
    PILOT_EVAL_DIR.mkdir(parents=True, exist_ok=True)
    metric_rows = []
    review = human[
        [
            "annotation_id",
            "context_id",
            "analysis_unit",
            "raw_form",
            "target_sentence_plus_adjacent",
            "substantive_target_discourse",
            "clinical_frame_present",
            "lived_experience_frame_present",
            "confidence",
            "human_derived_frame",
        ]
    ].copy()

    for reasoning in reasonings:
        labels_path = combine_dataset("pilot", reasoning)
        labels = pd.read_csv(labels_path)
        for column in [
            "substantive_target_discourse",
            "clinical_frame_present",
            "lived_experience_frame_present",
        ]:
            labels[column] = labels[column].map(normalise_human_bool)
        labels["codex_derived_frame"] = [
            derive_frame(substantive, clinical, lived)
            for substantive, clinical, lived in zip(
                labels["substantive_target_discourse"],
                labels["clinical_frame_present"],
                labels["lived_experience_frame_present"],
            )
        ]
        merged = human.merge(
            labels, on="annotation_id", suffixes=("_human", "_codex"), validate="one_to_one"
        )
        exact_columns = [
            "substantive_target_discourse",
            "clinical_frame_present",
            "lived_experience_frame_present",
        ]
        exact = pd.Series(True, index=merged.index)
        for column in exact_columns:
            exact &= merged[f"{column}_human"].eq(merged[f"{column}_codex"])
        human_substantive = merged["substantive_target_discourse_human"].eq(True)
        metric_rows.append(
            {
                "reasoning_effort": reasoning,
                "hierarchical_exact_match": float(exact.mean()),
                "stage0_macro_f1": f1_score(
                    merged["substantive_target_discourse_human"],
                    merged["substantive_target_discourse_codex"],
                    average="macro",
                    zero_division=0,
                ),
                "clinical_given_substantive_macro_f1": f1_score(
                    merged.loc[human_substantive, "clinical_frame_present_human"].map(bool_label),
                    merged.loc[human_substantive, "clinical_frame_present_codex"].map(bool_label),
                    labels=["false", "true"],
                    average="macro",
                    zero_division=0,
                ),
                "lived_given_substantive_macro_f1": f1_score(
                    merged.loc[human_substantive, "lived_experience_frame_present_human"].map(
                        bool_label
                    ),
                    merged.loc[human_substantive, "lived_experience_frame_present_codex"].map(
                        bool_label
                    ),
                    labels=["false", "true"],
                    average="macro",
                    zero_division=0,
                ),
                "derived_frame_macro_f1": f1_score(
                    merged["human_derived_frame"],
                    merged["codex_derived_frame"],
                    average="macro",
                    zero_division=0,
                ),
            }
        )
        comparison_path = PILOT_EVAL_DIR / f"pilot_comparison_{reasoning}.csv"
        merged.to_csv(comparison_path, index=False)
        codex_columns = labels[
            [
                "annotation_id",
                "substantive_target_discourse",
                "clinical_frame_present",
                "lived_experience_frame_present",
                "confidence",
                "codex_derived_frame",
            ]
        ].rename(
            columns={
                column: f"{column}_{reasoning}"
                for column in labels.columns
                if column != "annotation_id"
            }
        )
        review = review.merge(codex_columns, on="annotation_id", how="left", validate="one_to_one")

    metrics = pd.DataFrame(metric_rows)
    metrics.to_csv(PILOT_EVAL_DIR / "pilot_reasoning_metrics.csv", index=False)
    disagreement = pd.Series(False, index=review.index)
    for reasoning in reasonings:
        disagreement |= review[f"codex_derived_frame_{reasoning}"].ne(review["human_derived_frame"])
        disagreement |= review[f"confidence_{reasoning}"].isin(["low"])
    review = review.loc[disagreement].copy()
    review["adjudicated_substantive_target_discourse"] = ""
    review["adjudicated_clinical_frame_present"] = ""
    review["adjudicated_lived_experience_frame_present"] = ""
    review["adjudication_note"] = ""
    review_path = PILOT_EVAL_DIR / "pilot_disagreement_review.csv"
    archive_completed_review(review_path)
    review.to_csv(review_path, index=False)
    print(metrics.to_string(index=False))
    print(f"Wrote {len(review)} disagreement/low-confidence rows for optional adjudication.")


def print_dry_run(dataset: str, reasoning: str, selected_batch: str | None) -> None:
    for input_path in batch_paths(dataset, selected_batch):
        destination = run_dir(dataset, reasoning, input_path)
        final_path = destination / "attempt_01_final.json"
        command = codex_command(reasoning, final_path, task_prompt(input_path))
        print(" ".join(json.dumps(part) for part in command))


def self_test() -> None:
    expected = [
        {
            "annotation_id": "test_001",
            "context_id": "context_001",
            "target": "ADHD",
            "raw_form": "ADHD",
            "passage": "Test passage.",
        },
        {
            "annotation_id": "test_002",
            "context_id": "context_002",
            "target": "Autism",
            "raw_form": "autism",
            "passage": "Another test passage.",
        },
    ]
    valid = [
        {
            "annotation_id": "test_001",
            "substantive_target_discourse": True,
            "clinical_frame_present": True,
            "lived_experience_frame_present": False,
            "confidence": "high",
        },
        {
            "annotation_id": "test_002",
            "substantive_target_discourse": False,
            "clinical_frame_present": None,
            "lived_experience_frame_present": None,
            "confidence": "medium",
        },
    ]
    validate_annotation_rows(valid, expected, "valid")

    invalid_cases = {
        "missing row": valid[:1],
        "reordered IDs": list(reversed(valid)),
        "extra field": [{**valid[0], "reason": "not allowed"}, valid[1]],
        "invalid hierarchy": [
            valid[0],
            {**valid[1], "clinical_frame_present": False},
        ],
    }
    for name, rows in invalid_cases.items():
        try:
            validate_annotation_rows(rows, expected, name)
        except ValueError:
            continue
        raise AssertionError(f"Validator accepted invalid self-test case: {name}")
    print(f"Validator self-test passed: 1 valid and {len(invalid_cases)} invalid cases.")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    prepare = subparsers.add_parser(
        "prepare-pilot", help="Create label-free pilot calibration batches."
    )
    prepare.add_argument("--source", type=Path, default=PILOT_SOURCE)
    prepare.add_argument("--batch-size", type=int, default=75)

    extend = subparsers.add_parser(
        "prepare-production-extension",
        help="Append a disjoint, named tranche to a completed production annotation.",
    )
    extend.add_argument("--extension-name", required=True)
    extend.add_argument("--additional-rows", type=int, required=True)
    extend.add_argument("--batch-size", type=int, default=PRODUCTION_BATCH_SIZE)
    extend.add_argument("--seed", type=int, default=20260611)
    extend.add_argument("--reasoning", choices=["high", "xhigh"], default="high")

    for name in ["dry-run", "run", "validate"]:
        command = subparsers.add_parser(name)
        command.add_argument("--dataset", choices=["pilot", "production"], required=True)
        command.add_argument("--reasoning", choices=["high", "xhigh"], required=True)
        command.add_argument("--batch")
        if name == "run":
            command.add_argument("--max-attempts", type=int, default=2)
            command.add_argument("--no-resume", action="store_true")

    combine = subparsers.add_parser("combine")
    combine.add_argument("--dataset", choices=["pilot", "production"], required=True)
    combine.add_argument("--reasoning", choices=["high", "xhigh"], required=True)

    evaluate = subparsers.add_parser("evaluate-pilot")
    evaluate.add_argument("--reasoning", choices=["high", "xhigh"], action="append", required=True)
    evaluate.add_argument("--source", type=Path, default=PILOT_SOURCE)
    subparsers.add_parser("self-test", help="Exercise validator acceptance and rejection cases.")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    for path in [CODEBOOK_PATH, PROMPT_PATH, SCHEMA_PATH, SKILL_PATH]:
        if not path.exists():
            raise FileNotFoundError(f"Required annotation contract file not found: {path}")

    if args.command == "prepare-pilot":
        prepare_pilot(args.source, args.batch_size)
    elif args.command == "prepare-production-extension":
        prepare_production_extension(
            args.extension_name,
            args.additional_rows,
            args.batch_size,
            args.seed,
            args.reasoning,
        )
    elif args.command == "dry-run":
        print_dry_run(args.dataset, args.reasoning, args.batch)
    elif args.command == "run":
        paths = batch_paths(args.dataset, args.batch)
        for index, path in enumerate(paths, start=1):
            print(f"[{index}/{len(paths)}] {path.name}", flush=True)
            status = run_batch(
                args.dataset, args.reasoning, path, args.max_attempts, not args.no_resume
            )
            print(f"  {status}", flush=True)
    elif args.command == "validate":
        validate_dataset(args.dataset, args.reasoning, args.batch)
    elif args.command == "combine":
        combine_dataset(args.dataset, args.reasoning)
    elif args.command == "evaluate-pilot":
        evaluate_pilot(args.reasoning, args.source)
    elif args.command == "self-test":
        self_test()


if __name__ == "__main__":
    try:
        main()
    except (OSError, RuntimeError, ValueError) as error:
        raise SystemExit(f"ERROR: {error}")
