from pathlib import Path
from typing import Dict


def interim_base(config: Dict) -> Path:
    return Path(config.get("paths", {}).get("interim_base", "data/interim"))


def stage1_base(config: Dict) -> Path:
    return Path(
        config.get("paths", {}).get(
            "stage1_base", str(interim_base(config) / "stage1_pilot-dev")
        )
    )


def stage1_output_dir(config: Dict, default_stage: str = "stage1b") -> Path:
    project_out_dir = config.get("project", {}).get("out_dir")
    if project_out_dir:
        return Path(project_out_dir)

    stage = config.get("run_context", {}).get("stage", default_stage)
    if stage in {"stage1a", "stage1b", "stage1c"}:
        return stage1_base(config) / stage
    return interim_base(config) / stage
