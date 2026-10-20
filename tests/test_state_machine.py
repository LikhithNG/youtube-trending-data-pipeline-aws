import json
from pathlib import Path

import pytest

from scripts.render_state_machine import render

ROOT = Path(__file__).resolve().parents[1]
TEMPLATE = ROOT / "step_functions" / "pipeline_orchestration.template.json"
CONFIG = ROOT / "step_functions" / "config.example.json"


def test_example_config_renders_valid_definition():
    values = json.loads(CONFIG.read_text(encoding="utf-8"))
    definition = render(TEMPLATE.read_text(encoding="utf-8"), values)

    assert definition["StartAt"] == "IngestFromYouTubeAPI"
    reference = definition["States"]["ProcessSilverInParallel"]["Branches"][0]["States"]
    payload = reference["TransformReferenceData"]["Parameters"]["Payload"]
    assert payload["objects.$"] == "$.ingestion_result.Payload.reference_objects"
    assert values["SNS_TOPIC_ARN"].count(":") == 5


def test_render_rejects_missing_values():
    with pytest.raises(ValueError, match="Missing template values"):
        render('{"value": "${MISSING}"}', {})
