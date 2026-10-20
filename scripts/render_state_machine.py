#!/usr/bin/env python3
"""Render the portable Step Functions definition from a JSON configuration file."""

import argparse
import json
import re
from pathlib import Path

TOKEN = re.compile(r"\$\{([A-Z0-9_]+)\}")


def render(template: str, values: dict[str, str]) -> dict:
    required = set(TOKEN.findall(template))
    missing = sorted(required - values.keys())
    if missing:
        raise ValueError(f"Missing template values: {', '.join(missing)}")
    rendered = TOKEN.sub(lambda match: str(values[match.group(1)]), template)
    unresolved = TOKEN.findall(rendered)
    if unresolved:
        raise ValueError(f"Unresolved template values: {', '.join(sorted(set(unresolved)))}")
    return json.loads(rendered)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--template", type=Path, required=True)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()

    values = json.loads(args.config.read_text(encoding="utf-8"))
    definition = render(args.template.read_text(encoding="utf-8"), values)
    args.output.write_text(json.dumps(definition, indent=2) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
