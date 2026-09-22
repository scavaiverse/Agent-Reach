"""Executable entry point for ``python -m agent_reach.buzz``."""

from __future__ import annotations

import argparse
import json

from .run import run_buzz


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the edition-anchored OnTheRice Buzz Layer")
    parser.add_argument("--site", default="https://new.ontherice.org")
    parser.add_argument("--output", default="output")
    parser.add_argument("--channel-timeout", type=float, default=15.0)
    parser.add_argument("--global-timeout", type=float, default=240.0)
    args = parser.parse_args()
    result = run_buzz(output_dir=args.output, base_url=args.site, channel_timeout=args.channel_timeout, global_timeout=args.global_timeout)
    print(json.dumps({
        "run_id": result["run_id"], "edition_id": result["edition_id"],
        "raw_signals": len(result["raw_signals"]), "normalized_signals": len(result["normalized_signals"]),
        "rejected_signals": len(result["rejected_signals"]), "buzzpacks": len(result["buzzpacks"]),
        "coverage": result["platform_coverage"],
    }, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
