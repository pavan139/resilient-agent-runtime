from __future__ import annotations
import argparse
import json
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
from .evaluation import demo, evaluate


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Durable fictional SQLite workflow runtime")
    sub = parser.add_subparsers(dest="command", required=True)
    for command in ("demo", "evaluate", "outbox-demo", "outbox-evaluate"):
        child = sub.add_parser(command)
        child.add_argument("--output", type=Path)
    args = parser.parse_args(argv)
    try:
        if args.command in {"outbox-demo", "outbox-evaluate"}:
            from .outbox_experiments import demo as outbox_demo, evaluate as outbox_evaluate

            result = outbox_evaluate() if args.command == "outbox-evaluate" else outbox_demo()
        elif args.command == "evaluate":
            result = evaluate()
        else:
            with tempfile.TemporaryDirectory(prefix="runtime-demo-") as folder:
                result = demo(str(Path(folder) / "state.sqlite"))
        serialized = json.dumps(result, indent=2) + "\n"
        if args.output:
            args.output.parent.mkdir(parents=True, exist_ok=True)
            args.output.write_text(serialized)
        else:
            print(serialized, end="")
        return int(
            (args.command.endswith("evaluate") and result["passed"] != result["total"])
            or (args.command.endswith("demo") and not result["passed"])
        )
    except (OSError, sqlite3.Error, ValueError, subprocess.SubprocessError) as exc:
        print(json.dumps({"error": "runtime_experiment_failed", "error_type": type(exc).__name__}), file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
