import argparse
import sys

from pydantic import ValidationError

from eonje.cli.registry import JOBS
from eonje.config import get_settings


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="eonje")
    parser.add_argument("job", choices=sorted(JOBS))
    parser.add_argument(
        "--run-id", required=True, help="Airflow run_id; partition key for idempotency"
    )

    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)

    try:
        settings = get_settings()
    except ValidationError as e:
        print(f"invalid settings:\n{e}", file=sys.stderr)
        return 2

    try:
        return JOBS[args.job](args.run_id, settings)
    except NotImplementedError as e:
        print(str(e), file=sys.stderr)
        return 2
