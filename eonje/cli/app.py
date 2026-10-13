import argparse
import sys
from collections.abc import Sequence

from eonje.cli.registry import JOBS


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="eonje")
    parser.add_argument("job", choices=sorted(JOBS))
    parser.add_argument(
        "--run-id", required=True, help="Airflow run_id; partition key for idempotency"
    )

    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        return JOBS[args.job](args.run_id)
    except NotImplementedError as e:
        print(f"{e}: not implemented (run_id={args.run_id})", file=sys.stderr)
        return 2
