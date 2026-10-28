import argparse
import sys
from datetime import datetime

from pydantic import ValidationError

from eonje.cli.context import JobContext
from eonje.cli.logs import configure_logging
from eonje.cli.registry import JOBS
from eonje.config import get_settings


def _aware_datetime(value: str) -> datetime:

    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as e:
        raise argparse.ArgumentTypeError(f"not an ISO 8601 datetime: {value!r}") from e
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise argparse.ArgumentTypeError(f"--run-after needs a timezone offset: {value!r}")
    return parsed


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="eonje")
    parser.add_argument("job", choices=sorted(JOBS))
    parser.add_argument(
        "--run-id", required=True, help="Airflow run_id; partition key for idempotency"
    )
    parser.add_argument(
        "--run-after",
        required=True,
        type=_aware_datetime,
        help="Airflow dag_run.run_after (ISO 8601 with offset); fixes collected_at and dt",
    )

    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    configure_logging()

    try:
        settings = get_settings()
    except ValidationError as e:
        print(f"invalid settings:\n{e}", file=sys.stderr)
        return 2

    ctx = JobContext(
        run_id=args.run_id,
        run_after=args.run_after,
        settings=settings,
    )

    try:
        return JOBS[args.job](ctx)
    except NotImplementedError as e:
        print(str(e), file=sys.stderr)
        return 2
