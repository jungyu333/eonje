import subprocess
import sys

from eonje.cli.jobs import dedupe_load_obs
from eonje.cli.registry import JOBS


def test_load_dims_is_a_registered_job() -> None:
    # GIVEN the job registry
    # WHEN / THEN load-dims is dispatchable by its CLI name
    assert "load-dims" in JOBS


def test_load_dim_date_is_a_registered_job() -> None:
    # GIVEN the job registry
    # WHEN / THEN load-dim-date is dispatchable by its CLI name
    assert "load-dim-date" in JOBS


def test_dedupe_load_obs_is_registered_and_dispatches_to_its_job() -> None:
    # GIVEN the job registry
    # WHEN the CLI name dedupe-load-obs is looked up
    job = JOBS["dedupe-load-obs"]
    # THEN it is the dedupe_load_obs job's entrypoint
    assert job is dedupe_load_obs.run


def test_registry_imports_in_an_interpreter_without_pyspark() -> None:
    # GIVEN a fresh interpreter where any pyspark import fails, as in the collector image
    script = (
        "import sys\n"
        "sys.modules['pyspark'] = None\n"  # makes every 'import pyspark...' raise ImportError
        "import eonje.cli.registry\n"
    )
    # WHEN it imports the registry, which imports every job module
    result = subprocess.run(
        [sys.executable, "-c", script], capture_output=True, text=True, check=False
    )
    # THEN the import succeeds
    assert result.returncode == 0, result.stderr


def test_importing_the_registry_does_not_import_pyspark() -> None:
    # GIVEN a fresh interpreter (this process may already have pyspark loaded)
    script = (
        "import sys\n"
        "import eonje.cli.registry\n"
        "loaded = sorted(m for m in sys.modules if m == 'pyspark' or m.startswith('pyspark.'))\n"
        "print(','.join(loaded))\n"
    )
    # WHEN it imports the registry, as the collector image does
    result = subprocess.run(
        [sys.executable, "-c", script], capture_output=True, text=True, check=True
    )
    # THEN no pyspark module was imported
    assert result.stdout.strip() == ""
