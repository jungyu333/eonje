import subprocess
import sys

from eonje.cli.registry import JOBS


def test_load_dims_is_a_registered_job() -> None:
    # GIVEN the job registry
    # WHEN / THEN load-dims is dispatchable by its CLI name
    assert "load-dims" in JOBS


def test_load_dim_date_is_a_registered_job() -> None:
    # GIVEN the job registry
    # WHEN / THEN load-dim-date is dispatchable by its CLI name
    assert "load-dim-date" in JOBS


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
