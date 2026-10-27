"""Offline (``--sql``) checks for Alembic migrations. No database connection is made."""

import logging.config
from pathlib import Path

import pytest
from alembic import command
from alembic.config import Config
from alembic.script import ScriptDirectory

# The autouse fixture in conftest.py chdirs into tmp_path, so resolve paths from the repo root.
REPO_ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture
def alembic_cfg(database_url_env: str, monkeypatch: pytest.MonkeyPatch) -> Config:
    """Alembic config pointing at the repo's migrations, with a dummy DB URL in env.

    env.py calls ``logging.config.fileConfig`` on alembic.ini. Existing loggers stay
    enabled (disable_existing_loggers=False), but the root logger's handlers and level
    are still replaced for the rest of the session. Neutralize it so this test cannot
    leak logging state into other tests.
    """
    monkeypatch.setattr(logging.config, "fileConfig", lambda *args, **kwargs: None)
    cfg = Config(str(REPO_ROOT / "alembic.ini"))
    cfg.set_main_option("script_location", str(REPO_ROOT / "infra" / "migrations"))
    return cfg


def _upgrade_sql(cfg: Config, capsys: pytest.CaptureFixture[str]) -> str:
    command.upgrade(cfg, "head", sql=True)
    return capsys.readouterr().out


def test_migration_history_has_a_single_head(alembic_cfg: Config) -> None:
    # GIVEN the migration scripts in infra/migrations
    script = ScriptDirectory.from_config(alembic_cfg)

    # WHEN the heads are listed
    heads = script.get_heads()

    # THEN there is exactly one (no unmerged branches)
    assert len(heads) == 1


def test_offline_upgrade_sql_creates_api_call_log(
    alembic_cfg: Config, capsys: pytest.CaptureFixture[str]
) -> None:
    # GIVEN an alembic config and no reachable database
    # WHEN the upgrade to head is rendered as SQL only
    out = _upgrade_sql(alembic_cfg, capsys)

    # THEN the api_call_log table is created with a JSONB column
    assert "CREATE TABLE api_call_log" in out
    assert "JSONB" in out


def test_offline_upgrade_sql_uses_naming_convention_for_pk_and_index(
    alembic_cfg: Config, capsys: pytest.CaptureFixture[str]
) -> None:
    # GIVEN an alembic config and no reachable database
    # WHEN the upgrade to head is rendered as SQL only
    out = _upgrade_sql(alembic_cfg, capsys)

    # THEN constraint and index names follow the metadata naming convention
    assert "CONSTRAINT pk_api_call_log PRIMARY KEY (id)" in out
    assert "CREATE INDEX ix_api_call_log_called_at ON api_call_log (called_at)" in out


def test_offline_upgrade_sql_does_not_contain_database_password(
    alembic_cfg: Config, capsys: pytest.CaptureFixture[str]
) -> None:
    # GIVEN a database URL containing a password
    # WHEN the upgrade to head is rendered as SQL only
    out = _upgrade_sql(alembic_cfg, capsys)

    # THEN the password does not appear in the emitted SQL
    assert "db-pw-should-not-leak" not in out


def test_offline_downgrade_sql_drops_api_call_log(
    alembic_cfg: Config, capsys: pytest.CaptureFixture[str]
) -> None:
    # GIVEN the head revision
    head = ScriptDirectory.from_config(alembic_cfg).get_current_head()

    # WHEN the downgrade from head to base is rendered as SQL only
    command.downgrade(alembic_cfg, f"{head}:base", sql=True)
    out = capsys.readouterr().out

    # THEN the index and table are dropped
    assert "DROP INDEX ix_api_call_log_called_at" in out
    assert "DROP TABLE api_call_log" in out


def test_offline_upgrade_sql_is_deterministic(
    alembic_cfg: Config, capsys: pytest.CaptureFixture[str]
) -> None:
    # GIVEN an alembic config
    # WHEN the upgrade SQL is rendered twice
    first = _upgrade_sql(alembic_cfg, capsys)
    second = _upgrade_sql(alembic_cfg, capsys)

    # THEN both renders are identical
    assert first == second
