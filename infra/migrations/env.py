from logging.config import fileConfig

from alembic import context
from sqlalchemy import Connection, engine_from_config, pool

from eonje.config import get_database_settings
from eonje.storage.models import Base

# this is the Alembic Config object, which provides
# access to the values within the .ini file in use.
config = context.config

# Logging from alembic.ini. Keep loggers that already exist (ours, pytest's) enabled.
if config.config_file_name is not None:
    fileConfig(config.config_file_name, disable_existing_loggers=False)

# The URL comes only from settings (.env), never from alembic.ini.
# '%' is special in ini values, so escape it.
config.set_main_option(
    "sqlalchemy.url",
    get_database_settings().database_url.get_secret_value().replace("%", "%%"),
)

target_metadata = Base.metadata

# other values from the config, defined by the needs of env.py,
# can be acquired:
# my_important_option = config.get_main_option("my_important_option")
# ... etc.


def include_name(name: str | None, type_: str, parent_names: object) -> bool:
    """Alembic 비교 대상에서 iceberg 스키마를 뺀다 (Iceberg JDBC 카탈로그가 관리)."""
    if type_ == "schema":
        return name != "iceberg"
    return True


def _run_with(connection: Connection) -> None:
    context.configure(
        connection=connection,
        target_metadata=target_metadata,
        compare_type=True,
        compare_server_default=True,
        include_name=include_name,
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode.

    This configures the context with just a URL
    and not an Engine, though an Engine is acceptable
    here as well.  By skipping the Engine creation
    we don't even need a DBAPI to be available.

    Calls to context.execute() here emit the given string to the
    script output.

    """
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        compare_type=True,
        compare_server_default=True,
        include_name=include_name,
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """Run migrations in 'online' mode.

    In this scenario we need to create an Engine
    and associate a connection with the context.

    """
    connection = config.attributes.get("connection")

    if isinstance(connection, Connection):
        _run_with(connection)
        return

    connectable = engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )
    with connectable.connect() as conn:
        _run_with(conn)


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
