import pytest
from sqlalchemy import BigInteger, DateTime, Integer, SmallInteger, String, Table, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.engine import Dialect
from sqlalchemy.schema import CreateTable

from eonje.storage.models import NAMING_CONVENTION, ApiCallLog, Base

REQUIRED_COLUMNS = [
    "id",
    "called_at",
    "job",
    "run_id",
    "operation",
    "page_no",
    "attempt",
    "params",
    "elapsed_ms",
]
OPTIONAL_COLUMNS = ["http_status", "result_code", "total_count"]


def _ddl_lines(ddl: str) -> list[str]:
    """One stripped column/constraint definition per line, trailing commas removed."""
    return [line.strip().rstrip(",").strip() for line in ddl.splitlines() if line.strip()]


# Base metadata


def test_metadata_uses_the_project_naming_convention() -> None:
    # GIVEN the declarative base
    # WHEN its metadata naming convention is read
    convention = dict(Base.metadata.naming_convention)

    # THEN it is exactly the project convention
    assert convention == NAMING_CONVENTION


def test_api_call_log_model_is_registered_under_its_table_name(api_call_log: Table) -> None:
    # GIVEN the ApiCallLog model
    # WHEN the table is looked up in the shared metadata
    # THEN the model's table is the registered one
    assert ApiCallLog.__table__ is api_call_log
    assert api_call_log.name == "api_call_log"


# api_call_log: columns


def test_api_call_log_has_exactly_the_expected_columns(api_call_log: Table) -> None:
    # GIVEN the api_call_log table
    # WHEN its column names are listed
    names = [c.name for c in api_call_log.columns]

    # THEN they match the design, in order, with nothing extra
    assert names == [
        "id",
        "called_at",
        "job",
        "run_id",
        "operation",
        "page_no",
        "attempt",
        "params",
        "http_status",
        "result_code",
        "total_count",
        "elapsed_ms",
    ]


@pytest.mark.parametrize("name", REQUIRED_COLUMNS)
def test_api_call_log_required_column_is_not_nullable(api_call_log: Table, name: str) -> None:
    # GIVEN a column every call record must carry
    column = api_call_log.c[name]

    # WHEN its nullability is inspected
    # THEN NULL is rejected
    assert column.nullable is False


@pytest.mark.parametrize("name", OPTIONAL_COLUMNS)
def test_api_call_log_column_absent_on_failed_calls_is_nullable(
    api_call_log: Table, name: str
) -> None:
    # GIVEN a column with no value when the call fails before a response (timeout, gateway)
    column = api_call_log.c[name]

    # WHEN its nullability is inspected
    # THEN NULL is allowed so the failed call can still be logged
    assert column.nullable is True


@pytest.mark.parametrize(
    ("name", "type_", "length"),
    [
        ("job", String, 64),
        ("run_id", String, 250),
        ("operation", String, 64),
        ("result_code", String, 16),
    ],
)
def test_api_call_log_string_column_has_bounded_length(
    api_call_log: Table, name: str, type_: type, length: int
) -> None:
    # GIVEN a string column
    column_type = api_call_log.c[name].type

    # WHEN its type is inspected
    # THEN it is a VARCHAR with the expected limit
    assert isinstance(column_type, type_)
    assert column_type.length == length  # type: ignore[attr-defined]


def test_api_call_log_run_id_fits_airflow_run_id_length(api_call_log: Table) -> None:
    # GIVEN Airflow's run_id column is VARCHAR(250)
    # WHEN the run_id column length is inspected
    length = api_call_log.c.run_id.type.length  # type: ignore[attr-defined]

    # THEN any Airflow run_id fits without truncation
    assert length is not None
    assert length >= 250


@pytest.mark.parametrize(
    ("name", "type_"),
    [
        ("id", BigInteger),
        ("page_no", Integer),
        ("attempt", SmallInteger),
        ("http_status", SmallInteger),
        ("total_count", Integer),
        ("elapsed_ms", Integer),
    ],
)
def test_api_call_log_numeric_column_has_expected_integer_type(
    api_call_log: Table, name: str, type_: type
) -> None:
    # GIVEN a numeric column
    column_type = api_call_log.c[name].type

    # WHEN its type is inspected
    # THEN it is the exact integer width chosen in the model
    assert type(column_type) is type_


def test_api_call_log_called_at_is_timezone_aware(api_call_log: Table) -> None:
    # GIVEN the call timestamp column
    column_type = api_call_log.c.called_at.type

    # WHEN its type is inspected
    # THEN it stores an absolute instant (timestamptz), not a naive local time
    assert isinstance(column_type, DateTime)
    assert column_type.timezone is True


def test_api_call_log_params_is_jsonb(api_call_log: Table) -> None:
    # GIVEN the request parameter column
    # WHEN its type is inspected
    # THEN it is JSONB so arbitrary query params are kept as received
    assert isinstance(api_call_log.c.params.type, JSONB)


def test_api_call_log_has_no_server_defaults(api_call_log: Table) -> None:
    # GIVEN the api_call_log table
    # WHEN server defaults are collected
    defaults = {
        c.name: c.server_default for c in api_call_log.columns if c.server_default is not None
    }

    # THEN none exist: called_at and every other value are passed explicitly by the caller
    assert defaults == {}


# api_call_log: keys, constraints, indexes


def test_api_call_log_primary_key_is_the_surrogate_id(api_call_log: Table) -> None:
    # GIVEN the api_call_log table
    pk = api_call_log.primary_key

    # WHEN the primary key is inspected
    # THEN it is the single auto-incrementing id column, named by convention
    assert [c.name for c in pk.columns] == ["id"]
    assert api_call_log.c.id.autoincrement is True
    assert pk.name == "pk_api_call_log"


def test_api_call_log_allows_repeated_calls_for_the_same_page(api_call_log: Table) -> None:
    # GIVEN retries and reruns log the same (run_id, operation, page_no) more than once
    # WHEN unique constraints and unique indexes are collected
    uniques = [c for c in api_call_log.constraints if isinstance(c, UniqueConstraint)]
    unique_indexes = [ix for ix in api_call_log.indexes if ix.unique]

    # THEN there are none, so no call record is ever rejected
    assert uniques == []
    assert unique_indexes == []
    assert all(not c.unique for c in api_call_log.columns)


def test_api_call_log_called_at_is_indexed_for_daily_usage_lookup(api_call_log: Table) -> None:
    # GIVEN the budget lookup filters by called_at range
    # WHEN the table indexes are listed
    indexed = [[c.name for c in ix.columns] for ix in api_call_log.indexes]

    # THEN called_at has its own index
    assert indexed == [["called_at"]]


# api_call_log: compiled Postgres DDL


@pytest.mark.parametrize(
    "expected",
    [
        "id BIGSERIAL NOT NULL",
        "called_at TIMESTAMP WITH TIME ZONE NOT NULL",
        "job VARCHAR(64) NOT NULL",
        "run_id VARCHAR(250) NOT NULL",
        "operation VARCHAR(64) NOT NULL",
        "page_no INTEGER NOT NULL",
        "attempt SMALLINT NOT NULL",
        "params JSONB NOT NULL",
        "http_status SMALLINT",
        "result_code VARCHAR(16)",
        "total_count INTEGER",
        "elapsed_ms INTEGER NOT NULL",
        "CONSTRAINT pk_api_call_log PRIMARY KEY (id)",
    ],
)
def test_api_call_log_postgres_ddl_contains_definition(
    api_call_log_ddl: str, expected: str
) -> None:
    # GIVEN the table compiled to Postgres DDL
    # WHEN the DDL is split into definitions
    lines = _ddl_lines(api_call_log_ddl)

    # THEN the expected definition appears verbatim
    assert expected in lines


def test_api_call_log_postgres_ddl_has_no_unique_or_default_clauses(api_call_log_ddl: str) -> None:
    # GIVEN the table compiled to Postgres DDL
    # WHEN the DDL text is inspected
    ddl = api_call_log_ddl.upper()

    # THEN no UNIQUE or DEFAULT clause was generated
    assert "UNIQUE" not in ddl
    assert "DEFAULT" not in ddl


def test_api_call_log_postgres_index_ddl_uses_convention_name(
    api_call_log_index_ddl: list[str],
) -> None:
    # GIVEN the table indexes compiled to Postgres DDL
    # WHEN the statements are listed
    # THEN there is one non-unique index named by convention
    assert api_call_log_index_ddl == [
        "CREATE INDEX ix_api_call_log_called_at ON api_call_log (called_at)"
    ]


def test_api_call_log_postgres_ddl_compiles_identically_twice(
    api_call_log: Table, api_call_log_ddl: str, pg_dialect: Dialect
) -> None:
    # GIVEN the DDL already compiled once
    # WHEN the same table is compiled again
    again = str(CreateTable(api_call_log).compile(dialect=pg_dialect))

    # THEN the output is identical (model definition is deterministic)
    assert again == api_call_log_ddl
