import re

import pytest

from eonje.storage.lake.iceberg import (
    DEFAULT_PROPERTIES,
    DIM_CHARGER,
    DIM_STATION,
    TABLES,
    Column,
    IcebergTable,
    add_columns_sql,
    create_table_sql,
    missing_columns,
)

SCD2_COLUMNS = ("valid_from", "valid_to", "is_current", "row_hash", "source_run_key")
SNAKE_CASE = re.compile(r"^[a-z][a-z0-9]*(_[a-z0-9]+)*$")


def _table(**overrides: object) -> IcebergTable:
    values: dict[str, object] = {
        "namespace": "stg",
        "name": "sample",
        "columns": (
            Column("id", "STRING", nullable=False, comment="key"),
            Column("value", "INT"),
            Column("observed_date", "DATE", nullable=False),
        ),
    }
    values.update(overrides)
    return IcebergTable(**values)  # type: ignore[arg-type]


# --- IcebergTable -----------------------------------------------------------


def test_full_name_is_catalog_namespace_and_name() -> None:
    # GIVEN a table in the stg namespace
    table = _table(namespace="stg", name="obs_status")

    # WHEN its full name is read
    name = table.full_name

    # THEN it is addressed through the lake catalog
    assert name == "lake.stg.obs_status"


def test_each_table_gets_its_own_copy_of_default_properties() -> None:
    # GIVEN two tables built with default properties
    first = _table(name="first")
    second = _table(name="second")

    # WHEN one table's properties are mutated
    first.properties["write.merge.mode"] = "merge-on-read"  # type: ignore[index]

    # THEN the other table and the module default are unaffected
    assert second.properties == dict(DEFAULT_PROPERTIES)
    assert DEFAULT_PROPERTIES["write.merge.mode"] == "copy-on-write"
    assert first.properties is not second.properties
    assert first.properties is not DEFAULT_PROPERTIES


def test_table_rejects_duplicated_column_names() -> None:
    # GIVEN columns with a repeated name
    columns = (Column("id", "STRING"), Column("value", "INT"), Column("id", "INT"))

    # WHEN / THEN building the table fails and names the duplicate
    with pytest.raises(ValueError, match=r"duplicated columns \['id'\]"):
        _table(columns=columns)


def test_table_rejects_partition_column_not_in_table() -> None:
    # GIVEN a partition column that is not defined
    # WHEN / THEN building the table fails and names the unknown column
    with pytest.raises(ValueError, match=r"partition columns not in table \['at_date'\]"):
        _table(partitioned_by=("at_date",))


def test_table_accepts_partition_column_that_exists() -> None:
    # GIVEN a partition column that is defined
    # WHEN the table is built
    table = _table(partitioned_by=("observed_date",))

    # THEN the partition is kept
    assert table.partitioned_by == ("observed_date",)


# --- create_table_sql -------------------------------------------------------


def test_create_table_sql_renders_exact_statement() -> None:
    # GIVEN a partitioned table with explicit properties
    table = _table(partitioned_by=("observed_date",), properties={"format-version": "2"})

    # WHEN the DDL is rendered
    sql = create_table_sql(table)

    # THEN it is an idempotent Iceberg CREATE with NOT NULL, COMMENT and partition
    assert sql == (
        "CREATE TABLE IF NOT EXISTS lake.stg.sample (\n"
        "  `id` STRING NOT NULL COMMENT 'key',\n"
        "  `value` INT,\n"
        "  `observed_date` DATE NOT NULL\n"
        ")\n"
        "USING iceberg\n"
        "PARTITIONED BY (`observed_date`)\n"
        "TBLPROPERTIES ('format-version'='2')"
    )


def test_create_table_sql_omits_partition_clause_when_not_partitioned() -> None:
    # GIVEN a table without partition columns
    table = _table()

    # WHEN the DDL is rendered
    sql = create_table_sql(table)

    # THEN there is no PARTITIONED BY clause
    assert "PARTITIONED BY" not in sql
    assert sql.startswith("CREATE TABLE IF NOT EXISTS lake.stg.sample (")
    assert "\nUSING iceberg" in sql


def test_create_table_sql_omits_properties_clause_when_empty() -> None:
    # GIVEN a table with no properties
    table = _table(properties={})

    # WHEN the DDL is rendered
    sql = create_table_sql(table)

    # THEN it ends at USING iceberg
    assert "TBLPROPERTIES" not in sql
    assert sql.endswith(")\nUSING iceberg")


def test_create_table_sql_lists_properties_in_sorted_order() -> None:
    # GIVEN properties given out of order
    table = _table(properties={"z.key": "1", "a.key": "2", "m.key": "3"})

    # WHEN the DDL is rendered
    sql = create_table_sql(table)

    # THEN the properties are sorted by key
    assert sql.endswith("TBLPROPERTIES ('a.key'='2', 'm.key'='3', 'z.key'='1')")


def test_create_table_sql_renders_default_properties_sorted() -> None:
    # GIVEN a table using the default properties
    table = _table()

    # WHEN the DDL is rendered
    sql = create_table_sql(table)

    # THEN all defaults are present in key order
    assert sql.endswith(
        "TBLPROPERTIES ('format-version'='2', "
        "'write.delete.mode'='copy-on-write', "
        "'write.merge.mode'='copy-on-write', "
        "'write.update.mode'='copy-on-write')"
    )


def test_create_table_sql_escapes_single_quotes_in_comments() -> None:
    # GIVEN a comment containing a single quote
    table = _table(columns=(Column("note", "STRING", comment="operator's note"),))

    # WHEN the DDL is rendered
    sql = create_table_sql(table)

    # THEN the quote is escaped inside the string literal
    assert "`note` STRING COMMENT 'operator\\'s note'" in sql


def test_create_table_sql_escapes_backslash_before_quote() -> None:
    # GIVEN a comment ending in a backslash, which would otherwise escape the closing quote
    table = _table(columns=(Column("path", "STRING", comment="C:\\"),))

    # WHEN the DDL is rendered
    sql = create_table_sql(table)

    # THEN the backslash is doubled so the literal stays closed
    assert "`path` STRING COMMENT 'C:\\\\'" in sql


def test_create_table_sql_escapes_single_quotes_in_properties() -> None:
    # GIVEN a property value containing a single quote
    table = _table(properties={"comment": "it's"})

    # WHEN the DDL is rendered
    sql = create_table_sql(table)

    # THEN the value is escaped
    assert sql.endswith("TBLPROPERTIES ('comment'='it\\'s')")


def test_create_table_sql_is_deterministic() -> None:
    # GIVEN a table
    table = _table(partitioned_by=("observed_date",))

    # WHEN the DDL is rendered twice
    # THEN the output is identical
    assert create_table_sql(table) == create_table_sql(table)


# --- missing_columns --------------------------------------------------------


def test_missing_columns_returns_only_absent_columns_in_definition_order() -> None:
    # GIVEN a table that gained two nullable columns after creation
    table = _table(
        columns=(
            Column("id", "STRING", nullable=False),
            Column("b_new", "STRING"),
            Column("value", "INT"),
            Column("a_new", "INT", comment="added"),
        )
    )

    # WHEN compared with existing columns listed in a different order
    missing = missing_columns(table, ["value", "id"])

    # THEN only the absent columns come back, in definition order (not alphabetical)
    assert missing == (Column("b_new", "STRING"), Column("a_new", "INT", comment="added"))


def test_missing_columns_is_empty_for_up_to_date_table() -> None:
    # GIVEN an existing table that already has every column
    table = _table()

    # WHEN compared
    missing = missing_columns(table, ["id", "value", "observed_date"])

    # THEN nothing is missing
    assert missing == ()


def test_missing_columns_ignores_extra_existing_columns() -> None:
    # GIVEN an existing table with a column no longer in the definition
    table = _table()

    # WHEN compared
    missing = missing_columns(table, ["id", "value", "observed_date", "legacy"])

    # THEN the extra column is neither reported nor an error
    assert missing == ()


def test_missing_columns_accepts_a_one_shot_iterable() -> None:
    # GIVEN existing columns as a generator
    table = _table()
    existing = (name for name in ["id", "observed_date"])

    # WHEN compared
    missing = missing_columns(table, existing)

    # THEN the result is the same as for a list
    assert missing == (Column("value", "INT"),)


def test_missing_columns_rejects_adding_not_null_column() -> None:
    # GIVEN a definition whose new column is NOT NULL
    table = _table()

    # WHEN / THEN comparing against a table without it fails with the table and column
    with pytest.raises(ValueError, match=r"lake\.stg\.sample: .*NOT NULL.*\['observed_date'\]"):
        missing_columns(table, ["id", "value"])


def test_missing_columns_with_empty_existing_rejects_not_null_columns() -> None:
    # GIVEN an existing table reporting no columns at all
    table = _table()

    # WHEN / THEN every NOT NULL column is named in the error
    with pytest.raises(ValueError, match=r"\['id', 'observed_date'\]"):
        missing_columns(table, [])


def test_missing_columns_is_idempotent_after_adding() -> None:
    # GIVEN the missing columns of a table
    table = _table()
    existing = ["id", "observed_date"]
    missing = missing_columns(table, existing)

    # WHEN the comparison is repeated after those columns were added
    again = missing_columns(table, [*existing, *(c.name for c in missing)])

    # THEN nothing is left to add
    assert again == ()


# --- add_columns_sql --------------------------------------------------------


def test_add_columns_sql_renders_exact_statement() -> None:
    # GIVEN two nullable columns, one with a comment containing a quote
    table = _table()
    columns = (Column("a_new", "INT", comment="it's new"), Column("b_new", "STRING"))

    # WHEN the ALTER is rendered
    sql = add_columns_sql(table, columns)

    # THEN columns appear in the given order inside one ADD COLUMNS clause
    assert sql == (
        "ALTER TABLE lake.stg.sample ADD COLUMNS (`a_new` INT COMMENT 'it\\'s new', `b_new` STRING)"
    )


def test_add_columns_sql_for_single_column() -> None:
    # GIVEN one column
    table = _table()

    # WHEN the ALTER is rendered
    sql = add_columns_sql(table, [Column("x", "DOUBLE")])

    # THEN it holds exactly that column
    assert sql == "ALTER TABLE lake.stg.sample ADD COLUMNS (`x` DOUBLE)"


# --- TABLES -----------------------------------------------------------------


@pytest.mark.parametrize("table", TABLES, ids=lambda t: t.full_name)
def test_table_column_names_are_unique_snake_case(table: IcebergTable) -> None:
    # GIVEN a registered table
    names = [c.name for c in table.columns]

    # THEN every column name is snake_case and appears once
    assert [n for n in names if not SNAKE_CASE.match(n)] == []
    assert len(names) == len(set(names))


def test_table_full_names_are_unique() -> None:
    # GIVEN all registered tables
    names = [t.full_name for t in TABLES]

    # THEN no two tables share a name
    assert len(names) == len(set(names))


@pytest.mark.parametrize("table", [DIM_STATION, DIM_CHARGER], ids=lambda t: t.name)
def test_scd2_dimension_has_version_columns(table: IcebergTable) -> None:
    # GIVEN an SCD2 dimension
    by_name = {c.name: c for c in table.columns}

    # THEN it carries all SCD2 bookkeeping columns as NOT NULL
    assert [n for n in SCD2_COLUMNS if n not in by_name] == []
    assert [n for n in SCD2_COLUMNS if by_name[n].nullable] == []


@pytest.mark.parametrize(
    ("table", "key"),
    [(DIM_STATION, ("stat_id",)), (DIM_CHARGER, ("stat_id", "charger_id"))],
    ids=["dim_station", "dim_charger"],
)
def test_scd2_dimension_natural_key_is_not_null(table: IcebergTable, key: tuple[str, ...]) -> None:
    # GIVEN an SCD2 dimension and its natural key from docs/design.md
    by_name = {c.name: c for c in table.columns}

    # THEN every key column exists and is NOT NULL
    assert all(not by_name[k].nullable for k in key)


@pytest.mark.parametrize("table", TABLES, ids=lambda t: t.full_name)
def test_registered_table_renders_create_sql(table: IcebergTable) -> None:
    # GIVEN a registered table
    # WHEN its DDL is rendered
    sql = create_table_sql(table)

    # THEN it targets the table by catalog name and lists every column
    assert sql.startswith(f"CREATE TABLE IF NOT EXISTS {table.full_name} (")
    assert all(f"`{c.name}` {c.type}" in sql for c in table.columns)
