from __future__ import annotations

import snowflake.connector.errors
from duckdb import DuckDBPyConnection
from sqlglot import Expr, exp

from fakesnow.temporary import TemporaryObjects

_SCHEMA_EXISTS_SQL = """
    SELECT 1 FROM information_schema.schemata WHERE upper(catalog_name) = upper(?) AND upper(schema_name) = upper(?)
"""


def check_schema_exists(duck_conn: DuckDBPyConnection, database: str | None, schema: str | None) -> None:
    """Raise Snowflake's error if the schema doesn't exist."""
    if not duck_conn.execute(_SCHEMA_EXISTS_SQL, [database, schema]).fetchone():
        raise snowflake.connector.errors.ProgrammingError(
            msg=f"SQL compilation error:\nSchema '{database}.{schema}' does not exist or not authorized.",
            errno=2003,
            sqlstate="02000",
        )


def temporary_objects(
    expression: Expr,
    temporaries: TemporaryObjects,
    current_database: str | None,
    current_schema: str | None,
    duck_conn: DuckDBPyConnection,
) -> Expr:
    """Place temporary tables and views in DuckDB's temp.main, and resolve qualified references to them.

    DuckDB only creates temporary objects in the temp catalog, so CREATE TEMPORARY TABLE|VIEW loses its qualifiers.
    The schema it was created in is kept in temporary_owner for TemporaryObjects.record. A qualified reference to a
    temporary object resolves to it only from that schema, as in Snowflake, and any reference from another schema
    is qualified so it does not resolve to the temporary object. fakesnow's own scratch tables, marked
    internal_temporary, are left as they are.
    """
    if expression.args.get("internal_temporary"):
        return expression

    if (
        isinstance(expression, exp.Create)
        and expression.args.get("kind") in ("TABLE", "VIEW")
        and expression.find(exp.TemporaryProperty)
        and (table := expression.find(exp.Table))
    ):
        owner = (table.catalog or current_database, table.db or current_schema)
        # DuckDB would create it in temp.main whether or not its schema exists
        check_schema_exists(duck_conn, *owner)
        if (existing := temporaries.owner(table.name)) and existing != owner:
            # DuckDB names temporary objects without their schema, so this one would replace or clash with the other
            raise NotImplementedError("Temporary objects with the same name in different schemas")
        expression.args["temporary_owner"] = owner
        table.set("catalog", None)
        table.set("db", None)
        return expression

    if (
        isinstance(expression, exp.Table)
        and not _is_target(expression)
        and not _is_other_kind(expression)
        and not _is_cte(expression)
    ):
        resolve_table(expression, temporaries, current_database, current_schema)

    if (
        isinstance(expression, exp.Column)
        and expression.args.get("db")
        and (owner := temporaries.owner(expression.table))
        and owner == (expression.catalog or current_database, expression.db)
    ):
        # A schema-qualified column follows its temporary table to temp.main, or it would no longer match it.
        expression.set("catalog", exp.to_identifier("temp"))
        expression.set("db", exp.to_identifier("main"))

    return expression


def resolve_table(
    table: exp.Table, temporaries: TemporaryObjects, current_database: str | None, current_schema: str | None
) -> None:
    """Point a table reference at the temporary object it names in Snowflake, or away from one it doesn't."""
    if not (owner := temporaries.owner(table.name)):
        return
    catalog = table.catalog or current_database
    schema = table.db or current_schema
    if owner == (catalog, schema):
        # An unqualified reference already resolves to the temporary object, which DuckDB searches first.
        if table.args.get("db"):
            table.set("catalog", exp.to_identifier("temp"))
            table.set("db", exp.to_identifier("main"))
    elif catalog and schema:
        # Qualify fully so the reference resolves to a permanent object rather than the temporary one.
        table.set("catalog", exp.to_identifier(catalog))
        table.set("db", exp.to_identifier(schema))


def _is_target(table: exp.Table) -> bool:
    """Whether table is the object a CREATE makes or a COPY loads.

    A permanent CREATE must not redirect to temp, and COPY resolves its target's stage by its Snowflake name before
    resolving the target itself.
    """
    parent = table.parent.parent if isinstance(table.parent, exp.Schema) else table.parent
    return isinstance(parent, (exp.Create, exp.Copy)) and (parent.this is table or parent.this.this is table)


def _is_other_kind(table: exp.Table) -> bool:
    """Whether table names an object that cannot be a table or view, eg: DROP SEQUENCE or USE SCHEMA."""
    parent = table.parent
    return isinstance(parent, (exp.Use, exp.Show)) or (
        isinstance(parent, (exp.Drop, exp.Describe))
        and isinstance(kind := parent.args.get("kind"), str)
        and kind.upper() not in ("TABLE", "VIEW")
    )


def _is_cte(table: exp.Table) -> bool:
    """Whether table names a CTE in scope rather than a temporary object.

    A CTE is in scope from the query whose WITH defines it, including that WITH's other CTEs.
    """
    if table.args.get("db"):
        return False
    node = table.parent
    while node:
        if isinstance(with_ := node.args.get("with_"), exp.With) and any(
            cte.alias == table.name for cte in with_.expressions
        ):
            return True
        node = node.parent
    return False
