from __future__ import annotations

from duckdb import DuckDBPyConnection

# OID of the named object, by kind. Names are compared case-insensitively, as DuckDB resolves them.
_OID_SQL = {
    "DATABASE": "SELECT database_oid FROM duckdb_databases() WHERE lower(database_name) = lower(?)",
    "SCHEMA": """
        SELECT oid FROM duckdb_schemas() WHERE lower(database_name) = lower(?) AND lower(schema_name) = lower(?)
    """,
    "TABLE": """
        SELECT table_oid FROM duckdb_tables()
        WHERE lower(database_name) = lower(?) AND lower(schema_name) = lower(?) AND lower(table_name) = lower(?)
    """,
    "VIEW": """
        SELECT view_oid FROM duckdb_views()
        WHERE lower(database_name) = lower(?) AND lower(schema_name) = lower(?) AND lower(view_name) = lower(?)
    """,
}


class Owners:
    """Remember the role that owns each database, schema, table and view.

    In Snowflake the role that creates an object owns it, whichever role is active later. Owners are keyed by DuckDB
    OID, which survives renames and is never reused, so ownership follows ALTER ... RENAME and SWAP WITH, and an
    object created by CREATE OR REPLACE gets the replacing role. They are held here rather than in a DuckDB table
    because a transaction can only write to one attached database, and objects are often created inside one.

    One registry is shared by all connections to an instance, so each call takes the caller's DuckDB connection, which
    can see its own session's temporary objects.
    """

    def __init__(self) -> None:
        self._roles: dict[int, str] = {}

    def record(self, duck_conn: DuckDBPyConnection, kind: str, names: list[str], role: str) -> None:
        """Record role as the owner of the object just created, unless it already existed.

        names are the DuckDB [database, schema, name] parts, as many as kind needs.
        """
        if (oid := _oid(duck_conn, kind, names)) is not None:
            # CREATE ... IF NOT EXISTS succeeds without creating anything when the object exists, so keep its owner
            self._roles.setdefault(oid, role)

    def owner(self, duck_conn: DuckDBPyConnection, kind: str, names: list[str]) -> str | None:
        """The role that owns the named object, or None when its creation wasn't seen, eg: reopened from db_path."""
        oid = _oid(duck_conn, kind, names)
        return None if oid is None else self._roles.get(oid)


def _oid(duck_conn: DuckDBPyConnection, kind: str, names: list[str]) -> int | None:
    row = duck_conn.execute(_OID_SQL[kind], names).fetchone()
    return row[0] if row else None
