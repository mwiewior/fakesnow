from __future__ import annotations

import os
import threading
import weakref
from typing import Any

import duckdb

import fakesnow.fakes as fakes
from fakesnow import info_schema
from fakesnow.owners import Owners
from fakesnow.transforms import show

GLOBAL_DATABASE_NAME = "_fs_global"


class FakeSnow:
    def __init__(
        self,
        create_database_on_connect: bool = True,
        create_schema_on_connect: bool = True,
        db_path: str | os.PathLike | None = None,
        nop_regexes: list[str] | None = None,
    ):
        self.create_database_on_connect = create_database_on_connect
        self.create_schema_on_connect = create_schema_on_connect
        self.db_path = db_path
        self.nop_regexes = nop_regexes

        self.results_cache: dict[str, tuple] = {}
        self.owners = Owners()
        # connecting may create the database and schema, which concurrent connections would race on
        self._connect_lock = threading.Lock()
        # each has its own duckdb connection, which keeps the attached database files open until it is closed
        self._connections: weakref.WeakSet[fakes.FakeSnowflakeConnection] = weakref.WeakSet()
        # SQLGlot's DuckDB transpilation still emits legacy arrow lambdas; keep the
        # DuckDB 2.0 compatibility setting enabled.
        self.duck_conn = duckdb.connect(database=":memory:", config={"lambda_syntax": "ENABLE_SINGLE_ARROW"})

        # create a "global" database for storing objects which span databases.
        self.duck_conn.execute(f"ATTACH IF NOT EXISTS ':memory:' AS {GLOBAL_DATABASE_NAME}")
        # create the info schema extensions and show views
        self.duck_conn.execute(info_schema.fs_global_creation_sql())
        self.duck_conn.execute(show.fs_global_creation_sql())

        # use UTC instead of local time zone for consistent testing
        self.duck_conn.execute("SET GLOBAL TimeZone = 'UTC'")

    def connect(
        self,
        database: str | None = None,
        schema: str | None = None,
        nop_regexes: list[str] | None = None,
        **kwargs: Any,
    ) -> fakes.FakeSnowflakeConnection:
        # every time we connect, create a new cursor (ie: connection) so we can isolate each connection's
        # schema setting see
        # https://github.com/duckdb/duckdb/blob/18254ec/tools/pythonpkg/src/pyconnection.cpp#L1440
        # and to make connections thread-safe see
        # https://duckdb.org/docs/api/python/overview.html#using-connections-in-parallel-python-programs
        with self._connect_lock:
            conn = fakes.FakeSnowflakeConnection(
                self.duck_conn.cursor(),
                self.results_cache,
                database,
                schema,
                create_database=self.create_database_on_connect,
                create_schema=self.create_schema_on_connect,
                db_path=self.db_path,
                nop_regexes=nop_regexes or self.nop_regexes,
                owners=self.owners,
                instance=self,
                **kwargs,
            )
            self._connections.add(conn)
            return conn

    def close(self) -> None:
        """Close the instance and every connection to it, releasing the database files they have attached."""
        with self._connect_lock:
            for conn in list(self._connections):
                if not conn.is_closed():
                    conn.close()
            self.duck_conn.close()
