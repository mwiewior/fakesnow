"""The SQL behind SHOW TASKS and DESCRIBE TASK."""

from __future__ import annotations

import snowflake.connector.errors
from duckdb import DuckDBPyConnection

from fakesnow import checks
from fakesnow.tasks.catalog import DATABASE_DOES_NOT_EXIST, Task, tasks_table
from fakesnow.tasks.statements import ShowTasks
from fakesnow.transforms.temporary import check_schema_exists

OWNER_ROLE_TYPE = "ROLE"
OVERLAP_TRUE = "true"  # allow_overlapping_execution is rendered as text
OVERLAP_FALSE = "false"
UNSET = "null"  # how an unset integration, or a child's allow_overlapping_execution, is rendered
NO_OVERLAP = "NO_OVERLAP"  # a root's overlap_policy, unless set or allow_overlapping_execution is true
ALLOW_CHILD_OVERLAP = "ALLOW_CHILD_OVERLAP"
SCHEDULING_MODE = "NULL"  # SQL for the scheduling_mode value
EXECUTE_AS_USER = "NULL"  # SQL for the execute_as_user value

_TERSE_COLUMNS = "created_on, name, NULL::VARCHAR AS kind, database_name, schema_name, schedule"


def _literal(value: str) -> str:
    return "'{}'".format(value.replace("'", "''"))


def rows_sql(database: str) -> str:
    """The SHOW TASKS columns of every task in a database."""
    table = tasks_table(database)
    qualified = f"{_literal(database + '.')} || t.schema_name || '.' ||"
    # compact in task_relations, and one name per line in predecessors
    predecessors = f"to_json(list_transform(t.predecessors, p -> {qualified} p))"
    listed = (
        "CASE WHEN cardinality(t.predecessors) = 0 THEN '[]' "
        f"ELSE '[' || chr(10) || array_to_string(list_transform(t.predecessors, p -> '  ' || to_json({qualified} p)), "
        "',' || chr(10)) || chr(10) || ']' END"
    )
    relations = (
        f"(select {qualified} min(f.name) from {table} f where f.schema_name = t.schema_name and f.finalize = t.name)"
    )
    finalized = f"{qualified} t.finalize"
    is_child = "(cardinality(t.predecessors) > 0 or t.finalize is not null)"
    return f"""
    select
        t.created_on,
        t.name,
        t.id,
        {_literal(database)} as database_name,
        t.schema_name,
        t.owner,
        coalesce(t.comment, '') as comment,
        t.warehouse,
        t.schedule,
        {listed} as predecessors,
        t.state,
        t.definition,
        t.condition,
        CASE WHEN {is_child} THEN '{UNSET}'
             WHEN coalesce(t.allow_overlapping_execution, false) THEN '{OVERLAP_TRUE}' ELSE '{OVERLAP_FALSE}'
        END as allow_overlapping_execution,
        coalesce(t.error_integration, '{UNSET}') as error_integration,
        t.last_committed_on,
        t.last_suspended_on,
        '{OWNER_ROLE_TYPE}' as owner_role_type,
        CASE WHEN {is_child} THEN NULL ELSE t.config END as config,
        CASE
            WHEN t.finalize is not null and {relations} is not null
                THEN json_object('FinalizedRootTask', {finalized}, 'Predecessors', {predecessors},
                                 'FinalizerTask', {relations})
            WHEN t.finalize is not null
                THEN json_object('FinalizedRootTask', {finalized}, 'Predecessors', {predecessors})
            WHEN {relations} is not null
                THEN json_object('Predecessors', {predecessors}, 'FinalizerTask', {relations})
            ELSE json_object('Predecessors', {predecessors})
        END::VARCHAR as task_relations,
        t.last_suspended_reason,
        coalesce(t.success_integration, '{UNSET}') as success_integration,
        {SCHEDULING_MODE}::VARCHAR as scheduling_mode,
        t.properties->>'$.TARGET_COMPLETION_INTERVAL' as target_completion_interval,
        {EXECUTE_AS_USER}::VARCHAR as execute_as_user,
        CASE WHEN {is_child} THEN NULL
             ELSE coalesce(t.overlap_policy,
                           CASE WHEN t.allow_overlapping_execution THEN '{ALLOW_CHILD_OVERLAP}' ELSE '{NO_OVERLAP}' END)
        END as overlap_policy,
        t.created_by_user
    from {table} t"""


def describe_task_sql(task: Task) -> str:
    """The row of SHOW TASKS for one task."""
    return f"""select * from ({rows_sql(task.database)}) where schema_name = {_literal(task.schema_name)}
        and name = {_literal(task.name)}"""


def show_tasks_sql(
    stmt: ShowTasks, duck_conn: DuckDBPyConnection, current_database: str | None, current_schema: str | None
) -> str:
    """The SELECT that produces the rows of a SHOW [TERSE] TASKS statement."""
    database: str | None = None
    schema: str | None = None
    # without IN, the current schema, as IN SCHEMA without a name
    scope = stmt.scope or "SCHEMA"
    parts = stmt.scope_name or []
    if scope == "SCHEMA" and parts:
        schema = parts[-1]
        database = parts[-2] if len(parts) > 1 else current_database
    elif scope == "SCHEMA":
        # without a name, the current schema, else every schema of the current database, else the account
        database = current_database
        schema = current_schema if database else None
    elif scope == "DATABASE":
        # without a name, the current database, else the account
        database = parts[0] if parts else current_database
    if parts and not database:
        raise checks.no_current_database_error("SHOW TASKS")

    with duck_conn.cursor() as conn:
        databases = [
            name
            for (name,) in conn.execute(
                "select database_name from duckdb_tables() "
                "where schema_name = '_fs_information_schema' and table_name = '_fs_tasks' order by database_name"
            ).fetchall()
            if database is None or name.lower() == database.lower()
        ]
    if database and not databases:
        raise snowflake.connector.errors.ProgrammingError(
            msg=DATABASE_DOES_NOT_EXIST.format(database=database),
            errno=2003,
            sqlstate="02000",
        )
    if schema:
        with duck_conn.cursor() as conn:
            check_schema_exists(conn, database, schema)

    filters = []
    if schema:
        filters.append(f"schema_name = {_literal(schema)}")
    if stmt.like is not None:
        filters.append(f"name ILIKE {_literal(stmt.like)}")
    if stmt.starts_with is not None:
        filters.append(f"starts_with(name, {_literal(stmt.starts_with)})")
    if stmt.root_only:
        # a finalizer has no predecessors, but isn't a root
        filters.append("predecessors = '[]' and task_relations not like '%\"FinalizedRootTask\"%'")
    if stmt.limit_from is not None:
        # the rows from the name on
        filters.append(f"name >= {_literal(stmt.limit_from)}")

    rows = " union all ".join(rows_sql(d) for d in databases)
    where = f" where {' and '.join(filters)}" if filters else ""
    limit = f" limit {stmt.limit}" if stmt.limit is not None else ""
    columns = _TERSE_COLUMNS if stmt.terse else "*"
    return f"select {columns} from ({rows}) {where} order by database_name, schema_name, name{limit}"
