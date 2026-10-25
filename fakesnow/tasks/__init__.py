"""Snowflake tasks: the statements that manage them and the catalog that stores them."""

from __future__ import annotations

import contextlib
from typing import TYPE_CHECKING

import duckdb
from duckdb import DuckDBPyConnection
from sqlglot import Expr, exp

from fakesnow.tasks.catalog import TaskCatalog, status_sql
from fakesnow.tasks.functions import task_functions
from fakesnow.tasks.history import task_table_functions
from fakesnow.tasks.runner import EXECUTED, run_graph
from fakesnow.tasks.show import describe_task_sql, show_tasks_sql
from fakesnow.tasks.statements import (
    AlterTask,
    CreateTask,
    DescribeTask,
    DropTask,
    EnableDependents,
    ExecuteTask,
    ShowTasks,
    TaskStatement,
    TaskSyntaxError,
    parse_task_name,
    parse_task_statement,
)
from fakesnow.transforms.transforms import SUCCESS_NOP

if TYPE_CHECKING:
    from fakesnow.conn import FakeSnowflakeConnection


_COMMANDS: dict[type, str] = {
    CreateTask: "CREATE TASK",
    AlterTask: "ALTER TASK",
    DropTask: "DROP TASK",
    ExecuteTask: "EXECUTE TASK",
    EnableDependents: "SYSTEM$TASK_DEPENDENTS_ENABLE",
    ShowTasks: "SHOW TASKS",
    DescribeTask: "DESCRIBE TASK",
}
_ENABLE_DEPENDENTS = "SYSTEM$TASK_DEPENDENTS_ENABLE"


__all__ = [
    "commits_transaction",
    "drop_schema",
    "execute",
    "keeps_variables",
    "task_functions",
    "task_statement",
    "task_table_functions",
]


def commits_transaction(expression: Expr) -> bool:
    """Whether the task statement commits the session's active transaction: ALTER and DROP TASK do, CREATE and
    EXECUTE TASK don't."""
    return isinstance(expression.args.get("task_statement"), AlterTask | DropTask)


def keeps_variables(sql: str) -> bool:
    """Whether the statement defines a task body or condition, which keeps its session variables as written, to be
    resolved when the task runs."""
    try:
        statement = parse_task_statement(sql)
    except TaskSyntaxError:
        return False
    if isinstance(statement, CreateTask):
        return True
    return isinstance(statement, AlterTask) and statement.action in ("MODIFY_AS", "MODIFY_WHEN")


def task_statement(expression: Expr) -> Expr:
    """Replace a task statement, which sqlglot parses as an opaque command, with a placeholder carrying it."""
    statement = None
    if isinstance(expression, exp.Command):
        statement = parse_task_statement(expression.sql("snowflake"))
    elif isinstance(expression, exp.Select):
        statement = _enable_dependents(expression)
    elif isinstance(expression, exp.Anonymous) and expression.name.upper() == _ENABLE_DEPENDENTS:
        # a use other than SELECT SYSTEM$TASK_DEPENDENTS_ENABLE('<name>'), which is replaced before it is reached
        raise NotImplementedError(f"{_ENABLE_DEPENDENTS} is only supported as the sole column of a SELECT")
    if statement:
        placeholder = SUCCESS_NOP.copy()
        placeholder.args["task_statement"] = statement
        return placeholder
    return expression


def _enable_dependents(select: exp.Select) -> EnableDependents | None:
    """The statement if the SELECT is only a SYSTEM$TASK_DEPENDENTS_ENABLE call with a string literal argument."""
    if {k for k, v in select.args.items() if v} != {"expressions"} or len(select.expressions) != 1:
        return None
    call = select.expressions[0]
    if not (isinstance(call, exp.Anonymous) and call.name.upper() == _ENABLE_DEPENDENTS):
        return None
    if len(call.expressions) != 1 or not (isinstance(arg := call.expressions[0], exp.Literal) and arg.is_string):
        return None
    return EnableDependents(parse_task_name(arg.name), arg.name)


def execute(statement: TaskStatement, conn: FakeSnowflakeConnection) -> str:
    """Run a task statement, returning the SELECT that produces its result rows."""
    catalog = TaskCatalog(
        conn._duck_conn,  # noqa: SLF001
        conn.database if conn.database_set else None,
        conn.schema if conn.schema_set else None,
        conn.role,
        _COMMANDS[type(statement)],
        conn.user,
    )
    if isinstance(statement, CreateTask):
        return catalog.create(statement)
    if isinstance(statement, AlterTask):
        return catalog.alter(statement)
    if isinstance(statement, DropTask):
        return catalog.drop(statement)
    if isinstance(statement, EnableDependents):
        catalog.enable_dependents(statement.name, statement.argument)
        # named as Snowflake names the column of the call
        column = f"{_ENABLE_DEPENDENTS}('{statement.argument}')".upper().replace('"', '""')
        return f"SELECT 'OK' AS \"{column}\""
    if isinstance(statement, ExecuteTask):
        root = catalog.require(statement.name)
        run_graph(conn, catalog, root, statement.config)
        return status_sql(EXECUTED, root.name)
    if isinstance(statement, ShowTasks):
        return show_tasks_sql(statement, conn._duck_conn, catalog.current_database, catalog.current_schema)  # noqa: SLF001
    if isinstance(statement, DescribeTask):
        return describe_task_sql(catalog.require(statement.name))
    raise NotImplementedError(type(statement).__name__)


def drop_schema(duck_conn: DuckDBPyConnection, database: str, schema: str) -> None:
    """Delete the tasks of a dropped schema."""
    # a database that doesn't exist has no tasks
    with contextlib.suppress(duckdb.CatalogException):
        TaskCatalog(duck_conn, database, schema, "", "DROP SCHEMA").delete_schema(database, schema)
