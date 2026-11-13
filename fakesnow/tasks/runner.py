"""Run a task graph synchronously, as EXECUTE TASK does, and record its history."""

from __future__ import annotations

import json
import uuid
from datetime import datetime
from typing import TYPE_CHECKING, Any

import snowflake.connector.errors
from duckdb import DuckDBPyConnection

from fakesnow.tasks.catalog import (
    SUSPENDED,
    Task,
    TaskCatalog,
    check_runnable,
    graph_runs_table,
    history_table,
    now,
)
from fakesnow.tasks.functions import TaskRunContext

if TYPE_CHECKING:
    from fakesnow.conn import FakeSnowflakeConnection

EXECUTED = "Task {name} is scheduled to run immediately."
NOT_ROOT = (
    "Execute task cannot be called on non-root task {fqname}. Call EXECUTE TASK on the root task of its graph instead."
)
CYCLE = "Graph has at least one cycle containing task {name}."
UNREACHABLE = "Unreachable task {fqname} found. The graph has more than one root task (one without predecessors)."

SCHEDULED_FROM = "EXECUTE TASK"
ATTEMPT_NUMBER = 1

SUCCEEDED = "SUCCEEDED"
FAILED = "FAILED"
SKIPPED = "SKIPPED"
_NOT_RUN = "NOT_RUN"  # a predecessor failed or was skipped, so there is no history row
_SUSPENDED = "SUSPENDED"  # a suspended child, which has no history row either, and its dependents ignore


def run_graph(conn: FakeSnowflakeConnection, catalog: TaskCatalog, root: Task, config_override: str | None) -> None:
    """Run the graph of a standalone or root task, finalizer included, returning when it has finished. A failing
    task doesn't raise, its failure is recorded in the task history."""
    if catalog.root_of(root).name != root.name:
        raise snowflake.connector.errors.ProgrammingError(
            msg=NOT_ROOT.format(fqname=root.fqname), errno=91435, sqlstate="22000"
        )
    check_runnable(root)
    graph = catalog.graph(root)
    members = {t.name for t in graph}
    for task in graph:
        # a task with a predecessor outside the graph, eg: after REMOVE AFTER split a join, makes it unrunnable
        if unreachable := next((p for p in task.predecessors if p not in members), None):
            fqname = f"{root.database}.{root.schema_name}.{unreachable}"
            raise snowflake.connector.errors.ProgrammingError(
                msg=UNREACHABLE.format(fqname=fqname), errno=91419, sqlstate="22000"
            )
    if cyclic := catalog.cycle_member(root):
        raise snowflake.connector.errors.ProgrammingError(msg=CYCLE.format(name=cyclic), errno=91445, sqlstate="22000")
    # the run doesn't commit the caller's transaction, nor see its uncommitted writes, as it runs in sessions of its
    # own
    _GraphRun(conn, catalog, root, _merged_config(root.config, config_override)).run(graph)


def _merged_config(default: str | None, override: str | None) -> str | None:
    """The config of a run: the USING CONFIG override merged into the root's config, object by object, with the
    override's values winning. An override that can't be merged replaces the config."""
    if default is None or override is None:
        return default if override is None else override
    try:
        return json.dumps(_merge(json.loads(default), json.loads(override)), separators=(",", ":"))
    except ValueError:
        return override


def _merge(default: object, override: object) -> object:
    if isinstance(default, dict) and isinstance(override, dict):
        return default | {key: _merge(default.get(key), value) for key, value in override.items()}
    return override


class _GraphRun:
    def __init__(self, conn: FakeSnowflakeConnection, catalog: TaskCatalog, root: Task, config: str | None) -> None:
        self.conn = conn
        self.catalog = catalog
        self.root = root
        self.config = config
        self.scheduled_time = now()
        self.run_id = int(self.scheduled_time.timestamp() * 1000)
        self.graph_run_group_id = str(uuid.uuid4())
        self.states: dict[str, str] = {}
        self.return_values: dict[str, str | None] = {}
        self.root_query_start_time: datetime | None = None
        self.first_error: tuple[str, int, str] | None = None

    def run(self, graph: list[Task]) -> None:
        self.graph = graph
        for task in self.graph:
            if task.finalize:
                # the finalizer comes last, and runs whatever happened to the rest unless the root was skipped
                if self.states[self.root.name] != SKIPPED and task.state != SUSPENDED:
                    self._run_task(task)
            elif task.name == self.root.name:
                self._run_task(task)
            elif task.state == SUSPENDED:
                # a suspended child is skipped without a history row
                self.states[task.name] = _SUSPENDED
            else:
                # a task runs once all its resumed predecessors succeeded, and needs at least one of them: the
                # suspended ones are ignored, so a task after only suspended ones doesn't run
                resumed = [p for p in task.predecessors if self._state_of(p) != _SUSPENDED]
                if resumed and all(self._state_of(p) == SUCCEEDED for p in resumed):
                    self._run_task(task)
                else:
                    self.states[task.name] = _NOT_RUN
        self._record_graph_run()
        self.catalog.mark_committed(self.root)

    def _state_of(self, predecessor: str) -> str:
        """A predecessor's state in this run."""
        return self.states.get(predecessor, _NOT_RUN)

    def _ancestors(self, task: Task) -> set[str]:
        tasks = {t.name: t for t in self.graph}
        ancestors: set[str] = set()
        pending = list(task.predecessors)
        while pending:
            name = pending.pop()
            if name not in ancestors:
                ancestors.add(name)
                pending.extend(tasks[name].predecessors if name in tasks else [])
        return ancestors

    def _run_task(self, task: Task) -> None:
        scheduled_time = self.scheduled_time if task.name == self.root.name else now()
        context = TaskRunContext(
            task=task,
            root=self.root,
            graph_run_group_id=self.graph_run_group_id,
            run_id=self.run_id,
            config=self.config,
            # the enabled predecessors', a suspended one has none
            predecessor_return_values={
                p: self.return_values.get(p) for p in task.predecessors if self._state_of(p) != _SUSPENDED
            },
            ancestor_return_values={
                a: self.return_values.get(a) for a in self._ancestors(task) if self._state_of(a) != _SUSPENDED
            },
        )
        session = self.conn.open_session(
            task.database, task.schema_name, task.owner, task.warehouse, task.session_parameters
        )
        query_id = error_code = error_message = query_start_time = None
        try:
            session.task_context = context
            cur = session.cursor()
            if task.condition and not _holds(cur.execute(f"SELECT ({task.condition})").fetchone()):
                state = SKIPPED
            else:
                assert task.definition is not None
                query_start_time = now()
                query_id = cur.execute(task.definition).sfqid
                state = SUCCEEDED
        except snowflake.connector.errors.ProgrammingError as e:
            # the message without the code and sqlstate the connector prefixes it with
            state, error_code, error_message = FAILED, e.errno, e.raw_msg
        except Exception as e:
            state, error_code, error_message = FAILED, 0, str(e)
        finally:
            session.close()
        if state == FAILED and query_start_time and query_id is None:
            # the body failed, but it ran, so it has a query id like any other query
            query_id = str(uuid.uuid4())

        self.states[task.name] = state
        self.return_values[task.name] = context.return_value
        if task.name == self.root.name:
            self.root_query_start_time = query_start_time
        if state == FAILED and self.first_error is None:
            self.first_error = (task.name, error_code or 0, error_message or "")
        _insert(
            self.catalog.duck_conn,
            history_table(task.database),
            {
                "query_id": query_id,
                "name": task.name,
                "schema_name": task.schema_name,
                "query_text": task.definition,
                "condition_text": task.condition,
                "state": state,
                "error_code": error_code,
                "error_message": error_message,
                "scheduled_time": scheduled_time,
                "query_start_time": query_start_time,
                "completed_time": now(),
                "root_task_id": self.root.id,
                "run_id": self.run_id,
                "return_value": context.return_value,
                "scheduled_from": SCHEDULED_FROM,
                "attempt_number": ATTEMPT_NUMBER,
                "config": self.config,
                "graph_run_group_id": self.graph_run_group_id,
                "scheduled_by_user": self.conn.user,
            },
        )

    def _record_graph_run(self) -> None:
        state = SUCCEEDED
        if self.states[self.root.name] == SKIPPED:
            state = SKIPPED
        elif self.first_error:
            state = FAILED
        error_task_name, error_code, error_message = self.first_error or (None, None, None)
        _insert(
            self.catalog.duck_conn,
            graph_runs_table(self.root.database),
            {
                "root_task_name": self.root.name,
                "schema_name": self.root.schema_name,
                "state": state,
                "scheduled_from": SCHEDULED_FROM,
                "first_error_task_name": error_task_name,
                "first_error_code": error_code,
                "first_error_message": error_message,
                "scheduled_time": self.scheduled_time,
                "query_start_time": self.root_query_start_time,
                "completed_time": now(),
                "root_task_id": self.root.id,
                "run_id": self.run_id,
                "attempt_number": ATTEMPT_NUMBER,
                "config": self.config,
                "graph_run_group_id": self.graph_run_group_id,
                "scheduled_by_user": self.conn.user,
            },
        )


def _holds(row: tuple | dict | None) -> bool:
    """Whether a WHEN condition's result is true: false and NULL skip the task."""
    return isinstance(row, tuple) and bool(row[0])


def _insert(duck_conn: DuckDBPyConnection, table: str, row: dict[str, Any]) -> None:
    """Insert a history row through a separate cursor, so a rollback by the caller can't erase it."""
    with duck_conn.cursor() as conn:
        conn.execute(f"insert into {table} ({', '.join(row)}) values ({', '.join('?' * len(row))})", list(row.values()))
