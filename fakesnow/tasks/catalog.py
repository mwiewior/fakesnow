"""The task catalog, stored per database in ${catalog}._fs_information_schema._fs_tasks."""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass, field, replace
from datetime import datetime
from typing import Any

import snowflake.connector.errors
import sqlglot
from duckdb import DuckDBPyConnection
from sqlglot import exp
from sqlglot.errors import ParseError

from fakesnow import checks
from fakesnow.tasks.statements import AlterTask, CreateTask, DropTask, TaskName, parse_task_name
from fakesnow.transforms.temporary import check_schema_exists

CREATED = "Task {name} successfully created."
EXISTS = "{name} already exists, statement succeeded."
DROPPED = "{name} successfully dropped."
ALTERED = "Statement executed successfully."
DROP_MISSING = "Drop statement executed successfully ({name} already dropped)."
ALREADY_EXISTS = "SQL compilation error:\nObject '{name}' already exists."
DOES_NOT_EXIST = "SQL compilation error:\nTask '{fqname}' does not exist or not authorized."
DATABASE_DOES_NOT_EXIST = "SQL compilation error:\nDatabase '{database}' does not exist or not authorized."
INVALID_PROPERTY = "SQL compilation error:\ninvalid property '{prop}' for 'TASK'"
INVALID_ENABLE_ARGUMENT = "SQL compilation error:\ninvalid value [{argument}] for parameter 'RECURSIVE_TASK_ENABLE'"

STARTED_ROOT = "Unable to update graph with root task {root} since that root task is not suspended."
INVALID_PREDECESSOR = "Invalid predecessor {name} was specified."
SELF_PREDECESSOR = "Task {name} cannot use itself as its predecessor."
OTHER_SCHEMA_PREDECESSOR = "Cannot have predecessor {name} from a different schema."
SCHEDULED_CHILD = "Task {name} cannot have both a schedule and a predecessor."
NOT_A_PREDECESSOR = "Task {name} does not have predecessor {predecessor}."
CANNOT_RESUME = "Task {fqname} should have a SCHEDULE, AFTER, FINALIZE or WHEN to be resumed."
NEEDS_TARGET_COMPLETION_INTERVAL = (
    "TARGET_COMPLETION_INTERVAL must be set for serverless triggered task {fqname} to resume or execute."
)
ROOT_HAS_FINALIZER = (
    "This task has a finalizer and cannot be made a child task. Please remove the finalizer before adding a "
    "predecessor."
)
FINALIZER_HAS_PREDECESSORS = "Task {fqname} cannot have predecessors as a finalizer task."
FINALIZER_HAS_SCHEDULE = "Task {name} cannot have a schedule and be a finalizer task."
FINALIZES_NON_ROOT = "Task {fqname} cannot finalize a non-root task {root}."
INVALID_FINALIZED_ROOT = "Invalid finalized root task {name} was specified."
FINALIZER_EXISTS = "Finalize target {name} already have finalizer."
FINALIZE_OTHER_SCHEMA = "Cannot finalize graph {schema}.{name} in a different schema."
INVALID_CONDITION = (
    "Invalid expression for task condition expression. Only some data type conversions and the following functions "
    "are allowed: [SYSTEM$GET_PREDECESSOR_RETURN_VALUE, SYSTEM$PREDECESSORS_STATE, SYSTEM$STREAM_HAS_DATA, "
    "SYSTEM$TASK_COMPLETION_STATE]. Please see the documentation for more details."
)
# provisional, not seen on live Snowflake
INVALID_FINALIZER = "SQL compilation error:\nInvalid finalizer task {fqname}: {reason}."
FINALIZER_HAS_CHILDREN = "a finalizer cannot have child tasks"
LIMIT_EXCEEDED = "SQL compilation error:\nTask {fqname} exceeds the maximum of {limit} {what}."

MAX_PREDECESSORS = 100
MAX_CHILDREN = 100
MAX_GRAPH_TASKS = 1000

SUSPENDED = "suspended"
STARTED = "started"
CHILD_BECAME_ROOT = "CHILD_BECAME_ROOT"
FINALIZER_BECAME_ROOT = "FINALIZER_BECAME_ROOT"
USER_SUSPENDED = "USER_SUSPENDED"

# task properties with a column of their own, by property name
COLUMN_PROPERTIES = {
    "WAREHOUSE": "warehouse",
    "SCHEDULE": "schedule",
    "CONFIG": "config",
    "OVERLAP_POLICY": "overlap_policy",
    "ALLOW_OVERLAPPING_EXECUTION": "allow_overlapping_execution",
    "ERROR_INTEGRATION": "error_integration",
    "SUCCESS_INTEGRATION": "success_integration",
    "COMMENT": "comment",
    "FINALIZE": "finalize",
}
# task properties kept in the properties JSON column
OTHER_PROPERTIES = frozenset(
    {
        "USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE",
        "USER_TASK_TIMEOUT_MS",
        "SUSPEND_TASK_AFTER_NUM_FAILURES",
        "TASK_AUTO_RETRY_ATTEMPTS",
        "TARGET_COMPLETION_INTERVAL",
        "USER_TASK_MINIMUM_TRIGGER_INTERVAL_IN_SECONDS",
        "SERVERLESS_TASK_MIN_STATEMENT_SIZE",
        "SERVERLESS_TASK_MAX_STATEMENT_SIZE",
        "LOG_LEVEL",
        "TRACE_LEVEL",
    }
)
SESSION_PARAMETERS = frozenset(
    {
        "QUERY_TAG",
        "TIMEZONE",
        "AUTOCOMMIT",
        "QUOTED_IDENTIFIERS_IGNORE_CASE",
        "DATE_INPUT_FORMAT",
        "DATE_OUTPUT_FORMAT",
        "TIMESTAMP_INPUT_FORMAT",
        "TIMESTAMP_OUTPUT_FORMAT",
        "TIMESTAMP_TYPE_MAPPING",
        "STATEMENT_TIMEOUT_IN_SECONDS",
        "LOCK_TIMEOUT",
        "WEEK_START",
        "BINARY_INPUT_FORMAT",
        "JSON_INDENT",
        "ROWS_PER_RESULTSET",
    }
)

_COLUMNS = (
    "created_on, name, id, schema_name, owner, comment, warehouse, schedule, predecessors, state, definition, "
    "condition, finalize, config, error_integration, success_integration, overlap_policy, "
    "allow_overlapping_execution, properties, session_parameters, last_committed_on, last_suspended_on, "
    "last_suspended_reason, created_by_user"
)
# what a WHEN condition can be made of: constants, comparisons, logic, conversions and these functions
_CONDITION_NODES = (
    exp.Literal,
    exp.Boolean,
    exp.Null,
    exp.EQ,
    exp.NEQ,
    exp.GT,
    exp.GTE,
    exp.LT,
    exp.LTE,
    exp.Between,
    exp.And,
    exp.Or,
    exp.Not,
    exp.Paren,
    exp.Cast,
    exp.TryCast,
    exp.DataType,
    exp.DataTypeParam,
)
_CONDITION_FUNCTIONS = frozenset(
    {
        "SYSTEM$GET_PREDECESSOR_RETURN_VALUE",
        "SYSTEM$PREDECESSORS_STATE",
        "SYSTEM$STREAM_HAS_DATA",
        "SYSTEM$TASK_COMPLETION_STATE",
    }
)


def _error(msg: str, errno: int, sqlstate: str) -> snowflake.connector.errors.ProgrammingError:
    return snowflake.connector.errors.ProgrammingError(msg=msg, errno=errno, sqlstate=sqlstate)


@dataclass
class Task:
    database: str
    schema_name: str
    name: str
    created_on: datetime
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    owner: str = "PUBLIC"
    comment: str | None = None
    warehouse: str | None = None
    schedule: str | None = None
    predecessors: list[str] = field(default_factory=list)
    state: str = SUSPENDED
    definition: str | None = None
    condition: str | None = None
    finalize: str | None = None
    config: str | None = None
    error_integration: str | None = None
    success_integration: str | None = None
    overlap_policy: str | None = None
    allow_overlapping_execution: bool | None = None
    properties: dict[str, str] = field(default_factory=dict)
    session_parameters: dict[str, str] = field(default_factory=dict)
    last_committed_on: datetime | None = None
    last_suspended_on: datetime | None = None
    last_suspended_reason: str | None = None
    created_by_user: str | None = None

    @property
    def fqname(self) -> str:
        return f"{self.database}.{self.schema_name}.{self.name}"


class TaskCatalog:
    def __init__(
        self,
        duck_conn: DuckDBPyConnection,
        current_database: str | None,
        current_schema: str | None,
        role: str,
        command: str,
        user: str | None = None,
    ) -> None:
        self.duck_conn = duck_conn
        self.current_database = current_database
        self.current_schema = current_schema
        self.role = role
        self.command = command  # the statement being run, eg: CREATE TASK, for errors
        self.user = user

    def resolve(self, name: TaskName) -> tuple[str, str, str]:
        """Qualify a task name with the session's database and schema."""
        database = name.database or self.current_database
        if not database:
            raise checks.no_current_database_error(self.command)
        schema = name.schema or self.current_schema
        if not schema:
            raise checks.no_current_schema_error(self.command)
        return database, schema, name.name

    def get(self, database: str, schema: str, name: str) -> Task | None:
        # a separate cursor, so reads see what save committed without joining the caller's transaction
        with self.duck_conn.cursor() as conn:
            row = conn.execute(
                f"select {_COLUMNS} from {tasks_table(database)} where schema_name = ? and name = ?", [schema, name]
            ).fetchone()
        return _task(database, row) if row else None

    def require(self, name: TaskName) -> Task:
        database, schema, task_name = self.resolve(name)
        task = self.get(database, schema, task_name)
        if task is None:
            raise _error(DOES_NOT_EXIST.format(fqname=f"{database}.{schema}.{task_name}"), 2003, "02000")
        return task

    def root_of(self, task: Task) -> Task:
        """The root of the graph the task belongs to: the task itself if it has no predecessors or finalized root."""
        tasks = {t.name: t for t in self._schema_tasks(task.database, task.schema_name)}
        current = task
        seen = {task.name}
        while True:
            parent_name = current.finalize or (current.predecessors[0] if current.predecessors else None)
            parent = tasks.get(parent_name) if parent_name else None
            if parent is None or parent.name in seen:
                return current
            seen.add(parent.name)
            current = parent

    def children(self, task: Task) -> list[Task]:
        return [t for t in self._schema_tasks(task.database, task.schema_name) if task.name in t.predecessors]

    def finalizer_of(self, root: Task) -> Task | None:
        for t in self._schema_tasks(root.database, root.schema_name):
            if t.finalize == root.name and t.name != root.name:
                return t
        return None

    def graph(self, root: Task, proposed: Task | None = None) -> list[Task]:
        """The root, its descendants in the order they become ready to run, then its finalizer. A proposed task
        replaces the saved one of the same name. Descendants that never become ready, as they're in or after a cycle,
        come last by name."""
        ordered, cyclic = self._ordered(root, proposed)
        finalizer = self.finalizer_of(root)
        return ordered + cyclic + ([finalizer] if finalizer else [])

    def cycle_member(self, root: Task) -> str | None:
        """The first task by name in or after a cycle among the root's descendants, if any."""
        _, cyclic = self._ordered(root, None)
        return cyclic[0].name if cyclic else None

    def _ordered(self, root: Task, proposed: Task | None) -> tuple[list[Task], list[Task]]:
        """The root and its descendants in the order they become ready to run, and those that never do."""
        tasks = self._schema_tasks(root.database, root.schema_name)
        if proposed:
            tasks = [t for t in tasks if t.name != proposed.name] + [proposed]
        members = {root.name}
        pending = [t for t in tasks if t.predecessors]
        grew = True
        while grew:
            grew = False
            for t in pending:
                if t.name not in members and members.intersection(t.predecessors):
                    members.add(t.name)
                    grew = True
        # Kahn's algorithm, from the root as given, which may not be saved yet
        by_name = {t.name: t for t in tasks if t.name in members} | {root.name: root}
        waiting = {t.name: len(members.intersection(t.predecessors)) for t in by_name.values()}
        children: dict[str, list[str]] = {name: [] for name in by_name}
        for t in by_name.values():
            for pred in members.intersection(t.predecessors):
                children[pred].append(t.name)
        # in waves, as Snowflake runs a task once it's ready: the tasks a wave makes ready run after it, by name
        wave = [root.name]
        ordered: list[Task] = []
        while wave:
            ready: list[str] = []
            for name in sorted(wave):
                ordered.append(by_name[name])
                del waiting[name]
                # a child already ordered is in a cycle through the root, which the DDL accepts
                for child in (c for c in children[name] if c in waiting):
                    waiting[child] -= 1
                    if waiting[child] == 0:
                        ready.append(child)
            wave = ready
        return ordered, [by_name[name] for name in sorted(waiting)]

    def check_root_suspended(self, task: Task) -> None:
        """Raise if the root of the task's graph, which may be the task itself, is started."""
        root = self.root_of(task)
        if root.state == STARTED:
            raise _error(STARTED_ROOT.format(root=root.fqname), 91421, "22000")

    def create(self, stmt: CreateTask) -> str:
        database, schema, name = self.resolve(stmt.name)
        with self.duck_conn.cursor() as conn:
            if not conn.execute(
                "select 1 from information_schema.schemata where upper(catalog_name) = upper(?)", [database]
            ).fetchone():
                raise _error(DATABASE_DOES_NOT_EXIST.format(database=database), 2003, "02000")
            check_schema_exists(conn, database, schema)
        task = self._new(database, schema, name, stmt)
        existing = self.get(database, schema, name)
        if existing and not (stmt.replace or stmt.or_alter):
            if stmt.if_not_exists:
                return status_sql(EXISTS, name)
            raise _error(ALREADY_EXISTS.format(name=existing.name), 2002, "42710")
        if existing and not stmt.replace:
            # altering keeps the identity, state and history, and resets what the statement leaves out
            task = replace(
                task,
                id=existing.id,
                created_on=existing.created_on,
                state=existing.state,
                last_committed_on=existing.last_committed_on,
                last_suspended_on=existing.last_suspended_on,
                last_suspended_reason=existing.last_suspended_reason,
            )
        after = (
            [self.resolve(a) for a in stmt.after]
            if not stmt.clone_of
            else [(database, schema, p) for p in task.predecessors]
        )
        # a started root or standalone task can be replaced, or altered, but not a task in a started graph
        replacing = bool(
            (stmt.replace or stmt.or_alter)
            and existing
            and not (existing.predecessors or existing.finalize or task.predecessors or task.finalize)
        )
        self._store(task, existing, after, replacing=replacing)
        return status_sql(ALTERED if existing and stmt.or_alter else CREATED, name)

    def alter(self, stmt: AlterTask) -> str:
        database, schema, name = self.resolve(stmt.name)
        task = self.get(database, schema, name)
        if task is None:
            if stmt.if_exists:
                return status_sql(ALTERED, name)
            raise _error(DOES_NOT_EXIST.format(fqname=f"{database}.{schema}.{name}"), 2003, "02000")
        if stmt.action in ("RESUME", "SUSPEND"):
            # a root can always be resumed or suspended, a child only while its root is suspended
            if self.root_of(task).name != task.name:
                self.check_root_suspended(task)
            if stmt.action == "RESUME":
                self.save(_resumed(_check_resumable(task)))
            elif task.state != SUSPENDED:
                # suspending a suspended task changes nothing
                self.save(_suspended(task, USER_SUSPENDED))
            return status_sql(ALTERED, name)

        new = replace(task, predecessors=list(task.predecessors), properties=dict(task.properties))
        new.session_parameters = dict(task.session_parameters)
        after = [(database, schema, p) for p in task.predecessors]
        if stmt.action == "ADD_AFTER":
            # keep the resolved names, so a predecessor in another schema is rejected as it is by CREATE
            for pred in (self.resolve(p) for p in stmt.tasks):
                if pred not in after:
                    after.append(pred)
                    new.predecessors.append(pred[2])
        elif stmt.action == "REMOVE_AFTER":
            removed = [self.resolve(r) for r in stmt.tasks]
            for pred in removed:
                if pred[:2] != (database, schema) or pred[2] not in task.predecessors:
                    raise _error(NOT_A_PREDECESSOR.format(name=name, predecessor=pred[2]), 91417, "22000")
            new.predecessors = [p for p in new.predecessors if p not in {r[2] for r in removed}]
            if task.predecessors and not new.predecessors:
                new = _suspended(new, CHILD_BECAME_ROOT)
        elif stmt.action == "SET":
            for prop, value in stmt.properties.items():
                self._set_property(new, prop, value)
        elif stmt.action == "UNSET":
            for prop in stmt.unset:
                _unset_property(new, prop)
            if task.finalize and not new.finalize:
                new = _suspended(new, FINALIZER_BECAME_ROOT)
        elif stmt.action == "MODIFY_AS":
            new.definition = stmt.text
        elif stmt.action == "MODIFY_WHEN":
            new.condition = _check_condition(stmt.text)
        elif stmt.action == "REMOVE_WHEN":
            new.condition = None
        if stmt.action != "ADD_AFTER":
            after = [(database, schema, p) for p in new.predecessors]
        self._store(new, task, after)
        return status_sql(ALTERED, name)

    def enable_dependents(self, name: TaskName, argument: str) -> None:
        """Resume the task and all its dependents. The dependents are resumed first, so they stay resumed when the
        task itself can't be."""
        task = self.get(*self.resolve(name))
        if task is None:
            raise _error(INVALID_ENABLE_ARGUMENT.format(argument=argument), 1008, "22023")
        if self.root_of(task).name != task.name:
            self.check_root_suspended(task)
        for member in self.graph(task):
            if member.name != task.name:
                self.save(_resumed(member))
        self.save(_resumed(_check_resumable(task)))

    def is_standalone(self, task: Task) -> bool:
        """Whether the task has no predecessors, children or finalizer."""
        return not (task.predecessors or task.finalize or self.children(task) or self.finalizer_of(task))

    def _store(
        self, task: Task, existing: Task | None, after: list[tuple[str, str, str]], replacing: bool = False
    ) -> None:
        """Check the graph rules and save a new or changed task. A task already in a started graph can't change,
        nor can one join a started graph, except that a started root or standalone task can be replaced."""
        if existing and not replacing:
            self.check_root_suspended(existing)
        self._check_graph(task, after, replacing)
        self.save(task)

    def _check_graph(self, task: Task, after: list[tuple[str, str, str]], replacing: bool = False) -> None:
        """Check the rules for a task about to be saved, in the order Snowflake reports them."""
        preds: list[Task] = []
        for pred_db, pred_schema, pred_name in after:
            pred = self.get(pred_db, pred_schema, pred_name)
            if pred is None:
                raise _error(INVALID_PREDECESSOR.format(name=pred_name), 91085, "42601")
            if (pred.database, pred.schema_name, pred.name) == (task.database, task.schema_name, task.name):
                raise _error(SELF_PREDECESSOR.format(name=task.name), 91402, "22000")
            if (pred.database, pred.schema_name) != (task.database, task.schema_name):
                raise _error(OTHER_SCHEMA_PREDECESSOR.format(name=pred.name), 91413, "42601")
            if pred.finalize:
                raise _error(INVALID_FINALIZER.format(fqname=pred.fqname, reason=FINALIZER_HAS_CHILDREN), 2003, "02000")
            preds.append(pred)
        finalized = self.get(task.database, task.schema_name, task.finalize) if task.finalize else None
        if task.finalize and (finalized is None or task.finalize == task.name):
            raise _error(INVALID_FINALIZED_ROOT.format(name=task.finalize), 91471, "42601")
        # joining a started graph is rejected, as is changing a started task, unless it's replaced or altered
        for member in [*preds, finalized] if finalized else preds or ([] if replacing else [task]):
            self.check_root_suspended(member)
        if task.predecessors and self.finalizer_of(task):
            raise _error(ROOT_HAS_FINALIZER, 91856, "55000")
        if task.finalize and task.predecessors:
            raise _error(FINALIZER_HAS_PREDECESSORS.format(fqname=task.fqname), 91468, "42601")
        if task.finalize and task.schedule:
            raise _error(FINALIZER_HAS_SCHEDULE.format(name=task.name), 91470, "42601")
        if task.predecessors and task.schedule:
            raise _error(SCHEDULED_CHILD.format(name=task.name), 91400, "42601")
        if finalized:
            existing_finalizer = self.finalizer_of(finalized)
            if finalized.predecessors:
                raise _error(FINALIZES_NON_ROOT.format(fqname=task.fqname, root=finalized.fqname), 91497, "42601")
            if self.children(task):
                raise _error(INVALID_FINALIZER.format(fqname=task.fqname, reason=FINALIZER_HAS_CHILDREN), 2003, "02000")
            if existing_finalizer and existing_finalizer.name != task.name:
                raise _error(FINALIZER_EXISTS.format(name=finalized.name), 91472, "42601")
        # predecessors in different graphs, or a cycle, are accepted here, and rejected by EXECUTE TASK
        self._check_limits(task)

    def _check_limits(self, task: Task) -> None:
        limits = [(len(task.predecessors), MAX_PREDECESSORS, "predecessors", task)]
        for pred_name in task.predecessors:
            pred = self.get(task.database, task.schema_name, pred_name)
            assert pred
            siblings = {c.name for c in self.children(pred)} | {task.name}
            limits.append((len(siblings), MAX_CHILDREN, "child tasks", pred))
        graph = self.graph(self.root_of(task), proposed=task)
        limits.append((len({t.name for t in graph} | {task.name}), MAX_GRAPH_TASKS, "tasks in a graph", task))
        for count, limit, what, subject in limits:
            if count > limit:
                raise _error(LIMIT_EXCEEDED.format(fqname=subject.fqname, limit=limit, what=what), 2003, "02000")

    def drop(self, stmt: DropTask) -> str:
        database, schema, name = self.resolve(stmt.name)
        task = self.get(database, schema, name)
        if task is None:
            if stmt.if_exists:
                return status_sql(DROP_MISSING, name)
            raise _error(DOES_NOT_EXIST.format(fqname=f"{database}.{schema}.{name}"), 2003, "02000")
        if not self.is_standalone(task):
            self.check_root_suspended(task)
        # dependents of the dropped task become standalone
        for child in self.children(task):
            child.predecessors.remove(name)
            self.save(child if child.predecessors else _suspended(child, CHILD_BECAME_ROOT))
        finalizer = self.finalizer_of(task)
        if finalizer:
            self.save(_suspended(replace(finalizer, finalize=None), FINALIZER_BECAME_ROOT))
        self.delete(database, schema, name)
        return status_sql(DROPPED, name)

    def delete(self, database: str, schema: str, name: str) -> None:
        with self.duck_conn.cursor() as conn:
            conn.execute(f"delete from {tasks_table(database)} where schema_name = ? and name = ?", [schema, name])

    def delete_schema(self, database: str, schema: str) -> None:
        """Delete every task in a schema, and its run history."""
        with self.duck_conn.cursor() as conn:
            for table in (tasks_table(database), history_table(database), graph_runs_table(database)):
                conn.execute(f"delete from {table} where schema_name = ?", [schema])

    def mark_committed(self, task: Task) -> None:
        """Record the first time the task is run, keeping any change made since it was read, eg: by its body."""
        current = self.get(task.database, task.schema_name, task.name)
        if current and current.last_committed_on is None:
            self.save(replace(current, last_committed_on=now()))

    def save(self, task: Task) -> None:
        """Insert or replace the task. Writes go through a separate cursor, so they neither commit nor join the
        caller's transaction."""
        values: list[Any] = [
            task.created_on,
            task.name,
            task.id,
            task.schema_name,
            task.owner,
            task.comment,
            task.warehouse,
            task.schedule,
            task.predecessors,
            task.state,
            task.definition,
            task.condition,
            task.finalize,
            task.config,
            task.error_integration,
            task.success_integration,
            task.overlap_policy,
            task.allow_overlapping_execution,
            json.dumps(task.properties),
            json.dumps(task.session_parameters),
            task.last_committed_on,
            task.last_suspended_on,
            task.last_suspended_reason,
            task.created_by_user,
        ]
        with self.duck_conn.cursor() as conn:
            conn.execute(
                f"insert or replace into {tasks_table(task.database)} ({_COLUMNS}) "
                f"values ({', '.join('?' * len(values))})",
                values,
            )

    def _schema_tasks(self, database: str, schema: str) -> list[Task]:
        with self.duck_conn.cursor() as conn:
            rows = conn.execute(
                f"select {_COLUMNS} from {tasks_table(database)} where schema_name = ? order by created_on, name",
                [schema],
            ).fetchall()
        return [_task(database, row) for row in rows]

    def _new(self, database: str, schema: str, name: str, stmt: CreateTask) -> Task:
        if stmt.clone_of:
            source = self.require(stmt.clone_of)
            # everything but the identity, state and run history is copied. a clone by another role than the owner
            # can't have a predecessor.
            return replace(
                source,
                database=database,
                schema_name=schema,
                name=name,
                id=str(uuid.uuid4()),
                created_on=now(),
                owner=self.role,
                created_by_user=self.user,
                predecessors=source.predecessors if self.role == source.owner else [],
                state=SUSPENDED,
                last_committed_on=None,
                last_suspended_on=None,
                last_suspended_reason=None,
            )

        task = Task(
            database=database,
            schema_name=schema,
            name=name,
            created_on=now(),
            owner=self.role,
            created_by_user=self.user,
            predecessors=[after.name for after in stmt.after],
            condition=_check_condition(stmt.condition),
            definition=stmt.definition,
        )
        for prop, value in stmt.properties.items():
            self._set_property(task, prop, value)
        return task

    def _set_property(self, task: Task, prop: str, value: str) -> None:
        if prop in COLUMN_PROPERTIES:
            column = COLUMN_PROPERTIES[prop]
            if column == "allow_overlapping_execution":
                task.allow_overlapping_execution = value.upper() == "TRUE"
            elif column == "finalize":
                database, schema, root = self.resolve(parse_task_name(value))
                if (database, schema) != (task.database, task.schema_name):
                    raise _error(FINALIZE_OTHER_SCHEMA.format(schema=schema, name=root), 91479, "42601")
                task.finalize = root
            else:
                setattr(task, column, value)
        elif prop in OTHER_PROPERTIES:
            task.properties[prop] = value
        elif prop in SESSION_PARAMETERS and not (prop == "AUTOCOMMIT" and value.upper() == "FALSE"):
            task.session_parameters[prop] = value
        else:
            raise _error(INVALID_PROPERTY.format(prop=prop.lower()), 1420, "22023")


def _unset_property(task: Task, prop: str) -> None:
    if prop in COLUMN_PROPERTIES:
        setattr(task, COLUMN_PROPERTIES[prop], None)
    elif prop in OTHER_PROPERTIES:
        task.properties.pop(prop, None)
    elif prop in SESSION_PARAMETERS:
        task.session_parameters.pop(prop, None)
    else:
        raise _error(INVALID_PROPERTY.format(prop=prop.lower()), 1420, "22023")


def _check_resumable(task: Task) -> Task:
    """The task, if something can trigger it once it's resumed."""
    if not (task.schedule or task.predecessors or task.finalize or task.condition):
        raise _error(CANNOT_RESUME.format(fqname=task.fqname), 91453, "22000")
    return check_runnable(task)


def check_runnable(task: Task) -> Task:
    """The task, unless it's a serverless root triggered only by its WHEN, which needs a target completion interval
    to be resumed or executed."""
    triggered = task.condition and not (task.schedule or task.predecessors or task.finalize)
    if triggered and not task.warehouse and "TARGET_COMPLETION_INTERVAL" not in task.properties:
        raise _error(NEEDS_TARGET_COMPLETION_INTERVAL.format(fqname=task.fqname), 91865, "55000")
    return task


def _check_condition(condition: str | None) -> str | None:
    """The WHEN condition, if it's made only of what Snowflake allows in one."""
    if condition is None:
        return None
    try:
        parsed = sqlglot.parse_one(condition, read="snowflake")
    except ParseError:
        parsed = None
    for node in parsed.walk() if parsed else [None]:
        if isinstance(node, exp.Anonymous) and node.name.upper() in _CONDITION_FUNCTIONS:
            continue
        if not isinstance(node, _CONDITION_NODES) and not (
            isinstance(node, exp.Literal | exp.Identifier) and isinstance(node.parent, exp.Anonymous)
        ):
            raise _error(INVALID_CONDITION, 91464, "42601")
    return condition


def _resumed(task: Task) -> Task:
    return replace(task, state=STARTED, last_committed_on=task.last_committed_on or now())


def _suspended(task: Task, reason: str) -> Task:
    return replace(task, state=SUSPENDED, last_suspended_on=now(), last_suspended_reason=reason)


def now() -> datetime:
    return datetime.now().astimezone()


def tasks_table(database: str) -> str:
    return _table(database, "_fs_tasks")


def history_table(database: str) -> str:
    """One row per task run, the source of TASK_HISTORY."""
    return _table(database, "_fs_task_history")


def graph_runs_table(database: str) -> str:
    """One row per graph run, the source of COMPLETE_TASK_GRAPHS."""
    return _table(database, "_fs_task_graph_runs")


def _table(database: str, name: str) -> str:
    return '"{}"._fs_information_schema.{}'.format(database.replace('"', '""'), name)


def status_sql(template: str, name: str) -> str:
    return "SELECT '{}' AS status".format(template.format(name=name).replace("'", "''"))


def _task(database: str, row: tuple) -> Task:
    (created_on, name, id_, schema, owner, comment, warehouse, schedule, predecessors, state, definition) = row[:11]
    (condition, finalize, config, error_integration, success_integration, overlap_policy) = row[11:17]
    (allow_overlap, properties, session_parameters, committed, suspended, reason, created_by) = row[17:24]
    return Task(
        database=database,
        schema_name=schema,
        name=name,
        created_on=created_on,
        id=id_,
        owner=owner,
        comment=comment,
        warehouse=warehouse,
        schedule=schedule,
        predecessors=list(predecessors or []),
        state=state,
        definition=definition,
        condition=condition,
        finalize=finalize,
        config=config,
        error_integration=error_integration,
        success_integration=success_integration,
        overlap_policy=overlap_policy,
        allow_overlapping_execution=allow_overlap,
        properties=json.loads(properties) if properties else {},
        session_parameters=json.loads(session_parameters) if session_parameters else {},
        last_committed_on=committed,
        last_suspended_on=suspended,
        last_suspended_reason=reason,
        created_by_user=created_by,
    )
