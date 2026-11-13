import re
import tempfile
from datetime import datetime, timedelta

import pytest
import snowflake.connector
import snowflake.connector.cursor

import fakesnow
from fakesnow.conn import FakeSnowflakeConnection
from fakesnow.instance import FakeSnow
from tests.utils import dindent, indent


# stored session parameters have no live-visible equivalent (SHOW PARAMETERS IN TASK isn't implemented)
def _select(cur: snowflake.connector.cursor.SnowflakeCursor, columns: str, name: str | None = None) -> list[tuple]:
    where = f" where name = '{name.upper()}'" if name else ""
    cur.execute(f"select {columns} from db1._fs_information_schema._fs_tasks{where}")
    return cur.fetchall()


def _show(cur: snowflake.connector.cursor.SnowflakeCursor, columns: str, name: str | None = None) -> list[tuple]:
    """The columns of SHOW TASKS, for the named task or all tasks. A quoted name is matched as written, otherwise
    it is upper-cased."""
    cur.execute("show tasks")
    names = [d.name for d in cur.description]
    wanted = [c.strip() for c in columns.split(",")]
    return [
        tuple(row[names.index(c)] for c in wanted)
        for row in cur.fetchall()
        if name is None or row[names.index("name")] == (name[1:-1] if name.startswith('"') else name.upper())
    ]


def test_create_task_status(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task t1 as select 1")
    assert cur.fetchall() == [("Task T1 successfully created.",)]


def test_create_task_if_not_exists_existing(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task t1 as select 1")
    cur.execute("create task if not exists t1 as select 1")
    assert cur.fetchall() == [("T1 already exists, statement succeeded.",)]


def test_create_task_existing_raises(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task t1 as select 1")
    with pytest.raises(snowflake.connector.errors.ProgrammingError, match=r"Object 'T1' already exists\.") as e:
        cur.execute("create task t1 as select 1")
    assert (e.value.errno, e.value.sqlstate) == (2002, "42710")


def test_create_or_replace_task_gets_new_id(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task t1 as select 1")
    (before,) = _show(cur, "id")
    cur.execute("create or replace task t1 as select 2")
    (after,) = _show(cur, "id")
    assert before != after


def test_create_or_alter_task_keeps_id(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task t1 as select 1")
    (before,) = _show(cur, "id")
    cur.execute("create or alter task t1 as select 2")
    assert _show(cur, "id") == [before]


def test_create_or_alter_existing_task_status(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task t1 as select 1")
    cur.execute("create or alter task t1 as select 2")
    assert cur.fetchall() == [("Statement executed successfully.",)]


def test_create_or_alter_new_task_status(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create or alter task t1 as select 1")
    assert cur.fetchall() == [("Task T1 successfully created.",)]


def test_create_or_alter_task_replaces_definition(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task t1 as select 1")
    cur.execute("create or alter task t1 as select 2")
    assert _show(cur, "definition") == [("select 2",)]


def test_create_or_alter_task_resets_omitted_properties(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task t1 comment = 'c' as select 1")
    cur.execute("create or alter task t1 as select 1")
    assert _show(cur, "comment") == [("",)]


def test_create_or_alter_task_keeps_last_suspended_reason(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task root as select 1")
    cur.execute("create task child after root as select 1")
    cur.execute("alter task child remove after root")
    cur.execute("create or alter task child as select 2")
    assert _show(cur, "last_suspended_reason", "child") == [("CHILD_BECAME_ROOT",)]


def test_create_task_unknown_property_raises(cur: snowflake.connector.cursor.SnowflakeCursor):
    with pytest.raises(snowflake.connector.errors.ProgrammingError, match="invalid property 'nope' for 'TASK'"):
        cur.execute("create task t1 nope = 1 as select 1")


def test_create_task_autocommit_false_raises(cur: snowflake.connector.cursor.SnowflakeCursor):
    with pytest.raises(snowflake.connector.errors.ProgrammingError, match="invalid property 'autocommit' for 'TASK'"):
        cur.execute("create task t1 autocommit = false as select 1")


def test_create_task_is_suspended(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task t1 as select 1")
    assert _show(cur, "state") == [("suspended",)]


def test_create_task_owner_is_session_role(_fakesnow: None):
    with snowflake.connector.connect(database="db1", schema="schema1", role="SYSADMIN") as conn:
        cur = conn.cursor()
        cur.execute("create task t1 as select 1")
        assert _show(cur, "owner") == [("SYSADMIN",)]


def test_create_task_stores_warehouse(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task t1 warehouse = wh as select 1")
    assert _show(cur, "warehouse") == [("WH",)]


def test_create_task_stores_schedule(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task t1 schedule = '5 minute' as select 1")
    assert _show(cur, "schedule") == [("5 minute",)]


def test_create_task_stores_session_parameters(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task t1 timezone = 'UTC' as select 1")
    assert indent(_select(cur, "session_parameters::varchar")) == [('{\n  "TIMEZONE": "UTC"\n}',)]


@pytest.mark.parametrize(
    "prop, column, expected",
    [
        ("comment = 'c'", "comment", "c"),
        ("schedule = '1 minute'", "schedule", "1 minute"),
        ("", "definition", "select 1"),
    ],
)
def test_create_task_clone_copies_property(
    cur: snowflake.connector.cursor.SnowflakeCursor, prop: str, column: str, expected: str
):
    cur.execute(f"create task t1 {prop} as select 1")
    cur.execute("create task t2 clone t1")
    assert _show(cur, column, "t2") == [(expected,)]


def test_create_task_clone_gets_new_id(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task t1 as select 1")
    cur.execute("create task t2 clone t1")
    assert len({id_ for (id_,) in _show(cur, "id")}) == 2


def test_create_task_clone_is_suspended(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task t1 schedule = '60 minute' as select 1")
    cur.execute("alter task t1 resume")
    cur.execute("create task t2 clone t1")
    assert _show(cur, "state", "t2") == [("suspended",)]


@pytest.mark.parametrize("column", ["last_committed_on", "last_suspended_on", "last_suspended_reason"])
def test_create_task_clone_clears_last_column(cur: snowflake.connector.cursor.SnowflakeCursor, column: str):
    cur.execute("create task t1 schedule = '60 minute' as select 1")
    cur.execute("alter task t1 resume")
    cur.execute("alter task t1 suspend")
    cur.execute("create task t2 clone t1")
    assert _show(cur, column, "t2") == [(None,)]


def test_create_task_clone_by_owner_keeps_predecessors(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task root as select 1")
    cur.execute("create task child after root as select 1")
    cur.execute("create task copy clone child")
    assert indent(_show(cur, "predecessors", "copy")) == [('[\n  "DB1.SCHEMA1.ROOT"\n]',)]


def test_create_task_clone_by_other_role_has_no_predecessors(_fakesnow: None):
    with snowflake.connector.connect(database="db1", schema="schema1", role="SYSADMIN") as conn, conn.cursor() as cur:
        cur.execute("create task root as select 1")
        cur.execute("create task child after root as select 1")
    with snowflake.connector.connect(database="db1", schema="schema1", role="ANALYST") as conn, conn.cursor() as cur:
        cur.execute("create task copy clone child")
        assert indent(_show(cur, "predecessors", "copy")) == [("[]",)]


def test_create_task_clone_missing_raises(cur: snowflake.connector.cursor.SnowflakeCursor):
    with pytest.raises(snowflake.connector.errors.ProgrammingError, match=r"Task 'DB1\.SCHEMA1\.NOPE' does not exist"):
        cur.execute("create task t2 clone nope")


def test_drop_task_status(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task t1 as select 1")
    cur.execute("drop task t1")
    assert cur.fetchall() == [("T1 successfully dropped.",)]


def test_drop_task_removes_it(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task t1 as select 1")
    cur.execute("drop task t1")
    assert _show(cur, "id") == []


def test_drop_missing_task_raises(cur: snowflake.connector.cursor.SnowflakeCursor):
    with pytest.raises(
        snowflake.connector.errors.ProgrammingError,
        match=r"Task 'DB1\.SCHEMA1\.NOPE' does not exist or not authorized\.",
    ):
        cur.execute("drop task nope")


def test_drop_task_if_exists_missing(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("drop task if exists nope")
    assert cur.rowcount == 1


def test_drop_task_if_exists_missing_status(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("drop task if exists nope")
    assert cur.fetchall() == [("Drop statement executed successfully (NOPE already dropped).",)]


def test_create_task_quoted_name(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute('create task "My Task" as select 1')
    assert cur.fetchall() == [("Task My Task successfully created.",)]


def test_create_task_no_current_schema_raises():
    with fakesnow.patch(), snowflake.connector.connect(database="db1") as conn:
        with pytest.raises(snowflake.connector.errors.ProgrammingError, match="does not have a current schema") as e:
            conn.cursor().execute("create task t1 as select 1")
        assert e.value.errno == 90106


def test_create_task_missing_schema_raises(cur: snowflake.connector.cursor.SnowflakeCursor):
    with pytest.raises(
        snowflake.connector.errors.ProgrammingError,
        match=r"Schema 'DB1\.NOPE' does not exist or not authorized\.",
    ) as e:
        cur.execute("create task nope.t1 as select 1")
    assert (e.value.errno, e.value.sqlstate) == (2003, "02000")


def test_create_task_missing_database_raises(cur: snowflake.connector.cursor.SnowflakeCursor):
    with pytest.raises(
        snowflake.connector.errors.ProgrammingError,
        match=r"Database 'NOPE' does not exist or not authorized\.",
    ):
        cur.execute("create task nope.schema1.t1 as select 1")


def test_create_task_leaves_open_transaction_uncommitted(_fakesnow: None):
    with snowflake.connector.connect(database="db1", schema="schema1", autocommit=False) as conn:
        cur = conn.cursor()
        cur.execute("create table t (i int)")
        conn.commit()
        cur.execute("insert into t values (1)")
        cur.execute("create task t1 as select 1")
        conn.rollback()
        cur.execute("select i from t")
        assert cur.fetchall() == []


def test_create_task_leaves_explicit_transaction_uncommitted(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create table t (i int)")
    cur.execute("begin")
    cur.execute("insert into t values (1)")
    cur.execute("create task t1 as select 1")
    cur.execute("rollback")
    cur.execute("select i from t")
    assert cur.fetchall() == []


def test_create_task_survives_rollback(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("begin")
    cur.execute("create task t1 as select 1")
    cur.execute("rollback")
    assert _show(cur, "name") == [("T1",)]


@pytest.mark.parametrize("statement", ["alter task t1 set comment = 'x'", "drop task t1"])
def test_alter_and_drop_task_commit_open_transaction(_fakesnow: None, statement: str):
    with snowflake.connector.connect(database="db1", schema="schema1", autocommit=False) as conn:
        cur = conn.cursor()
        cur.execute("create table t (i int)")
        cur.execute("create task t1 as select 1")
        conn.commit()
        cur.execute("insert into t values (1)")
        cur.execute(statement)
        conn.rollback()
        cur.execute("select i from t")
        assert cur.fetchall() == [(1,)]


def test_create_task_no_current_database_raises():
    with fakesnow.patch(), snowflake.connector.connect() as conn:
        with pytest.raises(snowflake.connector.errors.ProgrammingError, match="does not have a current database") as e:
            conn.cursor().execute("create task t1 as select 1")
        assert e.value.errno == 90105


def test_tasks_persist_with_db_path():
    with tempfile.TemporaryDirectory(prefix="fakesnow-test") as db_path:
        with fakesnow.patch(db_path=db_path), snowflake.connector.connect(database="db1", schema="schema1") as conn:
            conn.cursor().execute("create task t1 as select 1")
        with (
            fakesnow.patch(db_path=db_path),
            snowflake.connector.connect(database="db1", schema="schema1") as conn,
            pytest.raises(snowflake.connector.errors.ProgrammingError, match="already exists"),
        ):
            conn.cursor().execute("create task t1 as select 1")


def test_drop_schema_drops_its_tasks(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create schema s2")
    cur.execute("create task s2.t1 as select 1")
    cur.execute("drop schema s2")
    cur.execute("create schema s2")
    cur.execute("create task s2.t1 as select 1")
    assert cur.fetchall() == [("Task T1 successfully created.",)]


def test_create_or_replace_schema_drops_its_tasks(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create schema s2")
    cur.execute("create task s2.t1 as select 1")
    cur.execute("create or replace schema s2")
    cur.execute("create task s2.t1 as select 1")
    assert cur.fetchall() == [("Task T1 successfully created.",)]


def _start(cur: snowflake.connector.cursor.SnowflakeCursor, name: str) -> None:
    """Resume a root or standalone task, giving it the schedule it needs to be resumed."""
    cur.execute(f"alter task {name} set schedule = '60 minute'")
    cur.execute(f"alter task {name} resume")


def _enable_dependents(cur: snowflake.connector.cursor.SnowflakeCursor, root: str) -> None:
    """Resume a graph, giving its root the schedule it needs to be resumed."""
    cur.execute(f"alter task {root} set schedule = '60 minute'")
    cur.execute(f"select system$task_dependents_enable('{root}')")


def _create_children(
    cur: snowflake.connector.cursor.SnowflakeCursor, count: int, after: str, prefix: str = "c"
) -> None:
    for i in range(count):
        cur.execute(f"create task {prefix}{i} after {after} as select 1")


def test_create_child_of_started_root_raises(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task root schedule = '1 minute' as select 1")
    _start(cur, "root")
    with pytest.raises(
        snowflake.connector.errors.ProgrammingError,
        match=r"Unable to update graph with root task DB1\.SCHEMA1\.ROOT since that root task is not suspended\.",
    ) as e:
        cur.execute("create task child after root as select 1")
    assert (e.value.errno, e.value.sqlstate) == (91421, "22000")


def test_create_child_with_missing_predecessor_raises(cur: snowflake.connector.cursor.SnowflakeCursor):
    with pytest.raises(
        snowflake.connector.errors.ProgrammingError, match=r"Invalid predecessor NOPE was specified\."
    ) as e:
        cur.execute("create task child after nope as select 1")
    assert (e.value.errno, e.value.sqlstate) == (91085, "42601")


def test_create_child_with_schedule_raises(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task root as select 1")
    with pytest.raises(
        snowflake.connector.errors.ProgrammingError,
        match=r"Task CHILD cannot have both a schedule and a predecessor\.",
    ):
        cur.execute("create task child schedule = '1 minute' after root as select 1")


def test_create_child_in_other_schema_raises(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create schema s2")
    cur.execute("create task s2.root as select 1")
    cur.execute("use schema schema1")  # create schema switches to the new schema
    with pytest.raises(
        snowflake.connector.errors.ProgrammingError,
        match=r"Cannot have predecessor ROOT from a different schema\.",
    ):
        cur.execute("create task child after s2.root as select 1")


def test_create_second_finalizer_raises(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task root as select 1")
    cur.execute("create task fin1 finalize = root as select 1")
    with pytest.raises(
        snowflake.connector.errors.ProgrammingError,
        match=r"Finalize target ROOT already have finalizer\.",
    ):
        cur.execute("create task fin2 finalize = root as select 1")


def test_create_finalizer_with_predecessor_raises(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task root as select 1")
    cur.execute("create task other as select 1")
    with pytest.raises(
        snowflake.connector.errors.ProgrammingError,
        match=r"Task DB1\.SCHEMA1\.FIN cannot have predecessors as a finalizer task\.",
    ):
        cur.execute("create task fin finalize = root after other as select 1")


def test_create_finalizer_with_children_raises(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task root as select 1")
    cur.execute("create task fin as select 1")
    cur.execute("create task child after fin as select 1")
    with pytest.raises(
        snowflake.connector.errors.ProgrammingError,
        match=r"Invalid finalizer task DB1\.SCHEMA1\.FIN: a finalizer cannot have child tasks\.",
    ):
        cur.execute("create or replace task fin finalize = root as select 1")


def test_create_task_cycle_is_accepted(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task root as select 1")
    cur.execute("create task child after root as select 1")
    cur.execute("create or replace task root after child as select 1")
    assert cur.fetchall() == [("Task ROOT successfully created.",)]


def test_create_or_replace_task_after_itself_raises(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task root as select 1")
    with pytest.raises(
        snowflake.connector.errors.ProgrammingError, match=r"Task ROOT cannot use itself as its predecessor\."
    ):
        cur.execute("create or replace task root after root as select 1")


def test_create_task_too_many_predecessors_raises(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task root as select 1")
    # three middle tasks keep every task within the child limit, in a single graph
    for m in range(3):
        cur.execute(f"create task m{m} after root as select 1")
        _create_children(cur, 34, after=f"m{m}", prefix=f"c{m}_")
    leaves = [f"c{m}_{i}" for m in range(3) for i in range(34)]
    after = ", ".join(leaves[:101])
    with pytest.raises(
        snowflake.connector.errors.ProgrammingError,
        match=r"Task DB1\.SCHEMA1\.JOIN exceeds the maximum of 100 predecessors\.",
    ):
        cur.execute(f"create task join after {after} as select 1")


def test_create_task_too_many_children_raises(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task root as select 1")
    _create_children(cur, 100, after="root")
    with pytest.raises(
        snowflake.connector.errors.ProgrammingError,
        match=r"Task DB1\.SCHEMA1\.ROOT exceeds the maximum of 100 child tasks\.",
    ):
        cur.execute("create task one_more after root as select 1")


def test_alter_task_add_after_joining_graphs_too_large_raises(
    cur: snowflake.connector.cursor.SnowflakeCursor, monkeypatch: pytest.MonkeyPatch
):
    # a smaller limit than Snowflake's 1000 keeps the graphs small
    monkeypatch.setattr("fakesnow.tasks.catalog.MAX_GRAPH_TASKS", 3)
    cur.execute("create task a as select 1")
    cur.execute("create task a_child after a as select 1")
    cur.execute("create task b as select 1")
    cur.execute("create task b_child after b as select 1")
    with pytest.raises(
        snowflake.connector.errors.ProgrammingError,
        match=r"Task DB1\.SCHEMA1\.B exceeds the maximum of 3 tasks in a graph\.",
    ):
        cur.execute("alter task b add after a")


def test_drop_only_predecessor_makes_child_standalone(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task root as select 1")
    cur.execute("create task child after root as select 1")
    cur.execute("drop task root")
    assert _show(cur, "last_suspended_reason", "child") == [("CHILD_BECAME_ROOT",)]


def test_drop_only_predecessor_clears_child_predecessors(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task root as select 1")
    cur.execute("create task child after root as select 1")
    cur.execute("drop task root")
    assert indent(_show(cur, "predecessors", "child")) == [("[]",)]


def test_drop_root_makes_finalizer_standalone(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task root as select 1")
    cur.execute("create task fin finalize = root as select 1")
    cur.execute("drop task root")
    assert _show(cur, "last_suspended_reason", "fin") == [("FINALIZER_BECAME_ROOT",)]


def test_drop_root_clears_finalizer_relation(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task root as select 1")
    cur.execute("create task fin finalize = root as select 1")
    cur.execute("drop task root")
    assert indent(_show(cur, "task_relations", "fin")) == [('{\n  "Predecessors": []\n}',)]


def test_drop_task_in_started_graph_raises(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task root schedule = '1 minute' as select 1")
    cur.execute("create task child after root as select 1")
    _start(cur, "root")
    with pytest.raises(snowflake.connector.errors.ProgrammingError, match="root task is not suspended"):
        cur.execute("drop task child")


def test_drop_started_standalone_task_succeeds(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task t1 schedule = '1 minute' as select 1")
    _start(cur, "t1")
    cur.execute("drop task t1")
    assert _show(cur, "name") == []


def test_create_or_replace_started_standalone_task_succeeds(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task t1 schedule = '1 minute' as select 1")
    _start(cur, "t1")
    cur.execute("create or replace task t1 schedule = '1 minute' as select 2")
    assert _show(cur, "definition") == [("select 2",)]


def test_create_or_replace_started_standalone_task_is_suspended(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task t1 schedule = '1 minute' as select 1")
    _start(cur, "t1")
    cur.execute("create or replace task t1 schedule = '1 minute' as select 2")
    assert _show(cur, "state") == [("suspended",)]


def test_create_or_alter_started_standalone_task_succeeds(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task t1 schedule = '1 minute' as select 1")
    _start(cur, "t1")
    cur.execute("create or alter task t1 schedule = '1 minute' as select 2")
    assert _show(cur, "definition") == [("select 2",)]


def test_create_or_alter_started_standalone_task_stays_started(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task t1 schedule = '1 minute' as select 1")
    _start(cur, "t1")
    cur.execute("create or alter task t1 schedule = '1 minute' as select 2")
    assert _show(cur, "state") == [("started",)]


def test_create_or_replace_started_root_with_dependents_succeeds(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task root schedule = '1 minute' as select 1")
    cur.execute("create task child after root as select 1")
    _start(cur, "root")
    cur.execute("create or replace task root schedule = '1 minute' as select 2")
    assert cur.fetchall() == [("Task ROOT successfully created.",)]


def test_create_or_replace_started_root_with_dependents_is_suspended(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task root schedule = '1 minute' as select 1")
    cur.execute("create task child after root as select 1")
    _start(cur, "root")
    cur.execute("create or replace task root schedule = '1 minute' as select 2")
    assert _show(cur, "state", "root") == [("suspended",)]


def test_drop_started_root_with_dependents_raises(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task root schedule = '1 minute' as select 1")
    cur.execute("create task child after root as select 1")
    _start(cur, "root")
    with pytest.raises(snowflake.connector.errors.ProgrammingError, match="root task is not suspended"):
        cur.execute("drop task root")


def test_create_child_after_finalizer_raises(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task root as select 1")
    cur.execute("create task fin finalize = root as select 1")
    with pytest.raises(
        snowflake.connector.errors.ProgrammingError,
        match=r"Invalid finalizer task DB1\.SCHEMA1\.FIN: a finalizer cannot have child tasks\.",
    ):
        cur.execute("create task child after fin as select 1")


def test_create_child_after_task_in_started_second_graph_raises(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task root1 as select 1")
    cur.execute("create task root2 schedule = '1 minute' as select 1")
    _start(cur, "root2")
    with pytest.raises(snowflake.connector.errors.ProgrammingError, match="root task is not suspended"):
        cur.execute("create task child after root1, root2 as select 1")


def test_create_child_with_predecessors_in_different_graphs_is_accepted(
    cur: snowflake.connector.cursor.SnowflakeCursor,
):
    cur.execute("create task root1 as select 1")
    cur.execute("create task root2 as select 1")
    cur.execute("create task child after root1, root2 as select 1")
    assert cur.fetchall() == [("Task CHILD successfully created.",)]


def test_create_child_with_predecessors_in_one_graph(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task root as select 1")
    cur.execute("create task a after root as select 1")
    cur.execute("create task b after root as select 1")
    cur.execute("create task join after a, b as select 1")
    assert indent(_show(cur, "predecessors", "join")) == [('[\n  "DB1.SCHEMA1.A",\n  "DB1.SCHEMA1.B"\n]',)]


def test_create_child_after_other_schema_task_ignores_same_named_started_local_task(
    cur: snowflake.connector.cursor.SnowflakeCursor,
):
    cur.execute("create task x schedule = '1 minute' as select 1")
    _start(cur, "x")
    cur.execute("create schema s2")
    cur.execute("create task s2.x as select 1")
    cur.execute("use schema schema1")
    with pytest.raises(snowflake.connector.errors.ProgrammingError, match="from a different schema"):
        cur.execute("create task child after s2.x as select 1")


def test_create_finalizer_of_non_root_raises(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task root as select 1")
    cur.execute("create task child after root as select 1")
    with pytest.raises(
        snowflake.connector.errors.ProgrammingError,
        match=r"Task DB1\.SCHEMA1\.FIN cannot finalize a non-root task DB1\.SCHEMA1\.CHILD\.",
    ):
        cur.execute("create task fin finalize = child as select 1")


def test_create_finalizer_of_quoted_root_with_period(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute('create task "root.task" as select 1')
    cur.execute('create task fin finalize = "root.task" as select 1')
    assert cur.fetchall() == [("Task FIN successfully created.",)]


def test_create_finalizer_of_root_in_other_schema_raises(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create schema s2")
    cur.execute("create task s2.root as select 1")
    cur.execute("use schema schema1")  # create schema switches to the new schema
    cur.execute("create task root as select 1")
    with pytest.raises(
        snowflake.connector.errors.ProgrammingError,
        match=r"Cannot finalize graph S2\.ROOT in a different schema\.",
    ):
        cur.execute("create task fin finalize = s2.root as select 1")


def test_create_task_finalizing_itself_raises(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task t1 as select 1")
    with pytest.raises(
        snowflake.connector.errors.ProgrammingError,
        match=r"Invalid finalized root task T1 was specified\.",
    ):
        cur.execute("create or replace task t1 finalize = t1 as select 1")


def test_create_or_replace_task_over_started_child_with_new_predecessor_raises(
    cur: snowflake.connector.cursor.SnowflakeCursor,
):
    cur.execute("create task root schedule = '1 minute' as select 1")
    cur.execute("create task child after root as select 1")
    cur.execute("create task other as select 1")
    _start(cur, "root")
    with pytest.raises(snowflake.connector.errors.ProgrammingError, match=r"graph with root task DB1\.SCHEMA1\.ROOT"):
        cur.execute("create or replace task child after other as select 1")


def test_create_or_alter_task_over_started_standalone_with_new_predecessor_raises(
    cur: snowflake.connector.cursor.SnowflakeCursor,
):
    cur.execute("create task root as select 1")
    cur.execute("create task t1 schedule = '1 minute' as select 1")
    _start(cur, "t1")
    with pytest.raises(snowflake.connector.errors.ProgrammingError, match=r"graph with root task DB1\.SCHEMA1\.T1"):
        cur.execute("create or alter task t1 after root as select 1")


def test_alter_task_resume_status(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task t1 schedule = '60 minute' as select 1")
    cur.execute("alter task t1 resume")
    assert cur.fetchall() == [("Statement executed successfully.",)]


def test_alter_task_resume_sets_state(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task t1 schedule = '60 minute' as select 1")
    cur.execute("alter task t1 resume")
    assert _show(cur, "state") == [("started",)]


def test_alter_task_first_resume_sets_last_committed_on(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task t1 schedule = '60 minute' as select 1")
    cur.execute("alter task t1 resume")
    assert _show(cur, "last_committed_on") != [(None,)]


def test_alter_task_second_resume_keeps_last_committed_on(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task t1 schedule = '60 minute' as select 1")
    cur.execute("alter task t1 resume")
    (before,) = _show(cur, "last_committed_on")
    cur.execute("alter task t1 suspend")
    cur.execute("alter task t1 resume")
    assert _show(cur, "last_committed_on") == [before]


def test_alter_task_suspend_sets_state(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task t1 schedule = '60 minute' as select 1")
    cur.execute("alter task t1 resume")
    cur.execute("alter task t1 suspend")
    assert _show(cur, "state") == [("suspended",)]


def test_alter_task_suspend_sets_reason(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task t1 schedule = '60 minute' as select 1")
    cur.execute("alter task t1 resume")
    cur.execute("alter task t1 suspend")
    assert _show(cur, "last_suspended_reason") == [("USER_SUSPENDED",)]


def test_alter_task_resume_without_trigger_raises(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task t1 as select 1")
    with pytest.raises(
        snowflake.connector.errors.ProgrammingError,
        match=r"Task DB1\.SCHEMA1\.T1 should have a SCHEDULE, AFTER, FINALIZE or WHEN to be resumed\.",
    ) as e:
        cur.execute("alter task t1 resume")
    assert (e.value.errno, e.value.sqlstate) == (91453, "22000")


def test_alter_serverless_task_triggered_by_when_raises(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task t1 when 1 = 1 as select 1")
    with pytest.raises(
        snowflake.connector.errors.ProgrammingError,
        match=r"TARGET_COMPLETION_INTERVAL must be set for serverless triggered task DB1\.SCHEMA1\.T1 to resume or "
        r"execute\.",
    ) as e:
        cur.execute("alter task t1 resume")
    assert (e.value.errno, e.value.sqlstate) == (91865, "55000")


def test_execute_serverless_task_triggered_by_when_raises(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task t1 when 1 = 1 as select 1")
    with pytest.raises(snowflake.connector.errors.ProgrammingError, match="TARGET_COMPLETION_INTERVAL must be set"):
        cur.execute("execute task t1")


def test_execute_serverless_task_triggered_by_when_with_target_completion_interval(
    cur: snowflake.connector.cursor.SnowflakeCursor,
):
    cur.execute("create task t1 target_completion_interval = '10 minutes' when 1 = 1 as select 1")
    cur.execute("execute task t1")
    assert _history(cur, "state") == [("SUCCEEDED",)]


def test_alter_task_resume_with_when(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task t1 warehouse = wh when 1 = 1 as select 1")
    cur.execute("alter task t1 resume")
    assert _show(cur, "state") == [("started",)]


@pytest.mark.parametrize("column", ["last_suspended_reason", "last_suspended_on"])
def test_alter_task_suspend_suspended_task_changes_nothing(
    cur: snowflake.connector.cursor.SnowflakeCursor, column: str
):
    cur.execute("create task t1 as select 1")
    cur.execute("alter task t1 suspend")
    assert _show(cur, column) == [(None,)]


def test_alter_task_suspend_sets_last_suspended_on(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task t1 schedule = '60 minute' as select 1")
    cur.execute("alter task t1 resume")
    cur.execute("alter task t1 suspend")
    assert _show(cur, "last_suspended_on") != [(None,)]


def test_alter_task_set_comment(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task t1 as select 1")
    cur.execute("alter task t1 set comment = 'c'")
    assert _show(cur, "comment") == [("c",)]


def test_alter_task_set_keeps_other_properties(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task t1 warehouse = wh as select 1")
    cur.execute("alter task t1 set comment = 'c'")
    assert _show(cur, "warehouse") == [("WH",)]


def test_alter_task_set_unknown_property_raises(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task t1 as select 1")
    with pytest.raises(snowflake.connector.errors.ProgrammingError, match="invalid property 'nope' for 'TASK'"):
        cur.execute("alter task t1 set nope = 1")


def test_alter_task_set_session_parameter(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task t1 as select 1")
    cur.execute("alter task t1 set timezone = 'UTC'")
    assert indent(_select(cur, "session_parameters::varchar")) == [('{\n  "TIMEZONE": "UTC"\n}',)]


def test_alter_task_unset_unknown_property_raises(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task t1 as select 1")
    with pytest.raises(snowflake.connector.errors.ProgrammingError, match="invalid property 'nope' for 'TASK'"):
        cur.execute("alter task t1 unset nope")


def test_alter_task_unset_comment(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task t1 comment = 'c' as select 1")
    cur.execute("alter task t1 unset comment")
    assert _show(cur, "comment") == [("",)]


def test_alter_task_unset_session_parameter(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task t1 timezone = 'UTC' as select 1")
    cur.execute("alter task t1 unset timezone")
    assert indent(_select(cur, "session_parameters::varchar")) == [("{}",)]


def test_alter_task_set_on_started_root_raises(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task t1 as select 1")
    _start(cur, "t1")
    with pytest.raises(snowflake.connector.errors.ProgrammingError, match="root task is not suspended"):
        cur.execute("alter task t1 set comment = 'c'")


def test_alter_child_resume_with_started_root_raises(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task root schedule = '1 minute' as select 1")
    cur.execute("create task child after root as select 1")
    _start(cur, "root")
    with pytest.raises(
        snowflake.connector.errors.ProgrammingError,
        match=r"graph with root task DB1\.SCHEMA1\.ROOT since that root task is not suspended",
    ):
        cur.execute("alter task child resume")


def test_alter_child_suspend_with_started_root_raises(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task root schedule = '1 minute' as select 1")
    cur.execute("create task child after root as select 1")
    cur.execute("alter task child resume")
    _start(cur, "root")
    with pytest.raises(snowflake.connector.errors.ProgrammingError, match="root task is not suspended"):
        cur.execute("alter task child suspend")


def test_alter_started_root_suspend_succeeds(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task root schedule = '1 minute' as select 1")
    cur.execute("create task child after root as select 1")
    _start(cur, "root")
    cur.execute("alter task root suspend")
    assert _show(cur, "state", "root") == [("suspended",)]


def test_alter_task_add_after(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task root as select 1")
    cur.execute("create task child as select 1")
    cur.execute("alter task child add after root")
    assert indent(_show(cur, "predecessors", "child")) == [('[\n  "DB1.SCHEMA1.ROOT"\n]',)]


def test_alter_task_add_after_started_root_raises(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task root as select 1")
    cur.execute("create task child as select 1")
    _start(cur, "root")
    with pytest.raises(snowflake.connector.errors.ProgrammingError, match="root task is not suspended"):
        cur.execute("alter task child add after root")


def test_alter_task_add_after_other_schema_raises(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task child as select 1")
    cur.execute("create schema s2")
    cur.execute("create task s2.p as select 1")
    cur.execute("use schema schema1")
    with pytest.raises(
        snowflake.connector.errors.ProgrammingError,
        match=r"Cannot have predecessor P from a different schema\.",
    ):
        cur.execute("alter task child add after s2.p")


def test_alter_task_add_after_cycle_is_accepted(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task root as select 1")
    cur.execute("create task child after root as select 1")
    cur.execute("alter task root add after child")
    assert cur.fetchall() == [("Statement executed successfully.",)]


def test_alter_task_add_after_itself_raises(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task root as select 1")
    with pytest.raises(
        snowflake.connector.errors.ProgrammingError, match=r"Task ROOT cannot use itself as its predecessor\."
    ):
        cur.execute("alter task root add after root")


def test_alter_task_add_after_itself_error_code(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task root as select 1")
    with pytest.raises(snowflake.connector.errors.ProgrammingError) as e:
        cur.execute("alter task root add after root")
    assert (e.value.errno, e.value.sqlstate) == (91402, "22000")


def test_alter_task_add_after_itself_keeps_predecessors(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task root as select 1")
    with pytest.raises(snowflake.connector.errors.ProgrammingError):
        cur.execute("alter task root add after root")
    assert indent(_show(cur, "predecessors", "root")) == [("[]",)]


def test_alter_task_add_after_to_scheduled_task_raises(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task root as select 1")
    cur.execute("create task t1 schedule = '1 minute' as select 1")
    with pytest.raises(
        snowflake.connector.errors.ProgrammingError, match="Task T1 cannot have both a schedule and a predecessor"
    ) as e:
        cur.execute("alter task t1 add after root")
    assert (e.value.errno, e.value.sqlstate) == (91400, "42601")


def test_alter_task_add_after_different_graph_is_accepted(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task root1 as select 1")
    cur.execute("create task root2 as select 1")
    cur.execute("create task child after root1 as select 1")
    cur.execute("alter task child add after root2")
    assert cur.fetchall() == [("Statement executed successfully.",)]


def test_alter_task_remove_after(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task root as select 1")
    cur.execute("create task a after root as select 1")
    cur.execute("create task b after root as select 1")
    cur.execute("create task child after a, b as select 1")
    cur.execute("alter task child remove after a")
    assert indent(_show(cur, "predecessors", "child")) == [('[\n  "DB1.SCHEMA1.B"\n]',)]


def test_alter_task_remove_last_predecessor_makes_child_standalone(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task root as select 1")
    cur.execute("create task child after root as select 1")
    cur.execute("alter task child remove after root")
    assert _show(cur, "last_suspended_reason", "child") == [("CHILD_BECAME_ROOT",)]


def test_alter_task_remove_last_predecessor_clears_predecessors(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task root as select 1")
    cur.execute("create task child after root as select 1")
    cur.execute("alter task child remove after root")
    assert indent(_show(cur, "predecessors", "child")) == [("[]",)]


def test_alter_task_remove_after_started_root_raises(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task root schedule = '1 minute' as select 1")
    cur.execute("create task child after root as select 1")
    _start(cur, "root")
    with pytest.raises(snowflake.connector.errors.ProgrammingError, match="root task is not suspended"):
        cur.execute("alter task child remove after root")


def test_alter_task_set_finalize(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task root as select 1")
    cur.execute("create task fin as select 1")
    cur.execute("alter task fin set finalize = root")
    assert indent(_show(cur, "task_relations", "fin")) == [
        ('{\n  "FinalizedRootTask": "DB1.SCHEMA1.ROOT",\n  "Predecessors": []\n}',)
    ]


def test_alter_task_set_finalize_of_non_root_raises(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task root as select 1")
    cur.execute("create task child after root as select 1")
    cur.execute("create task fin as select 1")
    with pytest.raises(snowflake.connector.errors.ProgrammingError, match="cannot finalize a non-root task"):
        cur.execute("alter task fin set finalize = child")


def test_alter_root_with_finalizer_add_after_raises(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task root as select 1")
    cur.execute("create task fin finalize = root as select 1")
    cur.execute("create task parent as select 1")
    with pytest.raises(
        snowflake.connector.errors.ProgrammingError,
        match=r"This task has a finalizer and cannot be made a child task\. Please remove the finalizer before "
        r"adding a predecessor\.",
    ) as e:
        cur.execute("alter task root add after parent")
    assert (e.value.errno, e.value.sqlstate) == (91856, "55000")


def test_alter_root_with_finalizer_set_finalize_is_accepted(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task other as select 1")
    cur.execute("create task root as select 1")
    cur.execute("create task fin finalize = root as select 1")
    cur.execute("alter task root set finalize = other")
    assert cur.fetchall() == [("Statement executed successfully.",)]


def test_alter_root_with_finalizer_set_finalize_relations(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task other as select 1")
    cur.execute("create task root as select 1")
    cur.execute("create task fin finalize = root as select 1")
    cur.execute("alter task root set finalize = other")
    assert indent(_show(cur, "task_relations", "root")) == [
        (
            '{\n  "FinalizedRootTask": "DB1.SCHEMA1.OTHER",\n  "Predecessors": [],\n'
            '  "FinalizerTask": "DB1.SCHEMA1.FIN"\n}',
        )
    ]


def test_alter_task_remove_after_splitting_join_is_accepted(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task root as select 1")
    cur.execute("create task a after root as select 1")
    cur.execute("create task b after root as select 1")
    cur.execute("create task c after a, b as select 1")
    cur.execute("alter task a remove after root")
    assert cur.fetchall() == [("Statement executed successfully.",)]


def test_alter_task_remove_after_non_predecessor_raises(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task root as select 1")
    cur.execute("create task other as select 1")
    cur.execute("create task child after root as select 1")
    with pytest.raises(
        snowflake.connector.errors.ProgrammingError, match="Task CHILD does not have predecessor OTHER"
    ) as e:
        cur.execute("alter task child remove after other")
    assert (e.value.errno, e.value.sqlstate) == (91417, "22000")


def test_alter_task_set_schedule_on_child_raises(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task root as select 1")
    cur.execute("create task child after root as select 1")
    with pytest.raises(
        snowflake.connector.errors.ProgrammingError, match="Task CHILD cannot have both a schedule and a predecessor"
    ):
        cur.execute("alter task child set schedule = '1 minute'")


def test_alter_task_unset_finalize_makes_finalizer_standalone(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task root as select 1")
    cur.execute("create task fin finalize = root as select 1")
    cur.execute("alter task fin unset finalize")
    assert _show(cur, "last_suspended_reason", "fin") == [("FINALIZER_BECAME_ROOT",)]


def test_alter_task_unset_finalize_clears_finalizer_relation(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task root as select 1")
    cur.execute("create task fin finalize = root as select 1")
    cur.execute("alter task fin unset finalize")
    assert indent(_show(cur, "task_relations", "fin")) == [('{\n  "Predecessors": []\n}',)]


def test_alter_task_modify_as(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task t1 as select 1")
    cur.execute("alter task t1 modify as select 2")
    assert _show(cur, "definition") == [("select 2",)]


def test_alter_task_modify_when(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task t1 as select 1")
    cur.execute("alter task t1 modify when 1 = 1")
    assert _show(cur, "condition") == [("1 = 1",)]


@pytest.mark.parametrize(
    "condition",
    [
        "cast('1' as int) = 1",
        "'1'::int = 1",
        "not (1 = 2)",
        "1 between 0 and 2",
        "true",
        "'a' = 'a' and true",
    ],
)
def test_create_task_when_allowed(cur: snowflake.connector.cursor.SnowflakeCursor, condition: str):
    cur.execute(f"create task t1 when {condition} as select 1")
    assert cur.fetchall() == [("Task T1 successfully created.",)]


def test_create_task_when_predecessor_return_value_allowed(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task root as select 1")
    cur.execute("create task t1 after root when system$get_predecessor_return_value() = 'x' as select 1")
    assert cur.fetchall() == [("Task T1 successfully created.",)]


@pytest.mark.parametrize(
    "condition", ["1 + 1 = 2", "1 in (1, 2)", "null is null", "'a' like 'a%'", "current_date() > '2000-01-01'"]
)
def test_create_task_when_not_allowed_raises(cur: snowflake.connector.cursor.SnowflakeCursor, condition: str):
    with pytest.raises(
        snowflake.connector.errors.ProgrammingError,
        match=r"Invalid expression for task condition expression\. Only some data type conversions and the following "
        r"functions are allowed: \[SYSTEM\$GET_PREDECESSOR_RETURN_VALUE, SYSTEM\$PREDECESSORS_STATE, "
        r"SYSTEM\$STREAM_HAS_DATA, SYSTEM\$TASK_COMPLETION_STATE\]\. Please see the documentation for more details\.",
    ) as e:
        cur.execute(f"create task t1 when {condition} as select 1")
    assert (e.value.errno, e.value.sqlstate) == (91464, "42601")


def test_alter_task_modify_when_not_allowed_raises(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task t1 as select 1")
    with pytest.raises(snowflake.connector.errors.ProgrammingError, match="Invalid expression for task condition"):
        cur.execute("alter task t1 modify when current_date() > '2000-01-01'")


def test_alter_task_remove_when(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task t1 when 1 = 1 as select 1")
    cur.execute("alter task t1 remove when")
    assert _show(cur, "condition") == [(None,)]


def test_alter_task_modify_as_started_root_raises(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task t1 as select 1")
    _start(cur, "t1")
    with pytest.raises(snowflake.connector.errors.ProgrammingError, match="root task is not suspended"):
        cur.execute("alter task t1 modify as select 2")


def test_alter_missing_task_raises(cur: snowflake.connector.cursor.SnowflakeCursor):
    with pytest.raises(
        snowflake.connector.errors.ProgrammingError,
        match=r"Task 'DB1\.SCHEMA1\.NOPE' does not exist or not authorized\.",
    ):
        cur.execute("alter task nope resume")


def test_alter_missing_task_if_exists(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("alter task if exists nope resume")
    assert cur.fetchall() == [("Statement executed successfully.",)]


def test_task_dependents_enable_column_name(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task root schedule = '60 minute' as select 1")
    cur.execute("select system$task_dependents_enable('root')")
    assert [d.name for d in cur.description] == ["SYSTEM$TASK_DEPENDENTS_ENABLE('ROOT')"]


def test_task_dependents_enable_returns_ok(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task root schedule = '60 minute' as select 1")
    cur.execute("select system$task_dependents_enable('root')")
    assert cur.fetchall() == [("OK",)]


def test_task_dependents_enable_resumes_graph(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task root schedule = '60 minute' as select 1")
    cur.execute("create task child after root as select 1")
    cur.execute("create task grandchild after child as select 1")
    cur.execute("select system$task_dependents_enable('root')")
    assert sorted(_show(cur, "name, state")) == [("CHILD", "started"), ("GRANDCHILD", "started"), ("ROOT", "started")]


def test_task_dependents_enable_resumes_finalizer(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task root schedule = '60 minute' as select 1")
    cur.execute("create task fin finalize = root as select 1")
    cur.execute("select system$task_dependents_enable('root')")
    assert _show(cur, "state", "fin") == [("started",)]


def test_task_dependents_enable_leaves_other_graphs_suspended(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task root schedule = '60 minute' as select 1")
    cur.execute("create task other as select 1")
    cur.execute("select system$task_dependents_enable('root')")
    assert _show(cur, "state", "other") == [("suspended",)]


def test_task_dependents_enable_root_without_trigger_raises(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task root as select 1")
    with pytest.raises(
        snowflake.connector.errors.ProgrammingError,
        match=r"Task DB1\.SCHEMA1\.ROOT should have a SCHEDULE, AFTER, FINALIZE or WHEN to be resumed\.",
    ):
        cur.execute("select system$task_dependents_enable('root')")


def test_task_dependents_enable_root_without_trigger_resumes_dependents(
    cur: snowflake.connector.cursor.SnowflakeCursor,
):
    cur.execute("create task root as select 1")
    cur.execute("create task child after root as select 1")
    with pytest.raises(snowflake.connector.errors.ProgrammingError):
        cur.execute("select system$task_dependents_enable('root')")
    assert _show(cur, "name, state") == [("CHILD", "started"), ("ROOT", "suspended")]


def test_task_dependents_enable_missing_task_raises(cur: snowflake.connector.cursor.SnowflakeCursor):
    with pytest.raises(
        snowflake.connector.errors.ProgrammingError,
        match=r"invalid value \[nope\] for parameter 'RECURSIVE_TASK_ENABLE'",
    ) as e:
        cur.execute("select system$task_dependents_enable('nope')")
    assert (e.value.errno, e.value.sqlstate) == (1008, "22023")


def test_task_dependents_enable_in_larger_select_not_implemented(cur: snowflake.connector.cursor.SnowflakeCursor):
    with pytest.raises(snowflake.connector.errors.ProgrammingError, match="only supported as the sole column"):
        cur.execute("select system$task_dependents_enable('root'), 1")


SHOW_TASKS_COLUMNS = [
    "created_on",
    "name",
    "id",
    "database_name",
    "schema_name",
    "owner",
    "comment",
    "warehouse",
    "schedule",
    "predecessors",
    "state",
    "definition",
    "condition",
    "allow_overlapping_execution",
    "error_integration",
    "last_committed_on",
    "last_suspended_on",
    "owner_role_type",
    "config",
    "task_relations",
    "last_suspended_reason",
    "success_integration",
    "scheduling_mode",
    "target_completion_interval",
    "execute_as_user",
    "overlap_policy",
    "created_by_user",
]


def _show_names(cur: snowflake.connector.cursor.SnowflakeCursor, show: str) -> list[str]:
    cur.execute(show)
    names = [d.name for d in cur.description]
    return [row[names.index("name")] for row in cur.fetchall()]


def test_show_tasks_columns(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("show tasks")
    assert [d.name for d in cur.description] == SHOW_TASKS_COLUMNS


_NEW_TASK_ROW = {
    "name": "T1",
    "database_name": "DB1",
    "schema_name": "SCHEMA1",
    "owner": "PUBLIC",
    "comment": "",
    "warehouse": None,
    "schedule": "5 minute",
    "predecessors": "[]",
    "state": "suspended",
    "definition": "select 1",
    "condition": None,
    "allow_overlapping_execution": "false",
    "error_integration": "null",
    "last_committed_on": None,
    "last_suspended_on": None,
    "owner_role_type": "ROLE",
    "config": None,
    "task_relations": '{\n  "Predecessors": []\n}',
    "last_suspended_reason": None,
    "success_integration": "null",
    "scheduling_mode": None,
    "target_completion_interval": None,
    "execute_as_user": None,
    "overlap_policy": "NO_OVERLAP",
    "created_by_user": None,
}


def _new_task_row(dcur: snowflake.connector.cursor.DictCursor, show: str) -> dict:
    dcur.execute("create task t1 schedule = '5 minute' as select 1")
    dcur.execute(show)
    (row,) = dcur.fetchall()
    return row


@pytest.mark.parametrize("column, expected", _NEW_TASK_ROW.items())
def test_show_tasks_new_task_row(dcur: snowflake.connector.cursor.DictCursor, column: str, expected: str | None):
    assert dindent([_new_task_row(dcur, "show tasks")])[0][column] == expected


@pytest.mark.parametrize("column", ["created_on", "id"])
def test_show_tasks_new_task_row_is_set(dcur: snowflake.connector.cursor.DictCursor, column: str):
    assert _new_task_row(dcur, "show tasks")[column]


def test_show_tasks_predecessors_are_qualified(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task root as select 1")
    cur.execute("create task a after root as select 1")
    cur.execute("create task b after root as select 1")
    cur.execute("create task join after a, b as select 1")
    assert indent(_show(cur, "predecessors", "join")) == [('[\n  "DB1.SCHEMA1.A",\n  "DB1.SCHEMA1.B"\n]',)]


@pytest.mark.parametrize("column", ["config", "overlap_policy"])
def test_show_tasks_child_has_null_root_only_column(cur: snowflake.connector.cursor.SnowflakeCursor, column: str):
    cur.execute("create task root config = '{\"k\": 1}' as select 1")
    cur.execute("create task child after root as select 1")
    assert _show(cur, column, "child") == [(None,)]


def test_show_tasks_child_allow_overlapping_execution_is_null_text(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task root allow_overlapping_execution = true as select 1")
    cur.execute("create task child after root as select 1")
    assert _show(cur, "allow_overlapping_execution", "child") == [("null",)]


def test_show_tasks_overlap_policy_follows_allow_overlapping_execution(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task root allow_overlapping_execution = true as select 1")
    assert _show(cur, "overlap_policy", "root") == [("ALLOW_CHILD_OVERLAP",)]


def test_show_tasks_created_by_user(_fakesnow: None):
    with snowflake.connector.connect(database="db1", schema="schema1", user="someone") as conn, conn.cursor() as cur:
        cur.execute("create task t1 as select 1")
        assert _show(cur, "created_by_user") == [("SOMEONE",)]


def test_show_tasks_task_relations_for_root_with_finalizer(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task root as select 1")
    cur.execute("create task fin finalize = root as select 1")
    assert indent(_show(cur, "task_relations", "root")) == [
        ('{\n  "Predecessors": [],\n  "FinalizerTask": "DB1.SCHEMA1.FIN"\n}',)
    ]


def test_show_tasks_task_relations_for_finalizer(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task root as select 1")
    cur.execute("create task fin finalize = root as select 1")
    assert indent(_show(cur, "task_relations", "fin")) == [
        ('{\n  "FinalizedRootTask": "DB1.SCHEMA1.ROOT",\n  "Predecessors": []\n}',)
    ]


def test_show_tasks_task_relations_for_child(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task root as select 1")
    cur.execute("create task child after root as select 1")
    assert indent(_show(cur, "task_relations", "child")) == [
        ('{\n  "Predecessors": [\n    "DB1.SCHEMA1.ROOT"\n  ]\n}',)
    ]


def test_show_tasks_target_completion_interval(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task t1 schedule = '5 minute' target_completion_interval = '10 minutes' as select 1")
    assert _show(cur, "target_completion_interval") == [("10 minutes",)]


def test_show_terse_tasks_columns(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("show terse tasks")
    assert [d.name for d in cur.description] == [
        "created_on",
        "name",
        "kind",
        "database_name",
        "schema_name",
        "schedule",
    ]


@pytest.mark.parametrize(
    "column, expected",
    [("name", "T1"), ("kind", None), ("database_name", "DB1"), ("schema_name", "SCHEMA1"), ("schedule", "5 minute")],
)
def test_show_terse_tasks_row(dcur: snowflake.connector.cursor.DictCursor, column: str, expected: str | None):
    assert _new_task_row(dcur, "show terse tasks")[column] == expected


def test_show_terse_tasks_row_has_created_on(dcur: snowflake.connector.cursor.DictCursor):
    assert _new_task_row(dcur, "show terse tasks")["created_on"]


def test_show_tasks_orders_by_name(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task b as select 1")
    cur.execute("create task a as select 1")
    assert _show_names(cur, "show tasks") == ["A", "B"]


def test_show_tasks_like(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task load_a as select 1")
    cur.execute("create task other as select 1")
    assert _show_names(cur, "show tasks like 'LOAD%'") == ["LOAD_A"]


def test_show_tasks_like_is_case_insensitive(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task load_a as select 1")
    assert _show_names(cur, "show tasks like 'load%'") == ["LOAD_A"]


def test_show_tasks_starts_with(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task load_a as select 1")
    cur.execute("create task other as select 1")
    assert _show_names(cur, "show tasks starts with 'LOAD'") == ["LOAD_A"]


def test_show_tasks_starts_with_is_case_sensitive(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task load_a as select 1")
    assert _show_names(cur, "show tasks starts with 'load'") == []


def test_show_tasks_root_only(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task root as select 1")
    cur.execute("create task child after root as select 1")
    assert _show_names(cur, "show tasks root only") == ["ROOT"]


def test_show_tasks_limit(cur: snowflake.connector.cursor.SnowflakeCursor):
    for name in ("a", "b", "c"):
        cur.execute(f"create task {name} as select 1")
    assert _show_names(cur, "show tasks limit 2") == ["A", "B"]


def test_show_tasks_limit_from(cur: snowflake.connector.cursor.SnowflakeCursor):
    for name in ("a", "b", "c"):
        cur.execute(f"create task {name} as select 1")
    # the FROM name itself is included, despite the docs saying "following", as live Snowflake does
    assert _show_names(cur, "show tasks limit 1 from 'B'") == ["B"]


def test_show_tasks_root_only_excludes_finalizer(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task root as select 1")
    cur.execute("create task fin finalize = root as select 1")
    assert _show_names(cur, "show tasks root only") == ["ROOT"]


def test_show_tasks_defaults_to_current_schema(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create schema schema2")
    cur.execute("create task schema2.other as select 1")
    cur.execute("use schema schema1")
    cur.execute("create task t1 as select 1")
    assert _show_names(cur, "show tasks") == ["T1"]


def test_show_tasks_without_current_schema_uses_current_database(_fakesnow: None):
    with snowflake.connector.connect(database="db1", schema="schema1") as conn, conn.cursor() as cur:
        cur.execute("create task t1 as select 1")
        cur.execute("create schema schema2")
        cur.execute("create task schema2.t2 as select 1")
    with snowflake.connector.connect(database="db1") as conn, conn.cursor() as cur:
        assert _show_names(cur, "show tasks") == ["T1", "T2"]


def test_show_tasks_in_missing_schema_raises(cur: snowflake.connector.cursor.SnowflakeCursor):
    with pytest.raises(
        snowflake.connector.errors.ProgrammingError, match=r"Schema 'DB1\.NOPE' does not exist or not authorized\."
    ):
        cur.execute("show tasks in schema nope")


def test_show_tasks_skips_other_database(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create database db2")
    cur.execute("create schema db2.schema1")
    cur.execute("use schema db1.schema1")
    cur.execute("create task db2.schema1.other as select 1")
    cur.execute("create task t1 as select 1")
    assert _show_names(cur, "show tasks") == ["T1"]


def test_show_tasks_in_database(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create database db2")
    cur.execute("create schema db2.schema1")
    cur.execute("use schema db1.schema1")
    cur.execute("create task db2.schema1.other as select 1")
    cur.execute("create task t1 as select 1")
    assert _show_names(cur, "show tasks in database db2") == ["OTHER"]


def test_show_tasks_in_schema_scopes_same_named_tasks(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create schema schema2")
    cur.execute("use schema schema1")
    cur.execute("create task schema1.t1 as select 1")
    cur.execute("create task schema2.t1 comment = 'two' as select 1")
    cur.execute("show tasks in schema schema2")
    names = [d.name for d in cur.description]
    assert [row[names.index("comment")] for row in cur.fetchall()] == ["two"]


def test_show_tasks_in_bare_schema_name_scopes_same_named_tasks(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create schema schema2")
    cur.execute("use schema schema1")
    cur.execute("create task schema1.t1 as select 1")
    cur.execute("create task schema2.t1 comment = 'two' as select 1")
    cur.execute("show tasks in schema2")
    names = [d.name for d in cur.description]
    assert [row[names.index("comment")] for row in cur.fetchall()] == ["two"]


def test_show_tasks_in_schema_without_name_uses_current_schema(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create schema schema2")
    cur.execute("use schema schema1")
    cur.execute("create task schema1.t1 as select 1")
    cur.execute("create task schema2.t2 as select 1")
    assert _show_names(cur, "show tasks in schema") == ["T1"]


def test_show_tasks_in_schema_with_database(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create database db2")
    cur.execute("create schema db2.schema1")
    cur.execute("use schema db1.schema1")
    cur.execute("create task db2.schema1.other as select 1")
    cur.execute("create task t1 as select 1")
    assert _show_names(cur, "show tasks in schema db2.schema1") == ["OTHER"]


def test_show_tasks_in_account(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create database db2")
    cur.execute("create schema db2.schema1")
    cur.execute("use schema db1.schema1")
    cur.execute("create task db2.schema1.other as select 1")
    cur.execute("create task t1 as select 1")
    assert _show_names(cur, "show tasks in account") == ["T1", "OTHER"]


def test_show_tasks_without_current_database_uses_account(_fakesnow: None):
    with snowflake.connector.connect() as conn, conn.cursor() as cur:
        cur.execute("create database db1")
        cur.execute("create schema db1.schema1")
        cur.execute("create task db1.schema1.t1 as select 1")
        assert _show_names(cur, "show tasks") == ["T1"]


def test_show_tasks_in_database_without_current_database_uses_account(_fakesnow: None):
    with snowflake.connector.connect() as conn, conn.cursor() as cur:
        cur.execute("create database db1")
        cur.execute("create schema db1.schema1")
        cur.execute("create task db1.schema1.t1 as select 1")
    with snowflake.connector.connect() as conn, conn.cursor() as cur:
        assert _show_names(cur, "show tasks in database") == ["T1"]


def test_show_tasks_in_schema_without_current_database_uses_account(_fakesnow: None):
    with snowflake.connector.connect() as conn, conn.cursor() as cur:
        cur.execute("create database db1")
        cur.execute("create schema db1.schema1")
        cur.execute("create task db1.schema1.t1 as select 1")
    with snowflake.connector.connect() as conn, conn.cursor() as cur:
        assert _show_names(cur, "show tasks in schema") == ["T1"]


def test_show_tasks_in_schema_without_current_schema_uses_current_database(_fakesnow: None):
    with snowflake.connector.connect() as conn, conn.cursor() as cur:
        cur.execute("create database db1")
        # no PUBLIC schema, so a connection to the database has no current schema
        cur.execute("drop schema if exists db1.public")
        cur.execute("create schema db1.schema1")
        cur.execute("create schema db1.schema2")
        cur.execute("create task db1.schema1.t1 as select 1")
        cur.execute("create task db1.schema2.t2 as select 1")
    with snowflake.connector.connect(database="db1") as conn, conn.cursor() as cur:
        assert _show_names(cur, "show tasks in schema") == ["T1", "T2"]


def test_show_tasks_in_account_without_databases_is_empty(_fakesnow: None):
    with snowflake.connector.connect() as conn, conn.cursor() as cur:
        cur.execute("show tasks in account")
        assert cur.fetchall() == []


def test_describe_task_row(dcur: snowflake.connector.cursor.DictCursor):
    dcur.execute("create task t1 schedule = '5 minute' comment = 'c' as select 1")
    dcur.execute("show tasks")
    (shown,) = dcur.fetchall()
    dcur.execute("describe task t1")
    assert dcur.fetchall() == [shown]


def test_describe_task_columns(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task t1 as select 1")
    cur.execute("desc task t1")
    assert [d.name for d in cur.description] == SHOW_TASKS_COLUMNS


@pytest.mark.parametrize("comment", ["/* inspect task */ ", "-- inspect task\n"])
def test_describe_task_after_leading_comment(cur: snowflake.connector.cursor.SnowflakeCursor, comment: str):
    cur.execute("create task t1 as select 1")
    cur.execute(f"{comment}desc task t1")
    assert [d.name for d in cur.description] == SHOW_TASKS_COLUMNS


@pytest.mark.parametrize("comment", ["/* inspect */", "-- inspect\n"])
def test_describe_task_with_comment_between_keywords(cur: snowflake.connector.cursor.SnowflakeCursor, comment: str):
    cur.execute("create task t1 as select 1")
    cur.execute(f"desc {comment} task t1")
    assert [d.name for d in cur.description] == SHOW_TASKS_COLUMNS


def test_describe_task_via_execute_string(conn: snowflake.connector.SnowflakeConnection):
    conn.cursor().execute("create task t1 as select 1")
    (cur,) = conn.execute_string("desc task t1")
    assert [d.name for d in cur.description] == SHOW_TASKS_COLUMNS


def test_describe_task_then_another_statement_via_execute_string(conn: snowflake.connector.SnowflakeConnection):
    conn.cursor().execute("create task t1 as select 1")
    _, cur = conn.execute_string("desc task t1; select 1")
    assert cur.fetchall() == [(1,)]


def test_describe_task_after_another_statement_via_execute_string(conn: snowflake.connector.SnowflakeConnection):
    conn.cursor().execute("create task t1 as select 1")
    _, cur = conn.execute_string("select 1; desc task t1")
    assert [d.name for d in cur.description] == SHOW_TASKS_COLUMNS


def test_describe_missing_task_raises(cur: snowflake.connector.cursor.SnowflakeCursor):
    with pytest.raises(snowflake.connector.errors.ProgrammingError, match=r"Task 'DB1\.SCHEMA1\.NOPE' does not exist"):
        cur.execute("describe task nope")


def test_show_tasks_quoted_mixed_case_name(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute('create task "My Task" as select 1')
    assert _show(cur, "name") == [("My Task",)]


def test_describe_task_quoted_mixed_case_name(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute('create task "My Task" as select 1')
    cur.execute('describe task "My Task"')
    names = [d.name for d in cur.description]
    assert [row[names.index("name")] for row in cur.fetchall()] == ["My Task"]


def test_show_tasks_in_account_skips_database_without_tasks_table(conn: snowflake.connector.SnowflakeConnection):
    conn._duck_conn.execute("attach ':memory:' as plain")  # type: ignore[attr-defined]  # noqa: SLF001
    cur = conn.cursor()
    cur.execute("create task t1 as select 1")
    assert _show_names(cur, "show tasks in account") == ["T1"]


def _history(cur: snowflake.connector.cursor.SnowflakeCursor, columns: str, name: str | None = None) -> list[tuple]:
    where = f" where name = '{name.upper()}'" if name else ""
    history = "table(information_schema.task_history(result_limit => 1000))"
    cur.execute(f"select {columns} from {history}{where} order by scheduled_time")
    return cur.fetchall()


def _graph_runs(cur: snowflake.connector.cursor.SnowflakeCursor, columns: str) -> list[tuple]:
    graphs = "table(information_schema.complete_task_graphs(result_limit => 1000))"
    cur.execute(f"select {columns} from {graphs} order by scheduled_time")
    return cur.fetchall()


def _log(cur: snowflake.connector.cursor.SnowflakeCursor) -> list[str]:
    cur.execute("select name from log order by seq")
    return [name for (name,) in cur.fetchall()]


def _logging_task(cur: snowflake.connector.cursor.SnowflakeCursor, name: str, clause: str = "") -> None:
    """Create a task that appends its name to the log table."""
    cur.execute(f"create task {name} {clause} as insert into log (name) values ('{name}')")


@pytest.fixture
def log(cur: snowflake.connector.cursor.SnowflakeCursor) -> None:
    cur.execute("create table log (seq int autoincrement, name varchar)")


def test_execute_task_status(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task t1 as select 1")
    cur.execute("execute task t1")
    assert cur.fetchall() == [("Task T1 is scheduled to run immediately.",)]


def test_execute_task_runs_body(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create table t (i int)")
    cur.execute("create task t1 as insert into t values (1)")
    cur.execute("execute task t1")
    cur.execute("select * from t")
    assert cur.fetchall() == [(1,)]


def test_execute_task_runs_graph_in_order(cur: snowflake.connector.cursor.SnowflakeCursor, log: None):
    _logging_task(cur, "root")
    _logging_task(cur, "child", "after root")
    _logging_task(cur, "grandchild", "after child")
    _enable_dependents(cur, "root")
    cur.execute("execute task root")
    assert _log(cur) == ["root", "child", "grandchild"]


def test_execute_task_failed_body_does_not_raise(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task t1 as select * from missing")
    cur.execute("execute task t1")
    assert cur.fetchall() == [("Task T1 is scheduled to run immediately.",)]


def test_execute_task_failed_task_skips_dependents(cur: snowflake.connector.cursor.SnowflakeCursor, log: None):
    cur.execute("create task root as select * from missing")
    _logging_task(cur, "child", "after root")
    _enable_dependents(cur, "root")
    cur.execute("execute task root")
    assert _log(cur) == []


def test_execute_task_finalizer_runs_after_failure(cur: snowflake.connector.cursor.SnowflakeCursor, log: None):
    cur.execute("create task root as select * from missing")
    _logging_task(cur, "child", "after root")
    _logging_task(cur, "fin", "finalize = root")
    _enable_dependents(cur, "root")
    cur.execute("execute task root")
    assert _log(cur) == ["fin"]


def test_execute_task_when_false_skips_task_and_dependents(cur: snowflake.connector.cursor.SnowflakeCursor, log: None):
    _logging_task(cur, "root")
    _logging_task(cur, "child", "after root when 1 = 2")
    _logging_task(cur, "grandchild", "after child")
    _enable_dependents(cur, "root")
    cur.execute("execute task root")
    assert _log(cur) == ["root"]


def test_execute_task_root_when_false_skips_finalizer(cur: snowflake.connector.cursor.SnowflakeCursor, log: None):
    _logging_task(cur, "root", "when 1 = 2")
    _logging_task(cur, "fin", "finalize = root")
    _enable_dependents(cur, "root")
    cur.execute("execute task root")
    assert _log(cur) == []


def test_execute_task_suspended_child_is_skipped(cur: snowflake.connector.cursor.SnowflakeCursor, log: None):
    _logging_task(cur, "root")
    _logging_task(cur, "child", "after root")
    cur.execute("execute task root")
    assert _log(cur) == ["root"]


def test_execute_task_runs_children_before_grandchildren(cur: snowflake.connector.cursor.SnowflakeCursor, log: None):
    _logging_task(cur, "root")
    _logging_task(cur, "m", "after root")
    _logging_task(cur, "z", "after root")
    _logging_task(cur, "a", "after m")
    _enable_dependents(cur, "root")
    cur.execute("execute task root")
    assert _log(cur) == ["root", "m", "z", "a"]


def test_execute_task_dependent_of_suspended_child_does_not_run(
    cur: snowflake.connector.cursor.SnowflakeCursor, log: None
):
    _logging_task(cur, "root")
    _logging_task(cur, "child", "after root")
    _logging_task(cur, "grandchild", "after child")
    cur.execute("alter task grandchild resume")
    cur.execute("execute task root")
    assert _log(cur) == ["root"]


def test_execute_task_join_with_all_predecessors_suspended_is_skipped(
    cur: snowflake.connector.cursor.SnowflakeCursor, log: None
):
    _logging_task(cur, "root")
    _logging_task(cur, "a", "after root")
    _logging_task(cur, "b", "after root")
    _logging_task(cur, "c", "after a, b")
    cur.execute("alter task c resume")
    cur.execute("execute task root")
    assert _log(cur) == ["root"]


def test_execute_task_join_ignores_suspended_predecessor_whose_predecessors_are_suspended(
    cur: snowflake.connector.cursor.SnowflakeCursor, log: None
):
    _logging_task(cur, "root")
    _logging_task(cur, "a", "after root")
    _logging_task(cur, "b", "after root")
    _logging_task(cur, "c", "after a, b")
    _logging_task(cur, "d", "after root, c")
    cur.execute("alter task d resume")
    cur.execute("execute task root")
    assert _log(cur) == ["root", "d"]


def test_execute_task_join_ignores_suspended_predecessor_after_failure(
    cur: snowflake.connector.cursor.SnowflakeCursor, log: None
):
    _logging_task(cur, "root")
    cur.execute("create task a after root as select * from missing")
    _logging_task(cur, "c", "after a")
    _logging_task(cur, "d", "after root, c")
    cur.execute("alter task a resume")
    cur.execute("alter task d resume")
    cur.execute("execute task root")
    assert _log(cur) == ["root", "d"]


def test_execute_task_skipped_root_skips_dependent_of_suspended_child(
    cur: snowflake.connector.cursor.SnowflakeCursor, log: None
):
    _logging_task(cur, "root", "warehouse = wh when 1 = 2")
    _logging_task(cur, "a", "after root")
    _logging_task(cur, "b", "after a")
    cur.execute("alter task b resume")
    cur.execute("execute task root")
    assert _log(cur) == []


def test_execute_task_join_with_one_predecessor_resumed_runs(
    cur: snowflake.connector.cursor.SnowflakeCursor, log: None
):
    _logging_task(cur, "root")
    _logging_task(cur, "a", "after root")
    _logging_task(cur, "b", "after root")
    _logging_task(cur, "c", "after a, b")
    cur.execute("alter task a resume")
    cur.execute("alter task c resume")
    cur.execute("execute task root")
    assert _log(cur) == ["root", "a", "c"]


def test_execute_task_after_drop_split_join_raises(cur: snowflake.connector.cursor.SnowflakeCursor):
    # A and B become roots when their predecessor is dropped, and C keeps both as predecessors
    cur.execute("create task root as select 1")
    cur.execute("create task a after root as select 1")
    cur.execute("create task b after root as select 1")
    cur.execute("create task c after a, b as select 1")
    cur.execute("drop task root")
    with pytest.raises(
        snowflake.connector.errors.ProgrammingError,
        match=r"Unreachable task DB1\.SCHEMA1\.B found\. The graph has more than one root task \(one without "
        r"predecessors\)\.",
    ) as e:
        cur.execute("execute task a")
    assert (e.value.errno, e.value.sqlstate) == (91419, "22000")


def test_execute_task_after_remove_after_split_join_raises(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task root as select 1")
    cur.execute("create task a after root as select 1")
    cur.execute("create task b after root as select 1")
    cur.execute("create task c after a, b as select 1")
    cur.execute("alter task a remove after root")
    with pytest.raises(snowflake.connector.errors.ProgrammingError, match=r"Unreachable task DB1\.SCHEMA1\.A found"):
        cur.execute("execute task root")


def test_execute_task_with_predecessors_in_different_graphs_raises(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task r1 as select 1")
    cur.execute("create task r2 as select 1")
    cur.execute("create task c after r1, r2 as select 1")
    with pytest.raises(snowflake.connector.errors.ProgrammingError, match=r"Unreachable task DB1\.SCHEMA1\.R2 found"):
        cur.execute("execute task r1")


def test_execute_task_in_cycle_raises(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task a as select 1")
    cur.execute("create task b after a as select 1")
    cur.execute("alter task a add after b")
    with pytest.raises(snowflake.connector.errors.ProgrammingError, match="cannot be called on non-root task"):
        cur.execute("execute task a")


def _descendant_cycle(cur: snowflake.connector.cursor.SnowflakeCursor) -> None:
    cur.execute("create task root as select 1")
    cur.execute("create task a after root as select 1")
    cur.execute("create task b after a as select 1")
    cur.execute("alter task a add after b")


def test_execute_task_descendant_cycle_raises(cur: snowflake.connector.cursor.SnowflakeCursor):
    _descendant_cycle(cur)
    with pytest.raises(
        snowflake.connector.errors.ProgrammingError, match=r"Graph has at least one cycle containing task A\."
    ):
        cur.execute("execute task root")


def test_execute_task_descendant_cycle_error_code(cur: snowflake.connector.cursor.SnowflakeCursor):
    _descendant_cycle(cur)
    with pytest.raises(snowflake.connector.errors.ProgrammingError) as e:
        cur.execute("execute task root")
    assert (e.value.errno, e.value.sqlstate) == (91445, "22000")


def test_execute_task_descendant_cycle_runs_nothing(cur: snowflake.connector.cursor.SnowflakeCursor):
    _descendant_cycle(cur)
    with pytest.raises(snowflake.connector.errors.ProgrammingError):
        cur.execute("execute task root")
    cur.execute("select count(*) from table(information_schema.task_history())")
    assert cur.fetchall() == [(0,)]


def test_execute_task_suspended_child_has_no_history_row(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task root as select 1")
    cur.execute("create task child after root as select 1")
    cur.execute("execute task root")
    assert _history(cur, "name") == [("ROOT",)]


def test_execute_child_task_raises(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task root as select 1")
    cur.execute("create task child after root as select 1")
    with pytest.raises(
        snowflake.connector.errors.ProgrammingError,
        match=r"Execute task cannot be called on non-root task DB1\.SCHEMA1\.CHILD\. Call EXECUTE TASK on the root "
        r"task of its graph instead\.",
    ) as e:
        cur.execute("execute task child")
    assert (e.value.errno, e.value.sqlstate) == (91435, "22000")


def test_execute_missing_task_raises(cur: snowflake.connector.cursor.SnowflakeCursor):
    with pytest.raises(snowflake.connector.errors.ProgrammingError, match=r"Task 'DB1\.SCHEMA1\.NOPE' does not exist"):
        cur.execute("execute task nope")


def test_execute_task_runs_in_own_session(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create table t (s varchar)")
    cur.execute("create task t1 as insert into db1.schema1.t select current_schema()")
    cur.execute("create schema s2")
    cur.execute("execute task schema1.t1")
    cur.execute("select * from schema1.t")
    assert cur.fetchall() == [("SCHEMA1",)]


def test_execute_task_runs_as_owner_role(_fakesnow: None):
    with snowflake.connector.connect(database="db1", schema="schema1", role="SYSADMIN") as conn:
        cur = conn.cursor()
        cur.execute("create table t (r varchar)")
        cur.execute("create task t1 as insert into t select current_role()")
    with snowflake.connector.connect(database="db1", schema="schema1") as conn:
        cur = conn.cursor()
        cur.execute("execute task t1")
        cur.execute("select * from t")
        assert cur.fetchall() == [("SYSADMIN",)]


def test_execute_task_leaves_callers_transaction_uncommitted(_fakesnow: None):
    with snowflake.connector.connect(database="db1", schema="schema1", autocommit=False) as conn:
        cur = conn.cursor()
        cur.execute("create table t (i int)")
        cur.execute("create task t1 as select 1")
        conn.commit()
        cur.execute("insert into t values (1)")
        cur.execute("execute task t1")
        conn.rollback()
        cur.execute("select i from t")
        assert cur.fetchall() == []


def test_execute_task_doesnt_see_callers_uncommitted_writes(_fakesnow: None):
    with snowflake.connector.connect(database="db1", schema="schema1", autocommit=False) as conn:
        cur = conn.cursor()
        cur.execute("create table t (i int)")
        cur.execute("create table copied (i int)")
        cur.execute("create task t1 as insert into copied select * from t")
        conn.commit()
        cur.execute("insert into t values (1)")
        cur.execute("execute task t1")
        conn.rollback()
        cur.execute("select i from copied")
        assert cur.fetchall() == []


def test_execute_task_quoted_name(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create table t (i int)")
    cur.execute('create task "My Task" as insert into t values (1)')
    cur.execute('execute task "My Task"')
    cur.execute("select * from t")
    assert cur.fetchall() == [(1,)]


def test_execute_task_with_plain_connection_raises():
    fs = FakeSnow()
    try:
        conn = FakeSnowflakeConnection(fs.duck_conn.cursor(), fs.results_cache, database="db1", schema="schema1")
        cur = conn.cursor()
        cur.execute("create task t1 as select 1")
        # the cursor reports the NotImplementedError as a ProgrammingError
        with pytest.raises(
            snowflake.connector.errors.ProgrammingError, match=r"needs a connection from fakesnow\.patch"
        ):
            cur.execute("execute task t1")
    finally:
        fs.close()


def test_execute_task_sets_last_committed_on(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task t1 as select 1")
    cur.execute("execute task t1")
    ((last_committed_on,),) = _show(cur, "last_committed_on")
    assert last_committed_on is not None


@pytest.mark.parametrize(
    "column, expected",
    [
        ("name", "T1"),
        ("schema_name", "SCHEMA1"),
        ("query_text", "select 1"),
        ("state", "SUCCEEDED"),
        ("error_code", None),
        ("scheduled_from", "EXECUTE TASK"),
        ("attempt_number", 1),
    ],
)
def test_execute_task_history_succeeded_row(
    cur: snowflake.connector.cursor.SnowflakeCursor, column: str, expected: object
):
    cur.execute("create task t1 as select 1")
    cur.execute("execute task t1")
    assert _history(cur, column) == [(expected,)]


def test_execute_task_history_failed_row_has_query_id(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task t1 as select * from missing")
    cur.execute("execute task t1")
    ((query_id,),) = _history(cur, "query_id")
    assert query_id is not None


def test_execute_task_history_failed_row_state(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task t1 as select * from missing")
    cur.execute("execute task t1")
    assert _history(cur, "state") == [("FAILED",)]


def test_execute_task_history_failed_row_has_error_code(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task t1 as select * from missing")
    cur.execute("execute task t1")
    assert _history(cur, "error_code") == [("002003",)]


def test_execute_task_history_failed_row_has_error_message(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task t1 as select * from missing")
    cur.execute("execute task t1")
    ((error_message,),) = _history(cur, "error_message")
    assert "MISSING" in error_message


@pytest.mark.parametrize("column, expected", [("state", "SKIPPED"), ("condition_text", "1 = 2"), ("query_id", None)])
def test_execute_task_history_when_false_row(
    cur: snowflake.connector.cursor.SnowflakeCursor, column: str, expected: str | None
):
    cur.execute("create task t1 warehouse = wh when 1 = 2 as select 1")
    cur.execute("execute task t1")
    assert _history(cur, column) == [(expected,)]


def test_execute_task_history_query_id_is_body_query(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task t1 as select 42 as answer")
    cur.execute("execute task t1")
    ((query_id,),) = _history(cur, "query_id")
    cur.execute(f"select * from table(result_scan('{query_id}'))")
    assert cur.fetchall() == [(42,)]


def test_execute_task_history_run_id_is_scheduled_time_in_ms(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task t1 as select 1")
    cur.execute("execute task t1")
    ((run_id, scheduled_time),) = _history(cur, "run_id, scheduled_time")
    assert run_id == int(scheduled_time.timestamp() * 1000)


def _run_graph_twice(cur: snowflake.connector.cursor.SnowflakeCursor) -> None:
    cur.execute("create task root as select 1")
    cur.execute("create task child after root as select 1")
    _enable_dependents(cur, "root")
    cur.execute("execute task root")
    cur.execute("execute task root")


def test_execute_task_graph_run_group_id_per_run(cur: snowflake.connector.cursor.SnowflakeCursor):
    _run_graph_twice(cur)
    cur.execute("select count(distinct graph_run_group_id) from table(information_schema.task_history())")
    assert cur.fetchall() == [(2,)]


def test_execute_task_run_id_per_run(cur: snowflake.connector.cursor.SnowflakeCursor):
    _run_graph_twice(cur)
    cur.execute("select count(distinct run_id) from table(information_schema.task_history())")
    assert cur.fetchall() == [(2,)]


def test_execute_task_history_root_task_id_shared(cur: snowflake.connector.cursor.SnowflakeCursor):
    _run_graph_twice(cur)
    cur.execute("select count(distinct root_task_id) from table(information_schema.task_history())")
    assert cur.fetchall() == [(1,)]


def test_execute_task_using_config_merges_config(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("""create task t1 config = '{"a": 1}' as select 1""")
    cur.execute("""execute task t1 using config = '{"b": 2}'""")
    assert indent(_history(cur, "config")) == [('{\n  "a": 1,\n  "b": 2\n}',)]


def test_execute_task_using_config_overrides_matching_field(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("""create task t1 config = '{"a": 1}' as select 1""")
    cur.execute("""execute task t1 using config = '{"a": 2}'""")
    assert indent(_history(cur, "config")) == [('{\n  "a": 2\n}',)]


def test_execute_task_using_config_merges_nested_fields(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("""create task t1 config = '{"env": "prod", "paths": {"logs": "l", "results": "r"}}' as select 1""")
    cur.execute("""execute task t1 using config = '{"paths": {"results": "t"}}'""")
    assert indent(_history(cur, "config")) == [
        ('{\n  "env": "prod",\n  "paths": {\n    "logs": "l",\n    "results": "t"\n  }\n}',)
    ]


def test_create_task_keeps_session_variable_in_body(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("set v = 'x'")
    cur.execute("create task t1 as select $v")
    assert _show(cur, "definition") == [("select $v",)]


def _execute_task_with_session_variable(cur: snowflake.connector.cursor.SnowflakeCursor) -> None:
    cur.execute("set v = 'x'")
    cur.execute("create task t1 as select $v")
    cur.execute("execute task t1")


def test_execute_task_with_session_variable_fails(cur: snowflake.connector.cursor.SnowflakeCursor):
    _execute_task_with_session_variable(cur)
    assert _history(cur, "state") == [("FAILED",)]


def test_execute_task_with_session_variable_error_message(cur: snowflake.connector.cursor.SnowflakeCursor):
    _execute_task_with_session_variable(cur)
    ((error_message,),) = _history(cur, "error_message")
    assert error_message.endswith("Session variable '$V' does not exist")


def test_execute_task_scheduled_by_user(_fakesnow: None):
    with snowflake.connector.connect(database="db1", schema="schema1", user="someone") as conn, conn.cursor() as cur:
        cur.execute("create task t1 as select 1")
        cur.execute("execute task t1")
        assert _history(cur, "scheduled_by_user") == [("SOMEONE",)]


def test_execute_task_history_records_root_config(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("""create task t1 config = '{"a": 1}' as select 1""")
    cur.execute("execute task t1")
    assert indent(_history(cur, "config")) == [('{\n  "a": 1\n}',)]


@pytest.mark.parametrize(
    "column, expected",
    [
        ("root_task_name", "ROOT"),
        ("schema_name", "SCHEMA1"),
        ("state", "SUCCEEDED"),
        ("scheduled_from", "EXECUTE TASK"),
        ("attempt_number", 1),
    ],
)
def test_execute_task_graph_run_succeeded(
    cur: snowflake.connector.cursor.SnowflakeCursor, column: str, expected: object
):
    cur.execute("create task root as select 1")
    cur.execute("execute task root")
    assert _graph_runs(cur, column) == [(expected,)]


@pytest.mark.parametrize(
    "column, expected", [("state", "FAILED"), ("first_error_task_name", "CHILD"), ("first_error_code", "002003")]
)
def test_execute_task_graph_run_failed_records_first_error(
    cur: snowflake.connector.cursor.SnowflakeCursor, column: str, expected: object
):
    cur.execute("create task root as select 1")
    cur.execute("create task child after root as select * from missing")
    _enable_dependents(cur, "root")
    cur.execute("execute task root")
    assert _graph_runs(cur, column) == [(expected,)]


def test_complete_task_graphs_excludes_skipped_root(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task root warehouse = wh when 1 = 2 as select 1")
    cur.execute("execute task root")
    assert _graph_runs(cur, "state") == []


def test_task_history_keeps_skipped_root(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task root warehouse = wh when 1 = 2 as select 1")
    cur.execute("execute task root")
    assert _history(cur, "state") == [("SKIPPED",)]


def _run_and_drop_schema(cur: snowflake.connector.cursor.SnowflakeCursor) -> None:
    cur.execute("create schema s2")
    cur.execute("create task s2.t1 as select 1")
    cur.execute("execute task s2.t1")
    cur.execute("drop schema s2")


def test_drop_schema_drops_its_task_history(cur: snowflake.connector.cursor.SnowflakeCursor):
    _run_and_drop_schema(cur)
    assert _history(cur, "name") == []


def test_drop_schema_drops_its_task_graph_runs(cur: snowflake.connector.cursor.SnowflakeCursor):
    _run_and_drop_schema(cur)
    assert _graph_runs(cur, "root_task_name") == []


def test_execute_task_in_quoted_schema_finds_sibling(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute('create schema "Mixed"')
    cur.execute("create task sibling as select 1")
    cur.execute("create task t as alter task sibling set comment = 'done'")
    cur.execute("execute task t")
    assert _show(cur, "comment", "sibling") == [("done",)]


def test_execute_task_session_uses_task_warehouse(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create table seen (v varchar)")
    cur.execute("create task t1 warehouse = wh as insert into seen select current_warehouse()")
    cur.execute("execute task t1")
    cur.execute("select v from seen")
    assert cur.fetchall() == [("WH",)]


def test_execute_task_runs_siblings_by_name(cur: snowflake.connector.cursor.SnowflakeCursor, log: None):
    _logging_task(cur, "root")
    _logging_task(cur, "b", "after root")
    _logging_task(cur, "a", "after root")
    _enable_dependents(cur, "root")
    cur.execute("execute task root")
    assert _log(cur) == ["root", "a", "b"]


def test_execute_task_suspended_finalizer_is_skipped(cur: snowflake.connector.cursor.SnowflakeCursor, log: None):
    _logging_task(cur, "root")
    _logging_task(cur, "fin", "finalize = root")
    cur.execute("execute task root")
    assert _log(cur) == ["root"]


def test_execute_task_keeps_changes_made_by_its_body(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task t1 as alter task t1 set comment = 'changed'")
    cur.execute("execute task t1")
    assert _show(cur, "comment") == [("changed",)]


def test_execute_task_doesnt_see_writes_after_explicit_begin(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create table t (i int)")
    cur.execute("create table copied (i int)")
    cur.execute("create task t1 as insert into copied select * from t")
    cur.execute("begin")
    cur.execute("insert into t values (1)")
    cur.execute("execute task t1")
    cur.execute("rollback")
    cur.execute("select i from copied")
    assert cur.fetchall() == []


def test_execute_task_leaves_explicit_transaction_uncommitted(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create table t (i int)")
    cur.execute("create task t1 as select 1")
    cur.execute("begin")
    cur.execute("insert into t values (1)")
    cur.execute("execute task t1")
    cur.execute("rollback")
    cur.execute("select i from t")
    assert cur.fetchall() == []


def test_execute_task_history_survives_callers_rollback(_fakesnow: None):
    with snowflake.connector.connect(database="db1", schema="schema1", autocommit=False) as conn:
        cur = conn.cursor()
        cur.execute("create table t (i int)")
        cur.execute("create task t1 as select 1")
        cur.execute("insert into t values (1)")
        cur.execute("execute task t1")
        cur.execute("insert into t values (2)")
        conn.rollback()
        assert _history(cur, "name") == [("T1",)]


def test_execute_task_using_dollar_quoted_config(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task t1 as select 1")
    cur.execute("""execute task t1 using config = $${"a": 1}$$""")
    assert indent(_history(cur, "config")) == [('{\n  "a": 1\n}',)]


def test_execute_task_quoted_mixed_case_graph(cur: snowflake.connector.cursor.SnowflakeCursor, log: None):
    cur.execute("""create task "My Root" as insert into log (name) values ('My Root')""")
    cur.execute("""create task "my child" after "My Root" as insert into log (name) values ('my child')""")
    _enable_dependents(cur, '"My Root"')
    cur.execute('execute task "My Root"')
    assert _log(cur) == ["My Root", "my child"]


def test_execute_task_status_quoted_name(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute('create task "My Task" as select 1')
    cur.execute('execute task "My Task"')
    assert cur.fetchall() == [("Task My Task is scheduled to run immediately.",)]


def test_execute_quoted_child_task_raises(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute('create task "My Root" as select 1')
    cur.execute('create task "my child" after "My Root" as select 1')
    with pytest.raises(
        snowflake.connector.errors.ProgrammingError,
        match=r"Execute task cannot be called on non-root task DB1\.SCHEMA1\.my child\.",
    ):
        cur.execute('execute task "my child"')


def _create_root_returning_value(cur: snowflake.connector.cursor.SnowflakeCursor) -> None:
    cur.execute("create table seen (v varchar)")
    cur.execute("create task root as select system$set_return_value('from root')")


def test_set_return_value_recorded_in_history(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task t1 as select system$set_return_value('done')")
    cur.execute("execute task t1")
    assert _history(cur, "return_value") == [("done",)]


def test_set_return_value_after_other_column(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task t1 as select 'unrelated', system$set_return_value('done')")
    cur.execute("execute task t1")
    assert _history(cur, "return_value") == [("done",)]


def test_set_return_value_inside_expression(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task t1 as select coalesce(system$set_return_value('done'), 'other')")
    cur.execute("execute task t1")
    assert _history(cur, "return_value") == [("done",)]


def test_set_return_value_in_scalar_subquery(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task t1 as select (select system$set_return_value('x'))")
    cur.execute("execute task t1")
    assert _history(cur, "return_value") == [("x",)]


def test_set_return_value_of_column_fails(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create table input_table (v varchar)")
    cur.execute("insert into input_table values ('x')")
    cur.execute("create task t1 as select system$set_return_value(v) from input_table")
    cur.execute("execute task t1")
    assert _history(cur, "state") == [("FAILED",)]


def test_set_return_value_of_column_error(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create table input_table (v varchar)")
    cur.execute("create task t1 as select system$set_return_value(v) from input_table")
    cur.execute("execute task t1")
    ((error_message,),) = _history(cur, "error_message")
    assert error_message.startswith(
        "SQL compilation error:\nargument 1 to function SYSTEM$SET_RETURN_VALUE needs to be constant, found '"
    )


def test_set_return_value_of_number_fails(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task t1 as select system$set_return_value(42)")
    cur.execute("execute task t1")
    assert _history(cur, "error_message") == [
        (
            "SQL compilation error:\nargument 1 to function SYSTEM$SET_RETURN_VALUE needs to be constant, found "
            "'CAST(42 AS VARCHAR(134217728))'",
        )
    ]


def test_set_return_value_of_concatenation(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task t1 as select system$set_return_value('a' || 'b')")
    cur.execute("execute task t1")
    assert _history(cur, "return_value") == [("ab",)]


def test_set_return_value_last_call_wins(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task t1 as select system$set_return_value('first'), system$set_return_value('second')")
    cur.execute("execute task t1")
    assert _history(cur, "return_value") == [("second",)]


def test_set_return_value_without_rows(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create table empty_table (v varchar)")
    cur.execute("create task t1 as select system$set_return_value('x') from empty_table")
    cur.execute("execute task t1")
    assert _history(cur, "return_value") == [("x",)]


def test_set_return_value_in_insert(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create table seen (v varchar)")
    cur.execute("create task t1 as insert into seen select system$set_return_value('done')")
    cur.execute("execute task t1")
    assert _history(cur, "return_value") == [("done",)]


def test_set_return_value_returns_success(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create table seen (v varchar)")
    cur.execute("create task t1 as insert into seen select system$set_return_value('done')")
    cur.execute("execute task t1")
    cur.execute("select v from seen")
    assert cur.fetchall() == [("SUCCESS",)]


def test_predecessor_return_value_in_child(cur: snowflake.connector.cursor.SnowflakeCursor):
    _create_root_returning_value(cur)
    cur.execute("create task child after root as insert into seen select system$get_predecessor_return_value('ROOT')")
    _enable_dependents(cur, "root")
    cur.execute("execute task root")
    cur.execute("select v from seen")
    assert cur.fetchall() == [("from root",)]


def test_predecessor_return_value_from_ancestor(cur: snowflake.connector.cursor.SnowflakeCursor):
    _create_root_returning_value(cur)
    cur.execute("create task mid after root as select 1")
    cur.execute("create task leaf after mid as insert into seen select system$get_predecessor_return_value('ROOT')")
    _enable_dependents(cur, "root")
    cur.execute("execute task root")
    cur.execute("select v from seen")
    assert cur.fetchall() == [("from root",)]


def test_set_return_value_passes_on_predecessor_value_with_suffix(cur: snowflake.connector.cursor.SnowflakeCursor):
    _create_root_returning_value(cur)
    cur.execute(
        "create task child after root as "
        "select system$set_return_value(system$get_predecessor_return_value('ROOT') || 'y')"
    )
    _enable_dependents(cur, "root")
    cur.execute("execute task root")
    assert _history(cur, "return_value", "child") == [("from rooty",)]


def test_predecessor_return_value_without_argument(cur: snowflake.connector.cursor.SnowflakeCursor):
    _create_root_returning_value(cur)
    cur.execute("create task child after root as insert into seen select system$get_predecessor_return_value()")
    _enable_dependents(cur, "root")
    cur.execute("execute task root")
    cur.execute("select v from seen")
    assert cur.fetchall() == [("from root",)]


def test_predecessor_return_value_quoted_name(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create table seen (v varchar)")
    cur.execute("""create task "My Root" as select system$set_return_value('quoted')""")
    cur.execute(
        """create task child after "My Root" as
        insert into seen select system$get_predecessor_return_value('My Root')"""
    )
    _enable_dependents(cur, '"My Root"')
    cur.execute('execute task "My Root"')
    cur.execute("select v from seen")
    assert cur.fetchall() == [("quoted",)]


def _unmatched_predecessor_lookup(cur: snowflake.connector.cursor.SnowflakeCursor) -> None:
    _create_root_returning_value(cur)
    # task names are case-sensitive, so 'root' doesn't match ROOT
    cur.execute("create task child after root as insert into seen select system$get_predecessor_return_value('root')")
    _enable_dependents(cur, "root")
    cur.execute("execute task root")


def test_predecessor_return_value_suspended_predecessor_name_fails(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create table seen (v varchar)")
    cur.execute("create task root as select 1")
    cur.execute("create task a after root as select system$set_return_value('a')")
    cur.execute("create task b after root as select 1")
    cur.execute("create task c after a, b as insert into seen select system$get_predecessor_return_value('B')")
    cur.execute("alter task a resume")
    cur.execute("alter task c resume")
    cur.execute("execute task root")
    assert _history(cur, "state", "c") == [("FAILED",)]


def test_predecessor_return_value_unmatched_name_error(cur: snowflake.connector.cursor.SnowflakeCursor):
    _unmatched_predecessor_lookup(cur)
    assert _history(cur, "error_message", "child") == [
        (
            "The predecessor root specified in call to system$get_predecessor_return_value() is not a valid or active "
            "predecessor for this task. Note that system$get_predecessor_return_value() does not support fully "
            "qualified names including database or schema.",
        )
    ]


def test_predecessor_return_value_unmatched_name_error_code(cur: snowflake.connector.cursor.SnowflakeCursor):
    _unmatched_predecessor_lookup(cur)
    assert _history(cur, "error_code", "child") == [("091451",)]


def test_predecessor_return_value_unmatched_name_fails(cur: snowflake.connector.cursor.SnowflakeCursor):
    _unmatched_predecessor_lookup(cur)
    assert _history(cur, "state", "child") == [("FAILED",)]


def test_predecessor_return_value_unmatched_name_writes_nothing(cur: snowflake.connector.cursor.SnowflakeCursor):
    _unmatched_predecessor_lookup(cur)
    cur.execute("select count(*) from seen")
    assert cur.fetchall() == [(0,)]


def test_predecessor_return_value_without_argument_and_one_enabled_predecessor(
    cur: snowflake.connector.cursor.SnowflakeCursor,
):
    cur.execute("create table seen (v varchar)")
    cur.execute("create task root as select 1")
    cur.execute("create task a after root as select system$set_return_value('from a')")
    cur.execute("create task b after root as select 1")
    cur.execute("create task c after a, b as insert into seen select system$get_predecessor_return_value()")
    cur.execute("alter task a resume")
    cur.execute("alter task c resume")
    cur.execute("execute task root")
    cur.execute("select v from seen")
    assert cur.fetchall() == [("from a",)]


def test_predecessor_return_value_in_when(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create table seen (v varchar)")
    cur.execute("create task root as select system$set_return_value('go')")
    cur.execute(
        """create task child after root
        when system$get_predecessor_return_value('ROOT') = 'go'
        as insert into seen values ('ran')"""
    )
    _enable_dependents(cur, "root")
    cur.execute("execute task root")
    cur.execute("select v from seen")
    assert cur.fetchall() == [("ran",)]


def test_get_task_graph_config(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create table seen (v varchar)")
    cur.execute("""create task t1 config = '{"a": 1}' as insert into seen select system$get_task_graph_config()""")
    cur.execute("execute task t1")
    cur.execute("select v from seen")
    assert indent(cur.fetchall()) == [('{\n  "a": 1\n}',)]


def test_get_task_graph_config_key(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create table seen (v varchar)")
    cur.execute("""create task t1 config = '{"a": "x"}' as insert into seen select system$get_task_graph_config('a')""")
    cur.execute("execute task t1")
    cur.execute("select v from seen")
    assert cur.fetchall() == [("x",)]


def _config_value(cur: snowflake.connector.cursor.SnowflakeCursor, config: str, path: str) -> list[tuple]:
    """The value a task sees for SYSTEM$GET_TASK_GRAPH_CONFIG(path), given the graph's config."""
    cur.execute("create or replace table seen (v varchar)")
    body = f"insert into seen select system$get_task_graph_config('{path}')"
    cur.execute(f"create or replace task t1 config = $${config}$$ as {body}")
    cur.execute("execute task t1")
    cur.execute("select v from seen")
    return cur.fetchall()


def test_get_task_graph_config_nested_path(cur: snowflake.connector.cursor.SnowflakeCursor):
    assert _config_value(cur, '{"sink": {"table": "events"}}', "sink.table") == [("events",)]


def test_get_task_graph_config_array_index_path(cur: snowflake.connector.cursor.SnowflakeCursor):
    assert _config_value(cur, '{"dirs": ["a", "b"]}', "dirs[1]") == [("b",)]


def test_get_task_graph_config_object_path(cur: snowflake.connector.cursor.SnowflakeCursor):
    assert indent(_config_value(cur, '{"o": {"p": 1}}', "o")) == [('{\n  "p": 1\n}',)]


def test_get_task_graph_config_missing_path(cur: snowflake.connector.cursor.SnowflakeCursor):
    assert _config_value(cur, '{"sink": {"table": "events"}}', "sink.missing") == [(None,)]


def test_get_task_graph_config_using_config(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create table seen (v varchar)")
    cur.execute("""create task t1 config = '{"a": 1}' as insert into seen select system$get_task_graph_config()""")
    cur.execute("""execute task t1 using config = '{"b": 2}'""")
    cur.execute("select v from seen")
    assert indent(cur.fetchall()) == [('{\n  "a": 1,\n  "b": 2\n}',)]


def test_current_user_task_name(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create table seen (v varchar)")
    cur.execute("create task t1 as insert into seen select system$current_user_task_name()")
    cur.execute("execute task t1")
    cur.execute("select v from seen")
    assert cur.fetchall() == [("DB1.SCHEMA1.T1",)]


def test_task_runtime_info_current_root_task_name(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create table seen (v varchar)")
    cur.execute("create task root as select 1")
    cur.execute(
        "create task child after root as insert into seen select system$task_runtime_info('CURRENT_ROOT_TASK_NAME')"
    )
    _enable_dependents(cur, "root")
    cur.execute("execute task root")
    cur.execute("select v from seen")
    assert cur.fetchall() == [("DB1.SCHEMA1.ROOT",)]


def test_task_runtime_info_graph_run_group_id(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create table seen (v varchar)")
    cur.execute("create task t1 as insert into seen select system$task_runtime_info('CURRENT_TASK_GRAPH_RUN_GROUP_ID')")
    cur.execute("execute task t1")
    cur.execute("select v from seen")
    assert cur.fetchall() == _history(cur, "graph_run_group_id")


def test_task_functions_match_case_insensitively(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create table seen (v varchar)")
    cur.execute("create task t1 as insert into seen select SYSTEM$Current_User_Task_Name()")
    cur.execute("execute task t1")
    cur.execute("select v from seen")
    assert cur.fetchall() == [("DB1.SCHEMA1.T1",)]


def test_current_user_task_name_outside_task_is_empty(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("select system$current_user_task_name()")
    assert cur.fetchall() == [("",)]


def test_current_user_task_name_outside_task_column_name(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("select system$current_user_task_name()")
    assert [d.name for d in cur.description] == ["SYSTEM$CURRENT_USER_TASK_NAME()"]


@pytest.mark.parametrize(
    "call, function",
    [
        ("system$task_runtime_info('CURRENT_TASK_NAME')", "SYSTEM$TASK_RUNTIME_INFO"),
        ("system$get_task_graph_config()", "SYSTEM$GET_TASK_GRAPH_CONFIG"),
        ("system$get_predecessor_return_value('t')", "SYSTEM$GET_PREDECESSOR_RETURN_VALUE"),
        ("system$set_return_value('x')", "SYSTEM$SET_RETURN_VALUE"),
    ],
)
def test_task_function_outside_task_raises(cur: snowflake.connector.cursor.SnowflakeCursor, call: str, function: str):
    with pytest.raises(
        snowflake.connector.errors.ProgrammingError,
        match=rf"Function {re.escape(function)} must be called from within a task\.",
    ) as e:
        cur.execute(f"select {call}")
    assert (e.value.errno, e.value.sqlstate) == (91430, "22000")


def _runtime_info(cur: snowflake.connector.cursor.SnowflakeCursor, field: str) -> str:
    """The value a task sees for a TASK_RUNTIME_INFO field."""
    cur.execute("create or replace table seen (v varchar)")
    cur.execute(f"create or replace task t1 as insert into seen select system$task_runtime_info('{field}')")
    cur.execute("execute task t1")
    cur.execute("select v from seen")
    ((value,),) = cur.fetchall()
    return value


def test_task_runtime_info_current_task_name(cur: snowflake.connector.cursor.SnowflakeCursor):
    assert _runtime_info(cur, "CURRENT_TASK_NAME") == "DB1.SCHEMA1.T1"


def test_task_runtime_info_current_root_task_uuid(cur: snowflake.connector.cursor.SnowflakeCursor):
    value = _runtime_info(cur, "CURRENT_ROOT_TASK_UUID")
    assert _show(cur, "id", "t1") == [(value,)]


def test_task_runtime_info_original_scheduled_timestamp_format(cur: snowflake.connector.cursor.SnowflakeCursor):
    # in UTC, to the millisecond, with nine fractional digits
    value = _runtime_info(cur, "CURRENT_TASK_GRAPH_ORIGINAL_SCHEDULED_TIMESTAMP")
    assert re.fullmatch(r"\d{4}-\d\d-\d\d \d\d:\d\d:\d\d\.\d{3}000000Z", value)


def test_task_runtime_info_original_scheduled_timestamp(cur: snowflake.connector.cursor.SnowflakeCursor):
    value = _runtime_info(cur, "CURRENT_TASK_GRAPH_ORIGINAL_SCHEDULED_TIMESTAMP")
    ((scheduled,),) = _history(cur, "scheduled_time")
    # to the millisecond
    assert 0 <= (scheduled - datetime.fromisoformat(value)).total_seconds() < 0.001


def _ambiguous_predecessor_lookup(cur: snowflake.connector.cursor.SnowflakeCursor) -> None:
    cur.execute("create table seen (v varchar)")
    cur.execute("create task root as select 1")
    cur.execute("create task a after root as select system$set_return_value('a')")
    cur.execute("create task b after root as select system$set_return_value('b')")
    cur.execute("create task c after a, b as insert into seen select system$get_predecessor_return_value()")
    _enable_dependents(cur, "root")
    cur.execute("execute task root")


def test_get_predecessor_return_value_without_argument_and_several_predecessors_error(
    cur: snowflake.connector.cursor.SnowflakeCursor,
):
    _ambiguous_predecessor_lookup(cur)
    assert _history(cur, "error_message", "c") == [
        ("No predecessor task specified in call to system$get_predecessor_return_value().",)
    ]


def test_get_predecessor_return_value_without_argument_and_several_predecessors_error_code(
    cur: snowflake.connector.cursor.SnowflakeCursor,
):
    _ambiguous_predecessor_lookup(cur)
    assert _history(cur, "error_code", "c") == [("091450",)]


def test_get_predecessor_return_value_without_argument_and_several_predecessors_fails(
    cur: snowflake.connector.cursor.SnowflakeCursor,
):
    _ambiguous_predecessor_lookup(cur)
    assert _history(cur, "state", "c") == [("FAILED",)]


def test_get_predecessor_return_value_without_argument_and_several_predecessors_writes_nothing(
    cur: snowflake.connector.cursor.SnowflakeCursor,
):
    _ambiguous_predecessor_lookup(cur)
    cur.execute("select count(*) from seen")
    assert cur.fetchall() == [(0,)]


def _table_function(cur: snowflake.connector.cursor.SnowflakeCursor, sql: str) -> list[dict]:
    cur.execute(sql)
    names = [d.name for d in cur.description]
    return [dict(zip(names, row, strict=True)) for row in cur.fetchall()]


@pytest.mark.parametrize(
    "column, expected",
    [
        ("NAME", "T1"),
        ("DATABASE_NAME", "DB1"),
        ("SCHEMA_NAME", "SCHEMA1"),
        ("QUERY_TEXT", "select 1"),
        ("STATE", "SUCCEEDED"),
        ("GRAPH_VERSION", 1),
    ],
)
def test_task_history_succeeded_row(cur: snowflake.connector.cursor.SnowflakeCursor, column: str, expected: object):
    cur.execute("create task t1 as select 1")
    cur.execute("execute task t1")
    (row,) = _table_function(cur, "select * from table(information_schema.task_history())")
    assert row[column] == expected


def test_task_history_columns(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("select * from table(information_schema.task_history())")
    assert [d.name for d in cur.description] == [
        "QUERY_ID",
        "NAME",
        "DATABASE_NAME",
        "SCHEMA_NAME",
        "QUERY_TEXT",
        "CONDITION_TEXT",
        "STATE",
        "ERROR_CODE",
        "ERROR_MESSAGE",
        "SCHEDULED_TIME",
        "QUERY_START_TIME",
        "NEXT_SCHEDULED_TIME",
        "COMPLETED_TIME",
        "ROOT_TASK_ID",
        "GRAPH_VERSION",
        "RUN_ID",
        "RETURN_VALUE",
        "SCHEDULED_FROM",
        "ATTEMPT_NUMBER",
        "CONFIG",
        "QUERY_HASH",
        "QUERY_HASH_VERSION",
        "QUERY_PARAMETERIZED_HASH",
        "QUERY_PARAMETERIZED_HASH_VERSION",
        "GRAPH_RUN_GROUP_ID",
        "BACKFILL_INFO",
        "SPCS_JOB_ID",
        "SCHEDULED_BY_USER",
        "GRAPH_ID",
    ]


def test_task_history_failed_row_state(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task t1 as select * from missing")
    cur.execute("execute task t1")
    (row,) = _table_function(cur, "select * from table(information_schema.task_history())")
    assert row["STATE"] == "FAILED"


def test_task_history_failed_row_has_error_code(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task t1 as select * from missing")
    cur.execute("execute task t1")
    (row,) = _table_function(cur, "select * from table(information_schema.task_history())")
    assert row["ERROR_CODE"] == "002003"


def test_task_history_failed_row_has_error_message(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task t1 as select * from missing")
    cur.execute("execute task t1")
    (row,) = _table_function(cur, "select * from table(information_schema.task_history())")
    assert "MISSING" in row["ERROR_MESSAGE"]


def test_task_history_database_qualified(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task t1 as select 1")
    cur.execute("execute task t1")
    cur.execute("select name from table(db1.information_schema.task_history())")
    assert cur.fetchall() == [("T1",)]


def test_task_history_alias(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task t1 as select 1")
    cur.execute("execute task t1")
    cur.execute("select h.name from table(information_schema.task_history()) h")
    assert cur.fetchall() == [("T1",)]


def test_task_history_task_name_filter(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task t1 as select 1")
    cur.execute("create task t2 as select 1")
    cur.execute("execute task t1")
    cur.execute("execute task t2")
    cur.execute("select name from table(information_schema.task_history(task_name => 't2'))")
    assert cur.fetchall() == [("T2",)]


def test_task_history_error_only(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task ok as select 1")
    cur.execute("create task bad as select * from missing")
    cur.execute("execute task ok")
    cur.execute("execute task bad")
    cur.execute("select name from table(information_schema.task_history(error_only => true))")
    assert cur.fetchall() == [("BAD",)]


def test_task_history_result_limit(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task t1 as select 1")
    cur.execute("create task t2 as select 1")
    cur.execute("execute task t1")
    cur.execute("execute task t2")
    cur.execute("select name from table(information_schema.task_history(result_limit => 1))")
    assert cur.fetchall() == [("T2",)]


def test_task_history_newest_first(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task t1 as select 1")
    cur.execute("create task t2 as select 1")
    cur.execute("execute task t1")
    cur.execute("execute task t2")
    cur.execute("select name from table(information_schema.task_history())")
    assert cur.fetchall() == [("T2",), ("T1",)]


def test_task_history_scheduled_time_range(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task t1 as select 1")
    cur.execute("execute task t1")
    cur.execute("select name from table(information_schema.task_history(scheduled_time_range_start => '2999-01-01'))")
    assert cur.fetchall() == []


def test_task_history_root_task_id(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task t1 as select 1")
    cur.execute("create task t2 as select 1")
    cur.execute("execute task t1")
    cur.execute("execute task t2")
    ((root_id,),) = _show(cur, "id", "t1")
    cur.execute(f"select name from table(information_schema.task_history(root_task_id => '{root_id}'))")
    assert cur.fetchall() == [("T1",)]


def test_task_history_non_literal_argument_raises(cur: snowflake.connector.cursor.SnowflakeCursor):
    with pytest.raises(snowflake.connector.errors.ProgrammingError, match="literal"):
        cur.execute("select * from table(information_schema.task_history(task_name => lower('T1')))")


def test_task_history_query_id_result_scan(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task t1 as select 42 as answer")
    cur.execute("execute task t1")
    cur.execute("select query_id from table(information_schema.task_history())")
    ((query_id,),) = cur.fetchall()
    cur.execute(f"select * from table(result_scan('{query_id}'))")
    assert cur.fetchall() == [(42,)]


def test_task_history_survives_caller_rollback(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task t1 as select 1")
    cur.execute("begin")
    cur.execute("execute task t1")
    cur.execute("rollback")
    cur.execute("select name from table(information_schema.task_history())")
    assert cur.fetchall() == [("T1",)]


@pytest.mark.parametrize(
    "column, expected",
    [("ROOT_TASK_NAME", "ROOT"), ("DATABASE_NAME", "DB1"), ("STATE", "FAILED"), ("FIRST_ERROR_TASK_NAME", "CHILD")],
)
def test_complete_task_graphs_failed_graph(cur: snowflake.connector.cursor.SnowflakeCursor, column: str, expected: str):
    cur.execute("create task root as select 1")
    cur.execute("create task child after root as select * from missing")
    _enable_dependents(cur, "root")
    cur.execute("execute task root")
    (row,) = _table_function(cur, "select * from table(information_schema.complete_task_graphs())")
    assert row[column] == expected


def test_complete_task_graphs_columns(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("select * from table(information_schema.complete_task_graphs())")
    assert [d.name for d in cur.description] == [
        "ROOT_TASK_NAME",
        "DATABASE_NAME",
        "SCHEMA_NAME",
        "STATE",
        "FIRST_ERROR_TASK_NAME",
        "FIRST_ERROR_CODE",
        "FIRST_ERROR_MESSAGE",
        "SCHEDULED_TIME",
        "QUERY_START_TIME",
        "NEXT_SCHEDULED_TIME",
        "COMPLETED_TIME",
        "ROOT_TASK_ID",
        "GRAPH_VERSION",
        "RUN_ID",
        "ATTEMPT_NUMBER",
        "SCHEDULED_FROM",
        "CONFIG",
        "GRAPH_RUN_GROUP_ID",
        "BACKFILL_INFO",
        "SCHEDULED_BY_USER",
        "GRAPH_ID",
        "GRAPH_NAME",
    ]


def test_task_history_unqualified_in_information_schema(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task t1 as select 1")
    cur.execute("execute task t1")
    # USE SCHEMA <db>.INFORMATION_SCHEMA isn't supported, a connection can start in it though
    with snowflake.connector.connect(database="db1", schema="information_schema") as conn, conn.cursor() as info:
        info.execute("select name from table(task_history())")
        assert info.fetchall() == [("T1",)]


def test_complete_task_graphs_excludes_runs_older_than_an_hour(
    cur: snowflake.connector.cursor.SnowflakeCursor, monkeypatch: pytest.MonkeyPatch
):
    cur.execute("create task t1 as select 1")
    monkeypatch.setattr("fakesnow.tasks.runner.now", lambda: datetime.now().astimezone() - timedelta(minutes=61))
    cur.execute("execute task t1")
    cur.execute("select count(*) from table(information_schema.complete_task_graphs())")
    assert cur.fetchall() == [(0,)]


def test_task_history_excludes_runs_older_than_seven_days(
    cur: snowflake.connector.cursor.SnowflakeCursor, monkeypatch: pytest.MonkeyPatch
):
    cur.execute("create task t1 as select 1")
    monkeypatch.setattr("fakesnow.tasks.runner.now", lambda: datetime.now().astimezone() - timedelta(days=8))
    cur.execute("execute task t1")
    cur.execute("select count(*) from table(information_schema.task_history())")
    assert cur.fetchall() == [(0,)]


def test_complete_task_graphs_default_result_limit(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task t1 as select 1")
    for _ in range(101):
        cur.execute("execute task t1")
    cur.execute("select count(*) from table(information_schema.complete_task_graphs())")
    assert cur.fetchall() == [(101,)]


def test_task_history_quoted_schema_name_matches_case_sensitively(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute('create schema "Mixed"')
    cur.execute("create task t1 as select 1")
    cur.execute("execute task t1")
    cur.execute(
        "select name from table(information_schema.task_history(database_name => 'DB1', schema_name => '\"Mixed\"'))"
    )
    assert cur.fetchall() == [("T1",)]


def _runs_in_two_databases(cur: snowflake.connector.cursor.SnowflakeCursor) -> None:
    cur.execute("create task t1 as select 1")
    cur.execute("execute task t1")
    cur.execute("create database db2")
    cur.execute("create schema db2.schema1")
    cur.execute("create task db2.schema1.t2 as select 1")
    cur.execute("execute task db2.schema1.t2")
    cur.execute("use schema db1.schema1")


def test_task_history_covers_all_databases(cur: snowflake.connector.cursor.SnowflakeCursor):
    _runs_in_two_databases(cur)
    cur.execute("select database_name, name from table(information_schema.task_history()) order by name")
    assert cur.fetchall() == [("DB1", "T1"), ("DB2", "T2")]


def test_task_history_database_name_selects_other_database(cur: snowflake.connector.cursor.SnowflakeCursor):
    _runs_in_two_databases(cur)
    cur.execute("select name from table(information_schema.task_history(database_name => 'db2'))")
    assert cur.fetchall() == [("T2",)]


def test_complete_task_graphs_covers_all_databases(cur: snowflake.connector.cursor.SnowflakeCursor):
    _runs_in_two_databases(cur)
    cur.execute(
        "select database_name, root_task_name from table(information_schema.complete_task_graphs()) "
        "order by root_task_name"
    )
    assert cur.fetchall() == [("DB1", "T1"), ("DB2", "T2")]


def test_complete_task_graphs_root_task_name_filter(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task r1 as select 1")
    cur.execute("create task r2 as select 1")
    cur.execute("execute task r1")
    cur.execute("execute task r2")
    cur.execute("select root_task_name from table(information_schema.complete_task_graphs(root_task_name => 'r1'))")
    assert cur.fetchall() == [("R1",)]


def test_complete_task_graphs_error_only(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task ok as select 1")
    cur.execute("create task bad as select * from missing")
    cur.execute("execute task ok")
    cur.execute("execute task bad")
    cur.execute("select root_task_name from table(information_schema.complete_task_graphs(error_only => true))")
    assert cur.fetchall() == [("BAD",)]


def _dependents_graph(cur: snowflake.connector.cursor.SnowflakeCursor) -> None:
    cur.execute("create task root as select 1")
    cur.execute("create task child after root as select 1")
    cur.execute("create task grandchild after child as select 1")
    cur.execute("create task other as select 1")


def test_task_dependents_recursive(cur: snowflake.connector.cursor.SnowflakeCursor):
    _dependents_graph(cur)
    cur.execute("select name from table(information_schema.task_dependents(task_name => 'root')) order by name")
    assert cur.fetchall() == [("CHILD",), ("GRANDCHILD",), ("ROOT",)]


def test_task_dependents_not_recursive(cur: snowflake.connector.cursor.SnowflakeCursor):
    _dependents_graph(cur)
    cur.execute(
        "select name from table(information_schema.task_dependents(task_name => 'root', recursive => false)) "
        "order by name"
    )
    assert cur.fetchall() == [("CHILD",), ("ROOT",)]


def test_task_dependents_reconverging_branches(cur: snowflake.connector.cursor.SnowflakeCursor):
    # a chain of diamonds has few tasks but exponentially many paths
    cur.execute("create task d0 as select 1")
    for i in range(1, 26):
        cur.execute(f"create task l{i} after d{i - 1} as select 1")
        cur.execute(f"create task r{i} after d{i - 1} as select 1")
        cur.execute(f"create task d{i} after l{i}, r{i} as select 1")
    cur.execute("select count(*) from table(information_schema.task_dependents(task_name => 'd0'))")
    assert cur.fetchall() == [(76,)]


def test_task_dependents_requested_task_first(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create task z_root as select 1")
    cur.execute("create task a_child after z_root as select 1")
    cur.execute("select name from table(information_schema.task_dependents(task_name => 'z_root'))")
    assert cur.fetchall() == [("Z_ROOT",), ("A_CHILD",)]


def test_task_dependents_columns(cur: snowflake.connector.cursor.SnowflakeCursor):
    _dependents_graph(cur)
    cur.execute("select * from table(information_schema.task_dependents(task_name => 'root'))")
    assert [d.name for d in cur.description] == [
        "CREATED_ON",
        "NAME",
        "DATABASE_NAME",
        "SCHEMA_NAME",
        "OWNER",
        "COMMENT",
        "WAREHOUSE",
        "SCHEDULE",
        "PREDECESSORS",
        "STATE",
        "DEFINITION",
        "CONDITION",
    ]


def test_task_dependents_qualified_task_name(cur: snowflake.connector.cursor.SnowflakeCursor):
    _dependents_graph(cur)
    cur.execute("select name from table(information_schema.task_dependents(task_name => 'db1.schema1.child'))")
    assert sorted(cur.fetchall()) == [("CHILD",), ("GRANDCHILD",)]


def _two_runs(cur: snowflake.connector.cursor.SnowflakeCursor) -> None:
    cur.execute("create task t1 as select 1")
    cur.execute("execute task t1")


def test_task_history_time_range_start_expression(cur: snowflake.connector.cursor.SnowflakeCursor):
    _two_runs(cur)
    cur.execute(
        "select name from table(information_schema.task_history("
        "scheduled_time_range_start => dateadd('hour', -1, current_timestamp())))"
    )
    assert cur.fetchall() == [("T1",)]


def test_task_history_time_range_start_expression_excludes_older(cur: snowflake.connector.cursor.SnowflakeCursor):
    _two_runs(cur)
    cur.execute(
        "select name from table(information_schema.task_history("
        "scheduled_time_range_start => dateadd('hour', 1, current_timestamp())))"
    )
    assert cur.fetchall() == []


def test_task_history_time_range_end_expression(cur: snowflake.connector.cursor.SnowflakeCursor):
    _two_runs(cur)
    cur.execute(
        "select name from table(information_schema.task_history("
        "scheduled_time_range_end => dateadd('hour', -1, current_timestamp())))"
    )
    assert cur.fetchall() == []


def test_task_history_time_range_end_includes_earlier(cur: snowflake.connector.cursor.SnowflakeCursor):
    _two_runs(cur)
    cur.execute(
        "select name from table(information_schema.task_history("
        "scheduled_time_range_end => dateadd('hour', 1, current_timestamp())))"
    )
    assert cur.fetchall() == [("T1",)]


def test_complete_task_graphs_error_only_false_string(cur: snowflake.connector.cursor.SnowflakeCursor):
    _two_runs(cur)
    cur.execute("select root_task_name from table(information_schema.complete_task_graphs(error_only => 'false'))")
    assert cur.fetchall() == [("T1",)]


def test_task_history_error_only_true_string(cur: snowflake.connector.cursor.SnowflakeCursor):
    _two_runs(cur)
    cur.execute("select name from table(information_schema.task_history(error_only => 'TRUE'))")
    assert cur.fetchall() == []


def test_task_history_error_only_invalid_raises(cur: snowflake.connector.cursor.SnowflakeCursor):
    with pytest.raises(snowflake.connector.errors.ProgrammingError, match="boolean"):
        cur.execute("select * from table(information_schema.task_history(error_only => 'maybe'))")


def test_task_dependents_recursive_false_string(cur: snowflake.connector.cursor.SnowflakeCursor):
    _dependents_graph(cur)
    cur.execute(
        "select name from table(information_schema.task_dependents(task_name => 'root', recursive => 'false')) "
        "order by name"
    )
    assert cur.fetchall() == [("CHILD",), ("ROOT",)]
