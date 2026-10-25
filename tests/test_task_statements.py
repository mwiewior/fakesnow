import pytest
from sqlglot import exp

import fakesnow.dialect
from fakesnow.tasks.statements import (
    AlterTask,
    CreateTask,
    DescribeTask,
    DropTask,
    ExecuteTask,
    ShowTasks,
    TaskName,
    TaskSyntaxError,
    parse_task_statement,
)


def test_parse_create_task():
    s = parse_task_statement("create task t1 as select 1")
    assert s == CreateTask(
        name=TaskName(None, None, "T1"),
        replace=False,
        if_not_exists=False,
        or_alter=False,
        properties={},
        after=[],
        condition=None,
        definition="select 1",
        clone_of=None,
    )


def test_parse_create_task_schedule():
    s = parse_task_statement("create task t1 schedule = '5 minute' as select 1")
    assert isinstance(s, CreateTask)
    assert s.properties == {"SCHEDULE": "5 minute"}


def test_parse_create_task_dml_body():
    s = parse_task_statement("create task t1 as insert into x values (1)")
    assert isinstance(s, CreateTask)
    assert s.definition == "insert into x values (1)"


def test_parse_create_task_qualified_quoted_name():
    s = parse_task_statement('create task db1.s1."Child" after a as select 1')
    assert isinstance(s, CreateTask)
    assert s.name == TaskName("DB1", "S1", "Child")


def test_parse_create_or_replace_task():
    s = parse_task_statement("create or replace task t as select 1")
    assert isinstance(s, CreateTask)
    assert s.replace


def test_parse_create_task_after():
    s = parse_task_statement("create task t after a, s1.b as select 1")
    assert isinstance(s, CreateTask)
    assert s.after == [TaskName(None, None, "A"), TaskName(None, "S1", "B")]


def test_parse_create_task_condition():
    s = parse_task_statement("create task t after a when cast(1 as int) = 1 as select 1")
    assert isinstance(s, CreateTask)
    assert s.condition == "cast(1 as int) = 1"


def test_parse_create_task_definition_keeps_spelling():
    s = parse_task_statement("create task t after a as select  'X'  from  Y;")
    assert isinstance(s, CreateTask)
    assert s.definition == "select  'X'  from  Y"


def test_parse_create_task_finalize_is_property():
    s = parse_task_statement("create task f1 finalize = r1 as select 1")
    assert isinstance(s, CreateTask)
    assert s.properties == {"FINALIZE": "r1"}


def test_parse_create_task_finalize_keeps_quoted_name_whole():
    s = parse_task_statement('create task f1 finalize = "root.task" as select 1')
    assert isinstance(s, CreateTask)
    assert s.properties == {"FINALIZE": '"root.task"'}


def test_parse_create_task_finalize_keeps_qualifiers():
    s = parse_task_statement("create task f1 finalize = db1.schema1.r1 as select 1")
    assert isinstance(s, CreateTask)
    assert s.properties == {"FINALIZE": "db1.schema1.r1"}


@pytest.mark.parametrize(
    "prop, expected",
    [
        ("warehouse = wh", {"WAREHOUSE": "WH"}),
        ("allow_overlapping_execution = true", {"ALLOW_OVERLAPPING_EXECUTION": "TRUE"}),
        ("retries = 3", {"RETRIES": "3"}),
    ],
)
def test_parse_create_task_property_value(prop: str, expected: dict[str, str]):
    s = parse_task_statement(f"create task t {prop} as select 1")
    assert isinstance(s, CreateTask)
    assert s.properties == expected


def test_parse_create_or_alter_task():
    s = parse_task_statement("create or alter task t as select 1")
    assert isinstance(s, CreateTask)
    assert s.or_alter


def test_parse_create_task_if_not_exists():
    s = parse_task_statement("create task if not exists t as select 1")
    assert isinstance(s, CreateTask)
    assert s.if_not_exists


def test_parse_create_task_clone():
    s = parse_task_statement("create task t2 clone t1")
    assert isinstance(s, CreateTask)
    assert s.clone_of == TaskName(None, None, "T1")


def test_parse_alter_task_resume():
    s = parse_task_statement("alter task t1 resume")
    assert isinstance(s, AlterTask)
    assert s.action == "RESUME"


def test_parse_alter_task_suspend():
    s = parse_task_statement("alter task t1 suspend")
    assert isinstance(s, AlterTask)
    assert s.action == "SUSPEND"


def test_parse_alter_task_if_exists():
    s = parse_task_statement("alter task if exists t1 suspend")
    assert isinstance(s, AlterTask)
    assert s.if_exists


def test_parse_alter_task_add_after():
    s = parse_task_statement("alter task t1 add after a, b")
    assert isinstance(s, AlterTask)
    assert s.action == "ADD_AFTER"
    assert s.tasks == [TaskName(None, None, "A"), TaskName(None, None, "B")]


def test_parse_alter_task_remove_after():
    s = parse_task_statement("alter task t1 remove after a")
    assert isinstance(s, AlterTask)
    assert s.action == "REMOVE_AFTER"
    assert s.tasks == [TaskName(None, None, "A")]


def test_parse_alter_task_set():
    s = parse_task_statement("alter task t1 set comment = 'x', user_task_timeout_ms = 10")
    assert isinstance(s, AlterTask)
    assert s.action == "SET"
    assert s.properties == {"COMMENT": "x", "USER_TASK_TIMEOUT_MS": "10"}


def test_parse_alter_task_unset():
    s = parse_task_statement("alter task t1 unset comment, schedule")
    assert isinstance(s, AlterTask)
    assert s.action == "UNSET"
    assert s.unset == ["COMMENT", "SCHEDULE"]


def test_parse_alter_task_modify_as():
    s = parse_task_statement("alter task t1 modify as select 2")
    assert isinstance(s, AlterTask)
    assert s.action == "MODIFY_AS"
    assert s.text == "select 2"


def test_parse_alter_task_modify_when():
    s = parse_task_statement("alter task t1 modify when 1 = 2")
    assert isinstance(s, AlterTask)
    assert s.action == "MODIFY_WHEN"
    assert s.text == "1 = 2"


def test_parse_alter_task_remove_when():
    s = parse_task_statement("alter task t1 remove when")
    assert isinstance(s, AlterTask)
    assert s.action == "REMOVE_WHEN"


def test_parse_drop_task():
    assert parse_task_statement("drop task db1.s1.t1") == DropTask(TaskName("DB1", "S1", "T1"), False)


def test_parse_drop_task_if_exists():
    assert parse_task_statement("drop task if exists t1") == DropTask(TaskName(None, None, "T1"), True)


@pytest.mark.parametrize("keyword", ["desc", "describe"])
def test_parse_describe_task(keyword: str):
    assert parse_task_statement(f"{keyword} task t1") == DescribeTask(TaskName(None, None, "T1"))


@pytest.mark.parametrize(
    "clause, field, expected",
    [
        ("terse tasks", "terse", True),
        ("tasks like 'T%'", "like", "T%"),
        ("tasks starts with 'T'", "starts_with", "T"),
        ("tasks root only", "root_only", True),
        ("tasks limit 5", "limit", 5),
        ("tasks limit 5 from 'T1'", "limit_from", "T1"),
    ],
)
def test_parse_show_tasks_clause(clause: str, field: str, expected: object):
    assert getattr(parse_task_statement(f"show {clause}"), field) == expected


def test_parse_show_tasks_all_clauses_together():
    # the interaction under test: every clause, in Snowflake's order, in one statement
    assert parse_task_statement(
        "show terse tasks like 'T%' in schema db1.s1 starts with 'T' root only limit 5 from 'T1'"
    ) == ShowTasks(
        terse=True,
        like="T%",
        scope="SCHEMA",
        scope_name=["DB1", "S1"],
        starts_with="T",
        root_only=True,
        limit=5,
        limit_from="T1",
    )


def test_parse_show_tasks_bare():
    assert parse_task_statement("show tasks") == ShowTasks(False, None, None, None, None, False, None, None)


def test_parse_show_tasks_in_account():
    s = parse_task_statement("show tasks in account")
    assert isinstance(s, ShowTasks)
    assert s.scope == "ACCOUNT"
    assert s.scope_name is None


def test_parse_show_tasks_in_bare_schema_name():
    s = parse_task_statement("show tasks in schema1")
    assert isinstance(s, ShowTasks)
    assert (s.scope, s.scope_name) == ("SCHEMA", ["SCHEMA1"])


def test_parse_show_tasks_in_qualified_schema_name():
    s = parse_task_statement("show tasks in db1.schema1")
    assert isinstance(s, ShowTasks)
    assert (s.scope, s.scope_name) == ("SCHEMA", ["DB1", "SCHEMA1"])


def test_parse_execute_task_using_config():
    assert parse_task_statement("""execute task t1 using config = '{"a": 1}'""") == ExecuteTask(
        TaskName(None, None, "T1"), '{"a": 1}'
    )


def test_parse_execute_task_without_config():
    assert parse_task_statement("execute task t1") == ExecuteTask(TaskName(None, None, "T1"), None)


def test_parse_non_task_statement_is_none():
    assert parse_task_statement("create table t (x int)") is None


def test_parse_malformed_task_statement_raises():
    with pytest.raises(TaskSyntaxError) as excinfo:
        parse_task_statement("alter task t1 frobnicate")
    assert excinfo.value.errno == 1003
    assert excinfo.value.sqlstate == "42000"


def test_describe_task_parses_as_command():
    assert isinstance(fakesnow.dialect.parse_one("desc task t1"), exp.Command)


@pytest.mark.parametrize("suffix", [";", " ;"])
def test_parse_execute_task_trailing_semicolon(suffix: str):
    assert parse_task_statement(f"execute task t1{suffix}") == ExecuteTask(TaskName(None, None, "T1"), None)


def test_parse_execute_task_using_config_trailing_semicolon():
    assert parse_task_statement("execute task t1 using config = '{}';") == ExecuteTask(TaskName(None, None, "T1"), "{}")


def test_parse_execute_task_using_dollar_quoted_config():
    assert parse_task_statement("""execute task t1 using config = $${"a": 1}$$""") == ExecuteTask(
        TaskName(None, None, "T1"), '{"a": 1}'
    )


def test_parse_create_task_dollar_quoted_property_kept_verbatim():
    s = parse_task_statement("""create task t1 config = $${"a": 1}$$ as select 1""")
    assert isinstance(s, CreateTask)
    assert s.properties == {"CONFIG": '{"a": 1}'}
