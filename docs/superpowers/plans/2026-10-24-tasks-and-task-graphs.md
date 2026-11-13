# Tasks and Task Graphs Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Create, alter, inspect and synchronously execute Snowflake tasks and task graphs (AFTER + finalizer) against fakesnow, with task history.

**Architecture:** All task statements parse as `exp.Command` (DESCRIBE TASK doesn't parse at all), so a new
`fakesnow/tasks/` package parses the command text into statement dataclasses, keeps task definitions and history
in per-database tables, and enforces Snowflake's graph rules. The cursor only dispatches: a transform marks a
task statement, and `_execute` hands it to `fakesnow.tasks.execute`, which returns the SQL of the result rows, the
way `COPY INTO` is handed to `fakesnow/copy_into.py`. `EXECUTE TASK` runs each task body in a new session on the
same `FakeSnow` instance.

**Tech Stack:** Python ≥ 3.11, DuckDB 2.0, sqlglot 30.22 (tokenizer only, for the command text), pytest.

**Spec:** `docs/superpowers/specs/2026-10-24-tasks-and-dbt-projects-design.md`, Part 2.

**Branch:** `tasks`, stacked on `dbt-task-support`; its PR targets `dbt-task-support`.

## Global Constraints

- Tests assert live Snowflake behaviour (AGENTS.md). Values the docs don't pin are provisional: each is a named
  constant in the module that owns it and is listed under "Live checks" at the end, to verify with `snowflake-check`.
- One behaviour per test (AGENTS.md "Keep tests behavior-focused"). JSON/VARIANT results compared as strings use
  `tests.utils.indent`/`dindent`, including text columns Snowflake renders compact, eg: `task_relations` and
  TASK_HISTORY `CONFIG`.
- Task logic lives in `fakesnow/tasks/`; the cursor only dispatches to it (spec anti-goal "Spreading responsibility
  across callers").
- Task definitions and history are stored per database under `<db>._fs_information_schema`, never in `_fs_global`.
- Task metadata is written through a separate DuckDB cursor (`duck_conn.cursor()`), as file formats are, so it
  neither commits nor conflicts with the caller's transaction.
- Identifiers: unquoted are upper-cased, quoted keep their case (`fakesnow.expr.normalise_ident`).
- Non-goals (spec): warehouses, privileges, integrations (stored, never called), stream triggers, retries,
  `RETRY LAST`, auto-suspend. Optional and **not in this plan**: `CALL`/scripting bodies, scheduler, async
  `EXECUTE TASK`. `USER_TASK_TIMEOUT_MS` is stored but not enforced (spec allows deferring).

## Review Focus

1. A task body that fails (bad SQL, missing table) — `EXECUTE TASK` still succeeds, history row `FAILED` with the
   error, dependents don't run, finalizer runs. Test in Task 6.
2. The caller has an open transaction when it runs `EXECUTE TASK` — it stays open and uncommitted, the task session
   doesn't see its writes, and a later caller `ROLLBACK` doesn't erase task history (see Live checks 1). Test in Task 6.
3. Quoted, mixed-case task names (`"My Task"`) through CREATE, AFTER, SHOW and EXECUTE. Test in Task 2 and Task 6.
4. Tasks in a database opened from `db_path` survive reconnecting to a new `FakeSnow` on the same path. Test in Task 2.
5. Two schemas each with a task of the same name — SHOW scoping and AFTER resolution keep them apart. Test in Task 5.

---

## File Structure

- Create `fakesnow/tasks/__init__.py` — `execute(statement, conn) -> str` dispatcher and `task_statement` transform.
- Create `fakesnow/tasks/statements.py` — statement dataclasses and `parse_task_statement`.
- Create `fakesnow/tasks/catalog.py` — `Task` dataclass, `TaskCatalog` (storage, lookup, graph rules, mutations).
- Create `fakesnow/tasks/show.py` — SQL for SHOW TASKS / DESCRIBE TASK rows.
- Create `fakesnow/tasks/runner.py` — `run_graph` (topological run, sessions, history).
- Create `fakesnow/tasks/functions.py` — `TaskRunContext` and the `SYSTEM$` task function transform.
- Create `fakesnow/tasks/history.py` — TASK_HISTORY / COMPLETE_TASK_GRAPHS / TASK_DEPENDENTS table functions.
- Modify `fakesnow/info_schema.py` — per-db `_fs_tasks`, `_fs_task_history`, `_fs_task_graph_runs` tables.
- Modify `fakesnow/dialect.py` — DESCRIBE/DESC TASK falls back to `exp.Command`.
- Modify `fakesnow/cursor.py` — wire the transforms, dispatch, `set_return_value` side channel.
- Modify `fakesnow/conn.py`, `fakesnow/instance.py` — a connection can open a sibling session.
- Modify `fakesnow/statement_type.py` — statement type ids for task statements.
- Tests: `tests/test_task_statements.py` (parser units), `tests/test_tasks.py` (behaviour).
- Docs: `README.md`, `CHANGELOG.md`.

---

### Task 1: Parse task statements

**Files:**
- Create: `fakesnow/tasks/__init__.py` (empty for now), `fakesnow/tasks/statements.py`
- Modify: `fakesnow/dialect.py`
- Test: `tests/test_task_statements.py`

**Interfaces:**
- Produces, in `fakesnow/tasks/statements.py`:
  - `TaskName(database: str | None, schema: str | None, name: str)` — parts already normalised.
  - `CreateTask(name, replace: bool, if_not_exists: bool, or_alter: bool, properties: dict[str, str],
    after: list[TaskName], condition: str | None, definition: str | None, clone_of: TaskName | None)`.
    `properties` keys are upper-cased property names; values are the value's text: string literals unquoted,
    identifiers normalised, numbers and booleans as written (booleans upper-cased). `FINALIZE` is a property whose
    value is the root's name text.
  - `AlterTask(name, if_exists: bool, action: Literal["RESUME", "SUSPEND", "ADD_AFTER", "REMOVE_AFTER", "SET",
    "UNSET", "MODIFY_AS", "MODIFY_WHEN", "REMOVE_WHEN"], tasks: list[TaskName], properties: dict[str, str],
    unset: list[str], text: str | None)` — `text` is the SQL after `MODIFY AS` / `MODIFY WHEN`.
  - `DropTask(name, if_exists: bool)`, `ExecuteTask(name, config: str | None)`, `DescribeTask(name)`.
  - `ShowTasks(terse: bool, like: str | None, scope: Literal["ACCOUNT", "DATABASE", "SCHEMA"] | None,
    scope_name: list[str] | None, starts_with: str | None, root_only: bool, limit: int | None,
    limit_from: str | None)`.
  - `TaskStatement = CreateTask | AlterTask | DropTask | ExecuteTask | ShowTasks | DescribeTask`.
  - `parse_task_statement(sql: str) -> TaskStatement | None` — `None` when `sql` isn't a task statement.
  - `TaskSyntaxError(ProgrammingError)` raised for a malformed task statement, errno 1003, sqlstate 42000.

- [ ] **Step 1: Write the failing tests**

```python
def test_parse_create_task_with_schedule_and_body():
    s = parse_task_statement("create task t1 schedule = '5 minute' as insert into x values (1)")
    assert s == CreateTask(
        name=TaskName(None, None, "T1"),
        replace=False,
        if_not_exists=False,
        or_alter=False,
        properties={"SCHEDULE": "5 minute"},
        after=[],
        condition=None,
        definition="insert into x values (1)",
        clone_of=None,
    )


def test_parse_create_task_after_when_finalize():
    s = parse_task_statement('create or replace task db1.s1."Child" after a, s1.b when 1 = 1 as select 1')
    assert s.name == TaskName("DB1", "S1", "Child") and s.replace
    assert s.after == [TaskName(None, None, "A"), TaskName(None, "S1", "B")]
    assert s.condition == "1 = 1" and s.definition == "select 1"


def test_parse_create_task_clone():
    assert parse_task_statement("create task t2 clone t1").clone_of == TaskName(None, None, "T1")


def test_parse_alter_task_forms():  # one assert per form is acceptable here: a parser table test
    assert parse_task_statement("alter task t1 resume").action == "RESUME"
    assert parse_task_statement("alter task if exists t1 remove after a, b").tasks == [
        TaskName(None, None, "A"),
        TaskName(None, None, "B"),
    ]
    assert parse_task_statement("alter task t1 set comment = 'x', user_task_timeout_ms = 10").properties == {
        "COMMENT": "x",
        "USER_TASK_TIMEOUT_MS": "10",
    }
    assert parse_task_statement("alter task t1 unset comment, schedule").unset == ["COMMENT", "SCHEDULE"]
    assert parse_task_statement("alter task t1 modify as select 2").text == "select 2"
    assert parse_task_statement("alter task t1 remove when").action == "REMOVE_WHEN"


def test_parse_show_tasks_filters():
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


def test_parse_execute_task_using_config():
    assert parse_task_statement("""execute task t1 using config = '{"a": 1}'""") == ExecuteTask(
        TaskName(None, None, "T1"), '{"a": 1}'
    )


def test_parse_non_task_statement_is_none():
    assert parse_task_statement("create table t (x int)") is None


def test_describe_task_parses_as_command():
    assert isinstance(fakesnow.dialect.parse_one("desc task t1"), exp.Command)
```

- [ ] **Step 2: Run** `uv run pytest tests/test_task_statements.py -q` — expected: FAIL (module not found).

- [ ] **Step 3: Implement `parse_task_statement` in `fakesnow/tasks/statements.py`**

  Tokenize with `sqlglot.tokenize(sql, read="snowflake")` and walk the tokens; slice the original `sql` by token
  `start`/`end` offsets for `definition`, `condition` and `MODIFY` text so the body keeps its exact spelling. `AS`
  ends the property list and everything after it is the body; `WHEN` runs until `AS`. In `fakesnow/dialect.py`,
  when `parse_one` raises `ParseError` and the text matches `^\s*(DESC|DESCRIBE)\s+TASK\b` (case-insensitive),
  return `exp.Command(this="DESCRIBE", expression=exp.Literal.string(<rest after DESC/DESCRIBE>))`.

- [ ] **Step 4: Run** `uv run pytest tests/test_task_statements.py -q` — expected: PASS.

- [ ] **Step 5: Commit** — `git commit -m "feat(tasks): parse task statements"`

---

### Task 2: Store tasks — CREATE, DROP, CLONE

**Files:**
- Create: `fakesnow/tasks/catalog.py`
- Modify: `fakesnow/tasks/__init__.py`, `fakesnow/info_schema.py` (`per_db_creation_sql`), `fakesnow/cursor.py`,
  `fakesnow/statement_type.py`
- Test: `tests/test_tasks.py`

**Interfaces:**
- Consumes: Task 1's statements.
- Produces:
  - `info_schema` per-db table `${catalog}._fs_information_schema._fs_tasks` with columns
    `created_on TIMESTAMPTZ, name VARCHAR, id VARCHAR, schema_name VARCHAR, owner VARCHAR, comment VARCHAR,
    warehouse VARCHAR, schedule VARCHAR, predecessors VARCHAR[], state VARCHAR, definition VARCHAR,
    condition VARCHAR, finalize VARCHAR, config VARCHAR, error_integration VARCHAR, success_integration VARCHAR,
    overlap_policy VARCHAR, allow_overlapping_execution BOOLEAN, properties JSON, session_parameters JSON,
    last_committed_on TIMESTAMPTZ, last_suspended_on TIMESTAMPTZ, last_suspended_reason VARCHAR,
    PRIMARY KEY (schema_name, name)`. `predecessors` and `finalize` hold unqualified names in the same schema.
  - `Task` dataclass in `catalog.py` mirroring those columns plus `database: str`, with property
    `fqname -> str` (`DB.SCHEMA.NAME`).
  - `TaskCatalog(duck_conn: DuckDBPyConnection, current_database: str | None, current_schema: str | None, role: str)`
    with `resolve(name: TaskName) -> tuple[str, str, str]`, `get(database, schema, name) -> Task | None`,
    `require(name: TaskName) -> Task` (raises the missing-task error), `create(stmt: CreateTask) -> str`,
    `drop(stmt: DropTask) -> str`, `save(task: Task) -> None`. Each mutating method returns the status SQL.
  - `fakesnow.tasks.execute(statement: TaskStatement, conn: FakeSnowflakeConnection) -> str` — dispatches to the
    catalog/runner and returns a SELECT producing the statement's result rows.
  - `fakesnow.tasks.task_statement(expression: Expr) -> Expr` — for an `exp.Command` whose text
    `parse_task_statement` recognises, returns `SUCCESS_NOP.copy()` with `args["task_statement"]` set.
  - Cursor: `_transform` applies `tasks.task_statement` right after `transforms.identifier`; `_execute` calls
    `sql = tasks.execute(statement, self._conn)` for `transformed.args.get("task_statement")` inside the existing
    `try` (so errors map like other statements) and executes `sql` on `self._duck_conn`.
  - `statement_type_id`: `CreateTask`/`AlterTask`/`DropTask` → `DDL`, `ShowTasks` → `SHOW`,
    `DescribeTask` → `DESCRIBE`, `ExecuteTask` → `UNKNOWN`.
  - `fakesnow.tasks.drop_schema(duck_conn: DuckDBPyConnection, database: str, schema: str) -> None` — deletes the
    schema's tasks (and, from Task 6, their history and graph runs). The cursor calls it after a successful
    `DROP SCHEMA`; `DROP DATABASE` needs nothing, since the tables go with the database.
- Pinned values (provisional, in `catalog.py`):
  - `CREATED = "Task {name} successfully created."`, `EXISTS = "{name} already exists, statement succeeded."`,
    `DROPPED = "{name} successfully dropped."`.
  - Missing task: `ProgrammingError(msg="SQL compilation error:\nTask '{fqname}' does not exist or not authorized.", errno=2003, sqlstate="02000")`.
  - Unknown property: `ProgrammingError(msg="SQL compilation error:\ninvalid property '{PROP}' for 'TASK'", errno=1008, sqlstate="22023")`.
  - Recognised properties: `WAREHOUSE, USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE, SCHEDULE, CONFIG, OVERLAP_POLICY,
    ALLOW_OVERLAPPING_EXECUTION, USER_TASK_TIMEOUT_MS, SUSPEND_TASK_AFTER_NUM_FAILURES, ERROR_INTEGRATION,
    SUCCESS_INTEGRATION, COMMENT, FINALIZE, TASK_AUTO_RETRY_ATTEMPTS, TARGET_COMPLETION_INTERVAL,
    USER_TASK_MINIMUM_TRIGGER_INTERVAL_IN_SECONDS, SERVERLESS_TASK_MIN_STATEMENT_SIZE,
    SERVERLESS_TASK_MAX_STATEMENT_SIZE, LOG_LEVEL, TRACE_LEVEL`; session parameters: `QUERY_TAG, TIMEZONE,
    AUTOCOMMIT, QUOTED_IDENTIFIERS_IGNORE_CASE, DATE_INPUT_FORMAT, DATE_OUTPUT_FORMAT, TIMESTAMP_INPUT_FORMAT,
    TIMESTAMP_OUTPUT_FORMAT, TIMESTAMP_TYPE_MAPPING, STATEMENT_TIMEOUT_IN_SECONDS, LOCK_TIMEOUT, WEEK_START,
    BINARY_INPUT_FORMAT, JSON_INDENT, ROWS_PER_RESULTSET`. `AUTOCOMMIT = FALSE` and `SEARCH_PATH` are rejected
    with the unknown-property error (docs: unsupported for tasks).
- Rules: new, replaced and cloned tasks are `suspended`; `OR REPLACE` gives a new `id` (`str(uuid.uuid4())`);
  `owner` is the creating session's role; `CREATE OR ALTER` on an existing task keeps `id`, `state`,
  `created_on` and history and resets omitted properties; `CLONE` copies every property except `id`,
  `created_on`, `state` (suspended), `last_*`. Unqualified names resolve against the session; with no current
  schema, raise the cursor's existing "This session does not have a current schema" error (errno 90106).

- [ ] **Step 1: Write the failing tests** in `tests/test_tasks.py` (each its own test):
  - `test_create_task_status` — `cur.execute("create task t1 as select 1")` → `[("Task T1 successfully created.",)]`.
  - `test_create_task_if_not_exists_existing` → `[("T1 already exists, statement succeeded.",)]`.
  - `test_create_task_existing_raises` — second plain create raises ProgrammingError `match="already exists"`.
  - `test_create_or_replace_task_gets_new_id` — `id` from `show tasks` differs before/after (uses Task 5's SHOW;
    until then read `_fs_tasks` through `select id from db1._fs_information_schema._fs_tasks`, and switch the test
    to SHOW TASKS in Task 5).
  - `test_create_task_unknown_property_raises` — `create task t1 nope = 1 as select 1` → `match="invalid property 'NOPE' for 'TASK'"`.
  - `test_drop_task_status` → `[("T1 successfully dropped.",)]`; `test_drop_missing_task_raises` →
    `match="Task 'DB1.SCHEMA1.NOPE' does not exist or not authorized."`; `test_drop_task_if_exists_missing` succeeds.
  - `test_create_task_quoted_name` — `create task "My Task" as select 1` → `[("Task My Task successfully created.",)]`.
  - `test_tasks_persist_with_db_path(tmp_path)` — create in one `fakesnow.patch(db_path=tmp_path)` block; a second
    block on the same path finds it (`create task t1 ...` raises "already exists").
  - `test_drop_schema_drops_its_tasks` — create schema s2, a task in it, drop s2, recreate s2; `create task s2.t1 ...`
    succeeds with the created status.

- [ ] **Step 2: Run** `uv run pytest tests/test_tasks.py -q` — expected: FAIL.
- [ ] **Step 3: Implement** the table in `per_db_creation_sql`, `TaskCatalog`, `execute`, `task_statement`, the
  cursor wiring and `statement_type_id`.
- [ ] **Step 4: Run** `uv run pytest tests/test_tasks.py tests/test_task_statements.py -q` and the full suite
  `uv run pytest -q` — expected: PASS.
- [ ] **Step 5: Commit** — `git commit -m "feat(tasks): create, drop and clone tasks"`

---

### Task 3: Graph rules on CREATE and DROP

**Files:** Modify `fakesnow/tasks/catalog.py`; Test `tests/test_tasks.py`

**Interfaces:**
- Consumes: Task 2's `TaskCatalog`, `Task`.
- Produces on `TaskCatalog`: `root_of(task: Task) -> Task`, `children(task: Task) -> list[Task]`,
  `graph(root: Task) -> list[Task]` (root, descendants, then its finalizer), `finalizer_of(root: Task) -> Task | None`,
  `check_root_suspended(task: Task) -> None`.
- Pinned errors (provisional, constants in `catalog.py`, all `ProgrammingError` errno 2003 sqlstate 02000 unless noted):
  - Started root: `"Unable to update graph with root task {root_fqname} since that root task is not suspended."` errno 91089.
  - Missing predecessor: the missing-task error for that predecessor.
  - Predecessor in another schema: `"SQL compilation error:\nTask {fqname} cannot depend on task {pred_fqname} in a different schema."`.
  - Schedule on a child or finalizer: `"SQL compilation error:\nTask {fqname} with predecessors or a finalized root cannot have a schedule."`.
  - Finalizer with predecessors / children, or second finalizer:
    `"SQL compilation error:\nInvalid finalizer task {fqname}: {reason}."` with reasons `"a finalizer cannot have predecessors"`,
    `"a finalizer cannot have child tasks"`, `"root task {root} already has a finalizer"`.
  - Cycle: `"SQL compilation error:\nTask graph cycle detected involving task {fqname}."`.
  - Limits: `"SQL compilation error:\nTask {fqname} exceeds the maximum of {limit} {what}."` for 100 predecessors,
    100 children, 1000 tasks per graph.
- Rules (spec "Catalog rules"): all apply on CREATE (incl. OR REPLACE / OR ALTER / CLONE) and DROP. DROP of a
  task's only predecessor makes the child standalone: `state='suspended'`, `last_suspended_reason='CHILD_BECAME_ROOT'`,
  `last_suspended_on=now()`. DROP of a root with a finalizer: the finalizer becomes standalone with
  `FINALIZER_BECAME_ROOT`.

- [ ] **Step 1: Write the failing tests** (each its own test): `test_create_child_of_started_root_raises`,
  `test_create_child_with_missing_predecessor_raises`, `test_create_child_with_schedule_raises`,
  `test_create_child_in_other_schema_raises`, `test_create_second_finalizer_raises`,
  `test_create_finalizer_with_predecessor_raises`, `test_create_task_cycle_raises` (via `create or replace` of the
  root `after` its child — root suspended), `test_drop_only_predecessor_makes_child_standalone` (asserts
  `state`, `last_suspended_reason` from `_fs_tasks`; switched to SHOW TASKS in Task 5),
  `test_drop_root_makes_finalizer_standalone`, `test_drop_task_in_started_graph_raises`.
  "Started root" setup uses `ALTER TASK ... RESUME` — until Task 4 lands, set `state` with
  `update db1._fs_information_schema._fs_tasks set state = 'started'` and switch to ALTER TASK in Task 4.
- [ ] **Step 2: Run** — expected FAIL. **Step 3: Implement.** **Step 4: Run** the file and full suite — PASS.
- [ ] **Step 5: Commit** — `git commit -m "feat(tasks): enforce task graph rules"`

---

### Task 4: ALTER TASK and SYSTEM$TASK_DEPENDENTS_ENABLE

**Files:** Modify `fakesnow/tasks/catalog.py`, `fakesnow/tasks/__init__.py`; Test `tests/test_tasks.py`

**Interfaces:**
- Consumes: Task 3's graph helpers.
- Produces: `TaskCatalog.alter(stmt: AlterTask) -> str`, `TaskCatalog.enable_dependents(name: TaskName) -> None`;
  `fakesnow.tasks.task_statement` also recognises a `SELECT SYSTEM$TASK_DEPENDENTS_ENABLE('<name>')` whose only
  projection is that call (other uses raise `NotImplementedError`).
- Pinned: ALTER status `"Statement executed successfully."`; `SYSTEM$TASK_DEPENDENTS_ENABLE` returns one row, one
  column named `SYSTEM$TASK_DEPENDENTS_ENABLE('<arg as written>')`, value `NULL` (provisional).
- Rules: RESUME/SUSPEND of a child, ADD/REMOVE AFTER, SET/UNSET (incl. FINALIZE), MODIFY AS/WHEN, REMOVE WHEN all
  require the graph's root suspended; RESUME/SUSPEND of the root itself is always allowed. First RESUME sets
  `last_committed_on`. SUSPEND sets `last_suspended_on` and `last_suspended_reason='USER_SUSPENDED'`. REMOVE AFTER
  leaving no predecessors → `CHILD_BECAME_ROOT`; UNSET FINALIZE → `FINALIZER_BECAME_ROOT`. SET re-validates
  properties and graph rules. `IF EXISTS` on a missing task succeeds with the ALTER status.
  `SYSTEM$TASK_DEPENDENTS_ENABLE` resumes the task and all its descendants.

- [ ] **Step 1: Write the failing tests** (each its own): `test_alter_task_resume_status`,
  `test_alter_task_resume_sets_state`, `test_alter_task_suspend_sets_reason`, `test_alter_task_set_comment`,
  `test_alter_task_unset_comment`, `test_alter_child_resume_with_started_root_raises`,
  `test_alter_task_add_after`, `test_alter_task_remove_last_predecessor_makes_child_standalone`,
  `test_alter_task_unset_finalize_makes_finalizer_standalone`, `test_alter_task_modify_as`,
  `test_alter_task_remove_when`, `test_alter_missing_task_if_exists`, `test_task_dependents_enable_resumes_graph`.
  Replace the temporary `update ... set state = 'started'` in Task 3's tests with `ALTER TASK ... RESUME`.
- [ ] **Step 2–4:** run (FAIL), implement, run file + full suite (PASS).
- [ ] **Step 5: Commit** — `git commit -m "feat(tasks): alter tasks and enable dependents"`

---

### Task 5: SHOW TASKS and DESCRIBE TASK

**Files:** Create `fakesnow/tasks/show.py`; Modify `fakesnow/tasks/__init__.py`; Test `tests/test_tasks.py`

**Interfaces:**
- Consumes: Task 2's `_fs_tasks` layout.
- Produces: `show_tasks_sql(stmt: ShowTasks, duck_conn, current_database, current_schema) -> str`,
  `describe_task_sql(task: Task) -> str`.
- Pinned columns, in order (docs; spec live check 1): `created_on, name, id, database_name, schema_name, owner,
  comment, warehouse, schedule, scheduling_mode, target_completion_interval, predecessors, state, definition,
  condition, allow_overlapping_execution, error_integration, success_integration, last_committed_on,
  last_suspended_on, owner_role_type, config, last_suspended_reason, task_relations, execute_as_user,
  overlap_policy`. `predecessors` is a JSON array of fully qualified names (`[]` when none), formatted as Snowflake
  formats arrays (`indent` in tests); `task_relations` is a JSON object with `Predecessors`, plus `FinalizerTask`
  on a root with a finalizer and `FinalizedRootTask` on a finalizer; `owner_role_type` is `'ROLE'`;
  `allow_overlapping_execution`, `config`, `overlap_policy` are NULL on child tasks; `scheduling_mode`,
  `target_completion_interval` (unless set), `execute_as_user` are NULL. TERSE columns: `created_on, name, kind
  (NULL), database_name, schema_name, schedule`. DESCRIBE TASK returns the SHOW TASKS columns for one task.
- Scope: no `IN` → current database if set, else account; `IN SCHEMA` without a name → current schema; account
  scope unions `_fs_tasks` of every attached database that has one. `LIKE` is case-insensitive, `STARTS WITH`
  case-sensitive, `ROOT ONLY` keeps tasks without predecessors, `LIMIT n FROM 'x'` keeps names `>= 'x'`, ordered
  by name.

- [ ] **Step 1: Write the failing tests** (each its own): `test_show_tasks_columns` (column names in order),
  `test_show_tasks_new_task_row` (dict row with pinned values for a fresh `create task t1 schedule = '5 minute' as select 1`),
  `test_show_tasks_predecessors_are_qualified`, `test_show_tasks_task_relations_for_finalizer`,
  `test_show_terse_tasks_columns`, `test_show_tasks_like`, `test_show_tasks_starts_with_is_case_sensitive`,
  `test_show_tasks_root_only`, `test_show_tasks_in_schema_scopes_same_named_tasks`, `test_show_tasks_limit_from`,
  `test_describe_task_row`. Switch Tasks 2–3's `_fs_tasks` reads to SHOW TASKS.
- [ ] **Step 2–4:** run (FAIL), implement, run file + full suite (PASS).
- [ ] **Step 5: Commit** — `git commit -m "feat(tasks): show and describe tasks"`

---

### Task 6: EXECUTE TASK

**Files:** Create `fakesnow/tasks/runner.py`; Modify `fakesnow/conn.py`, `fakesnow/instance.py`,
`fakesnow/info_schema.py`, `fakesnow/tasks/__init__.py`; Test `tests/test_tasks.py`

**Interfaces:**
- Consumes: Task 3's `graph`, `finalizer_of`; Task 7 later reads `TaskRunContext`.
- Produces:
  - `FakeSnowflakeConnection(..., instance: FakeSnow | None = None)`; `FakeSnow.connect` passes `instance=self`.
    `FakeSnowflakeConnection.open_session(database: str, schema: str, role: str, session_parameters: dict[str, str])
    -> FakeSnowflakeConnection` — a new session on the same instance; raises `NotImplementedError("Running tasks
    needs a connection from fakesnow.patch or fakesnow.server")` without an instance.
  - `FakeSnowflakeConnection.task_context: TaskRunContext | None` (Task 7's type; `None` outside a task run).
  - `run_graph(conn: FakeSnowflakeConnection, catalog: TaskCatalog, root: Task, config_override: str | None) -> None`.
  - Per-db tables `_fs_task_history` (columns: `query_id, name, schema_name, query_text, condition_text, state,
    error_code, error_message, scheduled_time, query_start_time, completed_time, root_task_id, run_id,
    return_value, scheduled_from, attempt_number, config, graph_run_group_id`) and `_fs_task_graph_runs`
    (`root_task_name, schema_name, state, scheduled_from, first_error_task_name, first_error_code,
    first_error_message, scheduled_time, query_start_time, completed_time, root_task_id, run_id, attempt_number,
    config, graph_run_group_id`).
- Pinned: EXECUTE TASK status `"Task {name} is scheduled to run immediately."` (provisional); child task error
  `ProgrammingError(msg="SQL compilation error:\nCannot execute task {fqname}: EXECUTE TASK can only run a standalone or root task.", errno=2003, sqlstate="02000")` (provisional).
  History `SCHEDULED_FROM = 'EXECUTE_TASK'`, `ATTEMPT_NUMBER = 1`, `RUN_ID` = scheduled time in epoch ms,
  one `GRAPH_RUN_GROUP_ID` (uuid4) per run.
- Algorithm (spec "Running a graph"):
  - If the caller is in a transaction, `conn.commit()` first; history and graph-run rows go through a separate
    DuckDB cursor.
  - Order the graph topologically (Kahn's algorithm; ties broken by name). A task runs when every predecessor that
    ran succeeded; a suspended non-root task is skipped with no history row and counts as succeeded; a task whose
    predecessor failed or was skipped by `WHEN` doesn't run and gets no row.
  - For each task to run: open a session with the task's database/schema, owner role and stored session
    parameters (passed as the connection's `session_parameters`, so `QUERY_TAG` applies; fakesnow has no function
    to observe it, so it has no test); if `condition` is set, evaluate `SELECT (<condition>)` there — false or NULL → row `SKIPPED`;
    otherwise run `definition`, row `SUCCEEDED` with the body's `sfqid` as `QUERY_ID`, or `FAILED` with the
    `ProgrammingError`'s `errno`/`msg` (or `0`/`str(e)` for other exceptions). Close the session.
  - The finalizer runs after every other task unless the root was skipped.
  - Graph state: `SKIPPED` if the root was skipped, else `FAILED` if any row failed, else `SUCCEEDED`.
  - `CONFIG`: the root's config, with `USING CONFIG` replacing it for this run (docs: "Previous configurations are
    replaced").
  - First EXECUTE TASK sets the root's `last_committed_on`.

- [ ] **Step 1: Write the failing tests** (each its own; tables created in the test):
  - `test_execute_task_status` → `[("Task T1 is scheduled to run immediately.",)]`.
  - `test_execute_task_runs_body` — body `insert into t values (1)`; `select * from t` → `[(1,)]` straight after.
  - `test_execute_task_runs_graph_in_order` — root/child/grandchild each insert their name into a log table with a
    sequence column; rows in order root, child, grandchild.
  - `test_execute_task_failed_body_does_not_raise` — body `select * from missing`; EXECUTE TASK returns the status.
  - `test_execute_task_failed_task_skips_dependents`, `test_execute_task_finalizer_runs_after_failure`,
    `test_execute_task_when_false_skips_task_and_dependents`, `test_execute_task_root_when_false_skips_finalizer`,
    `test_execute_task_suspended_child_is_skipped`, `test_execute_child_task_raises`,
    `test_execute_task_runs_in_own_session` (body `insert into t select current_schema()` from a caller whose
    current schema is another one → task's schema), `test_execute_task_commits_callers_transaction`,
    `test_execute_task_quoted_name`, `test_execute_task_with_plain_connection_raises` (a
    `FakeSnowflakeConnection` built without an instance → `match="needs a connection from fakesnow.patch"`).
- [ ] **Step 2–4:** run (FAIL), implement, run file + full suite (PASS).
- [ ] **Step 5: Commit** — `git commit -m "feat(tasks): execute task graphs synchronously"`

---

### Task 7: SYSTEM$ task functions

**Files:** Create `fakesnow/tasks/functions.py`; Modify `fakesnow/tasks/runner.py`, `fakesnow/cursor.py`;
Test `tests/test_tasks.py`

**Interfaces:**
- Consumes: Task 6's `task_context`, `run_graph`.
- Produces:
  - `TaskRunContext(task: Task, root: Task, graph_run_group_id: str, run_id: int, config: str | None,
    predecessor_return_values: dict[str, str | None], return_value: str | None = None)`.
  - `task_functions(expression: Expr, context: TaskRunContext | None) -> Expr` — a cursor transform:
    - `SYSTEM$CURRENT_USER_TASK_NAME()` → task name string.
    - `SYSTEM$TASK_RUNTIME_INFO('<field>')` → `CURRENT_TASK_NAME` (fqname), `CURRENT_ROOT_TASK_NAME`,
      `CURRENT_ROOT_TASK_UUID` (root id), `CURRENT_TASK_GRAPH_RUN_GROUP_ID`, `CURRENT_TASK_GRAPH_ORIGINAL_SCHEDULED_TIMESTAMP`.
    - `SYSTEM$GET_TASK_GRAPH_CONFIG(['<key>'])` → the run's config JSON string, or the key's value as a string.
    - `SYSTEM$GET_PREDECESSOR_RETURN_VALUE(['<task>'])` → that predecessor's return value; with no argument and a
      single predecessor, that one's.
    - `SYSTEM$SET_RETURN_VALUE(<expr>)` → `<expr>`, and sets `args["set_return_value"] = True` on the statement;
      the cursor stores `str(first column of first row)` in `conn.task_context.return_value`, and the runner writes
      it to the history row's `RETURN_VALUE` and to its dependents' `predecessor_return_values`.
    - Outside a task (`context is None`) every function returns `NULL` (provisional, spec live check 4).
  - `run_graph` also evaluates `WHEN` with these functions available.

- [ ] **Step 1: Write the failing tests** (each its own): `test_set_return_value_recorded_in_history` (needs Task 8;
  until then assert through `_fs_task_history`), `test_predecessor_return_value_in_child`,
  `test_predecessor_return_value_in_when`, `test_get_task_graph_config`, `test_get_task_graph_config_using_config`,
  `test_current_user_task_name`, `test_task_runtime_info_current_root_task_name`,
  `test_task_functions_outside_task_return_null`.
- [ ] **Step 2–4:** run (FAIL), implement, run file + full suite (PASS).
- [ ] **Step 5: Commit** — `git commit -m "feat(tasks): support SYSTEM$ task functions"`

---

### Task 8: TASK_HISTORY, COMPLETE_TASK_GRAPHS, TASK_DEPENDENTS

**Files:** Create `fakesnow/tasks/history.py`; Modify `fakesnow/cursor.py`; Test `tests/test_tasks.py`

**Interfaces:**
- Consumes: Task 6's history tables, Task 5's `show_tasks_sql` columns.
- Produces: `task_table_functions(expression: Expr, current_database: str | None) -> Expr` — rewrites
  `TABLE([<db>.]INFORMATION_SCHEMA.TASK_HISTORY(...))` (an `exp.TableFromRows` over `Dot(..., Anonymous(...))`),
  likewise `COMPLETE_TASK_GRAPHS` and `TASK_DEPENDENTS`, into a subquery keeping any alias. `<db>` defaults to the
  current database.
- Pinned:
  - TASK_HISTORY columns in the docs' order: `QUERY_ID, NAME, DATABASE_NAME, SCHEMA_NAME, QUERY_TEXT,
    CONDITION_TEXT, STATE, ERROR_CODE, ERROR_MESSAGE, SCHEDULED_TIME, QUERY_START_TIME, NEXT_SCHEDULED_TIME,
    COMPLETED_TIME, ROOT_TASK_ID, GRAPH_VERSION, RUN_ID, RETURN_VALUE, SCHEDULED_FROM, ATTEMPT_NUMBER, CONFIG,
    QUERY_HASH, QUERY_HASH_VERSION, QUERY_PARAMETERIZED_HASH, QUERY_PARAMETERIZED_HASH_VERSION,
    GRAPH_RUN_GROUP_ID, BACKFILL_INFO, SPCS_JOB_ID`; unrecorded columns NULL, `GRAPH_VERSION = 1`.
    Named args: `SCHEDULED_TIME_RANGE_START`, `SCHEDULED_TIME_RANGE_END`, `RESULT_LIMIT` (default 100, newest kept),
    `TASK_NAME` (case-insensitive), `DATABASE_NAME`, `SCHEMA_NAME`, `ERROR_ONLY` (FAILED or CANCELLED),
    `ROOT_TASK_ID`. Rows ordered by `SCHEDULED_TIME` descending.
  - COMPLETE_TASK_GRAPHS columns: `ROOT_TASK_NAME, DATABASE_NAME, SCHEMA_NAME, STATE, SCHEDULED_FROM,
    FIRST_ERROR_TASK_NAME, FIRST_ERROR_CODE, FIRST_ERROR_MESSAGE, SCHEDULED_TIME, QUERY_START_TIME,
    NEXT_SCHEDULED_TIME, COMPLETED_TIME, ROOT_TASK_ID, GRAPH_VERSION, RUN_ID, ATTEMPT_NUMBER, CONFIG,
    GRAPH_RUN_GROUP_ID, BACKFILL_INFO`; args `RESULT_LIMIT`, `ROOT_TASK_NAME`, `ERROR_ONLY`.
  - TASK_DEPENDENTS(TASK_NAME => '<name>', RECURSIVE => TRUE|FALSE default TRUE): SHOW TASKS columns for the task
    and its dependents (direct children only when `RECURSIVE => FALSE`).

- [ ] **Step 1: Write the failing tests** (each its own): `test_task_history_succeeded_row`,
  `test_task_history_failed_row_has_error`, `test_task_history_task_name_filter`,
  `test_task_history_error_only`, `test_task_history_result_limit`, `test_task_history_query_id_result_scan`
  (`select * from table(result_scan('<QUERY_ID>'))` returns the body's rows),
  `test_task_history_survives_caller_rollback`, `test_complete_task_graphs_failed_graph`,
  `test_task_dependents_recursive`. Switch Task 7's `_fs_task_history` read to TASK_HISTORY.
- [ ] **Step 2–4:** run (FAIL), implement, run file + full suite (PASS).
- [ ] **Step 5: Commit** — `git commit -m "feat(tasks): task history table functions"`

---

### Task 9: Docs

**Files:** Modify `README.md`, `CHANGELOG.md`

- [ ] **Step 1:** README: add tasks to the supported-features list — statements, synchronous `EXECUTE TASK`,
  graph rules, history functions, and the not-supported list (scheduler, async, `CALL`/scripting bodies,
  `RETRY LAST`, timeouts not enforced).
- [ ] **Step 2:** CHANGELOG: one entry under unreleased: "Tasks and task graphs: CREATE/ALTER/DROP/SHOW/DESCRIBE
  TASK, synchronous EXECUTE TASK, TASK_HISTORY".
- [ ] **Step 3: Run** `uv run ruff check . && uv run pyright && uv run pytest -q` — expected: clean, all pass.
- [ ] **Step 4: Commit** — `git commit -m "docs: tasks and task graphs"`

---

## Live checks

Verified with `snowflake-check` on 2026-10-25 (Snowflake 10.38, role ACCOUNTADMIN, serverless tasks), and fakesnow
corrected to match. What fakesnow now does, as Snowflake does:

1. Transactions: CREATE TASK neither commits the open transaction nor is undone by its rollback. ALTER TASK and DROP TASK
   commit it. EXECUTE TASK doesn't commit it, and the run doesn't see its uncommitted writes.
2. Resuming needs a trigger: a task without SCHEDULE, AFTER, FINALIZE or WHEN fails with 091453. A root or standalone
   task triggered only by WHEN, without a warehouse, needs TARGET_COMPLETION_INTERVAL to be resumed or executed (091865).
   SUSPEND of a suspended task changes nothing.
3. `SYSTEM$TASK_DEPENDENTS_ENABLE` returns `'OK'`, in a column named by the upper-cased call. It resumes the dependents
   first, so they stay resumed when the root can't be. A missing task fails with 1008 (`RECURSIVE_TASK_ENABLE`).
4. Graph shape is checked by EXECUTE TASK, not the DDL: predecessors in different graphs, a cycle, a join split by
   REMOVE AFTER or DROP, and SET FINALIZE on a root that has a finalizer are all accepted. EXECUTE TASK then fails with
   091419 (unreachable task), in a cycle through the root, 091435 (non-root task), or, in a cycle among the root's
   descendants, 091445 ("Graph has at least one cycle containing task A.") without running any task. A task can't be
   its own predecessor: ADD AFTER itself, or CREATE OR REPLACE of an existing task AFTER itself, fails with 091402
   (22000). CREATE OR REPLACE of a started root with dependents
   and CREATE OR ALTER of a started standalone task are accepted, the latter keeping its state.
5. A task runs once all its resumed predecessors succeeded, and needs at least one: suspended predecessors are ignored,
   so a task after only suspended ones, eg: a dependent of a suspended child, doesn't run. Tasks run in waves, children
   before grandchildren.
6. WHEN may only use constants, comparisons, AND/OR/NOT, BETWEEN, conversions and the four documented SYSTEM$ functions
   (091464 otherwise, at CREATE or MODIFY WHEN).
7. `SYSTEM$SET_RETURN_VALUE` returns `'SUCCESS'`. Its argument must be a string constant (concatenations of constants
   and task functions included), else the task fails with 1015. The value is set when the statement is compiled: the
   last call wins, even if no row is produced.
8. Outside a task, `SYSTEM$CURRENT_USER_TASK_NAME()` returns `''` and the other task functions fail with 091430.
9. `SYSTEM$GET_PREDECESSOR_RETURN_VALUE` fails with 091450 without a name when several predecessors are enabled, and
   with 091451 for a name matching no enabled predecessor or ancestor (case-sensitive).
10. SHOW TASKS without IN lists the current schema (else the current database, else the account). `LIMIT n FROM 'x'`
    includes x. ROOT ONLY leaves out finalizers. IN SCHEMA of a missing schema fails.
11. SHOW TASKS / DESCRIBE TASK columns are in Snowflake's order, with `created_by_user`. An unset comment is `''`,
    unset integrations and a child's `allow_overlapping_execution` are `'null'`, a root's `overlap_policy` is
    `NO_OVERLAP` or `ALLOW_CHILD_OVERLAP`. `predecessors` has one name per line, `task_relations` is compact with
    `FinalizedRootTask` first.
12. TASK_HISTORY / COMPLETE_TASK_GRAPHS: error codes are zero-padded text (`'002003'`), `SCHEDULED_FROM` is
    `EXECUTE TASK`, error messages have no code prefix, `SCHEDULED_BY_USER` / `GRAPH_ID` (and `GRAPH_NAME`) are present.
    A merged USING CONFIG is rendered compact, an unmerged one as written. TASK_DEPENDENTS returns its 12 documented
    columns in upper case.
13. `TASK_RUNTIME_INFO('CURRENT_TASK_GRAPH_ORIGINAL_SCHEDULED_TIMESTAMP')` is `YYYY-MM-DD HH:MM:SS.mmm000000Z`.
14. Session variables in a task body are kept as written, so a run fails when the task session doesn't have them.
15. Error codes and texts as Snowflake reports them: started root 091421 (22000), missing predecessor 091085, other-schema
    predecessor 091413, schedule with a predecessor 091400, finalizer rules 091468 / 091470 / 091471 / 091472 / 091497 /
    091479, a root with a finalizer made a child 091856, unknown property 1420, `Object '<name>' already exists.`, a
    missing database `Database '<name>' does not exist or not authorized.`, REMOVE AFTER of a non-predecessor 091417.

Still unverified, kept provisional:

- A finalizer with children (`INVALID_FINALIZER` / `FINALIZER_HAS_CHILDREN`), and the graph-size limits.
- The sqlstate of the `GET_PREDECESSOR_RETURN_VALUE` errors, and the `found` text of a non-constant `SET_RETURN_VALUE`
  argument for anything but a literal (Snowflake shows a resolved, qualified column name).
- `created_by_user` / `SCHEDULED_BY_USER`: fakesnow records the `user` given to connect, upper-cased, as it has no
  other notion of the current user.
- Snowflake appends a privilege hint to "does not exist or not authorized" messages, which fakesnow leaves out as it
  does elsewhere.
