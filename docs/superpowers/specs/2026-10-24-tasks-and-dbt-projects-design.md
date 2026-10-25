# Tasks, dbt Projects and dbt-snowflake compatibility — design

Date: 2026-10-24
Status: draft for review

## Intent

Speed up developers' local loop when they build Snowflake **tasks** and **dbt-core projects**.
Today a developer has to deploy to a real Snowflake account to see whether a task graph or a
dbt project works. With this work they run it against fakesnow — in-process (`fakesnow.patch`)
or in server mode — and assert on the results in tests in seconds.

Three things have to work, in this order, because each builds on the previous one. A small
server-mode change (Part 0) comes first, because all three need several connections to share one
instance:

1. **dbt-snowflake compatibility** — a normal dbt-core project runs repeatedly against fakesnow
   through the dbt-snowflake adapter.
2. **Tasks and task graphs** — create, alter, inspect and manually execute tasks, including graphs
   with `AFTER` dependencies and a finalizer.
3. **DBT PROJECT objects** — `CREATE/ALTER/DROP/SHOW/DESCRIBE DBT PROJECT` and
   `EXECUTE DBT PROJECT`, runnable directly and as a task body.

Each part is independently plannable and shippable. Parts 2 and 3 meet at one point: a task whose
body is `EXECUTE DBT PROJECT`.

## Goals

- SQL written for real Snowflake works unchanged against fakesnow, so the same deployment scripts
  and tests run in both places (`snow dbt deploy`/`snow dbt execute` included).
- Behaviour follows **live Snowflake**, not the docs alone (AGENTS.md: "Tests should assert the live
  Snowflake behavior"). Every new test is validated with the `snowflake-check` skill.
- Fast iteration: `EXECUTE TASK` is synchronous by default, and a dbt project can be read straight
  from a local folder so edits are picked up without redeploying.

## Non-goals

- Warehouses, serverless sizing, credits, budgets.
- Privileges and roles (`GRANT` is accepted, nothing is enforced).
- Notification integrations (`ERROR_INTEGRATION`, `SUCCESS_INTEGRATION`): stored, never called.
- Triggered tasks on streams (`WHEN SYSTEM$STREAM_HAS_DATA`): fakesnow has no streams.
- Automatic retries (`TASK_AUTO_RETRY_ATTEMPTS`), `EXECUTE TASK ... RETRY LAST`, auto-suspend after
  N failures.
- dbt Workspaces (`FROM WORKSPACE`, `snow://workspace/...`), `env.yml` secrets, `IMPORTS`,
  external access integrations (accepted and ignored), enforcing `DBT_VERSION`, dbt Fusion.
- The legacy numbered-version model of dbt projects (`VERSION$N`, `SET DEFAULT_VERSION`).

Optional (in scope only if cheap; not required for the parts to be done):

- Stored procedures / Snowflake Scripting as task bodies (`CALL`, `BEGIN ... END`,
  `EXECUTE IMMEDIATE $$...$$`). fakesnow supports none of these today.
- A real scheduler that fires resumed root tasks on their `SCHEDULE`.
- Asynchronous `EXECUTE TASK` (Snowflake's real semantics).

## Anti-goals

- **Tests that pass on fakesnow and fail on Snowflake.** Rules Snowflake enforces (root must be
  suspended before modifying a graph, `EXECUTE TASK` rejects child tasks, …) are enforced here too,
  so mistakes are caught locally rather than in prod.
- **fakesnow-only syntax a developer is forced to use.** The only fakesnow-only extension is the
  opt-in `file://` source for a dbt project; everything else is real Snowflake syntax.
- **Spreading responsibility across callers** (AGENTS.md "Keep behavior with its owner"). Task logic
  lives in a task module, dbt-project logic in a dbt-project module; the cursor only dispatches to
  them, the way it dispatches `COPY INTO` to `fakesnow/copy_into.py`.

## Constraints and current state

- fakesnow translates Snowflake SQL to DuckDB with sqlglot (`fakesnow/cursor.py` `execute` →
  `_transform` → `_execute`). Snowflake-only objects (stages, file formats, users) are emulated as
  metadata tables plus a create transform that rewrites to an `INSERT`, a `SQL_CREATED_*` status
  row in the cursor, and a `show_*` transform (`fakesnow/transforms/show.py`). New objects follow
  the same pattern.
- Snowflake-only metadata currently lives in `_fs_global`, which is always in-memory
  (`fakesnow/instance.py`) and is lost on restart even with `db_path`.
- No statement in either feature parses today (`CREATE TASK`, `EXECUTE TASK`, `CREATE DBT PROJECT`,
  `EXECUTE DBT PROJECT`, `CALL`, scripting blocks).
- Sessions: each `FakeSnow.connect()` gives a `FakeSnowflakeConnection` on its own DuckDB cursor of
  a shared DuckDB connection. A connection does not keep a reference to its `FakeSnow`, so it can't
  open a sibling session today. Role, warehouse and user are not tracked.
- `fakesnow.patch` replaces `snowflake.connector.connect` with `fs.connect` of one `FakeSnow`, so
  anything in the same process that calls the connector — including dbt-snowflake — lands on that
  instance. Server mode (`fakesnow/server.py`) has one shared `FakeSnow`, or one per login when
  `FAKESNOW_DB_PATH` is set, and runs each statement in a thread pool.
- `PUT` defaults to `AUTO_COMPRESS=TRUE`, so staged files are `.gz`. Internal stage files live under
  `LOCAL_BUCKET_PATH/{DB}/{SCHEMA}/{STAGE}/...` (`fakesnow/transforms/stage.py`). `GET` and
  `DROP STAGE` don't parse.
- Supported runtime: Python ≥ 3.11, DuckDB 2.0, sqlglot 30.22. dbt-core and dbt-snowflake are not
  dependencies today.

---

## Part 0 — Server mode: one instance per database path (decided)

Today `login_request` in `fakesnow/server.py` builds a new `FakeSnow` for every login that sets
`FAKESNOW_DB_PATH`. A second login on the same path fails with DuckDB's
`Unique file handle conflict: Cannot attach ...`. dbt, task sessions, and the
`snow dbt deploy` → `snow dbt execute` flow all open several connections, so this has to change
before server mode can support any of the three parts.

- Logins with the same `FAKESNOW_DB_PATH` (after normalising the path) share one `FakeSnow`. Each
  login still gets its own session (its own connection on that instance).
- `:isolated:` keeps its current meaning: a fresh in-memory instance per login.
- Logins without the parameter keep using the shared in-memory instance.
- This is a small behaviour change for existing `FAKESNOW_DB_PATH` users. Today the second login on
  a path fails anyway, so nothing that works today breaks. Note it in the CHANGELOG.
- When an instance is released (for example, server shutdown) is the builder's call.
- dbt-snowflake can't send arbitrary session parameters (only `QUERY_TAG`), so a dbt profile can't
  set `FAKESNOW_DB_PATH`. The server therefore also accepts a **default database path**:
  `fakesnow.server(db_path=...)`, and `fakesnow -s --db_path <path>`, which today ignores
  `--db_path`. It is used for logins that don't set the parameter. Without one, logins share the
  in-memory instance as today. (Designer's call, added while planning.)

---

## Part 1 — dbt-snowflake compatibility

### What "done" means

A dbt-core project using dbt-snowflake runs against fakesnow, in patch mode and in server mode,
**twice in a row** without errors, and the second run produces the right data. "Twice" matters: the
first run of incremental models and snapshots takes a different code path from every later run.

### Gaps to close

Found by running dbt-core 1.12.6 / dbt-snowflake 1.12.4 against fakesnow and logging every
statement. Each is a separate behaviour with its own tests.

| # | Statement dbt sends | Today | Needed |
|---|---|---|---|
| 1 | `create or replace temporary view DB.SCH.x__dbt_tmp as (...)` (incremental, 2nd+ run) | DuckDB: temporary names can only use the temp catalog | Database/schema-qualified temporary views work and resolve when referenced by the same qualified name |
| 2 | `create or replace temporary table "DB"."SCH"."X__dbt_tmp" ...` (snapshot 2nd+ run, `tmp_relation_type='table'`) | same error | Same as 1, for tables |
| 3 | `merge into ... when not matched then insert (cols) values (cols)` with unqualified `values` columns | "Ambiguous reference" | Resolves as Snowflake does (unqualified names in `VALUES` refer to the source) |
| 4 | `alter session set query_tag = '...'` / `alter session unset query_tag` | NotImplementedError | Stored per session |
| 5 | `show parameters like 'query_tag' in session` | 0 rows → dbt raises | Returns the row with the current value (empty string by default), Snowflake's columns |
| 6 | `grant ... to ...`, `revoke ...` (`grants` config) | parse error | Accepted, no effect, Snowflake's status row |
| 7 | `show grants on <object>` | parse error | Snowflake's columns, with the one row live Snowflake always has: `OWNERSHIP` held by the current role. dbt ignores OWNERSHIP when diffing grants. |
| 8 | `alter table a swap with b` | fails | Swaps the two tables' contents and names |
| 9 | `select current_role()`, `current_warehouse()` | function doesn't exist | Return a stable value (the connection's role/warehouse if given, else a default — check live what a fresh session returns) |
| 10 | `select system$cancel_all_queries(<session id>)` (dbt cleanup on Ctrl-C) | fails | Accepted, returns Snowflake's message |

Builder's call: the order of the fixes and how each is implemented, as long as each lands in the
module that already owns that kind of statement.

### Proof

- Unit tests for each gap above in the existing per-feature test files (`tests/test_ddl.py`,
  `tests/test_merge.py`, `tests/test_show.py`, …), each asserting live Snowflake output.
- One end-to-end test that runs a small dbt project through `dbtRunner` against fakesnow twice:
  `seed`, `run` (table, view, incremental with `merge`, `append` and `delete+insert`), `snapshot`,
  `test`, `docs generate`. Then it asserts on resulting table contents. It runs in patch mode
  (same `FakeSnow` across both invocations) and server mode.
- Server mode depends on Part 0. dbt connects through the profile's `host`/`port`/`protocol`, and
  all its connections land on the server's default instance.
- dbt-core and dbt-snowflake go into a new dependency group (e.g. `dbt`), not the core
  dependencies. The end-to-end test is skipped when dbt isn't installed. CI installs the group.

---

## Part 2 — Tasks and task graphs

### Statements

```sql
CREATE [ OR REPLACE | OR ALTER ] TASK [ IF NOT EXISTS ] <name>
  [ WAREHOUSE = <wh> | USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = '<size>' ]
  [ SCHEDULE = '<n> { SECONDS | MINUTES | HOURS }' | 'USING CRON <expr> <tz>' ]
  [ CONFIG = <json string> ]
  [ OVERLAP_POLICY = ... | ALLOW_OVERLAPPING_EXECUTION = TRUE|FALSE ]
  [ <session_parameter> = <value> ... ]
  [ USER_TASK_TIMEOUT_MS = n ] [ SUSPEND_TASK_AFTER_NUM_FAILURES = n ]
  [ ERROR_INTEGRATION = x ] [ SUCCESS_INTEGRATION = x ] [ COMMENT = '...' ]
  [ FINALIZE = <root> ] [ TASK_AUTO_RETRY_ATTEMPTS = n ] [ TARGET_COMPLETION_INTERVAL = '...' ]
  [ AFTER <task> [, ...] ]
  [ WHEN <boolean_expr> ]
AS <sql>

CREATE [ OR REPLACE ] TASK <name> CLONE <source>

ALTER TASK [ IF EXISTS ] <name> { RESUME | SUSPEND }
ALTER TASK [ IF EXISTS ] <name> { ADD | REMOVE } AFTER <task> [, ...]
ALTER TASK [ IF EXISTS ] <name> SET <prop> = <value> [, ...] | UNSET <prop> [, ...]
ALTER TASK [ IF EXISTS ] <name> SET FINALIZE = <root> | UNSET FINALIZE
ALTER TASK [ IF EXISTS ] <name> MODIFY AS <sql> | MODIFY WHEN <expr> | REMOVE WHEN

DROP TASK [ IF EXISTS ] <name>
SHOW [ TERSE ] TASKS [ LIKE '...' ] [ IN { ACCOUNT | DATABASE [d] | SCHEMA [s] } ]
                     [ STARTS WITH '...' ] [ ROOT ONLY ] [ LIMIT n [ FROM '...' ] ]
{ DESC | DESCRIBE } TASK <name>
EXECUTE TASK <name> [ USING CONFIG = <json string> ]

SELECT SYSTEM$TASK_DEPENDENTS_ENABLE('<task>')
```

Every property is parsed and stored, and shows up in `SHOW TASKS`/`DESCRIBE TASK`, even when
fakesnow does nothing with it (warehouse, integrations, sizes). Unknown properties raise the same
error Snowflake does.

### Catalog rules (enforced, matching Snowflake)

- New, replaced and cloned tasks are `suspended`. `CREATE OR REPLACE` gives a new `id`.
- All tasks in a graph share database and schema. A task's predecessors must exist.
- Child tasks and finalizers can't have a `SCHEDULE`. A finalizer can't have predecessors or
  children. A root has at most one finalizer.
- **Root must be suspended** before any task in its graph is created/replaced, altered, dropped,
  given/removed a predecessor, resumed or suspended, or before its finalizer changes. Exact error
  text from live Snowflake.
- `REMOVE AFTER` that leaves a child with no predecessors, or dropping its predecessor, makes it a
  standalone task, suspended, with `last_suspended_reason` set (`CHILD_BECAME_ROOT`).
  `UNSET FINALIZE` → `FINALIZER_BECAME_ROOT`.
- `SYSTEM$TASK_DEPENDENTS_ENABLE` resumes the task and all its dependents recursively.
- Cycles are rejected. Graph limits (1,000 tasks, 100 predecessors, 100 children) are enforced.

### Running a graph: `EXECUTE TASK`

- Accepted on a standalone or root task whether it's suspended or started. Rejected on a child
  task with Snowflake's error.
- **Synchronous by default**: the statement returns only after the whole graph run, finalizer
  included, has finished. A test can assert on tables right after it.
- The statement returns Snowflake's own result for `EXECUTE TASK` (verify live). **A failing task
  does not make `EXECUTE TASK` raise**; the failure is recorded in task history, the same as in
  Snowflake. This keeps test SQL portable.
- Run semantics:
  - Tasks run in topological order. A child runs only after all its resumed predecessors
    succeeded.
  - A suspended child (in a graph run started by `EXECUTE TASK` on a suspended root, children keep
    their own state) is skipped, and the graph continues as if it succeeded.
  - A failed task's dependents don't run. The graph is marked failed.
  - `WHEN` false → that task is `SKIPPED` and its dependents don't run.
  - The finalizer runs last, whether the rest succeeded or failed. It doesn't run if the root was
    skipped.
  - `USER_TASK_TIMEOUT_MS`: a task that runs longer is marked failed (best effort; a builder may
    defer this).
- Each task body runs in **its own new session** on the same `FakeSnow` instance — not in the
  caller's session — so it can't see or change the caller's transaction, variables or current
  schema. Current database and schema in that session are the task's own (verify live). Session
  parameters set on the task apply to that session (only the ones fakesnow already understands do
  anything).
- **Caller in an open transaction** (designer's call, pending live check): if live Snowflake rejects
  `EXECUTE TASK` / `EXECUTE DBT PROJECT` inside an explicit transaction, do the same. Otherwise,
  commit the caller's open transaction first, the way fakesnow already commits it before DDL, and
  then run. Either way, the caller's uncommitted writes never conflict with the task's session, and
  the caller sees the run's results straight after. Task history and metadata are written outside
  the caller's transaction, so a later rollback by the caller never erases them. The same rule
  applies to `EXECUTE DBT PROJECT`.
- `USING CONFIG` merges into the root's `CONFIG` for that run only.
- Supported bodies: any single SQL statement fakesnow supports, and `EXECUTE DBT PROJECT` (Part 3).
  `CALL` and scripting blocks are optional (see Non-goals).
- Supported in task bodies and `WHEN`:
  - `SYSTEM$SET_RETURN_VALUE`
  - `SYSTEM$GET_PREDECESSOR_RETURN_VALUE`
  - `SYSTEM$GET_TASK_GRAPH_CONFIG`
  - `SYSTEM$TASK_RUNTIME_INFO`
  - `SYSTEM$CURRENT_USER_TASK_NAME`

  They behave as in Snowflake when called outside a task (error or NULL — verify live).

### Observability

- `INFORMATION_SCHEMA.TASK_HISTORY(...)` with its named arguments (`SCHEDULED_TIME_RANGE_START/END`,
  `RESULT_LIMIT`, `TASK_NAME`, `ERROR_ONLY`, `ROOT_TASK_ID`) and Snowflake's columns. One row per
  task per graph run, with `STATE` (`SUCCEEDED`, `FAILED`, `SKIPPED`, `CANCELLED`, and `EXECUTING`
  in async mode), `ERROR_CODE`, `ERROR_MESSAGE`, `QUERY_ID` (the body's query id, usable with
  `RESULT_SCAN`), `RETURN_VALUE`, `GRAPH_RUN_GROUP_ID`, `RUN_ID`, `ATTEMPT_NUMBER = 1`,
  `SCHEDULED_FROM = 'EXECUTE_TASK'` (or `'SCHEDULE'` from the optional scheduler).
- `INFORMATION_SCHEMA.COMPLETE_TASK_GRAPHS(...)` and `TASK_DEPENDENTS(...)`.
  `CURRENT_TASK_GRAPHS(...)` only if async mode is built.
- `SHOW TASKS`: `last_committed_on` is set by the first resume or `EXECUTE TASK`.

### Optional: async execution and a scheduler

- **Async `EXECUTE TASK`**: opt-in, Snowflake's real behaviour. The statement returns immediately
  and the graph runs on a background thread. History shows `EXECUTING` until it ends.
  - Switched on per `FakeSnow` instance (constructor argument), and per server session through a
    session parameter in the style of the existing `FAKESNOW_DB_PATH`.
  - The parameter name is the builder's call.
- **Scheduler**: opt-in, off by default. It fires resumed root tasks on their `SCHEDULE` (intervals
  and cron). It hangs off `FakeSnow`, so it works in patch mode and server mode, and stops when the
  instance closes. Overlap follows `OVERLAP_POLICY`. If cron needs a new library, it goes in an
  optional extra.

### Storage

Tasks are schema objects, so task definitions and task history are stored **per database**
(alongside `<db>._fs_information_schema`), not in the in-memory `_fs_global`. They survive a
restart when `db_path` is used and disappear with `DROP DATABASE`/`DROP SCHEMA`. Table layout is
the builder's call.

---

## Part 3 — DBT PROJECT objects

Targets Snowflake's current **live-version model** (2026_06 behaviour change bundle): a project has
one editable version, `live`, at `snow://dbt/<db>.<schema>.<project>/versions/live/`.

### Statements

```sql
CREATE [ OR REPLACE ] DBT PROJECT [ IF NOT EXISTS ] <name>
  FROM '<source>'
  [ DBT_VERSION = '<v>' ] [ DEFAULT_TARGET = '<t>' ] [ DEFAULT_ENVIRONMENT = '<e>' ]
  [ EXTERNAL_ACCESS_INTEGRATIONS = ( ... ) | '<x>' ] [ AUTO_COMPILE = TRUE|FALSE ]
  [ DEFAULT_WRITEBACK = TRUE|FALSE ] [ COMMENT = '...' ]
  [ GIT_URL = '...' ] [ GIT_COMMIT = '...' ] [ GIT_BRANCH = '...' ]   -- sent by snow dbt deploy

ALTER DBT PROJECT <name> DEPLOY FROM '<source>'
ALTER DBT PROJECT <name> ADD VERSION [ <alias> ] FROM '<source>' [ GIT_* = ... ]   -- treated as DEPLOY
ALTER DBT PROJECT [ IF EXISTS ] <name> SET <prop> = <value> [, ...] | UNSET <prop> [, ...]
ALTER DBT PROJECT [ IF EXISTS ] <name> RENAME TO <new_name>
DROP DBT PROJECT [ IF EXISTS ] <name>
SHOW DBT PROJECTS [ LIKE '...' ] [ IN ... ] [ STARTS WITH '...' ] [ LIMIT n [ FROM '...' ] ]
SHOW VERSIONS IN DBT PROJECT <name>
{ DESC | DESCRIBE } DBT PROJECT <name>

EXECUTE DBT PROJECT [ IF EXISTS ] <name>
  [ ARGS = '<dbt command and flags>' ] [ DBT_VERSION = '...' ]
  [ ENV_VARS = ( 'DBT_X' = '<value>' [, ...] ) ] [ WRITEBACK = TRUE|FALSE ]
  [ ENVIRONMENT = '...' ] [ EXTERNAL_ACCESS_INTEGRATIONS = (...) ] [ IMPORTS = (...) ]
```

`ENVIRONMENT`, `EXTERNAL_ACCESS_INTEGRATIONS` and `IMPORTS` are parsed and ignored.

### Project sources (both supported — decided)

1. **Stage (faithful)**: `FROM '@db.sch.stage/path'`.
   - CREATE and DEPLOY copy the files into a fakesnow-managed `live` folder for the project and
     decompress `.gz` files written by `PUT` with `AUTO_COMPRESS=TRUE`.
   - The source must contain exactly one `dbt_project.yml` at its root, otherwise Snowflake's error.
   - This is the path `snow dbt deploy` uses (temporary stage, `PUT`, `CREATE`/`ADD VERSION`).
2. **Local folder (fakesnow-only extension)**: `FROM 'file:///abs/path/to/project'`.
   - Nothing is copied. Every `EXECUTE` runs against the folder as it is now, so edits show up
     immediately.
   - dbt writes `target/` and `logs/` into that folder, as a local `dbt run` would.

`AUTO_COMPILE = TRUE` (the default) runs `dbt compile` during CREATE/DEPLOY as in Snowflake. Its
failure fails the statement (verify live). The builder may make this a no-op for `file://` sources
if it slows the loop, recorded in the docs.

### Running: `EXECUTE DBT PROJECT`

- dbt runs **in-process** through dbt-core's `dbtRunner`, using whatever dbt-core and dbt-snowflake
  the developer has installed (new optional extra, e.g. `fakesnow[dbt]`).
  - If they're missing, the statement raises a clear error naming the extra.
  - `DBT_VERSION` is stored and shown but not enforced. `SHOW DBT PROJECTS` reports the installed
    versions in `dbt_version`/`dbt_snowflake_version`.
- **Every connection dbt opens lands on the same `FakeSnow` instance** as the session that issued
  `EXECUTE`.
  - In patch mode that is already true.
  - In server mode, fakesnow must route dbt-snowflake's `snowflake.connector.connect` calls to the
    server's instance for the duration of the run. Only one dbt run executes at a time per
    process, serialised with a lock.
  - Mechanism is the builder's call.
- **Profile**: read from `dbt_projects_profiles.yml` if present, else `profiles.yml`, in the project
  root (Snowflake's rule).
  - `type`, `database`, `schema`, `role`, `warehouse` must be present (Snowflake's validation).
  - `account`, `user` and credentials are ignored.
  - `database`/`schema` are honoured.
  - `threads` is honoured. If concurrent writes hit DuckDB conflicts, the builder may cap threads
    at 1 and document it.
  - `--profiles-dir` and `--project-dir` in ARGS are rejected as in Snowflake.
- **ARGS**: no command → `run`. `DEFAULT_TARGET` adds `--target` unless ARGS has one.
  - Supported commands are the ones Snowflake lists: build, clean, compile, deps, docs generate,
    list, parse, run, retry, run-operation, seed, show, snapshot, source freshness, test.
  - `deps` just runs, since a dev machine has network access.
- **ENV_VARS**: keys must be uppercase and start with `DBT_`. Literal values only. The
  `'{{ select ... }}'` SQL form is optional.
- **WRITEBACK** (default from `DEFAULT_WRITEBACK`, TRUE): for stage sources, TRUE runs in the `live`
  folder so `target/` persists between runs. FALSE runs in a temporary copy. Ignored for `file://`.
- **Result**: Snowflake's single row — success flag, `EXCEPTION`, `STDOUT`, `OUTPUT_ARCHIVE_URL`.
  Column names verified live: the docs say `0|1 Success`, the Snowflake CLI reads `SUCCESS`.
  - A dbt failure (model error, test failure) returns the row with success = FALSE and does
    **not** raise.
  - The statement raises only for what Snowflake raises on (missing project, no
    `dbt_project.yml`, invalid profile, rejected flags — verify live).
- **Artifacts per run**, keyed by the `EXECUTE` statement's query id:
  - Contents: `target/manifest.json`, `run_results.json`, `sources.json` when present,
    `logs/dbt.log`, and `dbt_artifacts.zip` of both.
  - Functions:
    - `SYSTEM$GET_DBT_LOG(query_id [, n])`
    - `SYSTEM$LOCATE_DBT_ARTIFACTS(query_id)`
    - `SYSTEM$LOCATE_DBT_ARCHIVE(query_id)`
  - Each returns Snowflake's `snow://dbt/<fqn>/results/query_id_<qid>/...` locations.
  - Reading the files back by `GET`/`LIST` on those locations is optional. fakesnow has no `GET`
    today.
- **History**: `DBT_PROJECT_EXECUTION_HISTORY(...)` with Snowflake's arguments and columns, called as
  `SNOWFLAKE.INFORMATION_SCHEMA.DBT_PROJECT_EXECUTION_HISTORY` (fakesnow has no `SNOWFLAKE`
  database today; how that name resolves is the builder's call).

### Snowflake CLI (in scope — decided)

`snow dbt deploy` and `snow dbt execute` run against fakesnow in server mode, and a test proves it.
That requires the SQL the CLI sends (from `snowflake-cli` `_plugins/dbt/manager.py`):

- `SELECT SYSTEM$SUPPORTED_DBT_VERSIONS()`, returning a JSON array that includes the installed
  dbt-core version.
- `CREATE TEMPORARY STAGE IF NOT EXISTS ...`.
- Recursive, uncompressed `PUT` into stage subfolders over HTTP.
- `DESCRIBE DBT PROJECT` to check whether the project exists. On a miss it must fail the way
  Snowflake does, because the CLI branches on that.
- `CREATE DBT PROJECT ... FROM @stage` with the `GIT_*` clauses, or
  `ALTER ... SET/UNSET` plus `ALTER ... ADD VERSION FROM @stage`, or `CREATE OR REPLACE` with
  `--force`.
- `EXECUTE DBT PROJECT ... args='...'`. The CLI reads the `SUCCESS` and `STDOUT` columns.
- `SHOW DBT PROJECTS` (`snow dbt list`) and `DROP DBT PROJECT`.

**Proof:** one end-to-end test in server mode runs the real CLI (`snowflake-cli` in the `dbt`
dependency group) against a sample project: `deploy`, `execute run`, a redeploy after an edit,
`execute run` again. It then asserts on the results. It is skipped when the CLI isn't installed.
`--run-async` is out of scope.

### As a task body

`CREATE TASK t ... AS EXECUTE DBT PROJECT p ARGS='run'` works. The dbt run happens in the task's
own session (Part 2). If the result row has success = FALSE, the task run is `FAILED` with the
`EXCEPTION` text as its error message, so dependents don't run (verify live how Snowflake reports
this in `TASK_HISTORY`).

### Storage

Project definitions and run history are stored per database, like tasks. Managed `live` folders and
per-run artifacts go on disk under a fakesnow-managed directory. It sits under `db_path` when one is
set (so they persist with the data) and otherwise under a temporary directory like
`LOCAL_BUCKET_PATH`.

---

## Verify against live Snowflake before fixing behaviour

The docs disagree with themselves or with memory on these. Settle each with the `snowflake-check`
skill and encode the live answer in tests:

1. `SHOW TASKS` / `DESCRIBE TASK` column list and order (docs list 26 columns, no `budget`).
2. Status rows for `CREATE TASK`, `ALTER TASK`, `DROP TASK`, `EXECUTE TASK`, and the DBT PROJECT
   statements.
3. Error texts: modifying a graph with a started root, `EXECUTE TASK` on a child, missing
   predecessor, second finalizer, non-existent project.
4. Current database/schema inside a task run. What the `SYSTEM$` task functions return outside a
   task.
5. Whether `WHEN` false on a child still lets the finalizer run.
6. `EXECUTE DBT PROJECT` result column names, and which failures raise vs return success = FALSE.
7. How a task whose body is a failed `EXECUTE DBT PROJECT` appears in `TASK_HISTORY`.
8. `SHOW DBT PROJECTS` / `DESCRIBE DBT PROJECT` / `SHOW VERSIONS IN DBT PROJECT` columns and values.
9. What `current_role()` / `current_warehouse()` return in a fresh session; `show parameters like
   'query_tag'` row shape.
10. Whether `EXECUTE TASK` / `EXECUTE DBT PROJECT` inside an explicit transaction is rejected or
    commits it.

## Left to the builder

- Parsing approach: sqlglot parser extensions in `fakesnow/dialect.py` vs handling `exp.Command`.
  Prefer extending the dialect where sqlglot can be taught the grammar.
- Module boundaries within "task logic in a task module, dbt-project logic in a dbt-project module".
- How a connection gets access to its `FakeSnow` to open the sibling sessions tasks and dbt need.
- Statement-type mapping in `fakesnow/statement_type.py` for the new statements, matching what
  Snowflake reports.
- Table layouts for definitions and history.
- README coverage list and CHANGELOG entries.

## References

- Snowflake tasks:
  - https://docs.snowflake.com/en/sql-reference/sql/create-task
  - https://docs.snowflake.com/en/sql-reference/sql/alter-task
  - https://docs.snowflake.com/en/sql-reference/sql/execute-task
  - https://docs.snowflake.com/en/sql-reference/sql/show-tasks
  - https://docs.snowflake.com/en/user-guide/tasks-graphs
  - https://docs.snowflake.com/en/sql-reference/functions/task_history
- dbt Projects on Snowflake:
  - https://docs.snowflake.com/en/user-guide/data-engineering/dbt-projects-on-snowflake
  - https://docs.snowflake.com/en/sql-reference/sql/create-dbt-project
  - https://docs.snowflake.com/en/sql-reference/sql/execute-dbt-project
  - https://docs.snowflake.com/en/release-notes/bcr-bundles/2026_06/bcr-2362
- Snowflake CLI dbt plugin (SQL it issues): https://github.com/snowflakedb/snowflake-cli/tree/main/src/snowflake/cli/_plugins/dbt
- dbt-snowflake gap evidence (statement logs from this session):
  `/private/tmp/claude-501/-Users-mwiewior-research-git-fakesnow/40d85982-d437-47e1-bcd8-b43e97e3e09a/scratchpad/`
