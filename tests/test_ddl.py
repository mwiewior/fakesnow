import contextlib

import pytest
import snowflake.connector
import snowflake.connector.cursor
import snowflake.connector.errors
import sqlglot

from fakesnow.transforms.ddl import alter_table_add_multiple_columns, alter_table_strip_cluster_by


def test_alter_table_add_multiple_columns() -> None:
    # Test multiple columns are split into separate statements
    sql = "alter table tab1 add column col1 int, col2 varchar(50), col3 boolean"
    result = alter_table_add_multiple_columns(sqlglot.parse_one(sql, dialect="snowflake"))

    assert len(result) == 3
    assert result[0].sql() == "ALTER TABLE tab1 ADD COLUMN col1 INT"
    assert result[1].sql() == "ALTER TABLE tab1 ADD COLUMN col2 VARCHAR(50)"
    assert result[2].sql() == "ALTER TABLE tab1 ADD COLUMN col3 BOOLEAN"

    # Test multiple columns with mix with one column having IF NOT EXISTS
    sql_if_not_exists = "alter table tab1 add column if not exists col1 int, col2 varchar(50)"
    result_if_not_exists = alter_table_add_multiple_columns(sqlglot.parse_one(sql_if_not_exists, dialect="snowflake"))

    assert len(result_if_not_exists) == 2
    assert result_if_not_exists[0].sql() == "ALTER TABLE tab1 ADD COLUMN IF NOT EXISTS col1 INT"
    assert result_if_not_exists[1].sql() == "ALTER TABLE tab1 ADD COLUMN col2 VARCHAR(50)"


def test_alter_table_strip_cluster_by() -> None:
    success_sql = "SELECT 'Statement executed successfully.' AS status"

    # ALTER TABLE ... CLUSTER BY
    assert (
        sqlglot.parse_one("alter table table1 cluster by (name)").transform(alter_table_strip_cluster_by).sql()
        == success_sql
    )

    # ALTER TABLE ... CLUSTER BY with multiple columns
    assert (
        sqlglot.parse_one("alter table table1 cluster by (name, id)").transform(alter_table_strip_cluster_by).sql()
        == success_sql
    )

    # ALTER TABLE ... DROP CLUSTERING KEY
    assert (
        sqlglot.parse_one("alter table table1 drop clustering key", read="snowflake")
        .transform(alter_table_strip_cluster_by)
        .sql()
        == success_sql
    )

    # ALTER TABLE ... RESUME RECLUSTER
    assert (
        sqlglot.parse_one("alter table table1 resume recluster", read="snowflake")
        .transform(alter_table_strip_cluster_by)
        .sql()
        == success_sql
    )

    # ALTER TABLE ... SUSPEND RECLUSTER
    assert (
        sqlglot.parse_one("alter table table1 suspend recluster", read="snowflake")
        .transform(alter_table_strip_cluster_by)
        .sql()
        == success_sql
    )

    # Regular ALTER TABLE should not be affected
    regular_alter = sqlglot.parse_one("alter table table1 add column name varchar(20)")
    assert regular_alter.transform(alter_table_strip_cluster_by).sql() != success_sql


def test_qualified_temporary_view(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create or replace temporary view db1.schema1.tv as select 1 as a")
    assert cur.fetchall() == [("View TV successfully created.",)]
    cur.execute("select a from db1.schema1.tv")
    assert cur.fetchall() == [(1,)]


def test_qualified_temporary_table(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute('create or replace temporary table "DB1"."SCHEMA1"."TT" as select 1 as a')
    assert cur.fetchall() == [("Table TT successfully created.",)]


def test_qualified_temporary_table_in_missing_schema(cur: snowflake.connector.cursor.SnowflakeCursor):
    with pytest.raises(snowflake.connector.errors.ProgrammingError, match="does not exist or not authorized"):
        cur.execute("create temporary table db1.missing_schema.tt (a int)")


def test_qualified_temporary_table_insert(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute('create or replace temporary table "DB1"."SCHEMA1"."TT" as select 1 as a')
    cur.execute("insert into db1.schema1.tt values (2)")
    cur.execute("select a from db1.schema1.tt order by a")
    assert cur.fetchall() == [(1,), (2,)]


def test_qualified_temporary_table_delete(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute('create or replace temporary table "DB1"."SCHEMA1"."TT" as select 1 as a')
    cur.execute("delete from db1.schema1.tt where a = 1")
    cur.execute("select a from db1.schema1.tt")
    assert cur.fetchall() == []


def test_qualified_temporary_table_schema_qualified_select(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute('create or replace temporary table "DB1"."SCHEMA1"."TT" as select 2 as a')
    cur.execute("select a from schema1.tt")
    assert cur.fetchall() == [(2,)]


def test_qualified_temporary_table_qualified_column(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create temporary table db1.schema1.tt (a int)")
    cur.execute("insert into db1.schema1.tt values (1)")
    cur.execute("select db1.schema1.tt.a from db1.schema1.tt")
    assert cur.fetchall() == [(1,)]


def test_qualified_temporary_table_describe(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create temporary table db1.schema1.tt (a int)")
    cur.execute("describe table db1.schema1.tt")
    assert [r[0] for r in cur.fetchall()] == ["A"]


def test_qualified_temporary_table_drop(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create temporary table db1.schema1.tt (a int)")
    cur.execute("drop table if exists db1.schema1.tt")
    assert cur.fetchall() == [("TT successfully dropped.",)]


def test_qualified_temporary_table_shadows_permanent(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create table db1.schema1.t (a int)")
    cur.execute("insert into db1.schema1.t values (1)")
    cur.execute("create temporary table db1.schema1.t (a int)")
    cur.execute("select count(*) from db1.schema1.t")
    assert cur.fetchall() == [(0,)]
    cur.execute("drop table db1.schema1.t")
    cur.execute("select a from db1.schema1.t")
    assert cur.fetchall() == [(1,)]


def test_qualified_temporary_table_not_visible_from_other_schema(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create schema db1.other")
    cur.execute("create temporary table db1.schema1.tt (a int)")
    with pytest.raises(snowflake.connector.errors.ProgrammingError, match="does not exist"):
        cur.execute("select * from db1.other.tt")


@pytest.mark.xfail(reason="temporary objects with the same name in different schemas are not implemented", strict=True)
def test_temporary_tables_with_the_same_name_in_different_schemas(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create schema db1.other")
    cur.execute("create temporary table db1.schema1.tt (a int)")
    cur.execute("insert into db1.schema1.tt values (1)")
    cur.execute("create temporary table db1.other.tt (a int)")
    cur.execute("insert into db1.other.tt values (2)")
    cur.execute("select a from db1.schema1.tt")
    assert cur.fetchall() == [(1,)]


def test_temporary_table_replaced_in_other_schema_keeps_first_rows(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create schema db1.other")
    cur.execute("create temporary table db1.schema1.tt (a int)")
    cur.execute("insert into db1.schema1.tt values (1)")
    # creating a same-named temporary table in another schema doesn't touch the first one
    with contextlib.suppress(snowflake.connector.errors.ProgrammingError):
        cur.execute("create or replace temporary table db1.other.tt (a int)")
    cur.execute("select a from db1.schema1.tt")
    assert cur.fetchall() == [(1,)]


def test_temporary_table_not_visible_unqualified_from_other_schema(
    cur: snowflake.connector.cursor.SnowflakeCursor,
):
    cur.execute("create table db1.schema1.t (a int)")
    cur.execute("insert into db1.schema1.t values (1)")
    cur.execute("create schema db1.other")
    cur.execute("create temporary table db1.other.t (a int)")
    cur.execute("use schema schema1")
    cur.execute("select a from t")
    assert cur.fetchall() == [(1,)]


def test_temporary_table_not_visible_beside_cte_in_nested_scope(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create table db1.schema1.t (a int)")
    cur.execute("insert into db1.schema1.t values (1)")
    cur.execute("create schema db1.other")
    cur.execute("create temporary table db1.other.t (a int)")
    cur.execute("insert into db1.other.t values (2)")
    cur.execute("use schema schema1")
    # the CTE only names t inside the subquery, so the outer t is the permanent table
    cur.execute("select a from t where exists (with t as (select 1 as x) select * from t)")
    assert cur.fetchall() == [(1,)]


def test_comment_on_shadowing_temporary_table_status(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create table t1 (a int)")
    cur.execute("create temporary table t1 (a int)")
    cur.execute("comment on table t1 is 'temporary'")
    assert cur.fetchall() == [("Statement executed successfully.",)]


def test_comment_on_table_status(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create table t1 (a int)")
    cur.execute("comment on table t1 is 'c'")
    assert cur.fetchall() == [("Statement executed successfully.",)]


def test_comment_on_shadowing_temporary_table_leaves_permanent_comment(
    cur: snowflake.connector.cursor.SnowflakeCursor,
):
    cur.execute("create table t1 (a int) comment = 'permanent'")
    cur.execute("create temporary table t1 (a int)")
    cur.execute("comment on table t1 is 'temporary'")
    cur.execute("select comment from information_schema.tables where table_name = 'T1' and table_type = 'BASE TABLE'")
    assert cur.fetchall() == [("permanent",)]


def test_add_column_to_shadowing_temporary_table_leaves_permanent_text_lengths(
    cur: snowflake.connector.cursor.SnowflakeCursor,
):
    cur.execute("create table t1 (a int, b varchar(20))")
    cur.execute("create temporary table t1 (a int)")
    cur.execute("alter table t1 add column b varchar(5)")
    # drops the temporary table, which shadows the permanent one
    cur.execute("drop table t1")
    cur.execute(
        "select character_maximum_length from information_schema.columns where table_name = 'T1' and column_name = 'B'"
    )
    assert cur.fetchall() == [(20,)]


def test_rename_to_name_of_temporary_table_in_other_schema(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create schema db1.other")
    cur.execute("create temporary table db1.other.a (x int)")
    cur.execute("use schema schema1")
    cur.execute("create table b (x int)")
    cur.execute("insert into b values (1)")
    cur.execute("alter table b rename to a")
    cur.execute("select x from db1.schema1.a")
    assert cur.fetchall() == [(1,)]


def test_swap_shadowing_temporary_tables_leaves_permanent_metadata(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create table a (c varchar(5)) comment = 'from a'")
    cur.execute("create table b (c varchar(20)) comment = 'from b'")
    cur.execute("create temporary table a (c int)")
    cur.execute("create temporary table b (c int)")
    cur.execute("alter table a swap with b")
    # drops the temporary tables, which shadow the permanent ones
    cur.execute("drop table a")
    cur.execute("drop table b")
    cur.execute("select table_name, comment from information_schema.tables where table_name in ('A', 'B') order by 1")
    assert cur.fetchall() == [("A", "from a"), ("B", "from b")]


def test_create_permanent_table_beside_temporary_keeps_text_lengths(
    cur: snowflake.connector.cursor.SnowflakeCursor,
):
    cur.execute("create temporary table t (c int)")
    cur.execute("create table t (c varchar(5))")
    # drops the temporary table, which shadows the permanent one
    cur.execute("drop table t")
    cur.execute(
        "select character_maximum_length from information_schema.columns where table_name = 'T' and column_name = 'C'"
    )
    assert cur.fetchall() == [(5,)]


def test_create_permanent_table_beside_temporary_keeps_comment(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create temporary table t (c int)")
    cur.execute("create table t (c int) comment = 'permanent'")
    cur.execute("drop table t")
    cur.execute("select comment from information_schema.tables where table_name = 'T'")
    assert cur.fetchall() == [("permanent",)]


def test_qualified_temporary_table_is_session_isolated(conn: snowflake.connector.SnowflakeConnection):
    conn.cursor().execute("create temporary table db1.schema1.tt (a int)")
    with (
        snowflake.connector.connect(database="db1", schema="schema1") as other,
        pytest.raises(snowflake.connector.errors.ProgrammingError, match="does not exist"),
    ):
        other.cursor().execute("select * from db1.schema1.tt")


def test_temporary_table_clone_is_created_in_its_schema(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create schema db1.other")
    cur.execute("use schema schema1")
    cur.execute("create table db1.schema1.y (id int)")
    cur.execute("insert into db1.schema1.y values (1)")
    cur.execute("create temporary table db1.other.x clone db1.schema1.y")
    cur.execute("select id from db1.other.x")
    assert cur.fetchall() == [(1,)]


def test_temporary_table_clone_is_not_created_in_current_schema(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create schema db1.other")
    cur.execute("use schema schema1")
    cur.execute("create table db1.schema1.y (id int)")
    cur.execute("create temporary table db1.other.x clone db1.schema1.y")
    with pytest.raises(snowflake.connector.errors.ProgrammingError, match="does not exist"):
        cur.execute("select * from db1.schema1.x")


def test_temporary_table_clone_is_session_isolated(conn: snowflake.connector.SnowflakeConnection):
    conn.cursor().execute("create table db1.schema1.y (id int)")
    conn.cursor().execute("create temporary table db1.schema1.x clone db1.schema1.y")
    with (
        snowflake.connector.connect(database="db1", schema="schema1") as other,
        pytest.raises(snowflake.connector.errors.ProgrammingError, match="does not exist"),
    ):
        other.cursor().execute("select * from db1.schema1.x")


def test_create_or_replace_temporary_table_clone_beside_permanent_of_same_name(
    cur: snowflake.connector.cursor.SnowflakeCursor,
):
    cur.execute("create schema db1.other")
    cur.execute("use schema schema1")
    cur.execute("create table db1.schema1.y (id int)")
    cur.execute("create table db1.schema1.z (id int)")
    cur.execute("create or replace temporary table db1.other.z clone db1.schema1.y")
    assert cur.fetchall() == [("Table Z successfully created.",)]


def test_temporary_table_text_lengths_leave_permanent_table_in_current_schema(
    cur: snowflake.connector.cursor.SnowflakeCursor,
):
    cur.execute("create schema db1.other")
    cur.execute("use schema schema1")
    cur.execute("create table db1.schema1.t (c varchar(5))")
    cur.execute("create temporary table db1.other.t (c varchar(20))")
    cur.execute("describe table db1.schema1.t")
    assert [(r[0], r[1]) for r in cur.fetchall()] == [("C", "VARCHAR(5)")]


def test_temporary_table_comment_leaves_permanent_table_in_current_schema(
    cur: snowflake.connector.cursor.SnowflakeCursor,
):
    cur.execute("create schema db1.other")
    cur.execute("use schema schema1")
    cur.execute("create table db1.schema1.t (id int) comment = 'permanent'")
    cur.execute("create temporary table db1.other.t (id int) comment = 'temporary'")
    cur.execute("select comment from db1.information_schema.tables where table_schema = 'SCHEMA1' and table_name = 'T'")
    assert cur.fetchall() == [("permanent",)]


def test_alter_table_swap_with(cur: snowflake.connector.cursor.SnowflakeCursor) -> None:
    cur.execute("create table a (x int)")
    cur.execute("insert into a values (1)")
    cur.execute("create table b (y varchar)")
    cur.execute("alter table a swap with b")
    assert cur.fetchall() == [("Statement executed successfully.",)]
    cur.execute("select * from a")
    assert cur.fetchall() == []
    assert [d.name for d in cur.description] == ["Y"]
    cur.execute("select x from b")
    assert cur.fetchall() == [(1,)]


def test_alter_table_swap_with_keeps_comments(cur: snowflake.connector.cursor.SnowflakeCursor) -> None:
    cur.execute("create table a (x int) comment = 'from a'")
    cur.execute("create table b (y int) comment = 'from b'")
    cur.execute("alter table a swap with b")
    cur.execute("select table_name, comment from information_schema.tables where table_name in ('A', 'B') order by 1")
    assert cur.fetchall() == [("A", "from b"), ("B", "from a")]


def test_alter_table_swap_with_keeps_text_lengths(cur: snowflake.connector.cursor.SnowflakeCursor) -> None:
    cur.execute("create table a (c varchar(5))")
    cur.execute("create table b (c varchar(20))")
    cur.execute("alter table a swap with b")
    cur.execute(
        "select table_name, character_maximum_length from information_schema.columns "
        "where table_name in ('A', 'B') order by 1"
    )
    assert cur.fetchall() == [("A", 20), ("B", 5)]


def test_create_table_with_quoted_name_containing_apostrophe(cur: snowflake.connector.cursor.SnowflakeCursor) -> None:
    cur.execute('create table "a\'b" (x int)')
    assert cur.fetchall() == [("Table a'b successfully created.",)]


def test_alter_table_swap_with_quoted_name_containing_apostrophe(
    cur: snowflake.connector.cursor.SnowflakeCursor,
) -> None:
    cur.execute('create table "a\'b" (x int)')
    cur.execute("create table c (y int)")
    cur.execute('alter table "a\'b" swap with c')
    cur.execute('select * from "a\'b"')
    assert [d.name for d in cur.description] == ["Y"]


def test_alter_table_swap_with_missing_table(cur: snowflake.connector.cursor.SnowflakeCursor) -> None:
    cur.execute("create table a (x int)")
    with pytest.raises(snowflake.connector.errors.ProgrammingError, match="does not exist"):
        cur.execute("alter table a swap with nope")


def test_alter_table_swap_with_missing_table_leaves_tables_intact(
    cur: snowflake.connector.cursor.SnowflakeCursor,
) -> None:
    cur.execute("create table a (x int)")
    cur.execute("insert into a values (1)")
    with pytest.raises(snowflake.connector.errors.ProgrammingError):
        cur.execute("alter table a swap with nope")
    cur.execute("select x from a")
    assert cur.fetchall() == [(1,)]


def test_alter_table_swap_with_itself_succeeds(cur: snowflake.connector.cursor.SnowflakeCursor) -> None:
    cur.execute("create table a (x int)")
    cur.execute("alter table a swap with schema1.a")
    assert cur.fetchall() == [("Statement executed successfully.",)]


def test_alter_table_swap_with_itself_leaves_table_intact(cur: snowflake.connector.cursor.SnowflakeCursor) -> None:
    cur.execute("create table a (x int)")
    cur.execute("insert into a values (1)")
    cur.execute("alter table a swap with schema1.a")
    cur.execute("select x from a")
    assert cur.fetchall() == [(1,)]


def test_alter_table_swap_with_view(cur: snowflake.connector.cursor.SnowflakeCursor) -> None:
    cur.execute("create table a (x int)")
    cur.execute("create view b as select 1 as x")
    with pytest.raises(
        snowflake.connector.errors.ProgrammingError,
        match=r"^002203 \(02000\): SQL compilation error: Object found is of type 'VIEW', not specified type "
        r"'TABLE'\.$",
    ):
        cur.execute("alter table a swap with b")


def test_alter_table_swap_with_view_leaves_table_intact(cur: snowflake.connector.cursor.SnowflakeCursor) -> None:
    cur.execute("create table a (x int)")
    cur.execute("insert into a values (1)")
    cur.execute("create view b as select 1 as x")
    with pytest.raises(snowflake.connector.errors.ProgrammingError):
        cur.execute("alter table a swap with b")
    cur.execute("select x from a")
    assert cur.fetchall() == [(1,)]


def test_alter_table_swap_temporary_with_permanent(cur: snowflake.connector.cursor.SnowflakeCursor) -> None:
    cur.execute("create table b (x int)")
    cur.execute("create temporary table a (x int)")
    with pytest.raises(
        snowflake.connector.errors.ProgrammingError,
        match=r"^001486 \(42601\): Swapping of a temporary table with another non-temporary table is not allowed\.$",
    ):
        cur.execute("alter table a swap with b")


def test_alter_table_swap_temporary_with_permanent_leaves_tables_intact(
    cur: snowflake.connector.cursor.SnowflakeCursor,
) -> None:
    cur.execute("create table a (x int)")
    cur.execute("create table b (x int)")
    cur.execute("create temporary table a (x int)")
    cur.execute("insert into a values (1)")
    with pytest.raises(snowflake.connector.errors.ProgrammingError):
        cur.execute("alter table a swap with b")
    cur.execute("select x from a")
    assert cur.fetchall() == [(1,)]


def test_alter_table_swap_with_qualified_second_in_session_schema(
    cur: snowflake.connector.cursor.SnowflakeCursor,
) -> None:
    cur.execute("create table a (x int)")
    cur.execute("create table b (y int)")
    cur.execute("alter table a swap with schema1.b")
    cur.execute("select * from a")
    assert [d.name for d in cur.description] == ["Y"]
