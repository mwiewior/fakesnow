import pytest
import snowflake.connector.cursor


def test_show_users_base_case(cur: snowflake.connector.cursor.SnowflakeCursor):
    result = cur.execute("SHOW USERS")
    assert result
    assert result.fetchall() == []


def test_show_users_with_users(cur: snowflake.connector.cursor.SnowflakeCursor):
    result = cur.execute("CREATE USER foo")
    result = cur.execute("CREATE USER bar")

    result = cur.execute("SHOW USERS")
    assert result

    rows = result.fetchall()
    names = [row[0] for row in rows]
    assert names == ["foo", "bar"]


def test_grant_is_accepted(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create table t1 (id int)")
    cur.execute("grant select on table db1.schema1.t1 to role public")
    assert cur.fetchall() == [("Statement executed successfully.",)]


def test_revoke_is_accepted(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create table t1 (id int)")
    cur.execute("revoke select on table db1.schema1.t1 from role public")
    # grants aren't recorded, so there's none to revoke
    assert cur.fetchall() == [("Statement executed successfully. 0 objects affected.",)]


def test_grant_on_all_tables_is_accepted(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("grant select, insert on all tables in schema schema1 to role public")
    assert cur.fetchall() == [("Statement executed successfully. 0 objects affected.",)]


def test_grant_on_all_tables_reports_tables_affected(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create table t1 (id int)")
    cur.execute("create table t2 (id int)")
    cur.execute("create view v1 as select 1 as id")
    cur.execute("grant select on all tables in schema schema1 to role public")
    assert cur.fetchall() == [("Statement executed successfully. 2 objects affected.",)]


def test_grant_on_future_tables_is_accepted(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("grant select on future tables in schema schema1 to role public")
    assert cur.fetchall() == [("Statement executed successfully.",)]


@pytest.mark.xfail(reason="CREATE ROLE is not implemented", strict=True)
def test_grant_role_to_role_is_accepted(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create role analyst")
    cur.execute("create role reporter")
    cur.execute("grant role analyst to role reporter")
    assert cur.fetchall() == [("Statement executed successfully.",)]


def test_revoke_on_all_tables_is_accepted(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("revoke select on all tables in schema schema1 from role public")
    assert cur.fetchall() == [("Statement executed successfully. 0 objects affected.",)]
