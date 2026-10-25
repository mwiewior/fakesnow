"""DDL (Data Definition Language) transforms for fakesnow.

This module contains transformations for DDL statements like ALTER TABLE, CREATE TABLE, etc.

Future DDL transforms can be added here:
- CREATE TABLE enhancements
- DROP TABLE transformations
- ALTER TABLE modifications (beyond ADD COLUMN)
- INDEX operations
- VIEW operations
- etc.
"""

from __future__ import annotations

import secrets

import snowflake.connector.errors
import sqlglot
from duckdb import DuckDBPyConnection
from sqlglot import Expr, exp

from fakesnow.temporary import TemporaryObjects
from fakesnow.transforms.transforms import SUCCESS_NOP


def alter_table_add_multiple_columns(
    expression: Expr,
) -> list[Expr]:
    """Transform ALTER TABLE ADD COLUMN with multiple columns into separate statements.

    Snowflake supports: ALTER TABLE IF EXISTS tab1 ADD COLUMN IF NOT EXISTS col1 INT, col2 VARCHAR(50), col3 BOOLEAN;
    DuckDB requires separate statements for each column.

    Args:
        expression: The expression to potentially transform

    Returns:
        List of expressions - multiple ALTER statements if transformation applied,
        otherwise single original expression
    """
    if not isinstance(expression, exp.Alter):
        return [expression]

    actions = expression.args.get("actions")
    if not (
        actions
        and isinstance(actions, list)
        and all(isinstance(action, exp.ColumnDef) for action in actions)
        and len(actions) > 1
    ):
        return [expression]

    # Create separate ALTER statements for each column
    alter_statements = []

    for action in actions:
        new_alter = exp.Alter(
            this=expression.this,
            kind=expression.args.get("kind"),
            exists=expression.args.get("exists"),
            actions=[action],
        )
        alter_statements.append(new_alter)

    return alter_statements


_KIND_SQL = """
    SELECT kind FROM _fs_global._fs_information_schema._fs_show_objects
    WHERE database_name = ? AND schema_name = ? AND name = ?
"""


def swap_table(
    expression: Expr,
    current_database: str | None,
    current_schema: str | None,
    duck_conn: DuckDBPyConnection,
    temporaries: TemporaryObjects,
) -> list[Expr]:
    """Transform ALTER TABLE a SWAP WITH b, which duckdb lacks, into renames through a temporary name.

    a -> tmp, b -> a, tmp -> b. The b -> a rename carries swap_metadata so the cursor can swap permanent tables'
    recorded comments, text lengths and load history, which are keyed by name. Both operands are checked to be tables,
    and read so that a missing one fails, before anything is renamed.
    """
    if not (
        isinstance(expression, exp.Alter)
        and expression.args.get("kind") == "TABLE"
        and isinstance(expression.this, exp.Table)
        and len(actions := expression.args.get("actions") or []) == 1
        and isinstance(actions[0], exp.SwapTable)
    ):
        return [expression]

    first = expression.this
    second = actions[0].this

    # unquoted identifiers are case-insensitive, quoted ones are not
    def fold(part: exp.Expr) -> str:
        return part.name if part.args.get("quoted") else part.name.upper()

    def location(table: exp.Table) -> tuple[str | None, str | None]:
        catalog = table.args.get("catalog")
        schema = table.args.get("db")
        return (fold(catalog) if catalog else current_database, fold(schema) if schema else current_schema)

    if location(first) != location(second):
        raise NotImplementedError("ALTER TABLE ... SWAP WITH across databases or schemas")
    if first.name.upper() == second.name.upper():
        # swapping a table with itself changes nothing. duckdb names are case-insensitive, so this is checked first.
        return [SUCCESS_NOP.copy()]
    catalog, schema = location(first)
    temporary: list[bool] = []
    for operand in (first, second):
        name = fold(operand.this)
        # a temporary object shadows a permanent one of the same name
        kind = temporaries.kind(name, catalog, schema) if catalog and schema else None
        temporary.append(bool(kind))
        if not kind and (row := duck_conn.execute(_KIND_SQL, [catalog, schema, name]).fetchone()):
            kind = row[0]
        if kind and kind != "TABLE":
            # duckdb would rename a view as though it were a table
            raise snowflake.connector.errors.ProgrammingError(
                msg=f"SQL compilation error: Object found is of type '{kind}', not specified type 'TABLE'.",
                errno=2203,
                sqlstate="02000",
            )
    if temporary[0] != temporary[1]:
        # see https://docs.snowflake.com/en/sql-reference/sql/alter-table#parameters
        raise snowflake.connector.errors.ProgrammingError(
            msg="Swapping of a temporary table with another non-temporary table is not allowed.",
            errno=1486,
            sqlstate="42601",
        )

    # rename to takes an unqualified name, and the new name must be in the same schema as the table
    tmp = f"_fs_swap_{secrets.token_hex(8)}"
    qualifiers = {"catalog": first.args.get("catalog"), "db": first.args.get("db")}

    def table(name: str | exp.Identifier) -> exp.Table:
        return exp.Table(this=exp.to_identifier(name), **{k: v.copy() for k, v in qualifiers.items() if v})

    def rename(source: str | exp.Identifier, target: str | exp.Identifier) -> exp.Alter:
        return exp.Alter(
            this=table(source),
            kind="TABLE",
            actions=[exp.AlterRename(this=exp.Table(this=exp.to_identifier(target)))],
        )

    second_to_first = rename(second.this, first.this)
    # temporary tables have no recorded metadata, and their names would swap the permanent tables' metadata
    second_to_first.set("swap_metadata", not any(temporary))
    probes = [exp.select("1").from_(t.copy()).where(exp.false()) for t in (first, second)]
    return [*probes, rename(first.this, tmp), second_to_first, rename(tmp, second.this)]


def _is_alter_table_cluster_by(expression: Expr) -> bool:
    if not isinstance(expression, exp.Alter):
        return False
    actions = expression.actions
    if not actions or len(actions) != 1:
        return False
    return isinstance(actions[0], (exp.Cluster, exp.ClusterProperty))


def _is_alter_table_drop_clustering_key(expression: Expr) -> bool:
    if not isinstance(expression, exp.Alter):
        return False
    actions = expression.actions
    if not (actions and len(actions) == 1 and isinstance(actions[0], exp.Command)):
        return False
    action = actions[0]
    return (
        isinstance(action.this, str)
        and action.this.upper() == "DROP"
        and isinstance(action.expression, str)
        and "CLUSTERING" in action.expression.upper()
    )


def _is_alter_table_recluster(expression: Expr) -> bool:
    if not (isinstance(expression, exp.Command) and isinstance(expression.this, str)):
        return False
    if expression.this.upper() != "ALTER":
        return False
    text = expression.expression
    return isinstance(text, str) and ("RESUME RECLUSTER" in text.upper() or "SUSPEND RECLUSTER" in text.upper())


def alter_table_strip_cluster_by(expression: Expr) -> Expr:
    """Turn Snowflake clustering operations into a no-op.

    DuckDB doesn't support clustering, so the following statements are ignored:
    - ALTER TABLE ... CLUSTER BY (col1, col2)
    - ALTER TABLE ... DROP CLUSTERING KEY
    - ALTER TABLE ... RESUME RECLUSTER
    - ALTER TABLE ... SUSPEND RECLUSTER
    """
    if _is_alter_table_cluster_by(expression):
        return SUCCESS_NOP
    if _is_alter_table_drop_clustering_key(expression):
        return SUCCESS_NOP
    if _is_alter_table_recluster(expression):
        return SUCCESS_NOP
    return expression


def create_table_autoincrement(
    expression: Expr,
) -> list[Expr]:
    """Split CREATE TABLE with AUTOINCREMENT into CREATE SEQUENCE + CREATE TABLE with DEFAULT NEXTVAL.

    Example transform:
        CREATE TABLE test_table (id NUMERIC NOT NULL AUTOINCREMENT, name VARCHAR)
        ->
        CREATE SEQUENCE test_table_id_seq START 1;
        CREATE TABLE test_table (id NUMERIC NOT NULL DEFAULT NEXTVAL('test_table_id_seq'), name VARCHAR)
    """

    if not (
        isinstance(expression, exp.Create)
        and expression.kind == "TABLE"
        and (schema := expression.this)
        and (table := schema.this)
        and isinstance(schema, exp.Schema)
        and isinstance(table, exp.Table)
        # Find AUTOINCREMENT/IDENTITY columns
        and (
            auto_cols := [
                cd
                for cd in (schema.expressions or [])
                if isinstance(cd, exp.ColumnDef)
                and (cd.find(exp.AutoIncrementColumnConstraint) or cd.find(exp.GeneratedAsIdentityColumnConstraint))
            ]
        )
    ):
        return [expression]

    if len(auto_cols) > 1:
        raise NotImplementedError("Multiple AUTOINCREMENT columns")

    auto = auto_cols[0]
    col_name = auto.this.name
    table_name = table.name
    # When recreating the same table with a sequence, we need to give the sequence a unique name to avoid
    # Dependency Error: Cannot drop entry "_FS_SEQ_..." because there are entries that depend on it.
    random_suffix = secrets.token_hex(4)
    seq_name = f"_fs_seq_{table_name}_{col_name}_{random_suffix}"

    # Build CREATE SEQUENCE, using START/INCREMENT if provided
    start_val = "1"
    increment_val = "1"

    identity = auto.find(exp.GeneratedAsIdentityColumnConstraint)
    if identity:
        s = identity.args.get("start")
        i = identity.args.get("increment")
        if isinstance(s, exp.Literal):
            start_val = s.this
        if isinstance(i, exp.Literal):
            increment_val = i.this

    seq_stmt = sqlglot.parse_one(
        f"CREATE SEQUENCE {seq_name} START WITH {start_val} INCREMENT BY {increment_val}",
        read="duckdb",
    )

    # Build modified CREATE TABLE with DEFAULT NEXTVAL('<seq_name>') and without AUTOINCREMENT
    new_create: exp.Create = expression.copy()
    new_schema: exp.Schema = new_create.this

    # Find the corresponding column in the copied schema
    target_col = next(
        cd for cd in (new_schema.expressions or []) if isinstance(cd, exp.ColumnDef) and cd.this.name == col_name
    )

    existing_constraints: list[Expr] = target_col.args.get("constraints", []) or []

    # Replace the AUTOINCREMENT/IDENTITY constraint in-place with DEFAULT NEXTVAL('<seq_name>')
    for c in existing_constraints:
        if isinstance(c, exp.ColumnConstraint) and isinstance(
            c.args.get("kind"),
            (
                exp.AutoIncrementColumnConstraint,
                exp.GeneratedAsIdentityColumnConstraint,
            ),
        ):
            c.set(
                "kind",
                exp.DefaultColumnConstraint(
                    this=exp.Anonymous(
                        this="NEXTVAL",
                        expressions=[exp.Literal(this=seq_name, is_string=True)],
                    )
                ),
            )
            break

    target_col.set("constraints", existing_constraints)

    return [seq_stmt, new_create]
