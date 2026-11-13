# pyright: reportOptionalMemberAccess=false

import shutil
from collections.abc import Iterator
from pathlib import Path
from typing import TYPE_CHECKING

import pytest
import snowflake.connector
from snowflake.connector.cursor import SnowflakeCursor

import fakesnow

dbt_main = pytest.importorskip("dbt.cli.main")

if TYPE_CHECKING:
    from dbt.cli.main import dbtRunnerResult

PROJECT = Path(__file__).parent / "dbt_project"

# each class runs a dbt project once and its tests check the outcome, so they share the
# fakesnow patch or server, which is why the fixtures are class scoped


def copy_project(tmp_path: Path) -> Path:
    # dbt writes target/ and logs/ into the project dir, so run from a copy
    project = tmp_path / "project"
    shutil.copytree(PROJECT, project)
    return project


def write_seed_rows(project: Path, *rows: str) -> None:
    (project / "seeds" / "raw_orders.csv").write_text("\n".join(["id,customer,amount,updated_at", *rows, ""]))


def run_dbt(project_dir: Path, *args: str) -> "dbtRunnerResult":
    result = dbt_main.dbtRunner().invoke([*args, "--project-dir", str(project_dir), "--profiles-dir", str(project_dir)])
    assert result.success, f"dbt {' '.join(args)} failed: {result.exception!r}, see {project_dir}/logs/dbt.log"
    return result


class TestBuildTwicePatchMode:
    @pytest.fixture(scope="class")
    def cur(self, tmp_path_factory: pytest.TempPathFactory) -> Iterator[SnowflakeCursor]:
        project = copy_project(tmp_path_factory.mktemp("dbt"))
        with fakesnow.patch():
            # the first build connects from several threads at once to a database that doesn't exist yet
            for _ in range(2):
                run_dbt(project, "build")
            with snowflake.connector.connect(database="dbt_db", schema="analytics") as conn:
                yield conn.cursor()

    # the seed is unchanged, so the second run adds nothing: merge and append filter on rows newer
    # than the max, delete+insert replaces the newest row, and the snapshot has no changes to record
    @pytest.mark.parametrize("table", ["inc_merge", "inc_delete_insert", "inc_append", "snapshots.orders_snap"])
    def test_second_run_keeps_rows(self, cur: SnowflakeCursor, table: str) -> None:
        assert cur.execute(f"select count(*) from {table}").fetchall() == [(3,)]


class TestIncrementalPatchMode:
    @pytest.fixture(scope="class")
    def cur(self, tmp_path_factory: pytest.TempPathFactory) -> Iterator[SnowflakeCursor]:
        project = copy_project(tmp_path_factory.mktemp("dbt"))
        with fakesnow.patch():
            run_dbt(project, "seed")
            run_dbt(project, "run")
            run_dbt(project, "snapshot")
            # alice's order is updated and dave's is new, both newer than the rows already loaded
            write_seed_rows(
                project,
                "1,alice,99,2024-01-05 00:00:00",
                "2,bob,20.0,2024-01-02 00:00:00",
                "3,carol,7.25,2024-01-03 00:00:00",
                "4,dave,40,2024-01-04 00:00:00",
            )
            run_dbt(project, "seed")
            run_dbt(project, "run")
            run_dbt(project, "snapshot")
            with snowflake.connector.connect(database="dbt_db", schema="analytics") as conn:
                yield conn.cursor()

    def test_merge_updates_and_inserts(self, cur: SnowflakeCursor) -> None:
        assert cur.execute("select id, amount from inc_merge order by id").fetchall() == [
            (1, 99.0),
            (2, 20.0),
            (3, 7.25),
            (4, 40.0),
        ]

    def test_append_inserts_only_new_rows(self, cur: SnowflakeCursor) -> None:
        # alice's original order stays alongside her update
        assert cur.execute("select id, amount from inc_append order by id, amount").fetchall() == [
            (1, 10.5),
            (1, 99.0),
            (2, 20.0),
            (3, 7.25),
            (4, 40.0),
        ]

    def test_delete_insert_replaces_rows(self, cur: SnowflakeCursor) -> None:
        assert cur.execute("select id, amount from inc_delete_insert order by id").fetchall() == [
            (1, 99.0),
            (2, 20.0),
            (3, 7.25),
            (4, 40.0),
        ]

    def test_snapshot_records_new_version(self, cur: SnowflakeCursor) -> None:
        assert cur.execute(
            "select id, amount, dbt_valid_to is null from snapshots.orders_snap order by id, dbt_valid_from"
        ).fetchall() == [
            (1, 10.5, False),
            (1, 99.0, True),
            (2, 20.0, True),
            (3, 7.25, True),
            (4, 40.0, True),
        ]


# a regression in server mode surfaces as a 500 the connector retries indefinitely, rather than an error
@pytest.mark.timeout(180)
class TestBuildTwiceServerMode:
    @pytest.fixture(scope="class")
    def server(self, tmp_path_factory: pytest.TempPathFactory) -> Iterator[tuple[Path, dict]]:
        tmp_path = tmp_path_factory.mktemp("dbt")
        project = copy_project(tmp_path)
        db_path = tmp_path / "db"
        db_path.mkdir()
        with fakesnow.server(db_path=str(db_path)) as kwargs, pytest.MonkeyPatch.context() as monkeypatch:
            monkeypatch.setenv("FAKESNOW_HOST", kwargs["host"])
            monkeypatch.setenv("FAKESNOW_PORT", str(kwargs["port"]))
            monkeypatch.setenv("FAKESNOW_PROTOCOL", "http")
            for _ in range(2):
                run_dbt(project, "build")
            yield project, kwargs

    @pytest.mark.parametrize("table", ["inc_merge", "inc_delete_insert", "inc_append", "snapshots.orders_snap"])
    def test_second_build_keeps_rows(self, server: tuple[Path, dict], table: str) -> None:
        _, kwargs = server
        with snowflake.connector.connect(**kwargs, database="dbt_db", schema="analytics") as conn:
            assert conn.cursor().execute(f"select count(*) from {table}").fetchall() == [(3,)]

    def test_docs_generate(self, server: tuple[Path, dict]) -> None:
        project, _ = server
        run_dbt(project, "docs", "generate")
