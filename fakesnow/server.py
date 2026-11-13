from __future__ import annotations

import gzip
import json
import logging
import os
import secrets
import threading
from base64 import b64encode
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any
from urllib.parse import quote

import snowflake.connector.errors
from sqlglot import Expr, exp
from starlette.applications import Starlette
from starlette.concurrency import run_in_threadpool
from starlette.requests import Request
from starlette.responses import JSONResponse, Response
from starlette.routing import Route

from fakesnow import statement_type
from fakesnow.arrow import to_ipc, to_sf
from fakesnow.converter import from_binding
from fakesnow.cursor import FakeSnowflakeCursor
from fakesnow.dialect import parse_one
from fakesnow.expr import normalise_ident
from fakesnow.fakes import FakeSnowflakeConnection
from fakesnow.instance import FakeSnow
from fakesnow.rowtype import ColumnInfo, describe_as_rowtype
from fakesnow.statement_type import DML_TYPE_IDS, statement_type_id
from fakesnow.transforms import SERVER_VERSION, stage

logger = logging.getLogger("fakesnow.server")
# use same format as uvicorn
logger.handlers = logging.getLogger("uvicorn").handlers
logger.setLevel(logging.INFO)

# server-wide default for logins that don't send FAKESNOW_DB_PATH. None is a shared in-memory database.
default_db_path: str | None = None
# instances keyed by realpath of their db_path. each server has its own event loop thread, so a lock guards them.
_instances: dict[str, FakeSnow] = {}
_instances_lock = threading.Lock()
# number of running servers defaulting to each instance key, so the instance outlives all but the last
_server_counts: dict[str, int] = {}
# the db_path of each running server, oldest first. the newest is the default, so stopping servers in any order
# leaves the default of one still running
_running_defaults: list[tuple[object, str | None]] = []
sessions: dict[str, FakeSnowflakeConnection] = {}


def _instance_key(db_path: str | None) -> str:
    return os.path.realpath(db_path) if db_path else ""


def instance_for(db_path: str | None) -> FakeSnow:
    """Get the instance for a db_path, creating it on first use, so logins with the same path share it.

    None is the default shared in-memory instance. ":isolated:" is always a new in-memory instance.
    """
    if db_path == ":isolated:":
        return FakeSnow()
    key = _instance_key(db_path)
    with _instances_lock:
        if key not in _instances:
            _instances[key] = FakeSnow(db_path=key) if db_path else FakeSnow()
        return _instances[key]


def set_default_db_path(db_path: str | None) -> Callable[[], None]:
    """Set the default db_path for a starting server and return a function to call when it stops.

    Stopping restores the default of the newest server still running, or None, and closes the instance for db_path
    once no other server in the process defaults to it, so other servers keep their instances and sessions.
    """
    global default_db_path
    token = object()
    key = _instance_key(db_path)
    with _instances_lock:
        _running_defaults.append((token, db_path))
        default_db_path = db_path
        if db_path:
            _server_counts[key] = _server_counts.get(key, 0) + 1

    def restore() -> None:
        global default_db_path
        with _instances_lock:
            _running_defaults[:] = [(t, p) for t, p in _running_defaults if t is not token]
            default_db_path = _running_defaults[-1][1] if _running_defaults else None
        if not db_path:
            return
        with _instances_lock:
            _server_counts[key] -= 1
            if _server_counts[key]:
                return
            del _server_counts[key]
            fs = _instances.pop(key, None)
        if fs:
            # closes sessions still open on the instance, so their connections don't keep its database files attached
            fs.close()
            for session_token, conn in list(sessions.items()):
                if conn.is_closed():
                    sessions.pop(session_token, None)

    return restore


@dataclass
class ServerError(Exception):
    status_code: int
    code: str
    message: str


async def login_request(request: Request) -> JSONResponse:
    database = (d := request.query_params.get("databaseName")) and normalise_ident(d)
    schema = (s := request.query_params.get("schemaName")) and normalise_ident(s)
    role = request.query_params.get("roleName")
    warehouse = request.query_params.get("warehouse")
    body = await request.body()
    if request.headers.get("Content-Encoding") == "gzip":
        body = gzip.decompress(body)
    body_json = json.loads(body)
    session_params: dict[str, Any] = body_json["data"]["SESSION_PARAMETERS"]
    nop_regexes = session_params.get("nop_regexes")
    autocommit = session_params.get("AUTOCOMMIT", True)

    fs = instance_for(session_params.get("FAKESNOW_DB_PATH") or default_db_path)
    token = secrets.token_urlsafe(32)
    logger.info(f"Session login {database=} {schema=} {nop_regexes=}")
    try:
        sessions[token] = fs.connect(
            database,
            schema,
            nop_regexes=nop_regexes,
            autocommit=autocommit,
            session_parameters=session_params,
            role=role,
            warehouse=warehouse,
        )
    except NotADirectoryError as e:
        # fail the login, rather than return a server error the client retries until it times out
        return JSONResponse({"data": None, "message": str(e), "success": False})
    return JSONResponse(
        {
            "data": {
                "token": token,
                # clients serve this as the jdbc database product version, and some refuse to run
                # against a version they consider too old
                "serverVersion": SERVER_VERSION,
                "parameters": [
                    {"name": "AUTOCOMMIT", "value": autocommit},
                    {"name": "CLIENT_SESSION_KEEP_ALIVE_HEARTBEAT_FREQUENCY", "value": 3600},
                    # clients read this to decide whether to bind an array inline or stage it
                    # first. 0 keeps them inline, ie: no PUT, which we don't support well yet.
                    {"name": "CLIENT_STAGE_ARRAY_BINDING_THRESHOLD", "value": 0},
                ],
                "sessionInfo": {
                    "databaseName": database,
                    "schemaName": schema,
                },
            },
            "success": True,
        }
    )


async def query_request(request: Request) -> JSONResponse:
    try:
        conn = to_conn(to_token(request))

        body = await request.body()
        if request.headers.get("Content-Encoding") == "gzip":
            body = gzip.decompress(body)

        body_json = json.loads(body)

        sql_text = body_json["sqlText"]

        params: Any = None
        # rows of params, when the client sends an array binding
        batch: list[tuple[Any, ...]] | None = None

        if bindings := body_json.get("bindings"):
            if not all(k.isdigit() for k in bindings):
                # named bindings: {'myid': {'type': 'FIXED', 'value': '10'}, ...} -> dict {'myid': 10, ...}
                params = {name: from_binding(b) for name, b in bindings.items()}
            elif any(isinstance(b["value"], list) for b in bindings.values()):
                # array bindings hold a list of values per placeholder, one per row, and are sent
                # when executing a batch, eg: cursor.executemany. transpose them into rows, ie:
                # {'1': {'type': 'FIXED', 'value': ['1', '2']}, ...} -> [(1, ...), (2, ...)]
                columns = [
                    [from_binding({**b, "value": v}) for v in b["value"]]
                    for b in (bindings[str(pos)] for pos in range(1, len(bindings) + 1))
                ]
                batch = list(zip(*columns, strict=True))
            else:
                # positional bindings: {'1': {'type': 'FIXED', 'value': '10'}, ...} -> tuple (10, ...)
                params = tuple(from_binding(bindings[str(pos)]) for pos in range(1, len(bindings) + 1))
            logger.debug(f"Bindings: {batch if batch is not None else params}")

        batch_rowcount = 0

        try:
            expr = parse_one(sql_text)
            type_id = statement_type_id(expr)
            cur = conn.cursor()

            if (
                body_json.get("describeOnly")
                and (described := await run_in_threadpool(cur._describe_only, sql_text)) is not None  # noqa: SLF001
            ):
                return describe_only_response(conn, cur, describe_as_rowtype(described), expr, type_id)
            # Statements without a describe-only shortcut are described by executing them.

            # only a single sql statement is sent at a time by the python snowflake connector
            if batch is None:
                await run_in_threadpool(cur.execute, sql_text, binding_params=params, server=True)
            else:
                # snowflake runs an array binding as one statement and reports the rows affected
                # across the whole batch, so accumulate them as we execute row by row
                for row in batch:
                    await run_in_threadpool(cur.execute, sql_text, binding_params=row, server=True)
                    batch_rowcount += cur.rowcount or 0
            rowtype = describe_as_rowtype(cur._describe_last_sql())  # noqa: SLF001

            expr = cur._last_transformed  # noqa: SLF001
            assert expr
            if put_stage_data := expr.args.get("put_stage_data"):
                if not params:
                    # rewrite the stage info so the client uploads the file over http to this
                    # server, rather than writing to a local path, which fails when the client
                    # doesn't share the server's filesystem (eg: the server runs in a container).
                    # GCS is the only location type the connector uploads to via a plain http
                    # presigned url, so mimic it. a PUT with a ? placeholder target keeps
                    # LOCAL_FS because the connector re-requests the presigned url by executing
                    # the command without bindings (see SnowflakeGCSRestClient).
                    put_stage_data["stageInfo"] = gcs_stage_info(request, expr, put_stage_data)
                # this is a PUT command, so return the stage data
                return JSONResponse(
                    {
                        "data": put_stage_data,
                        "success": True,
                    }
                )

        except (snowflake.connector.errors.ProgrammingError, snowflake.connector.errors.OperationalError) as e:
            logger.info(f"{sql_text=} {type(e).__name__} {e}")
            code = f"{e.errno:06d}"
            return JSONResponse(
                {
                    "data": {
                        "errorCode": code,
                        "sqlState": e.sqlstate,
                    },
                    "code": code,
                    "message": e.msg,
                    "success": False,
                }
            )
        except Exception as e:
            # we have a bug or use of an unsupported feature
            msg = f"{sql_text=} {params=} Unhandled exception"
            logger.error(msg, exc_info=e)
            # my guess at mimicking a 500 error as per https://docs.snowflake.com/en/developer-guide/sql-api/reference
            # and https://github.com/snowflakedb/gosnowflake/blob/8ed4c75ffd707dd712ad843f40189843ace683c4/restful.go#L318
            raise ServerError(status_code=500, code="261000", message=msg) from None

        arrow_table = cur._arrow_table  # noqa: SLF001

        if batch is not None and arrow_table and type_id in DML_TYPE_IDS:
            # the batch ran as multiple statements, but the client sent one, so report the total
            result: dict[str, Any] = {"queryResultFormat": "json", "rowset": [[str(batch_rowcount)]]}
        elif arrow_table and type_id in DML_TYPE_IDS:
            # DML results are returned as json, because clients read the affected row counts
            # from rowset, eg: SnowflakeCursor._init_result_and_meta
            result = {
                "queryResultFormat": "json",
                "rowset": [
                    [None if v is None else str(v) for v in row]
                    for row in zip(*[c.to_pylist() for c in arrow_table.columns], strict=True)
                ],
            }
        elif arrow_table:
            batch_bytes = to_ipc(to_sf(arrow_table, rowtype))
            result = {"queryResultFormat": "arrow", "rowsetBase64": b64encode(batch_bytes).decode("utf-8")}
        else:
            result = {"queryResultFormat": "arrow", "rowsetBase64": ""}

        return JSONResponse(
            {
                "data": {
                    "parameters": [
                        {"name": "TIMEZONE", "value": "Etc/UTC"},
                    ],
                    "rowtype": rowtype,
                    "total": 1 if batch is not None else arrow_table.num_rows if arrow_table else 0,
                    "queryId": cur.sfqid,
                    "statementTypeId": type_id,
                    "finalDatabaseName": conn.database,
                    "finalSchemaName": conn.schema,
                    **result,
                },
                "success": True,
            }
        )

    except ServerError as e:
        return JSONResponse(
            {"data": None, "code": e.code, "message": e.message, "success": False, "headers": None},
            status_code=e.status_code,
        )


def gcs_stage_info(request: Request, expr: Expr, put_stage_data: stage.UploadCommandDict) -> dict[str, Any]:
    """Stage info that uploads to this server over http via a GCS-style presigned url.

    The connector re-requests the presigned url with the destination file name as the source,
    so the url targets the file as actually uploaded, even when auto compression renames it.
    """
    stage_dir = put_stage_data["stageInfo"]["location"]
    dst = os.path.basename(put_stage_data["src_locations"][0])
    path = quote(os.path.relpath(os.path.join(stage_dir, dst), stage.LOCAL_BUCKET_PATH))
    return {
        "locationType": "GCS",
        "location": f"{expr.args['put_stage_name']}/",
        "creds": {},
        "presignedUrl": f"{request.url.scheme}://{request.url.netloc}/fs_bucket/{path}",
    }


def _write_bucket_file(path: str, body: bytes) -> str | None:
    """Write body to path within the local bucket, returning the target path, or None if invalid."""
    target = os.path.normpath(os.path.join(stage.LOCAL_BUCKET_PATH, path))
    if not target.startswith(stage.LOCAL_BUCKET_PATH + os.sep):
        return None

    os.makedirs(os.path.dirname(target), exist_ok=True)
    with open(target, "wb") as f:
        f.write(body)
    return target


async def bucket_upload(request: Request) -> Response:
    """Store an uploaded file in the local bucket, ie: the backing storage for internal stages."""
    body = await request.body()
    target = await run_in_threadpool(_write_bucket_file, request.path_params["path"], body)
    if not target:
        return Response(status_code=400)
    logger.info(f"Uploaded {len(body)} bytes to {target}")
    return Response()


def describe_only_response(
    conn: FakeSnowflakeConnection,
    cur: FakeSnowflakeCursor,
    rowtype: list[ColumnInfo],
    expr: Expr,
    type_id: int,
) -> JSONResponse:
    """Describe the statement's result without any rows, as snowflake does for describeOnly."""

    # snowflake returns json for everything but a query
    result = (
        {"queryResultFormat": "arrow", "rowsetBase64": ""}
        if type_id == statement_type.SELECT
        else {"queryResultFormat": "json", "rowset": []}
    )
    return JSONResponse(
        {
            "data": {
                "rowtype": rowtype,
                "total": 0,
                "returned": 0,
                "queryId": cur.sfqid,
                "statementTypeId": type_id,
                "numberOfBinds": len(list(expr.find_all(exp.Placeholder))),
                # clients read this to decide whether they can send the batch as an array binding.
                # snowflake only supports it for inserts.
                "arrayBindSupported": type_id == statement_type.INSERT,
                "finalDatabaseName": conn.database,
                "finalSchemaName": conn.schema,
                **result,
            },
            "success": True,
        }
    )


def to_token(request: Request) -> str:
    if not (auth := request.headers.get("Authorization")):
        raise ServerError(status_code=401, code="390103", message="Session token not found in the request data.")

    return auth[17:-1]


def to_conn(token: str) -> FakeSnowflakeConnection:
    if not (conn := sessions.get(token)):
        raise ServerError(status_code=401, code="390104", message="User must login again to access the service.")

    return conn


async def session(request: Request) -> JSONResponse:
    try:
        token = to_token(request)
        _ = to_conn(token)

        if bool(request.query_params.get("delete")):
            del sessions[token]

        return JSONResponse(
            {"data": None, "code": None, "message": None, "success": True},
        )

    except ServerError as e:
        return JSONResponse(
            {"data": None, "code": e.code, "message": e.message, "success": False, "headers": None},
            status_code=e.status_code,
        )


def monitoring_query(request: Request) -> JSONResponse:
    token = to_token(request)
    conn = to_conn(token)

    sfqid = request.path_params["sfqid"]
    if not conn.results_cache.get(sfqid):
        return JSONResponse({"data": {"queries": []}, "success": True})

    return JSONResponse({"data": {"queries": [{"status": "SUCCESS"}]}, "success": True})


routes = [
    Route(
        "/session/v1/login-request",
        login_request,
        methods=["POST"],
    ),
    Route("/session", session, methods=["POST"]),
    Route("/session/heartbeat", session, methods=["POST"]),
    Route(
        "/queries/v1/query-request",
        query_request,
        methods=["POST"],
    ),
    Route("/queries/v1/abort-request", lambda _: JSONResponse({"success": True}), methods=["POST"]),
    Route("/fs_bucket/{path:path}", bucket_upload, methods=["PUT"]),
    Route("/monitoring/queries/{sfqid}", monitoring_query, methods=["GET"]),
]

app = Starlette(debug=True, routes=routes)
