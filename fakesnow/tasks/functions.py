"""What a task body can know about the graph run it is part of."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from datetime import UTC, datetime

import snowflake.connector.errors
from sqlglot import Expr, exp

from fakesnow.tasks.catalog import Task


@dataclass
class TaskRunContext:
    """The run of one task in a graph run, set on the task's session while its body runs."""

    task: Task
    root: Task
    graph_run_group_id: str
    run_id: int
    config: str | None
    predecessor_return_values: dict[str, str | None]
    # every ancestor's, as a named lookup falls back to a matching ancestor
    ancestor_return_values: dict[str, str | None] = field(default_factory=dict)
    return_value: str | None = None


_PREFIX = "SYSTEM$"
SET_RETURN_VALUE = "SYSTEM$SET_RETURN_VALUE"
_FUNCTIONS = {
    "CURRENT_USER_TASK_NAME",
    "TASK_RUNTIME_INFO",
    "GET_TASK_GRAPH_CONFIG",
    "GET_PREDECESSOR_RETURN_VALUE",
    "SET_RETURN_VALUE",
}
_NULL = exp.Null()
_PATH_STEP = re.compile(r"""\[(\d+)\]|\.?"((?:[^"]|"")*)"|\.?([^.\["]+)|\['((?:[^']|'')*)'\]""")
# what SET_RETURN_VALUE returns, its argument becoming the task's return value
_SET = "SUCCESS"

OUTSIDE_TASK = "Function {name} must be called from within a task."
NOT_CONSTANT = (
    "SQL compilation error:\nargument 1 to function SYSTEM$SET_RETURN_VALUE needs to be constant, found '{found}'"
)
NO_PREDECESSOR_NAMED = "No predecessor task specified in call to system$get_predecessor_return_value()."
NOT_A_PREDECESSOR = (
    "The predecessor {name} specified in call to system$get_predecessor_return_value() is not a valid or active "
    "predecessor for this task. Note that system$get_predecessor_return_value() does not support fully qualified "
    "names including database or schema."
)


def task_functions(expression: Expr, context: TaskRunContext | None) -> Expr:
    """Replace the SYSTEM$ functions that read, or set, the state of the task run the statement is part of. Outside
    a task run CURRENT_USER_TASK_NAME returns an empty string, and the others fail."""
    if not isinstance(expression, exp.Anonymous):
        return expression
    name = _name(expression)
    if not name.startswith(_PREFIX) or name[len(_PREFIX) :] not in _FUNCTIONS:
        return expression
    if context is None and name != f"{_PREFIX}CURRENT_USER_TASK_NAME":
        raise snowflake.connector.errors.ProgrammingError(
            msg=OUTSIDE_TASK.format(name=name), errno=91430, sqlstate="22000"
        )
    args = expression.expressions
    if name == SET_RETURN_VALUE and context:
        # a replacement isn't descended into by the transform, so replace the task functions nested in the value
        value = args[0].copy().transform(lambda e: task_functions(e, context)) if args else _NULL.copy()
        # the value is set when the statement is compiled, so whether or not the call is evaluated, the last call's
        context.return_value = _constant(value)
        replacement: Expr = exp.Literal.string(_SET)
    else:
        value = _read(name[len(_PREFIX) :], args, context) if context else ""
        replacement = _NULL.copy() if value is None else exp.Literal.string(value)
    if isinstance(expression.parent, exp.Select):
        # named as Snowflake names the column of the call
        return exp.alias_(replacement, expression.sql("snowflake").upper(), quoted=True)
    return replacement


def _constant(value: Expr) -> str:
    """The string a SET_RETURN_VALUE argument evaluates to, which has to be made of string constants."""
    if isinstance(value, exp.Literal) and value.is_string:
        return value.name
    if isinstance(value, exp.Paren):
        return _constant(value.this)
    if isinstance(value, exp.DPipe):
        return _constant(value.this) + _constant(value.expression)
    if isinstance(value, exp.Null):
        found = "null"
    elif isinstance(value, exp.Column):
        found = value.sql("snowflake").upper()
    else:
        found = f"CAST({value.sql('snowflake').upper()} AS VARCHAR(134217728))"
    raise snowflake.connector.errors.ProgrammingError(
        msg=NOT_CONSTANT.format(found=found), errno=1015, sqlstate="22023"
    )


def _name(call: exp.Anonymous) -> str:
    return call.this.upper() if isinstance(call.this, str) else ""


def _read(function: str, args: list[Expr], context: TaskRunContext) -> str | None:
    arg = args[0].name if args else None
    if function == "CURRENT_USER_TASK_NAME":
        return context.task.fqname
    if function == "TASK_RUNTIME_INFO":
        return _runtime_info(arg, context)
    if function == "GET_TASK_GRAPH_CONFIG":
        return _config(arg, context.config)
    # GET_PREDECESSOR_RETURN_VALUE, with no argument the only predecessor's
    values = context.predecessor_return_values
    if arg is None and len(values) > 1:
        # the name is required with several, so the call fails
        raise _predecessor_error(NO_PREDECESSOR_NAMED, 91450)
    if arg is None:
        return next(iter(values.values()), None)
    # matched as given, as task names are case-sensitive here, falling back to a matching ancestor
    if arg in values:
        return values[arg]
    if arg in context.ancestor_return_values:
        return context.ancestor_return_values[arg]
    raise _predecessor_error(NOT_A_PREDECESSOR.format(name=arg), 91451)


def _predecessor_error(message: str, errno: int) -> snowflake.connector.errors.ProgrammingError:
    # the sqlstate isn't known, these errors are only seen in the task history
    return snowflake.connector.errors.ProgrammingError(msg=message, errno=errno, sqlstate="22000")


def _runtime_info(field: str | None, context: TaskRunContext) -> str | None:
    match (field or "").upper():
        case "CURRENT_TASK_NAME":
            return context.task.fqname
        case "CURRENT_ROOT_TASK_NAME":
            return context.root.fqname
        case "CURRENT_ROOT_TASK_UUID":
            return context.root.id
        case "CURRENT_TASK_GRAPH_RUN_GROUP_ID":
            return context.graph_run_group_id
        case "CURRENT_TASK_GRAPH_ORIGINAL_SCHEDULED_TIMESTAMP":
            # the run id is the root's scheduled time in epoch milliseconds
            # in UTC, to the millisecond, eg: 2026-10-25 05:25:33.861000000Z
            scheduled = datetime.fromtimestamp(context.run_id / 1000, UTC)
            return f"{scheduled:%Y-%m-%d %H:%M:%S}.{scheduled.microsecond // 1000:03d}000000Z"
        case _:
            return None


def _config(path: str | None, config: str | None) -> str | None:
    if path is None or config is None:
        return config
    value = json.loads(config)
    pos = 0
    # a GET_PATH path: keys, which can be quoted, separated by periods, and [index] or ['key'] subscripts
    while pos < len(path):
        step = _PATH_STEP.match(path, pos)
        if step is None:
            return None
        pos = step.end()
        index, quoted, key, subscript = step.groups()
        if index is not None:
            value = value[int(index)] if isinstance(value, list) and int(index) < len(value) else None
        else:
            if key is not None:
                name = key
            elif quoted is not None:
                name = quoted.replace('""', '"')
            else:
                name = subscript.replace("''", "'")
            value = value.get(name) if isinstance(value, dict) else None
        if value is None:
            return None
    return value if isinstance(value, str) else json.dumps(value, separators=(",", ":"))
