"""Parse Snowflake task statements (CREATE/ALTER/DROP/EXECUTE/SHOW/DESCRIBE TASK).

sqlglot parses most task statements as an opaque exp.Command (and DESCRIBE TASK not at all), so the statement
text is tokenized and walked here instead.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal, NoReturn

import snowflake.connector.errors
import sqlglot
from sqlglot.errors import TokenError
from sqlglot.tokenizer_core import Token, TokenType

from fakesnow.expr import normalise_ident


class TaskSyntaxError(snowflake.connector.errors.ProgrammingError):
    def __init__(self, msg: str) -> None:
        super().__init__(msg=msg, errno=1003, sqlstate="42000")


@dataclass(frozen=True)
class TaskName:
    database: str | None
    schema: str | None
    name: str


@dataclass
class CreateTask:
    name: TaskName
    replace: bool
    if_not_exists: bool
    or_alter: bool
    properties: dict[str, str] = field(default_factory=dict)
    after: list[TaskName] = field(default_factory=list)
    condition: str | None = None
    definition: str | None = None
    clone_of: TaskName | None = None


@dataclass
class AlterTask:
    name: TaskName
    if_exists: bool
    action: Literal[
        "RESUME", "SUSPEND", "ADD_AFTER", "REMOVE_AFTER", "SET", "UNSET", "MODIFY_AS", "MODIFY_WHEN", "REMOVE_WHEN"
    ]
    tasks: list[TaskName] = field(default_factory=list)
    properties: dict[str, str] = field(default_factory=dict)
    unset: list[str] = field(default_factory=list)
    text: str | None = None


@dataclass
class DropTask:
    name: TaskName
    if_exists: bool


@dataclass
class ExecuteTask:
    name: TaskName
    config: str | None


@dataclass
class EnableDependents:
    """SELECT SYSTEM$TASK_DEPENDENTS_ENABLE('<name>'), with the argument as written for the result column name."""

    name: TaskName
    argument: str


@dataclass
class DescribeTask:
    name: TaskName


@dataclass
class ShowTasks:
    terse: bool
    like: str | None
    scope: Literal["ACCOUNT", "DATABASE", "SCHEMA"] | None
    scope_name: list[str] | None
    starts_with: str | None
    root_only: bool
    limit: int | None
    limit_from: str | None


TaskStatement = CreateTask | AlterTask | DropTask | ExecuteTask | ShowTasks | DescribeTask | EnableDependents


# a quoted string, or a dollar-quoted one, which the tokenizer gives without its $$
_STRINGS = (TokenType.STRING, TokenType.RAW_STRING)


class _Tokens:
    def __init__(self, sql: str, tokens: list[Token]) -> None:
        self.sql = sql
        self.tokens = tokens
        self.pos = 0

    def at_end(self) -> bool:
        return self.pos >= len(self.tokens)

    def peek(self, offset: int = 0) -> str | None:
        """Upper-cased text of an upcoming token, or None past the end. Quoted identifiers never match keywords."""
        i = self.pos + offset
        if i >= len(self.tokens):
            return None
        t = self.tokens[i]
        return None if t.token_type in (TokenType.IDENTIFIER, *_STRINGS) else t.text.upper()

    def accept(self, *words: str) -> bool:
        """Consume the keyword sequence if it is next."""
        if all(self.peek(i) == w for i, w in enumerate(words)):
            self.pos += len(words)
            return True
        return False

    def expect(self, *words: str) -> None:
        if not self.accept(*words):
            self.fail(" ".join(words))

    def fail(self, expected: str) -> NoReturn:
        got = self.tokens[self.pos].text if not self.at_end() else "<end of statement>"
        raise TaskSyntaxError(f"SQL compilation error: syntax error, unexpected '{got}', expected {expected}.")

    def next(self) -> Token:
        if self.at_end():
            self.fail("more input")
        t = self.tokens[self.pos]
        self.pos += 1
        return t

    def string(self) -> str:
        t = self.next()
        if t.token_type not in _STRINGS:
            self.pos -= 1
            self.fail("a string literal")
        return t.text

    def ident(self) -> str:
        t = self.next()
        if t.token_type == TokenType.IDENTIFIER:
            return t.text
        if t.token_type in (*_STRINGS, TokenType.NUMBER, TokenType.COMMA, TokenType.DOT, TokenType.EQ):
            self.pos -= 1
            self.fail("an identifier")
        return normalise_ident(t.text)

    def parts(self) -> list[str]:
        parts = [self.ident()]
        while self.peek() == "." and self.tokens[self.pos].token_type == TokenType.DOT:
            self.pos += 1
            parts.append(self.ident())
        return parts

    def name(self) -> TaskName:
        parts = self.parts()
        if len(parts) > 3:
            self.fail("a task name")
        return TaskName(*([None] * (3 - len(parts))), *parts)  # type: ignore[arg-type]

    def names(self) -> list[TaskName]:
        names = [self.name()]
        while self.accept(","):
            names.append(self.name())
        return names

    def value(self) -> str:
        t = self.tokens[self.pos] if not self.at_end() else None
        if t and t.token_type in (*_STRINGS, TokenType.NUMBER):
            self.pos += 1
            return t.text
        if t and t.token_type in (TokenType.TRUE, TokenType.FALSE):
            self.pos += 1
            return t.text.upper()
        return ".".join(self.parts())

    def properties(self, stop: set[str]) -> dict[str, str]:
        """Parse `key = value` pairs separated by commas or whitespace until a stop keyword or the end."""
        props: dict[str, str] = {}
        while not self.at_end() and self.peek() not in stop:
            key = self.ident().upper()
            self.expect("=")
            if key == "FINALIZE":
                # the task name as written, for the catalog to resolve
                start = self.pos
                self.name()
                props[key] = self.sql[self.tokens[start].start : self.tokens[self.pos - 1].end + 1]
            else:
                props[key] = self.value()
            self.accept(",")
        return props

    def text_until(self, stop: str | None) -> str:
        """Original SQL from the next token up to a top-level stop keyword (or the end)."""
        start = self.pos
        depth = 0
        while not self.at_end():
            t = self.tokens[self.pos]
            if t.token_type == TokenType.L_PAREN:
                depth += 1
            elif t.token_type == TokenType.R_PAREN:
                depth -= 1
            elif depth == 0 and stop and self.peek() == stop:
                break
            self.pos += 1
        if self.pos == start:
            self.fail("an expression")
        return self.sql[self.tokens[start].start : self.tokens[self.pos - 1].end + 1]

    def expect_end(self) -> None:
        if not self.at_end():
            self.fail("end of statement")


def parse_task_name(text: str) -> TaskName:
    """Parse a possibly qualified task name, eg: the argument of SYSTEM$TASK_DEPENDENTS_ENABLE."""
    try:
        t = _Tokens(text, sqlglot.tokenize(text, read="snowflake"))
    except TokenError as e:
        raise TaskSyntaxError(f"SQL compilation error: invalid task name '{text}'.") from e
    name = t.name()
    t.expect_end()
    return name


def parse_task_statement(sql: str) -> TaskStatement | None:
    """Parse a task statement, or return None if sql isn't one. Raises TaskSyntaxError if it is malformed."""
    try:
        tokens = sqlglot.tokenize(sql, read="snowflake")
        while tokens and tokens[-1].token_type == TokenType.SEMICOLON:
            tokens.pop()
        if tokens and tokens[0].token_type == TokenType.EXECUTE and len(tokens) == 2:
            # the tokenizer treats everything after EXECUTE as one opaque string, so tokenize that remainder
            sql = tokens[1].text
            tokens = [tokens[0], *sqlglot.tokenize(sql, read="snowflake")]
            while tokens and tokens[-1].token_type == TokenType.SEMICOLON:
                tokens.pop()
    except TokenError:
        return None

    t = _Tokens(sql, tokens)
    if t.accept("CREATE"):
        if t.peek() == "TASK" or (t.peek() == "OR" and t.peek(2) == "TASK"):
            return _create(t)
    elif t.accept("ALTER", "TASK"):
        return _alter(t)
    elif t.accept("DROP", "TASK"):
        if_exists = t.accept("IF", "EXISTS")
        drop = DropTask(t.name(), if_exists)
        t.expect_end()
        return drop
    elif t.accept("EXECUTE", "TASK"):
        return _execute(t)
    elif t.accept("DESC", "TASK") or t.accept("DESCRIBE", "TASK"):
        desc = DescribeTask(t.name())
        t.expect_end()
        return desc
    elif t.peek() == "SHOW" and (t.peek(1) == "TASKS" or (t.peek(1) == "TERSE" and t.peek(2) == "TASKS")):
        t.pos += 1
        return _show(t)
    return None


def _create(t: _Tokens) -> CreateTask:
    replace = or_alter = False
    if t.accept("OR", "REPLACE"):
        replace = True
    elif t.accept("OR", "ALTER"):
        or_alter = True
    t.expect("TASK")
    if_not_exists = t.accept("IF", "NOT", "EXISTS")
    task = CreateTask(t.name(), replace, if_not_exists, or_alter)
    if t.accept("CLONE"):
        task.clone_of = t.name()
        t.expect_end()
        return task
    task.properties = t.properties({"AFTER", "WHEN", "AS"})
    if t.accept("AFTER"):
        task.after = t.names()
    if t.accept("WHEN"):
        task.condition = t.text_until("AS")
    t.expect("AS")
    task.definition = t.text_until(None)
    return task


def _alter(t: _Tokens) -> AlterTask:
    if_exists = t.accept("IF", "EXISTS")
    name = t.name()
    if t.accept("RESUME"):
        alter = AlterTask(name, if_exists, "RESUME")
    elif t.accept("SUSPEND"):
        alter = AlterTask(name, if_exists, "SUSPEND")
    elif t.accept("ADD", "AFTER"):
        alter = AlterTask(name, if_exists, "ADD_AFTER", tasks=t.names())
    elif t.accept("REMOVE", "AFTER"):
        alter = AlterTask(name, if_exists, "REMOVE_AFTER", tasks=t.names())
    elif t.accept("REMOVE", "WHEN"):
        alter = AlterTask(name, if_exists, "REMOVE_WHEN")
    elif t.accept("SET"):
        props = t.properties(set())
        if not props:
            t.fail("a property")
        alter = AlterTask(name, if_exists, "SET", properties=props)
    elif t.accept("UNSET"):
        unset = [t.ident().upper()]
        while t.accept(","):
            unset.append(t.ident().upper())
        alter = AlterTask(name, if_exists, "UNSET", unset=unset)
    elif t.accept("MODIFY", "AS"):
        alter = AlterTask(name, if_exists, "MODIFY_AS", text=t.text_until(None))
    elif t.accept("MODIFY", "WHEN"):
        alter = AlterTask(name, if_exists, "MODIFY_WHEN", text=t.text_until(None))
    else:
        t.fail("RESUME, SUSPEND, ADD, REMOVE, SET, UNSET or MODIFY")
    t.expect_end()
    return alter


def _execute(t: _Tokens) -> ExecuteTask:
    name = t.name()
    config = None
    if t.accept("USING", "CONFIG"):
        t.expect("=")
        config = t.string()
    t.expect_end()
    return ExecuteTask(name, config)


def _show(t: _Tokens) -> ShowTasks:
    terse = t.accept("TERSE")
    t.expect("TASKS")
    show = ShowTasks(terse, None, None, None, None, False, None, None)
    while not t.at_end():
        if t.accept("LIKE"):
            show.like = t.string()
        elif t.accept("IN"):
            if t.accept("ACCOUNT"):
                show.scope = "ACCOUNT"
            elif t.accept("DATABASE"):
                show.scope = "DATABASE"
            elif t.accept("SCHEMA"):
                show.scope = "SCHEMA"
            elif t.at_end() or t.peek() in ("STARTS", "ROOT", "LIMIT", "LIKE"):
                t.fail("ACCOUNT, DATABASE or SCHEMA")
            else:
                # a bare, possibly qualified, schema name
                show.scope = "SCHEMA"
                show.scope_name = t.parts()
                continue
            if show.scope != "ACCOUNT" and not t.at_end() and t.peek() not in ("STARTS", "ROOT", "LIMIT", "LIKE"):
                show.scope_name = t.parts()
        elif t.accept("STARTS", "WITH"):
            show.starts_with = t.string()
        elif t.accept("ROOT", "ONLY"):
            show.root_only = True
        elif t.accept("LIMIT"):
            n = t.next()
            if n.token_type != TokenType.NUMBER or not n.text.isdigit():
                t.pos -= 1
                t.fail("an integer")
            show.limit = int(n.text)
            if t.accept("FROM"):
                show.limit_from = t.string()
        else:
            t.fail("LIKE, IN, STARTS WITH, ROOT ONLY or LIMIT")
    return show
