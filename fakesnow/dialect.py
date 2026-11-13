"""Parse Snowflake SQL with Snowflake-compatible syntax errors."""

import snowflake.connector.errors
import sqlglot
from sqlglot import Expr, exp
from sqlglot.errors import ParseError
from sqlglot.tokenizer_core import Token, TokenType

_DESCRIBE = {"DESC", "DESCRIBE"}


def parse(command: str) -> list[Expr | None]:
    try:
        return sqlglot.parse(command, read="snowflake")
    except ParseError as error:
        statements = _statements(command)
        if any(_describe_task(s) for s in statements):
            # each statement on its own, so a DESCRIBE TASK can be anywhere in the command
            return [_describe_task(s) or parse_one(s) for s in statements]
        raise syntax_error(error) from None


def parse_one(command: str) -> Expr:
    try:
        return sqlglot.parse_one(command, read="snowflake")
    except ParseError as error:
        # the statement's own text, without any comments before it
        statements = _statements(command)
        if len(statements) == 1 and (describe := _describe_task(statements[0])):
            return describe
        raise syntax_error(error) from None


def _statements(command: str) -> list[str]:
    """The text of each statement in the command, split at the semicolons between them, from its first token to its
    last, so without the comments around it."""
    statements: list[list[Token]] = [[]]
    for token in sqlglot.tokenize(command, read="snowflake"):
        if token.token_type == TokenType.SEMICOLON:
            statements.append([])
        else:
            statements[-1].append(token)
    return [command[tokens[0].start : tokens[-1].end + 1] for tokens in statements if tokens]


def _describe_task(command: str) -> exp.Command | None:
    """sqlglot can't parse DESCRIBE TASK, so hand it on as a command like the other task statements."""
    # by token, so comments can be anywhere in it
    tokens = sqlglot.tokenize(command, read="snowflake")
    if len(tokens) > 2 and tokens[0].text.upper() in _DESCRIBE and tokens[1].text.upper() == "TASK":
        return exp.Command(this="DESCRIBE", expression=exp.Literal.string(command[tokens[1].start :].strip()))
    return None


def syntax_error(error: ParseError) -> snowflake.connector.errors.ProgrammingError:
    # Strip terminal highlighting from SQLGlot's error message.
    message = str(error).replace("\x1b[4m", "").replace("\x1b[0m", "")
    return snowflake.connector.errors.ProgrammingError(msg=message, errno=1003, sqlstate="42000")
