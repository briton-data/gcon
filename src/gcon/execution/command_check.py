"""
Checks on a job's command, so nonsense is turned away before it uses compute.

Three independent checks, from cheapest to most specific:

1. `check_command_shape` -- every submission, every caller. Blank, oversized or
   binary-junk commands are refused at the door instead of being queued.

2. `check_program_allowed` -- organizations only, using their plan's
   `allowed_programs`. The command must be ONE simple command whose first word
   is a listed program. A command that can run more than one thing (`;`, `&&`,
   `||`, `|`, `&`, redirections, command substitution, several lines) is refused
   on a restricted plan, because otherwise `python x.py; anything` would walk
   straight past the list.

   This is a coarse gate, not a security boundary. An allowed interpreter such
   as `python` can still run any code, so the real containment is the sandbox
   every customer job is forced into. What this check does is keep out typos,
   nonsense and programs a plan was never meant to run, before they cost a
   queue slot and a sandbox start.

3. `missing_program` -- on the worker, just before it starts a host process. If
   the command's program is not on the worker's PATH the job fails at once with
   a clear message instead of starting a shell to say the same thing. It only
   applies where it can be right: Linux/macOS hosts and simple commands. It is
   skipped on Windows (cmd.exe quoting and builtins are different, and a wrong
   "not found" would fail a good job) and for jobs run in Docker (the program
   lives in the image, which the host cannot see).
"""

from __future__ import annotations

import os
import re
import shlex
import shutil
from typing import Iterable, Optional

MAX_COMMAND_LENGTH = 10_000

# Everything below 0x20 except tab, newline and carriage return, plus DEL.
_CONTROL_CHARS = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]")
_ENV_ASSIGNMENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*=")
_OPERATOR_CHARS = set("();<>|&")

# Words the shell itself understands. They are not files on PATH, so the
# worker must not call them "missing".
_SHELL_BUILTINS = frozenset("""
    . : [ [[ alias bg bind break builtin caller case cd command compgen complete
    continue declare dirs disown do done elif else enable esac eval exec exit
    export false fc fg fi for function getopts hash help history if in jobs
    kill let local logout mapfile popd printf pushd pwd read readarray readonly
    return select set shift shopt source suspend test then time times trap true
    type typeset ulimit umask unalias unset until wait while
""".split())


def check_command_shape(command) -> None:
    """Refuse an obviously unusable command. Raises ValueError with a reason
    the submitter can act on."""
    if not isinstance(command, str):
        raise ValueError("command must be text.")
    if not command.strip():
        raise ValueError("command is empty. Give the program to run, for example: python train.py")
    if len(command) > MAX_COMMAND_LENGTH:
        raise ValueError(
            f"command is too long ({len(command):,} characters; the limit is "
            f"{MAX_COMMAND_LENGTH:,}). Keep the command short and put the work in a script."
        )
    if _CONTROL_CHARS.search(command):
        raise ValueError("command contains control or binary characters.")


def program_of(command: str) -> str:
    """The program a command starts with -- but only for a command that is a
    single simple command. Raises ValueError saying why when it is not."""
    if "\n" in command or "\r" in command:
        raise ValueError("only a single-line command is allowed")
    if "`" in command or "$(" in command:
        raise ValueError("command substitution (` or $(...)) is not allowed")
    try:
        lexer = shlex.shlex(command, posix=True, punctuation_chars=True)
        lexer.whitespace_split = True
        tokens = list(lexer)
    except ValueError as exc:
        raise ValueError(f"the command is malformed ({exc})") from None
    if not tokens:
        raise ValueError("command is empty")
    for token in tokens:
        if token and all(ch in _OPERATOR_CHARS for ch in token):
            raise ValueError(
                f"shell operators such as '{token}' are not allowed "
                "(one program per job)"
            )
    return tokens[0]


def check_program_allowed(command: str, allowed_programs: Optional[Iterable[str]]) -> None:
    """Raise ValueError unless `command` is one simple command starting with a
    listed program. `None`, an empty value or a list containing "*" means no
    restriction."""
    if not allowed_programs:
        return
    allowed = list(allowed_programs)
    if "*" in allowed:
        return
    listing = ", ".join(sorted(allowed))
    try:
        program = program_of(command)
    except ValueError as exc:
        reason = str(exc)
        raise ValueError(
            f"Your plan only allows simple commands starting with one of: {listing}. "
            f"{reason[0].upper()}{reason[1:]}."
        ) from None
    if program not in allowed:
        if _ENV_ASSIGNMENT.match(program):
            raise ValueError(
                f"Your plan only allows commands starting with one of: {listing}. "
                "Setting a variable before the program (NAME=value program) is not allowed."
            )
        raise ValueError(
            f"'{program}' is not an allowed program on your plan. Allowed: {listing}."
        )


def missing_program(command: str, path: Optional[str] = None) -> Optional[str]:
    """The program a worker would fail to start, or None if it should go ahead.

    Returns a name only when it is certain: a Linux/macOS host, a single simple
    command, a program that is neither a shell builtin nor found on `path`
    (default: the process PATH). Anything else returns None and the shell
    decides, exactly as before."""
    if os.name != "posix":
        return None
    try:
        program = program_of(command)
    except ValueError:
        return None
    if _ENV_ASSIGNMENT.match(program) or program in _SHELL_BUILTINS:
        return None
    return None if shutil.which(program, path=path) else program
