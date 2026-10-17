"""
What `command` means is part of the frozen /api/v1 contract. These tests pin the
rule the spec describes: one allowed program plus arguments the filter does not
inspect. If one fails, either the behavior or the spec text must change on purpose.
"""
import pytest

from gcon.api import api_v1
from gcon.execution.command_check import check_command_shape, check_program_allowed

ALLOWED = ["python", "python3", "nvidia-smi", "echo"]


def _accepted(command, allowed=ALLOWED):
    check_command_shape(command)
    check_program_allowed(command, allowed)


@pytest.mark.parametrize("command", [
    "python train.py", "python anything at all --x", "python", "echo hi",
])
def test_an_allowed_program_with_any_arguments_is_accepted(command):
    _accepted(command)


@pytest.mark.parametrize("command", [
    "nn", "bash -c 'ls'", "/usr/bin/python x.py", "FOO=1 python x.py",
    "python x; rm -rf /", "python x | tee o", "python x && echo", "echo $(whoami)",
    "python x\npython y", "python x > out.txt",
])
def test_anything_else_is_refused_when_the_plan_lists_programs(command):
    with pytest.raises(ValueError):
        _accepted(command)


def test_a_plan_with_no_program_list_accepts_any_command_line():
    _accepted("anything; at all | goes", None)


def test_the_spec_describes_this_rule_not_just_a_shell_command():
    description = api_v1.JobSubmitRequest.model_fields["command"].description
    assert description != "Shell command the job will run"
    for phrase in ("allowed_programs", "unchecked", "ONE simple command", "not a security"):
        assert phrase in description
