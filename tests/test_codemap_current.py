"""docs/CODEMAP.md is generated from the code. If this fails, something it lists changed (a route, table, migration,
environment variable, event, plan limit, permission, dashboard tab, test file or doc) and the file was not regenerated:

    python scripts/gen_codemap.py
"""
import importlib.util
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def _generator():
    spec = importlib.util.spec_from_file_location("gen_codemap", ROOT / "scripts" / "gen_codemap.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_the_code_map_is_current():
    expected = _generator().build_codemap()
    actual = (ROOT / "docs" / "CODEMAP.md").read_text(encoding="utf-8")
    if actual != expected:
        old, new = actual.splitlines(), expected.splitlines()
        first = next((i for i, (a, b) in enumerate(zip(old, new)) if a != b), min(len(old), len(new)))
        raise AssertionError(
            "docs/CODEMAP.md is out of date. Run: python scripts/gen_codemap.py\n"
            f"First difference at line {first + 1}:\n  file:      {old[first] if first < len(old) else '(end)'}\n"
            f"  generated: {new[first] if first < len(new) else '(end)'}")


def test_the_generator_is_deterministic():
    generator = _generator()
    assert generator.build_codemap() == generator.build_codemap()
