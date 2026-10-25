"""
Generate docs/CODEMAP.md: an inventory of what exists in this codebase, built FROM the code so it cannot be forgotten.

    python scripts/gen_codemap.py            # rewrite docs/CODEMAP.md
    python scripts/gen_codemap.py --check    # exit 1 if it is out of date (a test runs this check too)

It lists: modules, the customer API routes, the staff dashboard routes, database tables and migrations, environment
variables, events, plan limits, permissions and scopes, dashboard tabs, tests and docs. Nothing in it is written by
hand, and nothing in it depends on the time, the machine or the order things ran in.
"""
import ast
import os
import re
import sqlite3
import sys
import tempfile
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
OUT = ROOT / "docs" / "CODEMAP.md"
SKIP_PARTS = {"__pycache__", "node_modules", "htmlcov", ".git"}


def _py_files(*bases):
    for base in bases:
        for path in sorted((ROOT / base).rglob("*.py")):
            rel = path.relative_to(ROOT).as_posix()
            if SKIP_PARTS & set(path.parts) or "_pb2" in path.name or rel.startswith("gcon-rebuild/"):
                continue
            yield path


def _first_line(text, limit=110):
    if not text:
        return ""
    line = text.strip().splitlines()[0].strip()
    return (line[: limit - 1] + "…") if len(line) > limit else line


def _modules():
    out = []
    groups = defaultdict(list)
    for path in _py_files("src/gcon", "scripts"):
        rel = path.relative_to(ROOT).as_posix()
        try:
            doc = ast.get_docstring(ast.parse(path.read_text(encoding="utf-8")))
        except SyntaxError:
            doc = None
        if path.name == "__init__.py" and not doc:
            continue
        groups[str(Path(rel).parent.as_posix())].append((path.name, _first_line(doc)))
    for folder in sorted(groups):
        out.append(f"\n### `{folder}/`\n")
        for name, doc in sorted(groups[folder]):
            out.append(f"- `{name}`" + (f" — {doc}" if doc else ""))
    return "\n".join(out)


def _isolated_environment():
    """A throwaway data directory and password, so building the apps neither touches real data nor logs a secret."""
    tmp = tempfile.mkdtemp(prefix="gcon-codemap-")
    saved = {k: os.environ.get(k) for k in ("GCON_DB_PATH", "GCON_OWNER_PASSWORD", "GCON_DATA_DIR")}
    os.environ["GCON_DB_PATH"] = os.path.join(tmp, "gcon.db")
    os.environ["GCON_OWNER_PASSWORD"] = "codemap-only-password-1"
    os.environ.pop("GCON_DATA_DIR", None)
    return tmp, saved


def _restore(saved):
    for key, value in saved.items():
        if value is None:
            os.environ.pop(key, None)
        else:
            os.environ[key] = value


def _routes_and_schema():
    sys.path.insert(0, str(ROOT / "src"))
    import logging
    logging.disable(logging.CRITICAL)
    tmp, saved = _isolated_environment()
    try:
        from fastapi.routing import APIRoute
        from gcon.api.api_v1 import create_api_v1_app
        from gcon.cluster.coordinator import GCONCoordinator
        from gcon.dashboard.presentation import PresentationLayer
        from gcon.dashboard.web_server import WebServer
        from gcon.management.management_layer import ManagementLayer
        from gcon.persistence.control_plane import ControlPlane
        from gcon.persistence.migrations.registry import MIGRATIONS

        cp_path = os.path.join(tmp, "cp.db")
        coordinator = GCONCoordinator(control_plane=ControlPlane(path=cp_path))
        management = ManagementLayer(coordinator=coordinator, db_path=os.path.join(tmp, "mgmt.db"))
        presentation = PresentationLayer(coordinator)

        def routes(app, with_summary):
            rows = set()
            for r in app.routes:
                if not isinstance(r, APIRoute):
                    continue
                for method in sorted(r.methods - {"HEAD", "OPTIONS"}):
                    label = (r.summary or _first_line(r.endpoint.__doc__) or r.name) if with_summary \
                        else (_first_line(r.endpoint.__doc__) or r.name)
                    rows.add((r.path, method, label.replace("|", "/")))
            return sorted(rows)

        customer = routes(create_api_v1_app(management, presentation), True)
        staff = routes(WebServer(presentation).app, False)

        def schema(path):
            conn = sqlite3.connect(path)
            try:
                tables = [r[0] for r in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name")]
                return [(t, [c[1] for c in conn.execute(f'PRAGMA table_info("{t}")')]) for t in tables]
            finally:
                conn.close()

        coordinator.shutdown()
        migrations = sorted((m.version, m.name) for m in MIGRATIONS)
        return customer, staff, schema(cp_path), schema(os.path.join(tmp, "mgmt.db")), migrations
    finally:
        logging.disable(logging.NOTSET)
        _restore(saved)


def _route_table(rows, with_summary):
    out = ["| Method | Path | " + ("Summary" if with_summary else "Handler") + " |", "|---|---|---|"]
    out += [f"| {m} | `{p}` | {s} |" for p, m, s in rows]
    return "\n".join(out)


def _schema_block(tables):
    return "\n".join(f"- `{t}`({', '.join(cols)})" for t, cols in tables)


def _env_vars():
    pattern = re.compile(r"\b(GCON_[A-Z0-9_]+|RAILWAY_VOLUME_MOUNT_PATH)\b")
    where = defaultdict(set)
    for path in _py_files("src/gcon", "scripts"):
        text = path.read_text(encoding="utf-8", errors="replace")
        for name in set(pattern.findall(text)):
            if name.endswith("_"):
                continue
            where[name].add(path.relative_to(ROOT).as_posix())
    return "\n".join(f"- `{n}` — {', '.join(f'`{f}`' for f in sorted(where[n]))}" for n in sorted(where))


def _events():
    sys.path.insert(0, str(ROOT / "src"))
    from gcon.events.event_types import EventType
    names = sorted(n for n, v in vars(EventType).items() if n.isupper() and isinstance(v, str))
    lease = sorted(set(re.findall(r'"(WORKER_[A-Z_]+)"',
                                  (ROOT / "src/gcon/cluster/worker_leases.py").read_text(encoding="utf-8"))))
    return ", ".join(f"`{n}`" for n in names), ", ".join(f"`{n}`" for n in lease)


def _plans():
    """Read from the source file, not the imported module: a test run may adjust the module's table at runtime, and
    this must describe the code as written."""
    tree = ast.parse((ROOT / "src/gcon/management/plans.py").read_text(encoding="utf-8"))
    values = {}
    for node in tree.body:
        targets = node.targets if isinstance(node, ast.Assign) else [node.target] if isinstance(node, ast.AnnAssign) else []
        for target in targets:
            if isinstance(target, ast.Name) and target.id in ("LIMIT_FIELDS", "DEFAULT_PLAN_LIMITS", "DEFAULT_PLAN"):
                values[target.id] = ast.literal_eval(node.value)
    fields = list(values["LIMIT_FIELDS"])
    rows = ["| Plan | " + " | ".join(fields) + " |", "|---|" + "---|" * len(fields)]
    for tier, limits in values["DEFAULT_PLAN_LIMITS"].items():
        rows.append(f"| {tier}{' (default)' if tier == values['DEFAULT_PLAN'] else ''} | "
                    + " | ".join(str(limits[f]) for f in fields) + " |")
    return "\n".join(rows)


def _grants():
    text_staff = (ROOT / "src/gcon/dashboard/web_server.py").read_text(encoding="utf-8")
    text_api = (ROOT / "src/gcon/api/api_v1.py").read_text(encoding="utf-8")
    perms = sorted(set(re.findall(r'require_permission\("([^"]+)"\)', text_staff)))
    scopes = sorted(set(re.findall(r'require_scope\("([^"]+)"\)', text_api)))
    return ", ".join(f"`{p}`" for p in perms), ", ".join(f"`{s}`" for s in scopes)


def _tabs():
    html = (ROOT / "templates/dashboard.html").read_text(encoding="utf-8")
    tabs = sorted(set(re.findall(r'id="(tab-[a-z0-9-]+)"', html)))
    panels = sorted(p.name for p in (ROOT / "templates/panels").glob("*.html"))
    return ", ".join(f"`{t}`" for t in tabs), ", ".join(f"`{p}`" for p in panels)


def _tests():
    out = []
    base = ROOT / "tests"
    folders = defaultdict(list)
    for path in sorted(base.rglob("test_*.py")):
        if SKIP_PARTS & set(path.parts):
            continue
        folders[path.parent.relative_to(ROOT).as_posix()].append(path.name)
    for folder in sorted(folders):
        names = folders[folder]
        out.append(f"- `{folder}/` ({len(names)}): " + ", ".join(f"`{n}`" for n in names))
    return "\n".join(out)


def _docs():
    out = []
    for path in sorted((ROOT / "docs").glob("*.md")):
        if path.name == "CODEMAP.md":
            continue
        title = next((l.lstrip("# ").strip() for l in path.read_text(encoding="utf-8").splitlines()
                      if l.startswith("#")), "")
        out.append(f"- `docs/{path.name}` — {_first_line(title)}")
    return "\n".join(out)


def build_codemap():
    customer, staff, cp_tables, mgmt_tables, migrations = _routes_and_schema()
    events, lease_events = _events()
    perms, scopes = _grants()
    tabs, panels = _tabs()
    parts = [
        "# GCON code map",
        "",
        "**Generated by `scripts/gen_codemap.py`. Do not edit by hand.** After changing a route, table, migration, "
        "environment variable, event, plan limit, permission, dashboard tab, test file or doc, run "
        "`python scripts/gen_codemap.py`. A test fails when this file is out of date.",
        "",
        "Hand-written guides are listed under *Docs*. Start with `docs/WORKERS_AND_ENROLLMENT.md` for workers and "
        "`docs/CUSTOMER_SIDE_CARRYOVER.md` for wiring the customer site to the backend.",
        "",
        "## Docs", "", _docs(),
        "",
        "## Customer API routes (organization API key)", "",
        f"{len(customer)} routes. Scopes used: {scopes}.", "", _route_table(customer, True),
        "",
        "## Staff dashboard routes", "",
        f"{len(staff)} routes. Permissions used: {perms}.", "", _route_table(staff, False),
        "",
        "## Dashboard", "", f"Tabs: {tabs}.", "", f"Panels (`templates/panels/`): {panels}.",
        "",
        "## Database", "",
        "Migrations: " + ", ".join(f"{v} `{n}`" for v, n in migrations) + ".", "",
        "### Control plane (`cp.db`)", "", _schema_block(cp_tables), "",
        "### Management (users, organizations, API keys)", "", _schema_block(mgmt_tables),
        "",
        "## Plan limits (starting values)", "", _plans(),
        "",
        "## Events", "", f"`EventType`: {events}.", "", f"Lease events (published by name): {lease_events}.",
        "",
        "## Environment variables", "", _env_vars(),
        "",
        "## Tests", "", _tests(),
        "",
        "## Modules",
        _modules(),
        "",
    ]
    return "\n".join(parts)


def main(argv):
    text = build_codemap()
    if "--check" in argv:
        current = OUT.read_text(encoding="utf-8") if OUT.exists() else ""
        if current != text:
            print("docs/CODEMAP.md is out of date. Run: python scripts/gen_codemap.py")
            return 1
        print("docs/CODEMAP.md is current.")
        return 0
    OUT.write_text(text, encoding="utf-8", newline="\n")
    print(f"Wrote {OUT.relative_to(ROOT)} ({len(text.splitlines())} lines).")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
