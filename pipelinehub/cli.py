"""ph — pipelinehub CLI for local run inspection."""

import datetime
import json
import os
import sqlite3
import sys
import time
from typing import Any, Dict, List, Optional, Set


_HELP = """ph — pipelinehub CLI

Usage:
  ph runs list              List recent runs
  ph runs last              Show last run summary
  ph runs show <id>         Show step detail for a run
  ph runs diff <id> <id>    Compare two runs
  ph runs watch             Tail runs as they execute
  ph status                 Pipeline health overview
  ph stats                  Aggregate stats across all runs
  ph doctor                 Check setup
  ph init                   Create .pipelinehub/ in current directory
"""


def _find_db() -> Optional[str]:
    path = os.getcwd()
    while True:
        candidate = os.path.join(path, ".pipelinehub", "runs.db")
        if os.path.exists(candidate):
            return candidate
        parent = os.path.dirname(path)
        if parent == path:
            return None
        path = parent


def _require_db() -> str:
    db = _find_db()
    if db is None:
        print("No .pipelinehub/runs.db found. Run a pipeline first or `ph init`.")
        sys.exit(1)
    return db


def _query(db_path: str, sql: str, params: tuple = ()) -> List[Dict[str, Any]]:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        return [dict(r) for r in conn.execute(sql, params).fetchall()]
    finally:
        conn.close()


def _short(run_id: str) -> str:
    return run_id[:8]


def _fmt_ts(ts: Optional[str]) -> str:
    if not ts:
        return "-"
    try:
        return datetime.datetime.fromisoformat(ts).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return ts


def _icon(status: str) -> str:
    return {"success": "✅", "failed": "❌", "running": "⏳"}.get(status, "?")


def _parse_snap(raw: Optional[str]) -> Dict[str, Any]:
    return json.loads(raw) if raw else {}


def _row_col_summary(snap_before: Dict, snap_after: Dict):
    dtype = snap_after.get("dtype", "")
    pb = snap_before.get("profile", {})
    pa = snap_after.get("profile", {})
    if dtype == "dataframe":
        rows_str = "{0}→{1}".format(pb.get("rows", "?"), pa.get("rows", "?"))
        nulls_b = sum(pb.get("null_counts", {}).values())
        nulls_a = sum(pa.get("null_counts", {}).values())
        nulls_str = "{0}→{1}".format(nulls_b, nulls_a)
    elif dtype in ("sequence", "array"):
        rows_str = "{0}→{1}".format(pb.get("length", "?"), pa.get("length", "?"))
        nulls_str = "-"
    else:
        rows_str = "-"
        nulls_str = "-"
    return rows_str, nulls_str


def cmd_runs_list(args: List[str]) -> None:
    db = _require_db()
    limit = 20
    pipeline_filter = None
    i = 0
    while i < len(args):
        if args[i] in ("--limit", "-n") and i + 1 < len(args):
            limit = int(args[i + 1])
            i += 2
        elif args[i] in ("--pipeline", "-p") and i + 1 < len(args):
            pipeline_filter = args[i + 1]
            i += 2
        else:
            i += 1

    conditions = []
    params: List[Any] = []
    if pipeline_filter:
        conditions.append("pipeline_name = ?")
        params.append(pipeline_filter)
    where = "WHERE {0}".format(" AND ".join(conditions)) if conditions else ""
    params.append(limit)

    rows = _query(
        db,
        "SELECT run_id, pipeline_name, started_at, finished_at, status, total_steps "
        "FROM runs {0} ORDER BY started_at DESC LIMIT ?".format(where),
        tuple(params),
    )

    if not rows:
        print("No runs found.")
        return

    print("  {0:<10} {1:<22} {2:<22} {3:<12} {4}".format("ID", "Pipeline", "Started", "Status", "Steps"))
    print("  " + "─" * 74)
    for r in rows:
        icon = _icon(r["status"])
        print("  {0:<10} {1:<22} {2:<22} {3} {4:<10} {5}".format(
            _short(r["run_id"]),
            (r["pipeline_name"] or "")[:21],
            _fmt_ts(r["started_at"]),
            icon,
            r["status"],
            r["total_steps"] if r["total_steps"] is not None else "-",
        ))


def cmd_runs_last(args: List[str]) -> None:
    db = _require_db()
    pipeline_filter = None
    i = 0
    while i < len(args):
        if args[i] in ("--pipeline", "-p") and i + 1 < len(args):
            pipeline_filter = args[i + 1]
            i += 2
        else:
            i += 1

    conditions = ["status = 'success'"]
    params: List[Any] = []
    if pipeline_filter:
        conditions.append("pipeline_name = ?")
        params.append(pipeline_filter)
    where = "WHERE {0}".format(" AND ".join(conditions))

    rows = _query(db, "SELECT * FROM runs {0} ORDER BY started_at DESC LIMIT 1".format(where), tuple(params))
    if not rows:
        print("No completed runs found.")
        return

    run = rows[0]
    steps = _query(db, "SELECT * FROM step_snapshots WHERE run_id = ? ORDER BY step_index", (run["run_id"],))
    _print_run(run, steps)


def cmd_runs_show(args: List[str]) -> None:
    if not args:
        print("Usage: ph runs show <run_id>")
        sys.exit(1)
    db = _require_db()
    prefix = args[0]

    rows = _query(db, "SELECT * FROM runs WHERE run_id LIKE ?", (prefix + "%",))
    if not rows:
        print("Run not found: {0}".format(prefix))
        sys.exit(1)
    if len(rows) > 1:
        print("Ambiguous prefix '{0}' — matches {1} runs. Use more characters.".format(prefix, len(rows)))
        sys.exit(1)

    run = rows[0]
    steps = _query(db, "SELECT * FROM step_snapshots WHERE run_id = ? ORDER BY step_index", (run["run_id"],))
    failure = _query(db, "SELECT * FROM failures WHERE run_id = ? LIMIT 1", (run["run_id"],))

    _print_run(run, steps, verbose=True)

    if failure:
        f = failure[0]
        print("\n  ❌ Failed at '{0}': [{1}] {2}".format(f["step_name"], f["exception_type"], f["exception_message"]))


def _print_run(run: Dict[str, Any], steps: List[Dict[str, Any]], verbose: bool = False) -> None:
    icon = _icon(run["status"])
    print("\n  {0} run {1}  pipeline={2}  started={3}".format(
        icon, _short(run["run_id"]), run["pipeline_name"], _fmt_ts(run["started_at"])
    ))

    if not steps:
        print("  No steps recorded.")
        return

    print("\n  {0:<26} {1:<22} {2:<20} {3}".format("Step", "Rows (in→out)", "Nulls (in→out)", "Duration"))
    print("  " + "─" * 74)

    for s in steps:
        snap_before = _parse_snap(s["snapshot_before"])
        snap_after = _parse_snap(s["snapshot_after"])
        rows_str, nulls_str = _row_col_summary(snap_before, snap_after)
        dur = "{0:.1f}s".format(s["duration_seconds"]) if s["duration_seconds"] is not None else "-"
        print("  {0:<26} {1:<22} {2:<20} {3}".format(s["step_name"], rows_str, nulls_str, dur))

        if verbose and snap_after.get("dtype") == "dataframe":
            dtypes = snap_after.get("profile", {}).get("dtypes", {})
            if dtypes:
                cols_preview = ", ".join("{0}({1})".format(c, t) for c, t in list(dtypes.items())[:6])
                print("    columns: {0}".format(cols_preview))


def cmd_runs_diff(args: List[str]) -> None:
    if len(args) < 2:
        print("Usage: ph runs diff <run_id_a> <run_id_b>")
        sys.exit(1)
    db = _require_db()

    def resolve(prefix: str) -> Dict[str, Any]:
        rows = _query(db, "SELECT * FROM runs WHERE run_id LIKE ?", (prefix + "%",))
        if not rows:
            print("Run not found: {0}".format(prefix))
            sys.exit(1)
        if len(rows) > 1:
            print("Ambiguous prefix '{0}'. Use more characters.".format(prefix))
            sys.exit(1)
        return rows[0]

    run_a = resolve(args[0])
    run_b = resolve(args[1])

    def get_steps(run_id: str) -> Dict[str, Dict]:
        rows = _query(db, "SELECT * FROM step_snapshots WHERE run_id = ? ORDER BY step_index", (run_id,))
        return {r["step_name"]: r for r in rows}

    steps_a = get_steps(run_a["run_id"])
    steps_b = get_steps(run_b["run_id"])

    print("\n  Comparing:")
    print("    A: {0}  ({1})".format(_short(run_a["run_id"]), _fmt_ts(run_a["started_at"])))
    print("    B: {0}  ({1})\n".format(_short(run_b["run_id"]), _fmt_ts(run_b["started_at"])))

    seen: Dict[str, int] = {}
    for step_name in list(steps_a) + list(steps_b):
        seen[step_name] = seen.get(step_name, 0) + 1
    all_steps = list(seen)

    for step_name in all_steps:
        if step_name not in steps_a:
            print("  {0}: only in B (new step)".format(step_name))
            continue
        if step_name not in steps_b:
            print("  {0}: only in A (removed)".format(step_name))
            continue

        sa = steps_a[step_name]
        sb = steps_b[step_name]
        snap_a = _parse_snap(sa["snapshot_after"])
        snap_b = _parse_snap(sb["snapshot_after"])
        pa = snap_a.get("profile", {})
        pb = snap_b.get("profile", {})
        dtype_a = snap_a.get("dtype", "")
        dtype_b = snap_b.get("dtype", "")

        findings: List[str] = []

        if dtype_a == "dataframe" and dtype_b == "dataframe":
            rows_a = pa.get("rows")
            rows_b = pb.get("rows")
            if rows_a is not None and rows_b is not None:
                delta = rows_b - rows_a
                if rows_a == 0:
                    pct_str = ""
                else:
                    pct_str = " ({0:+.1f}%)".format(100.0 * delta / rows_a)
                findings.append("rows: {0}→{1} {2:+d}{3}".format(rows_a, rows_b, delta, pct_str))

            nulls_a = pa.get("null_counts", {})
            nulls_b = pb.get("null_counts", {})
            for col in sorted(set(list(nulls_a) + list(nulls_b))):
                va, vb = nulls_a.get(col, 0), nulls_b.get(col, 0)
                if va != vb:
                    findings.append("  ⚠  {0} nulls: {1}→{2}".format(col, va, vb))

            dtypes_a = pa.get("dtypes", {})
            dtypes_b = pb.get("dtypes", {})
            for col in sorted(set(list(dtypes_a) + list(dtypes_b))):
                ta, tb = dtypes_a.get(col), dtypes_b.get(col)
                if ta and tb and ta != tb:
                    findings.append("  ⚠  {0} dtype: {1}→{2}".format(col, ta, tb))

            cols_a = set(pa.get("columns", []))
            cols_b = set(pb.get("columns", []))
            for col in sorted(cols_b - cols_a):
                findings.append("  +  column added: {0}".format(col))
            for col in sorted(cols_a - cols_b):
                findings.append("  -  column removed: {0}".format(col))

        elif dtype_a in ("sequence", "array") and dtype_b in ("sequence", "array"):
            la = pa.get("length") or (pa.get("shape") or [None])[0]
            lb = pb.get("length") or (pb.get("shape") or [None])[0]
            if la is not None and lb is not None:
                findings.append("length: {0}→{1}".format(la, lb))

        if findings:
            print("  {0}:".format(step_name))
            for finding in findings:
                print("    {0}".format(finding))
        else:
            print("  {0}: no changes".format(step_name))


def cmd_runs_watch(args: List[str]) -> None:
    db = _require_db()
    print("Watching for running pipelines... (Ctrl+C to stop)\n")

    announced: Set[str] = set()
    seen_steps: Dict[str, Set[int]] = {}

    try:
        while True:
            running = _query(
                db,
                "SELECT run_id, pipeline_name, started_at FROM runs WHERE status = 'running' ORDER BY started_at DESC LIMIT 1",
            )

            if not running:
                time.sleep(1)
                continue

            run = running[0]
            run_id = run["run_id"]

            if run_id not in announced:
                print("  ⏳ run {0}  pipeline={1}  started={2}\n".format(
                    _short(run_id), run["pipeline_name"], _fmt_ts(run["started_at"])
                ))
                print("  {0:<26} {1:<22} {2}".format("Step", "Rows (in→out)", "Duration"))
                print("  " + "─" * 56)
                announced.add(run_id)
                seen_steps[run_id] = set()

            steps = _query(
                db,
                "SELECT * FROM step_snapshots WHERE run_id = ? ORDER BY step_index",
                (run_id,),
            )
            for s in steps:
                idx = s["step_index"]
                if idx in seen_steps.get(run_id, set()):
                    continue
                seen_steps.setdefault(run_id, set()).add(idx)

                snap_before = _parse_snap(s["snapshot_before"])
                snap_after = _parse_snap(s["snapshot_after"])
                rows_str, _ = _row_col_summary(snap_before, snap_after)
                dur = "{0:.1f}s".format(s["duration_seconds"]) if s["duration_seconds"] is not None else "-"
                print("  {0:<26} {1:<22} {2}".format(s["step_name"], rows_str, dur))

            final = _query(db, "SELECT status, finished_at FROM runs WHERE run_id = ?", (run_id,))
            if final and final[0]["status"] in ("success", "failed"):
                status = final[0]["status"]
                icon = _icon(status)
                print("\n  {0} {1}  finished={2}".format(icon, status, _fmt_ts(final[0]["finished_at"])))

                if status == "failed":
                    failure = _query(db, "SELECT * FROM failures WHERE run_id = ? LIMIT 1", (run_id,))
                    if failure:
                        f = failure[0]
                        print("  ❌ step '{0}': [{1}] {2}".format(
                            f["step_name"], f["exception_type"], f["exception_message"]
                        ))

                announced.discard(run_id)
                seen_steps.pop(run_id, None)
                print()

            time.sleep(1)

    except KeyboardInterrupt:
        print("\nStopped.")


def cmd_status(args: List[str]) -> None:
    db = _require_db()
    pipelines = _query(db, "SELECT DISTINCT pipeline_name FROM runs ORDER BY pipeline_name")
    if not pipelines:
        print("No pipelines found.")
        return

    print("\n  {0:<32} {1:<22} {2:<14} {3}".format("Pipeline", "Last run", "Status", "Success rate"))
    print("  " + "─" * 76)

    for row in pipelines:
        name = row["pipeline_name"]
        last = _query(
            db,
            "SELECT status, started_at FROM runs WHERE pipeline_name = ? ORDER BY started_at DESC LIMIT 1",
            (name,),
        )
        total = _query(db, "SELECT COUNT(*) as n FROM runs WHERE pipeline_name = ?", (name,))[0]["n"]
        success = _query(
            db,
            "SELECT COUNT(*) as n FROM runs WHERE pipeline_name = ? AND status = 'success'",
            (name,),
        )[0]["n"]

        last_status = last[0]["status"] if last else "-"
        last_time = _fmt_ts(last[0]["started_at"]) if last else "-"
        icon = _icon(last_status)
        rate = "{0}%".format(100 * success // total) if total else "-"
        print("  {0:<32} {1:<22} {2} {3:<12} {4}".format(name[:31], last_time, icon, last_status, rate))


def cmd_stats(args: List[str]) -> None:
    db = _require_db()
    total = _query(db, "SELECT COUNT(*) as n FROM runs")[0]["n"]
    success = _query(db, "SELECT COUNT(*) as n FROM runs WHERE status = 'success'")[0]["n"]
    failed = _query(db, "SELECT COUNT(*) as n FROM runs WHERE status = 'failed'")[0]["n"]
    running = _query(db, "SELECT COUNT(*) as n FROM runs WHERE status = 'running'")[0]["n"]
    step_failures = _query(db, "SELECT COUNT(*) as n FROM failures")[0]["n"]

    print("\n  Total runs:     {0}".format(total))
    print("  Successful:     {0}".format(success))
    print("  Failed:         {0}".format(failed))
    print("  Running:        {0}".format(running))
    print("  Step failures:  {0}".format(step_failures))
    if total:
        print("  Success rate:   {0}%".format(100 * success // total))

    top_fail = _query(
        db,
        "SELECT pipeline_name, COUNT(*) as n FROM runs WHERE status = 'failed' "
        "GROUP BY pipeline_name ORDER BY n DESC LIMIT 5",
    )
    if top_fail:
        print("\n  Most failures:")
        for r in top_fail:
            print("    {0}: {1}".format(r["pipeline_name"], r["n"]))


def cmd_doctor(args: List[str]) -> None:
    db_path = _find_db()
    if db_path is None:
        print("  ❌ No .pipelinehub/runs.db found — run a pipeline first or `ph init`")
        return
    print("  ✅ DB found: {0}".format(db_path))

    try:
        total = _query(db_path, "SELECT COUNT(*) as n FROM runs")[0]["n"]
        print("  ✅ DB readable — {0} runs on record".format(total))
    except Exception as e:
        print("  ❌ DB read error: {0}".format(e))
        return

    running = _query(db_path, "SELECT COUNT(*) as n FROM runs WHERE status = 'running'")[0]["n"]
    if running:
        print("  ⚠  {0} run(s) stuck in 'running' state (process may have crashed)".format(running))


def cmd_init(args: List[str]) -> None:
    target = os.path.join(os.getcwd(), ".pipelinehub")
    if os.path.exists(target):
        print("  Already exists: {0}".format(target))
        return
    os.makedirs(target, exist_ok=True)
    gitignore = os.path.join(target, ".gitignore")
    with open(gitignore, "w") as fh:
        fh.write("runs.db\n")
    print("  ✅ Created {0}".format(target))
    print("  ✅ Created {0} (runs.db excluded from git)".format(gitignore))


def main() -> None:
    args = sys.argv[1:]
    if not args or args[0] in ("--help", "-h", "help"):
        print(_HELP)
        return

    cmd = args[0]
    rest = args[1:]

    if cmd == "runs":
        if not rest:
            print(_HELP)
            return
        sub = rest[0]
        sub_args = rest[1:]
        dispatch = {
            "list": cmd_runs_list,
            "last": cmd_runs_last,
            "show": cmd_runs_show,
            "diff": cmd_runs_diff,
            "watch": cmd_runs_watch,
        }
        if sub not in dispatch:
            print("Unknown subcommand: ph runs {0}".format(sub))
            sys.exit(1)
        dispatch[sub](sub_args)
    elif cmd == "status":
        cmd_status(rest)
    elif cmd == "stats":
        cmd_stats(rest)
    elif cmd == "doctor":
        cmd_doctor(rest)
    elif cmd == "init":
        cmd_init(rest)
    else:
        print("Unknown command: {0}".format(cmd))
        print(_HELP)
        sys.exit(1)


if __name__ == "__main__":
    main()
