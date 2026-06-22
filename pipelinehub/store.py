"""RunStore: SQLite-backed persistence for pipeline run history and snapshots."""

import datetime
import json
import os
import sqlite3
import uuid
from contextlib import contextmanager
from typing import Any, Dict, Generator, List, Optional


_SCHEMA = """
CREATE TABLE IF NOT EXISTS runs (
    run_id TEXT PRIMARY KEY,
    pipeline_name TEXT,
    started_at TEXT,
    finished_at TEXT,
    status TEXT,
    total_steps INTEGER
);

CREATE TABLE IF NOT EXISTS step_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT,
    step_name TEXT,
    step_index INTEGER,
    snapshot_before TEXT,
    snapshot_after TEXT,
    duration_seconds REAL,
    FOREIGN KEY (run_id) REFERENCES runs(run_id)
);

CREATE TABLE IF NOT EXISTS failures (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT,
    step_name TEXT,
    step_index INTEGER,
    snapshot_before TEXT,
    exception_type TEXT,
    exception_message TEXT,
    FOREIGN KEY (run_id) REFERENCES runs(run_id)
);
"""


class RunStore:
    """Persists pipeline run history to a local SQLite database."""

    def __init__(self, db_path: str = ".pipelinehub/runs.db") -> None:
        """
        Create directory if needed, connect to SQLite, create tables.

        Args:
            db_path: Path to the SQLite database file, or ":memory:" for in-memory.
        """
        self._db_path = db_path
        self._persist_conn: Optional[sqlite3.Connection] = None

        if db_path == ":memory:":
            self._persist_conn = sqlite3.connect(":memory:")
        else:
            os.makedirs(os.path.dirname(os.path.abspath(db_path)), exist_ok=True)

        self._create_tables()

    @contextmanager
    def _get_conn(self) -> Generator[sqlite3.Connection, None, None]:
        """Context manager: yields a connection, commits on exit, closes if file-based."""
        if self._persist_conn is not None:
            yield self._persist_conn
            self._persist_conn.commit()
        else:
            conn = sqlite3.connect(self._db_path)
            try:
                yield conn
                conn.commit()
            except Exception:
                conn.rollback()
                raise
            finally:
                conn.close()

    def _create_tables(self) -> None:
        with self._get_conn() as conn:
            conn.executescript(_SCHEMA)

    def start_run(self, pipeline_name: str, total_steps: int) -> str:
        """Insert a new run record. Returns the generated run_id."""
        run_id = str(uuid.uuid4())
        started_at = datetime.datetime.utcnow().isoformat()
        with self._get_conn() as conn:
            conn.execute(
                "INSERT INTO runs (run_id, pipeline_name, started_at, status, total_steps) VALUES (?, ?, ?, ?, ?)",
                (run_id, pipeline_name, started_at, "running", total_steps),
            )
        return run_id

    def save_step(
        self,
        run_id: str,
        step_name: str,
        step_index: int,
        snapshot_before: dict,
        snapshot_after: dict,
        duration_seconds: float,
    ) -> None:
        """Save successful step snapshots."""
        with self._get_conn() as conn:
            conn.execute(
                "INSERT INTO step_snapshots (run_id, step_name, step_index, snapshot_before, snapshot_after, duration_seconds) VALUES (?, ?, ?, ?, ?, ?)",
                (run_id, step_name, step_index, json.dumps(snapshot_before), json.dumps(snapshot_after), duration_seconds),
            )

    def save_failure(
        self,
        run_id: str,
        step_name: str,
        step_index: int,
        snapshot_before: dict,
        exception: Exception,
    ) -> None:
        """Save failure context."""
        with self._get_conn() as conn:
            conn.execute(
                "INSERT INTO failures (run_id, step_name, step_index, snapshot_before, exception_type, exception_message) VALUES (?, ?, ?, ?, ?, ?)",
                (run_id, step_name, step_index, json.dumps(snapshot_before), type(exception).__name__, str(exception)),
            )

    def finish_run(self, run_id: str, status: str, finished_at: str) -> None:
        """Update run record with finish time and status."""
        with self._get_conn() as conn:
            conn.execute(
                "UPDATE runs SET status = ?, finished_at = ? WHERE run_id = ?",
                (status, finished_at, run_id),
            )

    def get_last_run(self, pipeline_name: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """Return the most recent run with all step snapshots. None if no runs exist."""
        with self._get_conn() as conn:
            conn.row_factory = sqlite3.Row
            if pipeline_name:
                row = conn.execute(
                    "SELECT * FROM runs WHERE pipeline_name = ? ORDER BY started_at DESC LIMIT 1",
                    (pipeline_name,),
                ).fetchone()
            else:
                row = conn.execute("SELECT * FROM runs ORDER BY started_at DESC LIMIT 1").fetchone()
            if row is None:
                return None
            return self._hydrate_run(conn, dict(row))

    def get_run(self, run_id: str) -> Optional[Dict[str, Any]]:
        """Return a specific run with all step snapshots. None if not found."""
        with self._get_conn() as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute("SELECT * FROM runs WHERE run_id = ?", (run_id,)).fetchone()
            if row is None:
                return None
            return self._hydrate_run(conn, dict(row))

    def _hydrate_run(self, conn: sqlite3.Connection, run: Dict[str, Any]) -> Dict[str, Any]:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT * FROM step_snapshots WHERE run_id = ? ORDER BY step_index",
            (run["run_id"],),
        ).fetchall()
        run["steps"] = [
            {
                **dict(r),
                "snapshot_before": json.loads(r["snapshot_before"]) if r["snapshot_before"] else None,
                "snapshot_after": json.loads(r["snapshot_after"]) if r["snapshot_after"] else None,
            }
            for r in rows
        ]
        return run

    def compare_runs(self, run_id_a: str, run_id_b: str) -> Dict[str, Any]:
        """Diff two runs step by step. Returns structured diff dict."""
        run_a = self.get_run(run_id_a)
        run_b = self.get_run(run_id_b)
        if not run_a or not run_b:
            return {"error": "One or both runs not found"}

        steps_a = {s["step_name"]: s for s in run_a.get("steps", [])}
        steps_b = {s["step_name"]: s for s in run_b.get("steps", [])}
        diff: Dict[str, Any] = {"steps": {}}

        for step_name in set(list(steps_a) + list(steps_b)):
            if step_name not in steps_a:
                diff["steps"][step_name] = {"status": "added_in_b"}
                continue
            if step_name not in steps_b:
                diff["steps"][step_name] = {"status": "only_in_a"}
                continue

            pa = (steps_a[step_name].get("snapshot_after") or {}).get("profile", {})
            pb = (steps_b[step_name].get("snapshot_after") or {}).get("profile", {})
            da = (steps_a[step_name].get("snapshot_after") or {}).get("dtype", "")
            db_ = (steps_b[step_name].get("snapshot_after") or {}).get("dtype", "")

            step_diff: Dict[str, Any] = {"status": "compared"}

            if da == "dataframe" and db_ == "dataframe":
                step_diff["rows"] = {"a": pa.get("rows"), "b": pb.get("rows")}

                nulls_a = pa.get("null_counts", {})
                nulls_b = pb.get("null_counts", {})
                null_diff = {}
                for col in set(list(nulls_a) + list(nulls_b)):
                    va, vb = nulls_a.get(col, 0), nulls_b.get(col, 0)
                    if va != vb:
                        null_diff[col] = {"a": va, "b": vb}
                step_diff["null_count_changes"] = null_diff

                stats_a = pa.get("numeric_stats", {})
                stats_b = pb.get("numeric_stats", {})
                mean_shifts = {}
                for col in set(list(stats_a) + list(stats_b)):
                    ma = (stats_a.get(col) or {}).get("mean")
                    mb = (stats_b.get(col) or {}).get("mean")
                    if ma is not None and mb is not None and ma != 0:
                        shift = abs(mb - ma) / abs(ma)
                        if shift > 0.10:
                            mean_shifts[col] = {"a": ma, "b": mb, "shift_pct": round(shift * 100, 2)}
                step_diff["mean_shifts"] = mean_shifts

                cols_a = set(pa.get("columns", []))
                cols_b = set(pb.get("columns", []))
                step_diff["schema_changes"] = {
                    "added": sorted(cols_b - cols_a),
                    "removed": sorted(cols_a - cols_b),
                }

            elif da in ("sequence", "array") and db_ in ("sequence", "array"):
                len_a = pa.get("length") or (pa.get("shape") or [None])[0]
                len_b = pb.get("length") or (pb.get("shape") or [None])[0]
                step_diff["length"] = {"a": len_a, "b": len_b}

            diff["steps"][step_name] = step_diff

        return diff

    def prune_old_runs(self, days: int = 30) -> None:
        """Delete runs (and their snapshots) older than `days` days."""
        cutoff = (datetime.datetime.utcnow() - datetime.timedelta(days=days)).isoformat()
        with self._get_conn() as conn:
            old = conn.execute("SELECT run_id FROM runs WHERE started_at < ?", (cutoff,)).fetchall()
            for (rid,) in old:
                conn.execute("DELETE FROM step_snapshots WHERE run_id = ?", (rid,))
                conn.execute("DELETE FROM failures WHERE run_id = ?", (rid,))
            conn.execute("DELETE FROM runs WHERE started_at < ?", (cutoff,))

    def list_runs(self, pipeline_name: Optional[str] = None, limit: int = 10) -> List[Dict[str, Any]]:
        """List recent runs with metadata (no step snapshots)."""
        with self._get_conn() as conn:
            conn.row_factory = sqlite3.Row
            if pipeline_name:
                rows = conn.execute(
                    "SELECT run_id, pipeline_name, started_at, finished_at, status, total_steps "
                    "FROM runs WHERE pipeline_name = ? ORDER BY started_at DESC LIMIT ?",
                    (pipeline_name, limit),
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT run_id, pipeline_name, started_at, finished_at, status, total_steps "
                    "FROM runs ORDER BY started_at DESC LIMIT ?",
                    (limit,),
                ).fetchall()
            return [dict(r) for r in rows]
