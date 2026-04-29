"""Independent regression scenarios, including abrupt child-process termination."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import subprocess
import sys
import threading
import time

from .runtime import Action, CrashInjected, LeaseLost, Limits, Runtime, TransientToolError

REVIEW_SCENARIOS = (
    "late_action_deadline",
    "late_run_deadline",
    "late_lease_expiry",
    "late_cancellation",
    "post_commit_observer_error",
    "invalid_backoff_policy",
    "stale_worker_fencing",
    "persisted_attempt_ceiling",
    "consistent_snapshot",
    "concurrent_workers",
    "process_death_before_commit",
    "process_death_after_commit",
)


def reviewed_scenario(name: str, database: str) -> dict:
    from .evaluation import ManualClock, OnceFault

    clock = ManualClock()
    options = {"clock": clock, "timer": clock}
    actions = [Action("one", amount=10), Action("two", amount=7)]
    if name.startswith("late_"):
        state = {"cancelled": False}

        def fault(stage, *_):
            if stage == "before_commit":
                if name == "late_cancellation":
                    state["cancelled"] = True
                else:
                    clock.advance(2)

        limits = Limits(
            action_timeout_seconds=10 if name in {"late_run_deadline", "late_lease_expiry"} else 1,
            run_timeout_seconds=1 if name == "late_run_deadline" else 10,
            lease_seconds=0.5 if name == "late_lease_expiry" else 10,
        )
        with Runtime(database, limits=limits, fault=fault, **options) as worker:
            worker.submit("example", actions)
            result = worker.run("example", cancelled=lambda: state["cancelled"])
            expected = {
                "late_action_deadline": "tool_timeout",
                "late_run_deadline": "run_deadline",
                "late_lease_expiry": "lease_lost",
                "late_cancellation": "cancelled",
            }[name]
            passed = (
                result["stop_reason"] == expected
                and result["ledger_count"] == 0
                and result["checkpoint"] == 0
                and result["accounts"] == {}
            )
            audit = worker.audit("example")
    elif name == "post_commit_observer_error":
        with Runtime(
            database, limits=Limits(max_attempts=1), fault=OnceFault("after_commit", TransientToolError), **options
        ) as worker:
            worker.submit("example", actions)
            initial = worker.run("example")
            result = worker.run("example")
            passed = (
                initial["checkpoint"] == 1
                and initial["steps"][0]["status"] == "completed"
                and result["status"] == "completed"
                and result["accounts"] == {"demo": 17}
                and result["ledger_count"] == 2
            )
            audit = worker.audit("example")
    elif name == "invalid_backoff_policy":
        with Runtime(
            database, fault=OnceFault("before_action", TransientToolError), backoff=lambda *_: float("nan"), **options
        ) as worker:
            worker.submit("example", actions)
            result = worker.run("example")
            passed = (
                result["stop_reason"] == "backoff_policy_error"
                and not result["lease_active"]
                and result["ledger_count"] == 0
            )
            audit = worker.audit("example")
    elif name == "stale_worker_fencing":
        with Runtime(database, **options) as old, Runtime(database, **options) as current:
            old.submit("example", actions)
            stale = old.acquire("example")
            clock.advance(11)
            replacement = current.acquire("example")
            try:
                old.heartbeat(stale)
            except LeaseLost:
                fenced = True
            else:
                fenced = False
            refused_release = not old.release(stale)
            rejected = old.run("example", lease=stale)
            result = current.run("example", lease=replacement)
            passed = (
                fenced
                and refused_release
                and replacement.epoch == stale.epoch + 1
                and rejected["stop_reason"] == "lease_lost"
                and result["accounts"] == {"demo": 17}
                and result["ledger_count"] == 2
            )
            audit = current.audit("example")
    elif name == "persisted_attempt_ceiling":
        with Runtime(database, limits=Limits(max_attempts=1), fault=OnceFault("before_commit"), **options) as worker:
            worker.submit("example", actions)
            try:
                worker.run("example")
            except CrashInjected:
                pass
        clock.advance(11)
        with Runtime(database, limits=Limits(max_attempts=10), **options) as restarted:
            result = restarted.run("example")
            passed = (
                result["stop_reason"] == "retry_exhausted"
                and result["max_attempts"] == 1
                and result["steps"][0]["attempts"] == 1
                and result["ledger_count"] == 0
            )
            audit = restarted.audit("example")
    elif name == "consistent_snapshot":
        with Runtime(database, **options) as reader, Runtime(database, **options) as writer:
            reader.submit("example", actions)
            commits = []

            def interleave(statement):
                if statement.startswith("SELECT position") and not commits:
                    commits.append(writer.run("example"))

            reader.db.set_trace_callback(interleave)
            observed = reader.snapshot("example")
            reader.db.set_trace_callback(None)
            result = reader.snapshot("example")
            passed = (
                bool(commits)
                and observed["checkpoint"] == observed["ledger_count"] == 0
                and observed["accounts"] == {}
                and result["checkpoint"] == result["ledger_count"] == 2
            )
            audit = reader.audit("example")
    elif name == "concurrent_workers":
        with Runtime(database) as worker:
            worker.submit("example", actions)
        barrier = threading.Barrier(4)

        def execute(_):
            with Runtime(database) as worker:
                barrier.wait(timeout=5)
                return worker.run("example")

        with ThreadPoolExecutor(max_workers=4) as pool:
            outcomes = list(pool.map(execute, range(4)))
        with Runtime(database) as worker:
            result = worker.snapshot("example")
            passed = (
                any(outcome["status"] == "completed" for outcome in outcomes)
                and result["accounts"] == {"demo": 17}
                and result["ledger_count"] == 2
            )
            audit = worker.audit("example")
    elif name in {"process_death_before_commit", "process_death_after_commit"}:
        stage = "before_commit" if name.endswith("before_commit") else "after_commit"
        child = """import os,sys
sys.path.insert(0,sys.argv[3])
from resilient_runtime import Runtime,Action,Limits
def crash(stage,action,attempt):
    if stage==sys.argv[2]: os._exit(23)
with Runtime(sys.argv[1],limits=Limits(lease_seconds=10),fault=crash) as worker:
    worker.submit('example',[Action('one',amount=10),Action('two',amount=7)])
    worker.run('example')
"""
        exited = subprocess.run(
            [sys.executable, "-c", child, database, stage, str(Path(__file__).resolve().parents[1])],
            capture_output=True,
            timeout=15,
        )
        with Runtime(database, clock=lambda: time.time() + 11) as restarted:
            initial = restarted.snapshot("example")
            result = restarted.run("example")
            expected_before = 0 if stage == "before_commit" else 1
            expected_attempts = 2 if stage == "before_commit" else 1
            passed = (
                exited.returncode == 23
                and initial["checkpoint"] == initial["ledger_count"] == expected_before
                and result["status"] == "completed"
                and result["accounts"] == {"demo": 17}
                and result["ledger_count"] == 2
                and result["steps"][0]["attempts"] == expected_attempts
            )
            audit = restarted.audit("example")
        result["child_exit_code"] = exited.returncode
        result["crash_boundary"] = stage
    else:
        raise ValueError("unknown review scenario")
    return {
        "passed": passed and audit["valid"],
        "final": result,
        "audit": audit,
        "provenance": "real local SQLite; controlled clocks/faults except real threads and abrupt child-process exits",
    }
