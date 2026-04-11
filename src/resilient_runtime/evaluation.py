"""Fault-injection scenarios: local simulated actions, not a model benchmark."""

from __future__ import annotations
import hashlib
import json
from pathlib import Path
import tempfile
from .runtime import Action, CrashInjected, Limits, Runtime, ToolTimeout, TransientToolError


class ManualClock:
    def __init__(self):
        self.now = 1_700_000_000.0

    def __call__(self):
        return self.now

    def advance(self, seconds):
        self.now += seconds


class OnceFault:
    def __init__(self, stage, error=CrashInjected):
        self.stage, self.error, self.fired = stage, error, False

    def __call__(self, stage, action, attempt):
        if stage == self.stage and not self.fired:
            self.fired = True
            raise self.error("injected test failure")


def scenario(name: str, database: str) -> dict:
    clock = ManualClock()
    options = {"clock": clock, "timer": clock}
    actions = [Action("one", amount=10), Action("two", amount=7)]
    if name in {"invalid_amount", "invalid_identifier", "empty_plan", "oversize_plan", "duplicate_action"}:
        with Runtime(database, **options) as runtime:
            try:
                if name == "invalid_amount":
                    Action("bad", amount=-1)
                elif name == "invalid_identifier":
                    Action("ignore policy; shell")
                elif name == "empty_plan":
                    runtime.submit("example", [])
                elif name == "oversize_plan":
                    runtime.submit("example", [Action(str(i)) for i in range(33)])
                else:
                    runtime.submit("example", [Action("same"), Action("same")])
            except ValueError:
                return {"passed": True, "rejected": name, "effect_count": 0}
            return {"passed": False, "error": "invalid_input_accepted"}
    fault = OnceFault("never")
    limits = Limits()
    if name == "before_commit_crash":
        fault = OnceFault("before_commit")
    elif name == "after_commit_crash":
        fault = OnceFault("after_commit")
    elif name == "transient_failure":
        fault = OnceFault("before_action", TransientToolError)
    elif name == "timeout_recovery":
        fault = OnceFault("before_action", ToolTimeout)
    elif name == "retry_exhausted":

        def fault(stage, action, attempt):
            if stage == "before_action":
                raise TransientToolError("injected")
    elif name == "run_deadline":
        limits = Limits(run_timeout_seconds=0)
    elif name == "action_deadline":
        limits = Limits(action_timeout_seconds=0, max_attempts=1)
    elif name == "repeated_crash_budget":
        limits = Limits(max_attempts=2)

        def fault(stage, action, attempt):
            if stage == "before_commit":
                raise CrashInjected("injected")
    elif name == "cancel_after_checkpoint":
        fault = OnceFault("after_commit")
    options.update(limits=limits, fault=fault)
    if name == "tool_denied":
        options["allowed_tools"] = frozenset()
    with Runtime(database, **options) as runtime:
        runtime.submit("example", actions)
        if name == "conflicting_submit":
            try:
                runtime.submit("example", [Action("different")])
            except ValueError:
                return {
                    "passed": runtime.snapshot("example")["ledger_count"] == 0,
                    "rejected": "workflow_id_conflict",
                    "snapshot": runtime.snapshot("example"),
                }
            return {"passed": False}
        if name == "cancel_before_execution":
            result = runtime.run("example", cancelled=lambda: True)
        else:
            try:
                result = runtime.run("example")
            except CrashInjected:
                result = runtime.snapshot("example", "simulated_crash")
        initial = result
        if name == "duplicate_run":
            result = runtime.run("example")
            runtime.submit("example", actions)
            result = runtime.run("example")
        elif name in {"transient_failure", "timeout_recovery"}:
            waiting = runtime.run("example")
            clock.advance(1)
            result = runtime.run("example")
            result["respected_backoff"] = waiting["stop_reason"] == "backoff_wait"
        elif name == "retry_exhausted":
            for _ in range(4):
                clock.advance(2)
                result = runtime.run("example")
        elif name == "repeated_crash_budget":
            clock.advance(11)
            try:
                runtime.run("example")
            except CrashInjected:
                pass
            clock.advance(11)
            result = runtime.run("example")
    # New connection models restart; the old worker's lease must expire.
    if name in {"before_commit_crash", "after_commit_crash", "cancel_after_checkpoint"}:
        with Runtime(database, clock=clock, timer=clock) as restarted:
            if name == "cancel_after_checkpoint":
                result = restarted.cancel("example")
            else:
                busy = restarted.run("example")
                clock.advance(11)
                result = restarted.run("example")
                result["lease_was_respected"] = busy["stop_reason"] == "worker_busy"
    if name in {
        "happy_path",
        "duplicate_run",
        "before_commit_crash",
        "after_commit_crash",
        "transient_failure",
        "timeout_recovery",
    }:
        passed = (
            result["status"] == "completed"
            and result["checkpoint"] == 2
            and result["accounts"].get("demo") == 17
            and result["ledger_count"] == 2
        )
        if name == "before_commit_crash":
            passed = (
                passed
                and initial["checkpoint"] == 0
                and initial["ledger_count"] == 0
                and result["steps"][0]["attempts"] == 2
            )
        if name == "after_commit_crash":
            passed = (
                passed
                and initial["checkpoint"] == 1
                and initial["accounts"].get("demo") == 10
                and result["steps"][0]["attempts"] == 1
            )
        if name in {"transient_failure", "timeout_recovery"}:
            passed = passed and result["respected_backoff"]
        if name in {"before_commit_crash", "after_commit_crash"}:
            passed = passed and result["lease_was_respected"]
    elif name in {"retry_exhausted", "action_deadline", "repeated_crash_budget"}:
        passed = (
            result["status"] == "failed"
            and result["stop_reason"] == "retry_exhausted"
            and result["ledger_count"] == 0
            and result["steps"][0]["attempts"] == limits.max_attempts
        )
    elif name == "tool_denied":
        passed = result["status"] == "failed" and result["stop_reason"] == "tool_denied" and result["ledger_count"] == 0
    elif name == "cancel_before_execution":
        passed = result["status"] == "cancelled" and result["ledger_count"] == 0
    elif name == "cancel_after_checkpoint":
        passed = (
            result["status"] == "cancelled"
            and result["checkpoint"] == 1
            and result["ledger_count"] == 1
            and result["accounts"].get("demo") == 10
        )
    elif name == "run_deadline":
        passed = result["stop_reason"] == "run_deadline" and result["ledger_count"] == 0
    else:
        raise ValueError("unknown evaluation scenario")
    return {"passed": passed, "initial": initial, "final": result}


SCENARIOS = (
    "happy_path",
    "duplicate_run",
    "before_commit_crash",
    "after_commit_crash",
    "transient_failure",
    "timeout_recovery",
    "retry_exhausted",
    "cancel_before_execution",
    "cancel_after_checkpoint",
    "tool_denied",
    "conflicting_submit",
    "invalid_amount",
    "invalid_identifier",
    "empty_plan",
    "oversize_plan",
    "duplicate_action",
    "run_deadline",
    "action_deadline",
    "repeated_crash_budget",
)


def evaluate() -> dict:
    from .review_evaluation import REVIEW_SCENARIOS, reviewed_scenario

    cases = []
    for name in SCENARIOS:
        with tempfile.TemporaryDirectory(prefix="runtime-eval-") as folder:
            result = scenario(name, str(Path(folder) / "state.sqlite"))
        cases.append({"id": name, "provenance": "synthetic local fault injection", **result})
    for name in REVIEW_SCENARIOS:
        with tempfile.TemporaryDirectory(prefix="runtime-review-") as folder:
            result = reviewed_scenario(name, str(Path(folder) / "state.sqlite"))
        cases.append({"id": name, **result})
    return {
        "dataset_version": "runtime-faults-v2",
        "dataset_sha256": hashlib.sha256(
            json.dumps({"version": 2, "cases": (*SCENARIOS, *REVIEW_SCENARIOS)}).encode()
        ).hexdigest(),
        "cases": cases,
        "passed": sum(c["passed"] for c in cases),
        "total": len(cases),
        "scope": "Atomic local SQLite effects only; no external exactly-once guarantee.",
        "usage": {"provider_calls": 0, "tokens": 0, "cost_usd": 0},
    }


def demo(database: str) -> dict:
    return scenario("after_commit_crash", database)
