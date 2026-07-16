"""Independent regressions for failures reproduced during the heavy review."""

import json
import io
from contextlib import redirect_stderr
import math
import sqlite3
from pathlib import Path
import tempfile
import unittest

from resilient_runtime import Action, CrashInjected, LeaseLost, Limits, Runtime, TransientToolError
from resilient_runtime.evaluation import ManualClock, OnceFault
from resilient_runtime.__main__ import main


class ReviewInvariantTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.database = str(Path(self.directory.name) / "state.sqlite")
        self.clock = ManualClock()
        self.options = {"clock": self.clock, "timer": self.clock}

    def tearDown(self):
        self.directory.cleanup()

    def test_action_deadline_after_effect_rolls_back_before_commit(self):
        def late(stage, *_):
            if stage == "before_commit":
                self.clock.advance(2)

        with Runtime(self.database, fault=late, **self.options) as runtime:
            runtime.submit("wf", [Action("a")])
            result = runtime.run("wf")
            self.assertEqual(result["stop_reason"], "tool_timeout")
            self.assertEqual((result["checkpoint"], result["ledger_count"], result["accounts"]), (0, 0, {}))
            self.assertEqual(result["steps"][0]["attempts"], 1)
            self.assertFalse(any(event["kind"] == "action_committed" for event in result["events"]))

    def test_run_deadline_after_effect_is_resumable_without_commit(self):
        def late(stage, *_):
            if stage == "before_commit":
                self.clock.advance(2)

        limits = Limits(action_timeout_seconds=10, run_timeout_seconds=1)
        with Runtime(self.database, fault=late, limits=limits, **self.options) as runtime:
            runtime.submit("wf", [Action("a")])
            result = runtime.run("wf")
            self.assertEqual(result["stop_reason"], "run_deadline")
            self.assertEqual((result["status"], result["ledger_count"], result["checkpoint"]), ("pending", 0, 0))
            self.assertFalse(result["lease_active"])
        with Runtime(self.database, **self.options) as runtime:
            self.assertEqual(runtime.run("wf")["accounts"], {"demo": 1})

    def test_expired_lease_before_commit_rolls_back_effect(self):
        def late(stage, *_):
            if stage == "before_commit":
                self.clock.advance(1)

        limits = Limits(action_timeout_seconds=10, run_timeout_seconds=10, lease_seconds=0.5)
        with Runtime(self.database, fault=late, limits=limits, **self.options) as runtime:
            runtime.submit("wf", [Action("a")])
            result = runtime.run("wf")
            self.assertEqual(result["stop_reason"], "lease_lost")
            self.assertEqual((result["checkpoint"], result["ledger_count"], result["accounts"]), (0, 0, {}))

    def test_cancel_requested_after_effect_rolls_back_uncommitted_credit(self):
        state = {"cancelled": False}

        def request_cancel(stage, *_):
            if stage == "before_commit":
                state["cancelled"] = True

        with Runtime(self.database, fault=request_cancel, **self.options) as runtime:
            runtime.submit("wf", [Action("a"), Action("b")])
            result = runtime.run("wf", cancelled=lambda: state["cancelled"])
            self.assertEqual(result["status"], "cancelled")
            self.assertEqual((result["checkpoint"], result["ledger_count"], result["accounts"]), (0, 0, {}))
            self.assertEqual([step["status"] for step in result["steps"]], ["cancelled", "cancelled"])

    def test_postcommit_observer_error_never_downgrades_committed_step(self):
        with Runtime(
            self.database,
            fault=OnceFault("after_commit", TransientToolError),
            limits=Limits(max_attempts=1),
            **self.options,
        ) as runtime:
            runtime.submit("wf", [Action("a"), Action("b")])
            result = runtime.run("wf")
            self.assertEqual(
                (result["checkpoint"], result["ledger_count"], result["steps"][0]["status"]), (1, 1, "completed")
            )
            self.assertEqual(result["stop_reason"], "post_commit_hook_error")
            resumed = runtime.run("wf")
            self.assertEqual(
                (resumed["status"], resumed["accounts"], resumed["ledger_count"]), ("completed", {"demo": 2}, 2)
            )
            self.assertEqual([step["status"] for step in resumed["steps"]], ["completed", "completed"])

    def test_final_postcommit_error_preserves_completed_workflow(self):
        with Runtime(self.database, fault=OnceFault("after_commit", RuntimeError), **self.options) as runtime:
            runtime.submit("wf", [Action("a")])
            result = runtime.run("wf")
            self.assertEqual((result["status"], result["ledger_count"]), ("completed", 1))
            self.assertEqual(result["stop_reason"], "post_commit_hook_error")
            self.assertEqual(runtime.run("wf")["accounts"], {"demo": 1})

    def test_invalid_backoff_fails_stably_and_releases_worker(self):
        for invalid in (math.nan, math.inf, True, -1, 61, "later"):
            with self.subTest(value=invalid):
                database = str(Path(self.directory.name) / f"backoff-{str(invalid)}.sqlite")
                with Runtime(
                    database,
                    fault=OnceFault("before_action", TransientToolError),
                    backoff=lambda *_: invalid,
                    **self.options,
                ) as runtime:
                    runtime.submit("wf", [Action("a")])
                    result = runtime.run("wf")
                    self.assertEqual((result["status"], result["stop_reason"]), ("failed", "backoff_policy_error"))
                    self.assertFalse(result["lease_active"])
                    self.assertEqual(result["ledger_count"], 0)

    def test_backoff_exception_does_not_leak_raw_message(self):
        def broken(*_):
            raise RuntimeError("private-message")

        with Runtime(
            self.database, fault=OnceFault("before_action", TransientToolError), backoff=broken, **self.options
        ) as runtime:
            runtime.submit("wf", [Action("a")])
            result = runtime.run("wf")
            self.assertEqual(result["stop_reason"], "backoff_policy_error")
            self.assertNotIn("private-message", json.dumps(result))

    def test_arbitrary_value_error_is_not_an_idempotency_conflict(self):
        with Runtime(self.database, fault=OnceFault("before_action", ValueError), **self.options) as runtime:
            runtime.submit("wf", [Action("a")])
            self.assertEqual(runtime.run("wf")["stop_reason"], "tool_error")

    def test_cancellation_callback_failure_releases_lease(self):
        def broken():
            raise RuntimeError("private callback text")

        with Runtime(self.database, **self.options) as runtime:
            runtime.submit("wf", [Action("a")])
            result = runtime.run("wf", cancelled=broken)
            self.assertEqual(result["stop_reason"], "control_error")
            self.assertFalse(result["lease_active"])
            self.assertNotIn("private callback text", json.dumps(result))

    def test_lease_tokens_fence_old_owner_and_cannot_be_resurrected(self):
        with Runtime(self.database, **self.options) as first, Runtime(self.database, **self.options) as second:
            first.submit("wf", [Action("a")])
            original = first.acquire("wf")
            self.assertIsNotNone(original)
            self.assertIsNone(second.acquire("wf"))
            self.clock.advance(5)
            renewed = first.heartbeat(original)
            self.assertGreater(renewed.expires_at, original.expires_at)
            self.clock.advance(11)
            with self.assertRaises(LeaseLost):
                first.heartbeat(original)
            replacement = second.acquire("wf")
            self.assertEqual(replacement.epoch, original.epoch + 1)
            with self.assertRaises(LeaseLost):
                first.heartbeat(original)
            self.assertFalse(first.release(original))
            self.assertEqual(first.run("wf", lease=original)["stop_reason"], "lease_lost")
            self.assertEqual(second.run("wf", lease=replacement)["accounts"], {"demo": 1})

    def test_takeover_after_attempt_reservation_fences_old_worker(self):
        replacement = []

        def takeover(stage, *_):
            if stage == "after_attempt_reserved":
                self.clock.advance(11)
                with Runtime(self.database, **self.options) as competitor:
                    replacement.append(competitor.acquire("wf"))

        with Runtime(self.database, fault=takeover, **self.options) as runtime:
            runtime.submit("wf", [Action("a")])
            result = runtime.run("wf")
            self.assertEqual(result["stop_reason"], "lease_lost")
            self.assertEqual((result["lease_epoch"], result["ledger_count"]), (2, 0))
            self.assertTrue(result["lease_active"])
        with Runtime(self.database, **self.options) as current:
            completed = current.run("wf", lease=replacement[0])
            self.assertEqual(
                (completed["status"], completed["accounts"], completed["steps"][0]["attempts"]),
                ("completed", {"demo": 1}, 2),
            )

    def test_snapshot_is_one_consistent_wal_read_view(self):
        with Runtime(self.database, **self.options) as reader, Runtime(self.database, **self.options) as writer:
            reader.submit("wf", [Action("a")])
            committed = []

            def interleave(statement):
                if statement.startswith("SELECT position") and not committed:
                    committed.append(writer.run("wf"))

            reader.db.set_trace_callback(interleave)
            snapshot = reader.snapshot("wf")
            reader.db.set_trace_callback(None)
            self.assertEqual(committed[0]["status"], "completed")
            self.assertEqual((snapshot["checkpoint"], snapshot["ledger_count"], snapshot["accounts"]), (0, 0, {}))
            self.assertEqual(reader.snapshot("wf")["ledger_count"], 1)

    def test_mutating_original_allowlist_does_not_grant_permission(self):
        allowed = set()
        with Runtime(self.database, allowed_tools=allowed, **self.options) as runtime:
            allowed.add("credit_demo_account")
            runtime.submit("wf", [Action("a")])
            self.assertEqual(runtime.run("wf")["stop_reason"], "tool_denied")

    def test_time_limits_reject_nonfinite_boolean_and_nonnumeric_values(self):
        for key in ("action_timeout_seconds", "run_timeout_seconds", "base_backoff_seconds", "lease_seconds"):
            for value in (math.nan, math.inf, True, "1"):
                with self.subTest(key=key, value=value), self.assertRaises(ValueError):
                    Limits(**{key: value})

    def test_control_apis_validate_workflow_identifiers_and_plan_types(self):
        with Runtime(self.database, **self.options) as runtime:
            for invalid in (None, 1, "", "bad:id"):
                for method in (runtime.snapshot, runtime.acquire, runtime.cancel, runtime.run):
                    with self.subTest(method=method.__name__, invalid=invalid), self.assertRaises(ValueError):
                        method(invalid)
            for invalid in (None, "actions", {"a": Action("a")}):
                with self.assertRaises(ValueError):
                    runtime.submit("wf", invalid)

    def test_crash_after_reservation_consumes_attempt_without_effect(self):
        with Runtime(self.database, fault=OnceFault("after_attempt_reserved"), **self.options) as runtime:
            runtime.submit("wf", [Action("a")])
            with self.assertRaises(CrashInjected):
                runtime.run("wf")
            before = runtime.snapshot("wf")
            self.assertEqual((before["steps"][0]["attempts"], before["ledger_count"]), (1, 0))
        self.clock.advance(11)
        with Runtime(self.database, **self.options) as restarted:
            result = restarted.run("wf")
            self.assertEqual(
                (result["status"], result["steps"][0]["attempts"], result["accounts"]), ("completed", 2, {"demo": 1})
            )

    def test_new_worker_cannot_increase_persisted_retry_ceiling(self):
        with Runtime(
            self.database, limits=Limits(max_attempts=1), fault=OnceFault("before_commit"), **self.options
        ) as runtime:
            runtime.submit("wf", [Action("a")])
            with self.assertRaises(CrashInjected):
                runtime.run("wf")
        self.clock.advance(11)
        with Runtime(self.database, limits=Limits(max_attempts=10), **self.options) as restarted:
            result = restarted.run("wf")
            self.assertEqual(
                (result["status"], result["stop_reason"], result["max_attempts"]), ("failed", "retry_exhausted", 1)
            )
            self.assertEqual((result["steps"][0]["attempts"], result["ledger_count"]), (1, 0))

    def test_independent_audit_detects_corrupted_ledger_and_cursor(self):
        with Runtime(self.database, **self.options) as runtime:
            runtime.submit("wf", [Action("a")])
            self.assertTrue(runtime.audit("wf")["valid"])
            runtime.run("wf")
            self.assertTrue(runtime.audit("wf")["valid"])
            runtime.db.execute("UPDATE ledger SET action_hash='bad' WHERE workflow_id='wf'")
            self.assertIn("ledger_fingerprint:0", runtime.audit("wf")["violations"])
            runtime.db.execute("UPDATE workflows SET checkpoint=0 WHERE workflow_id='wf'")
            violations = runtime.audit("wf")["violations"]
            self.assertIn("ledger_checkpoint_count", violations)
            self.assertIn("completed_checkpoint", violations)

    def test_snapshot_bounds_trace_exports_without_deleting_history(self):
        with Runtime(self.database, **self.options) as runtime:
            runtime.submit("wf", [Action("a")])
            with runtime.transaction():
                for _ in range(1005):
                    runtime._event("wf", "synthetic_heartbeat")
            result = runtime.snapshot("wf")
            self.assertEqual(result["event_count"], 1006)
            self.assertEqual(len(result["events"]), 1000)
            self.assertTrue(result["trace_truncated"])

    def test_commit_failure_rolls_back_and_connection_remains_usable(self):
        class FailingCommit:
            def __init__(self, connection):
                self.connection = connection
                self.rolled_back = False

            def __getattr__(self, name):
                return getattr(self.connection, name)

            def commit(self):
                raise sqlite3.OperationalError("simulated disk failure")

            def rollback(self):
                self.rolled_back = True
                self.connection.rollback()

        with Runtime(self.database, **self.options) as runtime:
            original = runtime.db
            proxy = FailingCommit(original)
            runtime.db = proxy
            with self.assertRaises(sqlite3.OperationalError):
                with runtime.transaction():
                    runtime.db.execute("INSERT INTO accounts VALUES('demo',99)")
            runtime.db = original
            self.assertTrue(proxy.rolled_back)
            self.assertFalse(original.in_transaction)
            self.assertEqual(original.execute("SELECT COUNT(*) FROM accounts").fetchone()[0], 0)
            runtime.submit("wf", [Action("a")])
            self.assertEqual(runtime.run("wf")["accounts"], {"demo": 1})

    def test_malformed_ledger_result_is_an_audit_violation(self):
        with Runtime(self.database, **self.options) as runtime:
            runtime.submit("wf", [Action("a")])
            runtime.run("wf")
            runtime.db.execute("UPDATE ledger SET result_json='invalid-json' WHERE workflow_id='wf'")
            self.assertIn("ledger_result:0", runtime.audit("wf")["violations"])

    def test_cli_output_failure_has_stable_json_error(self):
        error = io.StringIO()
        with redirect_stderr(error):
            code = main(["demo", "--output", self.directory.name])
        self.assertEqual(code, 2)
        payload = json.loads(error.getvalue())
        self.assertEqual(payload["error"], "runtime_experiment_failed")
        self.assertEqual(payload["error_type"], "IsADirectoryError")


if __name__ == "__main__":
    unittest.main()
