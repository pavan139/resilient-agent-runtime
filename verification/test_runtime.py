import concurrent.futures
import json
from pathlib import Path
import tempfile
import threading
import unittest
from resilient_runtime import Action, CrashInjected, Limits, Runtime, ToolTimeout, TransientToolError
from resilient_runtime.evaluation import ManualClock, OnceFault, evaluate


class RuntimeTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.database = str(Path(self.directory.name) / "state.sqlite")
        self.clock = ManualClock()
        self.options = {"clock": self.clock, "timer": self.clock}
        self.actions = [Action("a", amount=10), Action("b", amount=7)]

    def tearDown(self):
        self.directory.cleanup()

    def test_effect_ledger_and_checkpoint_commit_together(self):
        with Runtime(self.database, **self.options) as runtime:
            runtime.submit("wf", self.actions)
            first = runtime.run("wf")
            second = runtime.run("wf")
            self.assertEqual(first["status"], "completed")
            self.assertEqual(first["checkpoint"], 2)
            self.assertEqual(second["accounts"], {"demo": 17})
            self.assertEqual(second["ledger_count"], 2)
            self.assertEqual([s["attempts"] for s in second["steps"]], [1, 1])

    def test_precommit_crash_rolls_back_effect_and_checkpoint_but_consumes_attempt(self):
        with Runtime(self.database, fault=OnceFault("before_commit"), **self.options) as runtime:
            runtime.submit("wf", self.actions)
            with self.assertRaises(CrashInjected):
                runtime.run("wf")
            crashed = runtime.snapshot("wf")
            self.assertEqual(crashed["accounts"], {})
            self.assertEqual(crashed["checkpoint"], 0)
            self.assertEqual(crashed["ledger_count"], 0)
            self.assertEqual(crashed["steps"][0]["attempts"], 1)
        self.clock.advance(11)
        with Runtime(self.database, **self.options) as resumed:
            result = resumed.run("wf")
            self.assertEqual(result["accounts"], {"demo": 17})
            self.assertEqual(result["steps"][0]["attempts"], 2)

    def test_postcommit_crash_resumes_after_committed_action(self):
        with Runtime(self.database, fault=OnceFault("after_commit"), **self.options) as runtime:
            runtime.submit("wf", self.actions)
            with self.assertRaises(CrashInjected):
                runtime.run("wf")
            crashed = runtime.snapshot("wf")
            self.assertEqual(crashed["accounts"], {"demo": 10})
            self.assertEqual(crashed["checkpoint"], 1)
        with Runtime(self.database, **self.options) as resumed:
            self.assertEqual(resumed.run("wf")["stop_reason"], "worker_busy")
            self.clock.advance(11)
            result = resumed.run("wf")
            self.assertEqual(result["accounts"], {"demo": 17})
            self.assertEqual(result["ledger_count"], 2)
            self.assertEqual(result["steps"][0]["attempts"], 1)

    def test_conflicting_submission_does_not_replace_plan(self):
        with Runtime(self.database, **self.options) as runtime:
            runtime.submit("wf", self.actions)
            runtime.submit("wf", self.actions)
            with self.assertRaisesRegex(ValueError, "workflow_id_conflict"):
                runtime.submit("wf", [Action("other")])
            result = runtime.run("wf")
            self.assertEqual(result["accounts"], {"demo": 17})

    def test_cancel_prevents_remaining_effects(self):
        with Runtime(self.database, fault=OnceFault("after_commit"), **self.options) as runtime:
            runtime.submit("wf", self.actions)
            with self.assertRaises(CrashInjected):
                runtime.run("wf")
            result = runtime.cancel("wf")
            self.assertEqual(result["status"], "cancelled")
            self.assertEqual(runtime.run("wf")["accounts"], {"demo": 10})
            self.assertEqual(result["ledger_count"], 1)

    def test_cancel_callback_is_checked_before_any_effect(self):
        with Runtime(self.database, **self.options) as runtime:
            runtime.submit("wf", self.actions)
            result = runtime.run("wf", cancelled=lambda: True)
            self.assertEqual(result["status"], "cancelled")
            self.assertEqual(result["accounts"], {})

    def test_backoff_is_persisted_and_retries_are_bounded(self):
        def failing(stage, action, attempt):
            if stage == "before_action":
                raise TransientToolError("secret must not be logged")

        with Runtime(self.database, fault=failing, **self.options) as runtime:
            runtime.submit("wf", self.actions)
            first = runtime.run("wf")
            self.assertEqual(first["status"], "waiting")
            self.assertAlmostEqual(first["next_run_at"] - self.clock(), 0.1, places=5)
            self.assertEqual(runtime.run("wf")["stop_reason"], "backoff_wait")
            self.assertEqual(runtime.snapshot("wf")["steps"][0]["attempts"], 1)
            self.clock.advance(1)
            second = runtime.run("wf")
            self.assertAlmostEqual(second["next_run_at"] - self.clock(), 0.2, places=5)
            self.clock.advance(1)
            final = runtime.run("wf")
            self.assertEqual(final["status"], "failed")
            self.assertEqual(final["stop_reason"], "retry_exhausted")
            self.assertEqual(final["steps"][0]["attempts"], 3)
            self.assertEqual(final["ledger_count"], 0)
            self.assertNotIn("secret must", json.dumps(final))

    def test_timeout_is_classified_without_effect(self):
        with Runtime(self.database, fault=OnceFault("before_action", ToolTimeout), **self.options) as runtime:
            runtime.submit("wf", self.actions)
            result = runtime.run("wf")
            self.assertEqual(result["stop_reason"], "tool_timeout")
            self.assertEqual(result["accounts"], {})

    def test_elapsed_action_time_is_checked_before_effect(self):
        def slow(stage, action, attempt):
            if stage == "before_action":
                self.clock.advance(2)

        with Runtime(self.database, fault=slow, **self.options) as runtime:
            runtime.submit("wf", self.actions)
            result = runtime.run("wf")
            self.assertEqual(result["stop_reason"], "tool_timeout")
            self.assertEqual(result["ledger_count"], 0)

    def test_run_deadline_releases_lease_and_can_resume(self):
        with Runtime(self.database, limits=Limits(run_timeout_seconds=0), **self.options) as runtime:
            runtime.submit("wf", self.actions)
            self.assertEqual(runtime.run("wf")["stop_reason"], "run_deadline")
        with Runtime(self.database, **self.options) as runtime:
            self.assertEqual(runtime.run("wf")["status"], "completed")

    def test_denied_tool_cannot_make_effect_or_consume_attempt(self):
        with Runtime(self.database, allowed_tools=frozenset(), **self.options) as runtime:
            runtime.submit("wf", self.actions)
            result = runtime.run("wf")
            self.assertEqual(result["stop_reason"], "tool_denied")
            self.assertEqual(result["steps"][0]["attempts"], 0)
            self.assertEqual(result["accounts"], {})
        with self.assertRaises(ValueError):
            Runtime(self.database, allowed_tools=frozenset({"shell"}))

    def test_stale_checkpoint_replay_uses_ledger(self):
        with Runtime(self.database, **self.options) as runtime:
            runtime.submit("wf", self.actions)
            runtime.run("wf")
            # Deliberate stale-checkpoint fault: ledger must still prevent another effect.
            runtime.db.execute("UPDATE workflows SET checkpoint=0,status='pending' WHERE workflow_id='wf'")
            result = runtime.run("wf")
            self.assertEqual(result["accounts"], {"demo": 17})
            self.assertEqual(result["ledger_count"], 2)
            self.assertEqual(sum(e["kind"] == "duplicate_suppressed" for e in result["events"]), 2)

    def test_ledger_payload_conflict_fails_without_mutation(self):
        with Runtime(self.database, **self.options) as runtime:
            runtime.submit("wf", self.actions)
            runtime.run("wf")
            runtime.db.execute("UPDATE workflows SET checkpoint=0,status='pending' WHERE workflow_id='wf'")
            runtime.db.execute("UPDATE ledger SET action_hash='corrupted' WHERE idempotency_key='wf:a'")
            result = runtime.run("wf")
            self.assertEqual(result["stop_reason"], "idempotency_conflict")
            self.assertEqual(result["accounts"], {"demo": 17})

    def test_repeated_precommit_crashes_cannot_bypass_attempt_budget(self):
        def crash(stage, action, attempt):
            if stage == "before_commit":
                raise CrashInjected()

        with Runtime(self.database, fault=crash, limits=Limits(max_attempts=2), **self.options) as runtime:
            runtime.submit("wf", self.actions)
            for _ in range(2):
                with self.assertRaises(CrashInjected):
                    runtime.run("wf")
                self.clock.advance(11)
            result = runtime.run("wf")
            self.assertEqual(result["stop_reason"], "retry_exhausted")
            self.assertEqual(result["accounts"], {})

    def test_unexpected_tool_error_has_stable_code_and_no_raw_message(self):
        with Runtime(self.database, fault=OnceFault("before_action", RuntimeError), **self.options) as runtime:
            runtime.submit("wf", self.actions)
            result = runtime.run("wf")
            self.assertEqual(result["stop_reason"], "tool_error")
            self.assertNotIn("injected test failure", json.dumps(result))

    def test_workers_do_not_duplicate_effects(self):
        with Runtime(self.database) as runtime:
            runtime.submit("wf", self.actions)
        barrier = threading.Barrier(3)

        def work():
            with Runtime(self.database) as runtime:
                barrier.wait(timeout=3)
                return runtime.run("wf")

        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as pool:
            results = list(pool.map(lambda _: work(), range(3)))
        self.assertTrue(any(r["status"] == "completed" for r in results))
        with Runtime(self.database) as runtime:
            result = runtime.snapshot("wf")
            self.assertEqual(result["accounts"], {"demo": 17})
            self.assertEqual(result["ledger_count"], 2)

    def test_malformed_plans_and_parameters_are_rejected(self):
        for amount in (0, -1, True, 1_000_001):
            with self.subTest(amount=amount), self.assertRaises(ValueError):
                Action("x", amount=amount)
        for name in ("", "shell; echo", "x" * 65):
            with self.subTest(name=name), self.assertRaises(ValueError):
                Action(name)
        with Runtime(self.database, **self.options) as runtime:
            for plan in ([], [Action("same"), Action("same")], [Action(str(i)) for i in range(33)]):
                with self.subTest(plan=plan), self.assertRaises(ValueError):
                    runtime.submit("wf", plan)
        for options in ({"max_attempts": 0}, {"max_actions": 129}, {"lease_seconds": 0}, {"run_timeout_seconds": -1}):
            with self.subTest(options=options), self.assertRaises(ValueError):
                Limits(**options)

    def test_fault_evaluation(self):
        report = evaluate()
        self.assertGreaterEqual(report["total"], 12)
        self.assertEqual(report["passed"], report["total"])


if __name__ == "__main__":
    unittest.main()
