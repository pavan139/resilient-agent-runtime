"""Two-database transactional invariants and deliberately uncertain ACK outcomes."""

from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from pathlib import Path
import tempfile
import threading
import unittest

from resilient_runtime import Action, CrashInjected, Runtime
from resilient_runtime.evaluation import ManualClock, OnceFault
from resilient_runtime.outbox import (
    AdmissionFull,
    DeliveryLimits,
    Dispatcher,
    Envelope,
    OutboxRuntime,
    PayloadConflict,
    Receipt,
    Receiver,
)
from resilient_runtime.outbox_experiments import evaluate as evaluate_outbox


class OutboxTests(unittest.TestCase):
    def setUp(self):
        self.folder = tempfile.TemporaryDirectory()
        self.sender = Path(self.folder.name) / "workflow.sqlite"
        self.receiver = Path(self.folder.name) / "receiver.sqlite"
        self.clock = ManualClock()
        self.options = {"clock": self.clock, "timer": self.clock}

    def tearDown(self):
        self.folder.cleanup()

    def enqueue(self, *, capacity=128, delivery_attempts=5):
        with OutboxRuntime(
            self.sender, capacity=capacity, delivery_attempts=delivery_attempts, **self.options
        ) as workflow:
            workflow.submit("wf", [Action("one", tool="enqueue_credit", amount=7)])
            workflow.run("wf")

    def test_workflow_commit_contains_outbox_intent_not_receiver_effect(self):
        with OutboxRuntime(self.sender, **self.options) as workflow:
            workflow.submit("wf", [Action("one", tool="enqueue_credit", amount=7)])
            self.assertEqual(workflow.deliveries()["reserved"], 1)
            result = workflow.run("wf")
            self.assertEqual((result["checkpoint"], result["ledger_count"], result["accounts"]), (1, 1, {}))
            self.assertEqual(workflow.deliveries()["states"], {"pending": 1})
            self.assertTrue(workflow.audit("wf")["valid"])
        with Receiver(self.receiver) as receiver, Dispatcher(self.sender, receiver, **self.options) as dispatcher:
            self.assertEqual(receiver.snapshot()["inbox_count"], 0)
            self.assertEqual(dispatcher.dispatch_one()["status"], "delivered")
            self.assertEqual(receiver.snapshot(), {"inbox_count": 1, "accounts": {"demo": 7}})

    def test_workflow_precommit_crash_rolls_back_outbox_and_checkpoint_together(self):
        with OutboxRuntime(self.sender, fault=OnceFault("before_commit"), **self.options) as workflow:
            workflow.submit("wf", [Action("one", tool="enqueue_credit", amount=7)])
            with self.assertRaises(CrashInjected):
                workflow.run("wf")
            self.assertEqual(workflow.deliveries()["message_count"], 0)
            self.assertEqual(workflow.deliveries()["reserved"], 1)
            self.assertEqual(workflow.snapshot("wf")["ledger_count"], 0)
        self.clock.advance(11)
        with OutboxRuntime(self.sender, **self.options) as restarted:
            self.assertEqual(restarted.run("wf")["checkpoint"], 1)
            self.assertEqual(restarted.deliveries()["message_count"], 1)

    def test_receiver_commit_then_sender_crash_is_duplicate_safe_after_restart(self):
        self.enqueue()

        def crash(stage, *_):
            if stage == "after_receiver_commit":
                raise CrashInjected()

        with (
            Receiver(self.receiver) as receiver,
            Dispatcher(self.sender, receiver, fault=crash, **self.options) as dispatcher,
        ):
            with self.assertRaises(CrashInjected):
                dispatcher.dispatch_one()
            self.assertEqual(receiver.snapshot()["accounts"], {"demo": 7})
        self.clock.advance(11)
        with Receiver(self.receiver) as receiver, Dispatcher(self.sender, receiver, **self.options) as restarted:
            result = restarted.dispatch_one()
            self.assertEqual(
                (result["status"], result["attempt"], result["receiver_duplicate"]), ("delivered", 2, True)
            )
            self.assertEqual(receiver.snapshot(), {"inbox_count": 1, "accounts": {"demo": 7}})

    def test_receiver_rollback_does_not_leave_an_inbox_without_effect(self):
        self.enqueue()

        def crash(stage, *_):
            if stage == "before_receiver_commit":
                raise CrashInjected()

        with (
            Receiver(self.receiver, fault=crash) as receiver,
            Dispatcher(self.sender, receiver, **self.options) as dispatcher,
        ):
            with self.assertRaises(CrashInjected):
                dispatcher.dispatch_one()
            self.assertEqual(receiver.snapshot(), {"inbox_count": 0, "accounts": {}})
        self.clock.advance(11)
        with Receiver(self.receiver) as receiver, Dispatcher(self.sender, receiver, **self.options) as restarted:
            self.assertTrue(restarted.dispatch_one()["delivered"])
            self.assertEqual(receiver.snapshot()["accounts"], {"demo": 7})

    def test_outage_backoff_survives_connection_restart(self):
        self.enqueue()
        with (
            Receiver(self.receiver, available=False) as receiver,
            Dispatcher(self.sender, receiver, **self.options) as dispatcher,
        ):
            self.assertEqual(dispatcher.dispatch_one()["status"], "pending")
        with Receiver(self.receiver) as receiver, Dispatcher(self.sender, receiver, **self.options) as restarted:
            self.assertEqual(restarted.dispatch_one()["status"], "idle")
            self.clock.advance(1)
            self.assertEqual(restarted.dispatch_one()["attempt"], 2)
            self.assertEqual(receiver.snapshot()["accounts"], {"demo": 7})

    def test_concurrent_receiver_duplicates_apply_once(self):
        envelope = Envelope("a" * 32, "wf", "one", "demo", 7)
        with Receiver(self.receiver):
            pass
        barrier = threading.Barrier(8)

        def deliver(_):
            with Receiver(self.receiver) as receiver:
                barrier.wait(timeout=5)
                return receiver.receive(envelope).duplicate

        with ThreadPoolExecutor(max_workers=8) as pool:
            duplicates = list(pool.map(deliver, range(8)))
        self.assertEqual(sum(duplicates), 7)
        with Receiver(self.receiver) as receiver:
            self.assertEqual(receiver.snapshot(), {"inbox_count": 1, "accounts": {"demo": 7}})

    def test_closed_receiver_connection_recovers_through_a_new_endpoint(self):
        self.enqueue()
        stopped = Receiver(self.receiver)
        stopped.close()
        with Dispatcher(self.sender, stopped, **self.options) as dispatcher:
            result = dispatcher.dispatch_one()
            self.assertEqual((result["status"], result["error"]), ("pending", "receiver_unavailable"))
        self.clock.advance(1)
        with Receiver(self.receiver) as restarted, Dispatcher(self.sender, restarted, **self.options) as dispatcher:
            self.assertTrue(dispatcher.dispatch_one()["delivered"])
            self.assertEqual(restarted.snapshot()["accounts"], {"demo": 7})

    def test_two_database_failure_experiments_include_real_process_death(self):
        report = evaluate_outbox()
        self.assertEqual((report["passed"], report["total"]), (12, 12))
        cases = {case["id"]: case for case in report["cases"]}
        self.assertEqual(cases["real_process_death_ack_gap"]["process_exit"], 29)
        self.assertTrue(cases["uncertain_last_ack"]["uncertain_sender_outcome"])

    def test_same_message_id_different_payload_is_rejected(self):
        envelope = Envelope("b" * 32, "wf", "one", "demo", 7)
        with Receiver(self.receiver) as receiver:
            receiver.receive(envelope)
            with self.assertRaises(PayloadConflict):
                receiver.receive(replace(envelope, amount=9))
            self.assertEqual(receiver.snapshot()["accounts"], {"demo": 7})

    def test_stale_dispatcher_cannot_ack_or_rewrite_replacement(self):
        self.enqueue()
        with (
            Receiver(self.receiver) as receiver,
            Dispatcher(self.sender, receiver, **self.options) as old,
            Dispatcher(self.sender, receiver, **self.options) as current,
        ):
            stale = old.claim()
            self.clock.advance(11)
            replacement = current.claim()
            receipt = receiver.receive(stale.message)
            self.assertFalse(old.acknowledge(stale, receipt))
            self.assertEqual(replacement.epoch, stale.epoch + 1)
            self.assertTrue(current.acknowledge(replacement, receiver.receive(replacement.message)))
            self.assertEqual(receiver.snapshot()["accounts"], {"demo": 7})

    def test_overlapping_dispatchers_deliver_duplicate_but_only_current_owner_acks(self):
        self.enqueue()
        entered, replacement_finished = threading.Event(), threading.Event()

        def old_worker():
            def paused(stage, *_):
                if stage == "after_send_gate":
                    entered.set()
                    if not replacement_finished.wait(timeout=5):
                        raise RuntimeError("replacement did not finish")

            with (
                Receiver(self.receiver) as receiver,
                Dispatcher(
                    self.sender, receiver, limits=DeliveryLimits(call_timeout_seconds=60), fault=paused, **self.options
                ) as dispatcher,
            ):
                return dispatcher.dispatch_one()

        with ThreadPoolExecutor(max_workers=1) as pool:
            stale = pool.submit(old_worker)
            self.assertTrue(entered.wait(timeout=5))
            self.clock.advance(11)
            try:
                with Receiver(self.receiver) as receiver, Dispatcher(self.sender, receiver, **self.options) as current:
                    self.assertTrue(current.dispatch_one()["delivered"])
            finally:
                replacement_finished.set()
            result = stale.result(timeout=5)
        self.assertEqual((result["status"], result["receiver_duplicate"]), ("lease_lost", True))
        with Receiver(self.receiver) as receiver:
            self.assertEqual(receiver.snapshot(), {"inbox_count": 1, "accounts": {"demo": 7}})

    def test_no_sender_transaction_is_held_while_receiver_runs(self):
        self.enqueue()
        observed = []
        with Receiver(self.receiver) as receiver, Dispatcher(self.sender, receiver, **self.options) as dispatcher:
            receiver.fault = lambda *_: observed.append(dispatcher.db.in_transaction)
            self.assertTrue(dispatcher.dispatch_one()["delivered"])
        self.assertEqual(observed, [False])

    def test_admission_counts_pending_reservations_and_queued_messages(self):
        with OutboxRuntime(self.sender, capacity=1, **self.options) as workflow:
            plan = [Action("one", tool="enqueue_credit")]
            workflow.submit("first", plan)
            with self.assertRaises(AdmissionFull):
                workflow.submit("second", plan)
            workflow.submit("first", plan)  # An idempotent submit does not consume another slot.
            workflow.cancel("first")
            workflow.submit("second", plan)
            workflow.run("second")
            with self.assertRaises(AdmissionFull):
                workflow.submit("third", plan)
            with Receiver(self.receiver) as receiver, Dispatcher(self.sender, receiver, **self.options) as dispatcher:
                dispatcher.dispatch_one()
            workflow.submit("third", plan)
            self.assertEqual(workflow.deliveries()["reserved"], 1)

    def test_concurrent_admission_cannot_oversubscribe(self):
        with OutboxRuntime(self.sender, capacity=1):
            pass
        barrier = threading.Barrier(2)

        def admit(index):
            with OutboxRuntime(self.sender, capacity=1) as workflow:
                barrier.wait(timeout=5)
                try:
                    workflow.submit(f"wf{index}", [Action("one", tool="enqueue_credit")])
                except AdmissionFull:
                    return False
                return True

        with ThreadPoolExecutor(max_workers=2) as pool:
            self.assertEqual(sum(pool.map(admit, range(2))), 1)

    def test_last_attempt_ack_loss_remains_explicitly_uncertain(self):
        self.enqueue(delivery_attempts=1)

        def crash(stage, *_):
            if stage == "after_receiver_commit":
                raise CrashInjected()

        with (
            Receiver(self.receiver) as receiver,
            Dispatcher(self.sender, receiver, fault=crash, **self.options) as dispatcher,
        ):
            with self.assertRaises(CrashInjected):
                dispatcher.dispatch_one()
            self.clock.advance(11)
            self.assertEqual(dispatcher.dispatch_one()["status"], "idle")
            status = dispatcher.db.execute("SELECT status,last_error FROM outbox").fetchone()
            self.assertEqual(tuple(status), ("dead_letter", "delivery_attempts_exhausted"))
            self.assertEqual(receiver.snapshot()["accounts"], {"demo": 7})

    def test_payload_bound_receipt_cannot_ack_another_message(self):
        self.enqueue()
        with Receiver(self.receiver) as receiver, Dispatcher(self.sender, receiver, **self.options) as dispatcher:
            token = dispatcher.claim()
            with self.assertRaises(PayloadConflict):
                dispatcher.acknowledge(token, Receipt("forged", token.message.fingerprint, 7, False))
            self.assertEqual(dispatcher.db.execute("SELECT status FROM outbox").fetchone()[0], "inflight")

    def test_sender_namespaces_avoid_cross_database_workflow_collisions(self):
        self.enqueue()
        second = Path(self.folder.name) / "second-sender.sqlite"
        with OutboxRuntime(second, **self.options) as workflow:
            workflow.submit("wf", [Action("one", tool="enqueue_credit", amount=7)])
            workflow.run("wf")
        with Receiver(self.receiver) as receiver:
            for source in (self.sender, second):
                with Dispatcher(source, receiver, **self.options) as dispatcher:
                    dispatcher.dispatch_one()
            self.assertEqual(receiver.snapshot(), {"inbox_count": 2, "accounts": {"demo": 14}})

    def test_local_runtime_cannot_silently_grant_enqueue_tool(self):
        with self.assertRaises(ValueError):
            Runtime(str(self.sender), allowed_tools=frozenset({"enqueue_credit"}))

    def test_two_independent_database_files_are_required(self):
        self.enqueue()
        with Receiver(self.sender) as receiver:
            with self.assertRaisesRegex(ValueError, "two independently"):
                Dispatcher(self.sender, receiver)
        for path in (":memory:", ""):
            with self.assertRaises(ValueError):
                Receiver(path)

    def test_persisted_admission_and_retry_policies_cannot_be_widened(self):
        self.enqueue(capacity=1, delivery_attempts=2)
        with self.assertRaisesRegex(ValueError, "policy mismatch"):
            OutboxRuntime(self.sender, capacity=2, delivery_attempts=2)
        with self.assertRaisesRegex(ValueError, "policy mismatch"):
            OutboxRuntime(self.sender, capacity=1, delivery_attempts=3)

    def test_corrupted_outbox_binding_fails_audit_and_never_reaches_receiver(self):
        self.enqueue()
        with OutboxRuntime(self.sender, **self.options) as workflow:
            workflow.db.execute("UPDATE outbox SET payload_hash='corrupted'")
            self.assertIn("outbox_binding:0", workflow.audit("wf")["violations"])
        with Receiver(self.receiver) as receiver, Dispatcher(self.sender, receiver, **self.options) as dispatcher:
            self.assertEqual(dispatcher.dispatch_one()["status"], "idle")
            self.assertEqual(
                dispatcher.db.execute("SELECT last_error FROM outbox").fetchone()[0], "invalid_stored_payload"
            )
            self.assertEqual(receiver.snapshot()["inbox_count"], 0)

    def test_protocol_and_admission_inputs_are_strictly_bounded(self):
        for override in ({"capacity": True}, {"capacity": 0}, {"delivery_attempts": 11}):
            with self.assertRaises(ValueError):
                OutboxRuntime(self.sender, **override)
        for override in (
            {"max_batch": 0},
            {"max_batch": True},
            {"lease_seconds": float("nan")},
            {"call_timeout_seconds": True},
        ):
            with self.assertRaises(ValueError):
                DeliveryLimits(**override)
        for payload in ("{}", "[]", "x" * 2049):
            with self.assertRaises(ValueError):
                Envelope.parse(payload)
        with self.assertRaises(ValueError):
            Envelope("a" * 32, "wf", "a", "demo", True)

    def test_cooperative_timeout_after_receiver_commit_retries_safely(self):
        self.enqueue()

        def late(stage, *_):
            if stage == "after_receiver_commit":
                self.clock.advance(2)

        with (
            Receiver(self.receiver) as receiver,
            Dispatcher(
                self.sender, receiver, limits=DeliveryLimits(call_timeout_seconds=1), fault=late, **self.options
            ) as dispatcher,
        ):
            result = dispatcher.dispatch_one()
            self.assertEqual(result["error"], "delivery_timeout")
            self.assertEqual(receiver.snapshot()["accounts"], {"demo": 7})
        self.clock.advance(1)
        with Receiver(self.receiver) as receiver, Dispatcher(self.sender, receiver, **self.options) as restarted:
            self.assertTrue(restarted.dispatch_one()["receiver_duplicate"])
            self.assertEqual(receiver.snapshot()["inbox_count"], 1)


if __name__ == "__main__":
    unittest.main()
