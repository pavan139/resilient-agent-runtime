"""Executable two-database failure experiments; no network or model dependencies."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import threading
import time

from .evaluation import ManualClock, OnceFault
from .runtime import Action, CrashInjected
from .outbox import AdmissionFull, Dispatcher, Envelope, OutboxRuntime, PayloadConflict, Receiver

SCENARIOS = (
    "atomic_sender_intent",
    "receiver_rollback",
    "ack_gap_restart",
    "outage_restart",
    "concurrent_duplicate_delivery",
    "payload_conflict",
    "stale_ack_fenced",
    "admission_race",
    "bounded_retry_exhaustion",
    "uncertain_last_ack",
    "real_process_death_ack_gap",
    "independent_sender_namespaces",
)


def enqueue(sender: Path, clock: ManualClock, *, attempts: int = 5, fault=lambda *_: None):
    with OutboxRuntime(sender, delivery_attempts=attempts, clock=clock, timer=clock, fault=fault) as workflow:
        workflow.submit("wf", [Action("one", tool="enqueue_credit", amount=7)])
        workflow.run("wf")


def ack_gap(sender: Path, target: Path, clock: ManualClock, *, attempts: int = 5) -> dict:
    enqueue(sender, clock, attempts=attempts)
    options = {"clock": clock, "timer": clock}

    def crash(stage, *_):
        if stage == "after_receiver_commit":
            raise CrashInjected("lost sender ACK")

    with Receiver(target) as receiver, Dispatcher(sender, receiver, fault=crash, **options) as dispatcher:
        try:
            dispatcher.dispatch_one()
        except CrashInjected:
            pass
        initial_receiver = receiver.snapshot()
    with OutboxRuntime(sender, delivery_attempts=attempts, **options) as workflow:
        initial_sender = workflow.deliveries()
    clock.advance(11)
    with Receiver(target) as receiver, Dispatcher(sender, receiver, **options) as dispatcher:
        retry = dispatcher.dispatch_one()
        final_receiver = receiver.snapshot()
    with OutboxRuntime(sender, delivery_attempts=attempts, **options) as workflow:
        final_sender = workflow.deliveries()
        audit = workflow.audit("wf")
    if attempts == 1:
        passed = initial_receiver == final_receiver == {"inbox_count": 1, "accounts": {"demo": 7}} and final_sender[
            "states"
        ] == {"dead_letter": 1}
    else:
        passed = (
            initial_sender["states"] == {"inflight": 1}
            and final_sender["states"] == {"delivered": 1}
            and retry.get("receiver_duplicate") is True
            and final_receiver == {"inbox_count": 1, "accounts": {"demo": 7}}
        )
    return {
        "passed": passed and audit["valid"],
        "initial_sender": initial_sender,
        "initial_receiver": initial_receiver,
        "retry": retry,
        "final_sender": final_sender,
        "final_receiver": final_receiver,
        "sender_audit": audit,
        "uncertain_sender_outcome": attempts == 1,
    }


def run_case(name: str, folder: Path) -> dict:
    sender, target = folder / "workflow.sqlite", folder / "receiver.sqlite"
    clock = ManualClock()
    options = {"clock": clock, "timer": clock}
    if name in {"ack_gap_restart", "uncertain_last_ack"}:
        return ack_gap(sender, target, clock, attempts=1 if name == "uncertain_last_ack" else 5)
    if name == "atomic_sender_intent":
        try:
            enqueue(sender, clock, fault=OnceFault("before_commit"))
        except CrashInjected:
            pass
        with OutboxRuntime(sender, **options) as workflow:
            before = {"workflow": workflow.snapshot("wf"), "outbox": workflow.deliveries()}
            clock.advance(11)
            after = workflow.run("wf")
            queued = workflow.deliveries()
            audit = workflow.audit("wf")
        return {
            "passed": before["workflow"]["checkpoint"]
            == before["workflow"]["ledger_count"]
            == before["outbox"]["message_count"]
            == 0
            and after["checkpoint"] == after["ledger_count"] == queued["message_count"] == 1
            and after["accounts"] == {}
            and audit["valid"],
            "before": before,
            "after": after,
            "outbox": queued,
            "sender_audit": audit,
        }
    if name == "receiver_rollback":
        enqueue(sender, clock)

        def crash(*_):
            raise CrashInjected()

        with Receiver(target, fault=crash) as receiver, Dispatcher(sender, receiver, **options) as dispatcher:
            try:
                dispatcher.dispatch_one()
            except CrashInjected:
                pass
            before = receiver.snapshot()
        clock.advance(11)
        with Receiver(target) as receiver, Dispatcher(sender, receiver, **options) as dispatcher:
            delivered = dispatcher.dispatch_one()
            after = receiver.snapshot()
        return {
            "passed": before == {"inbox_count": 0, "accounts": {}}
            and delivered["delivered"]
            and after == {"inbox_count": 1, "accounts": {"demo": 7}},
            "before": before,
            "after": after,
            "delivery": delivered,
        }
    if name == "outage_restart":
        enqueue(sender, clock)
        with Receiver(target, available=False) as receiver, Dispatcher(sender, receiver, **options) as dispatcher:
            failed = dispatcher.dispatch_one()
        with Receiver(target) as receiver, Dispatcher(sender, receiver, **options) as dispatcher:
            waiting = dispatcher.dispatch_one()
            clock.advance(1)
            delivered = dispatcher.dispatch_one()
            state = receiver.snapshot()
        return {
            "passed": failed["status"] == "pending"
            and waiting["status"] == "idle"
            and delivered.get("attempt") == 2
            and state == {"inbox_count": 1, "accounts": {"demo": 7}},
            "outage": failed,
            "before_due": waiting,
            "recovered": delivered,
            "receiver": state,
        }
    if name == "concurrent_duplicate_delivery":
        message = Envelope("a" * 32, "wf", "one", "demo", 7)
        with Receiver(target):
            pass
        barrier = threading.Barrier(8)

        def deliver(_):
            with Receiver(target) as receiver:
                barrier.wait(timeout=10)
                return receiver.receive(message).duplicate

        with ThreadPoolExecutor(max_workers=8) as pool:
            duplicates = list(pool.map(deliver, range(8)))
        with Receiver(target) as receiver:
            state = receiver.snapshot()
        return {
            "passed": sum(duplicates) == 7 and state == {"inbox_count": 1, "accounts": {"demo": 7}},
            "delivery_calls": 8,
            "duplicate_receipts": sum(duplicates),
            "receiver": state,
        }
    if name == "payload_conflict":
        message = Envelope("b" * 32, "wf", "one", "demo", 7)
        with Receiver(target) as receiver:
            receiver.receive(message)
            try:
                receiver.receive(replace(message, amount=9))
            except PayloadConflict:
                rejected = True
            else:
                rejected = False
            state = receiver.snapshot()
        return {
            "passed": rejected and state == {"inbox_count": 1, "accounts": {"demo": 7}},
            "conflict_rejected": rejected,
            "receiver": state,
        }
    if name == "stale_ack_fenced":
        enqueue(sender, clock)
        with (
            Receiver(target) as receiver,
            Dispatcher(sender, receiver, **options) as old,
            Dispatcher(sender, receiver, **options) as current,
        ):
            stale = old.claim()
            clock.advance(11)
            replacement = current.claim()
            old_ack = old.acknowledge(stale, receiver.receive(stale.message))
            duplicate = receiver.receive(replacement.message)
            new_ack = current.acknowledge(replacement, duplicate)
            state = receiver.snapshot()
        return {
            "passed": not old_ack
            and new_ack
            and duplicate.duplicate
            and replacement.epoch == stale.epoch + 1
            and state["accounts"] == {"demo": 7},
            "stale_ack_accepted": old_ack,
            "current_ack_accepted": new_ack,
            "receiver": state,
        }
    if name == "admission_race":
        with OutboxRuntime(sender, capacity=1):
            pass
        barrier = threading.Barrier(2)

        def admit(index):
            with OutboxRuntime(sender, capacity=1) as workflow:
                barrier.wait(timeout=10)
                try:
                    workflow.submit(f"wf{index}", [Action("one", tool="enqueue_credit")])
                except AdmissionFull:
                    return False
                return True

        with ThreadPoolExecutor(max_workers=2) as pool:
            accepted = sum(pool.map(admit, range(2)))
        with OutboxRuntime(sender, capacity=1) as workflow:
            state = workflow.deliveries()
        return {"passed": accepted == 1 and state["reserved"] == 1, "accepted": accepted, "outbox": state}
    if name == "bounded_retry_exhaustion":
        enqueue(sender, clock, attempts=2)
        with Receiver(target, available=False) as receiver, Dispatcher(sender, receiver, **options) as dispatcher:
            first = dispatcher.dispatch_one()
            clock.advance(1)
            second = dispatcher.dispatch_one()
            stopped = dispatcher.dispatch_one()
            state = receiver.snapshot()
        return {
            "passed": first["status"] == "pending"
            and second["status"] == "dead_letter"
            and second["attempt"] == 2
            and stopped["status"] == "idle"
            and state["inbox_count"] == 0,
            "attempts": [first, second],
            "after_ceiling": stopped,
            "receiver": state,
        }
    if name == "independent_sender_namespaces":
        enqueue(sender, clock)
        second = folder / "second-sender.sqlite"
        enqueue(second, clock)
        with Receiver(target) as receiver:
            for source in (sender, second):
                with Dispatcher(source, receiver, **options) as dispatcher:
                    dispatcher.dispatch_one()
            state = receiver.snapshot()
        return {"passed": state == {"inbox_count": 2, "accounts": {"demo": 14}}, "receiver": state}
    if name == "real_process_death_ack_gap":
        program = """import os,sys
sys.path.insert(0,sys.argv[3])
from resilient_runtime import Action
from resilient_runtime.outbox import OutboxRuntime,Receiver,Dispatcher
def crash(stage,token):
    if stage=='after_receiver_commit': os._exit(29)
with OutboxRuntime(sys.argv[1]) as workflow:
    workflow.submit('wf',[Action('one',tool='enqueue_credit',amount=7)])
    workflow.run('wf')
with Receiver(sys.argv[2]) as receiver,Dispatcher(sys.argv[1],receiver,fault=crash) as dispatcher:
    dispatcher.dispatch_one()
"""
        child = subprocess.run(
            [sys.executable, "-c", program, str(sender), str(target), str(Path(__file__).resolve().parents[1])],
            timeout=20,
            capture_output=True,
        )
        with Receiver(target) as receiver, Dispatcher(sender, receiver, clock=lambda: time.time() + 11) as dispatcher:
            before = receiver.snapshot()
            retried = dispatcher.dispatch_one()
            after = receiver.snapshot()
        return {
            "passed": child.returncode == 29
            and before == after == {"inbox_count": 1, "accounts": {"demo": 7}}
            and retried.get("receiver_duplicate") is True
            and retried.get("attempt") == 2
            and retried["delivered"],
            "process_exit": child.returncode,
            "receiver_before_restart": before,
            "receiver_after_restart": after,
            "retried": retried,
        }
    raise ValueError("unknown outbox scenario")


def demo() -> dict:
    with tempfile.TemporaryDirectory(prefix="outbox-demo-") as directory:
        folder = Path(directory)
        result = ack_gap(folder / "workflow.sqlite", folder / "receiver.sqlite", ManualClock())
    return {
        "experiment": "two-database ACK gap",
        "databases_per_delivery": 2,
        "transport": "in-process receiver adapter; independent SQLite transactions",
        **result,
    }


def evaluate() -> dict:
    rows = []
    for name in SCENARIOS:
        with tempfile.TemporaryDirectory(prefix="outbox-eval-") as directory:
            result = run_case(name, Path(directory))
        rows.append({"id": name, **result})
    return {
        "dataset_version": "transactional-outbox-inbox-v1",
        "dataset_sha256": hashlib.sha256(json.dumps(SCENARIOS).encode()).hexdigest(),
        "passed": sum(row["passed"] for row in rows),
        "total": len(rows),
        "cases": rows,
        "scope": "real separate SQLite commits, fictional credits, in-process transport, real threads and one abrupt child-process exit",
        "limitations": [
            "At-least-once retries plus payload-bound receiver deduplication, not a universal distributed exactly-once guarantee.",
            "Finite retries can dead-letter an unacknowledged message whose receiver effect already committed.",
            "Durable inbox retention, stable sender namespace/message identity, eventual availability and continued scheduling are assumptions; no network transport is benchmarked.",
        ],
        "usage": {"model_calls": 0, "provider_calls": 0},
    }
