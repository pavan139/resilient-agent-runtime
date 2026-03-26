"""Transactional outbox/inbox across independently committed SQLite databases.

No distributed transaction exists. Delivery attempts are at least once; receiver
deduplication is payload-bound and atomic with its fictional account effect.
"""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import asdict, dataclass
import hashlib
import json
import math
from pathlib import Path
import re
import sqlite3
import time
from typing import Callable
import uuid

from .runtime import Action, CrashInjected, IDENTIFIER, Runtime


class AdmissionFull(ValueError):
    pass


class PayloadConflict(ValueError):
    pass


class ReceiverUnavailable(RuntimeError):
    pass


class DeliveryTimeout(ReceiverUnavailable):
    pass


def durable_path(path: str | Path) -> str:
    if not isinstance(path, (str, Path)) or not str(path) or str(path) == ":memory:":
        raise ValueError("an explicit durable SQLite file is required")
    return str(Path(path).expanduser().resolve())


def canonical(value: dict) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


@dataclass(frozen=True)
class Envelope:
    sender_id: str
    workflow_id: str
    action_id: str
    account: str
    amount: int
    protocol: str = "fictional-credit-v1"

    def __post_init__(self):
        if type(self.sender_id) is not str or not re.fullmatch(r"[0-9a-f]{32}", self.sender_id):
            raise ValueError("invalid sender namespace")
        Action(self.action_id, account=self.account, amount=self.amount)
        if type(self.workflow_id) is not str or not IDENTIFIER.fullmatch(self.workflow_id):
            raise ValueError("invalid envelope workflow")
        if self.protocol != "fictional-credit-v1":
            raise ValueError("unsupported message protocol")

    @property
    def message_id(self) -> str:
        return f"{self.sender_id}:{self.workflow_id}:{self.action_id}"

    @property
    def payload(self) -> str:
        return canonical(asdict(self))

    @property
    def fingerprint(self) -> str:
        return hashlib.sha256(self.payload.encode()).hexdigest()

    @classmethod
    def parse(cls, payload: str) -> "Envelope":
        if type(payload) is not str or len(payload) > 2048:
            raise ValueError("invalid envelope size")
        try:
            data = json.loads(payload)
            if not isinstance(data, dict) or set(data) != {
                "sender_id",
                "workflow_id",
                "action_id",
                "account",
                "amount",
                "protocol",
            }:
                raise ValueError("invalid envelope schema")
            return cls(**data)
        except (TypeError, RecursionError) as exc:
            raise ValueError("invalid envelope schema") from exc


OUTBOX_SCHEMA = """
CREATE TABLE IF NOT EXISTS outbox_settings (
 singleton INTEGER PRIMARY KEY CHECK(singleton=1), sender_id TEXT NOT NULL,
 capacity INTEGER NOT NULL, retry_ceiling INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS outbox_reservations (
 workflow_id TEXT NOT NULL REFERENCES workflows(workflow_id), position INTEGER NOT NULL,
 PRIMARY KEY(workflow_id,position)
);
CREATE TABLE IF NOT EXISTS outbox (
 message_id TEXT PRIMARY KEY, workflow_id TEXT NOT NULL REFERENCES workflows(workflow_id),
 position INTEGER NOT NULL, payload_json TEXT NOT NULL, payload_hash TEXT NOT NULL,
 status TEXT NOT NULL DEFAULT 'pending', attempts INTEGER NOT NULL DEFAULT 0,
 retry_ceiling INTEGER NOT NULL, next_attempt_at REAL NOT NULL DEFAULT 0,
 lease_owner TEXT, lease_epoch INTEGER NOT NULL DEFAULT 0, lease_expires REAL NOT NULL DEFAULT 0,
 created_at REAL NOT NULL, delivered_at REAL, last_error TEXT,
 UNIQUE(workflow_id,position)
);
CREATE INDEX IF NOT EXISTS outbox_dispatch ON outbox(status,next_attempt_at,lease_expires,created_at);
"""


def reserved_count(db: sqlite3.Connection) -> int:
    return db.execute("""SELECT COUNT(*) FROM outbox_reservations r JOIN workflows w USING(workflow_id)
                         WHERE w.status NOT IN ('completed','failed','cancelled')""").fetchone()[0]


def outbox_snapshot(db: sqlite3.Connection) -> dict:
    owns_read = not db.in_transaction
    if owns_read:
        db.execute("BEGIN")
    try:
        states = dict(db.execute("SELECT status,COUNT(*) FROM outbox GROUP BY status").fetchall())
        rows = db.execute("""SELECT message_id,status,attempts,retry_ceiling,lease_epoch,lease_expires,
                              next_attempt_at,last_error,payload_hash FROM outbox ORDER BY message_id LIMIT 1000""").fetchall()
        result = {
            "states": states,
            "reserved": reserved_count(db),
            "messages": [dict(row) for row in rows],
            "message_count": sum(states.values()),
            "truncated": sum(states.values()) > len(rows),
        }
        if owns_read:
            db.commit()
        return result
    except BaseException:
        if owns_read:
            db.rollback()
        raise


class OutboxRuntime(Runtime):
    """An enqueue action commits with the existing workflow ledger and checkpoint."""

    SUPPORTED_TOOLS = frozenset({"credit_demo_account", "enqueue_credit"})

    def __init__(self, database: str | Path, *, capacity: int = 128, delivery_attempts: int = 5, **kwargs):
        if type(capacity) is not int or not 1 <= capacity <= 1024:
            raise ValueError("outbox capacity must be 1..1024")
        if type(delivery_attempts) is not int or not 1 <= delivery_attempts <= 10:
            raise ValueError("delivery retry ceiling must be 1..10")
        kwargs.setdefault("allowed_tools", frozenset({"enqueue_credit"}))
        super().__init__(durable_path(database), **kwargs)
        try:
            self.db.executescript(OUTBOX_SCHEMA)
            with self.transaction():
                settings = self.db.execute("SELECT * FROM outbox_settings WHERE singleton=1").fetchone()
                if settings is None:
                    self.sender_id = uuid.uuid4().hex
                    self.db.execute(
                        "INSERT INTO outbox_settings VALUES(1,?,?,?)", (self.sender_id, capacity, delivery_attempts)
                    )
                else:
                    if (settings["capacity"], settings["retry_ceiling"]) != (capacity, delivery_attempts):
                        raise ValueError("persisted outbox admission/retry policy mismatch")
                    self.sender_id = settings["sender_id"]
            self.capacity, self.delivery_attempts = capacity, delivery_attempts
        except BaseException:
            self.close()
            raise

    def envelope(self, workflow_id: str, action: Action) -> Envelope:
        return Envelope(self.sender_id, workflow_id, action.action_id, action.account, action.amount)

    def _admit(self, workflow_id: str, actions: list[Action]) -> None:
        requested = sum(action.tool == "enqueue_credit" for action in actions)
        outstanding = self.db.execute("SELECT COUNT(*) FROM outbox WHERE status!='delivered'").fetchone()[0]
        if outstanding + reserved_count(self.db) + requested > self.capacity:
            raise AdmissionFull("outbox admission capacity exceeded")

    def _on_submit(self, workflow_id: str, actions: list[Action]) -> None:
        self.db.executemany(
            "INSERT INTO outbox_reservations VALUES(?,?)",
            [(workflow_id, i) for i, action in enumerate(actions) if action.tool == "enqueue_credit"],
        )

    def _expected_result(self, workflow_id: str, action: Action) -> dict:
        if action.tool != "enqueue_credit":
            return super()._expected_result(workflow_id, action)
        return {"enqueued_message_id": self.envelope(workflow_id, action).message_id, "amount": action.amount}

    def _apply_effect(self, workflow_id: str, position: int, action: Action) -> dict:
        if action.tool != "enqueue_credit":
            return super()._apply_effect(workflow_id, position, action)
        message = self.envelope(workflow_id, action)
        self.db.execute(
            """INSERT INTO outbox(message_id,workflow_id,position,payload_json,payload_hash,retry_ceiling,created_at)
                          VALUES(?,?,?,?,?,?,?)""",
            (
                message.message_id,
                workflow_id,
                position,
                message.payload,
                message.fingerprint,
                self.delivery_attempts,
                self.clock(),
            ),
        )
        self.db.execute("DELETE FROM outbox_reservations WHERE workflow_id=? AND position=?", (workflow_id, position))
        return self._expected_result(workflow_id, action)

    def deliveries(self) -> dict:
        return outbox_snapshot(self.db)

    def audit(self, workflow_id: str) -> dict:
        owns_read = not self.db.in_transaction
        if owns_read:
            self.db.execute("BEGIN")
        try:
            result = super().audit(workflow_id)
            for step in self.db.execute(
                "SELECT position,action_json FROM steps WHERE workflow_id=? AND status='completed'", (workflow_id,)
            ):
                action = Action(**json.loads(step["action_json"]))
                if action.tool != "enqueue_credit":
                    continue
                message = self.envelope(workflow_id, action)
                row = self.db.execute("SELECT * FROM outbox WHERE message_id=?", (message.message_id,)).fetchone()
                if (
                    row is None
                    or row["payload_hash"] != message.fingerprint
                    or row["payload_json"] != message.payload
                    or row["position"] != step["position"]
                ):
                    result["violations"].append(f"outbox_binding:{step['position']}")
            result["valid"] = not result["violations"]
            result["scope"] += "; committed enqueue actions also bind to the sender outbox, not receiver outcomes"
            if owns_read:
                self.db.commit()
            return result
        except BaseException:
            if owns_read:
                self.db.rollback()
            raise


class SQLiteEndpoint:
    def __init__(self, database: str | Path):
        self.database = durable_path(database)
        self.db = sqlite3.connect(self.database, timeout=2, isolation_level=None)
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA foreign_keys=ON")

    def close(self):
        self.db.close()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()

    @contextmanager
    def transaction(self):
        self.db.execute("BEGIN IMMEDIATE")
        try:
            yield
            self.db.commit()
        except BaseException:
            self.db.rollback()
            raise


@dataclass(frozen=True)
class Receipt:
    message_id: str
    payload_hash: str
    amount: int
    duplicate: bool


class Receiver(SQLiteEndpoint):
    """Inbox insertion and the fictional account credit share only receiver DB's transaction."""

    def __init__(self, database: str | Path, *, available: bool = True, fault: Callable = lambda *_: None):
        if type(available) is not bool or not callable(fault):
            raise ValueError("receiver availability must be boolean and fault must be callable")
        super().__init__(database)
        self.available, self.fault = available, fault
        self.db.executescript("""
            CREATE TABLE IF NOT EXISTS inbox (
              message_id TEXT PRIMARY KEY,payload_hash TEXT NOT NULL,payload_json TEXT NOT NULL,
              result_json TEXT NOT NULL,received_at REAL NOT NULL
            );
            CREATE TABLE IF NOT EXISTS receiver_accounts(account TEXT PRIMARY KEY,balance INTEGER NOT NULL);
        """)

    def receive(self, envelope: Envelope) -> Receipt:
        if not isinstance(envelope, Envelope):
            raise ValueError("validated envelope required")
        if not self.available:
            raise ReceiverUnavailable("fictional receiver unavailable")
        with self.transaction():
            existing = self.db.execute("SELECT * FROM inbox WHERE message_id=?", (envelope.message_id,)).fetchone()
            if existing:
                if existing["payload_hash"] != envelope.fingerprint or existing["payload_json"] != envelope.payload:
                    raise PayloadConflict("message identity reused with a different payload")
                duplicate = True
            else:
                self.db.execute(
                    """INSERT INTO receiver_accounts VALUES(?,?) ON CONFLICT(account)
                                  DO UPDATE SET balance=balance+excluded.balance""",
                    (envelope.account, envelope.amount),
                )
                self.db.execute(
                    "INSERT INTO inbox VALUES(?,?,?,?,?)",
                    (
                        envelope.message_id,
                        envelope.fingerprint,
                        envelope.payload,
                        canonical({"account": envelope.account, "amount": envelope.amount}),
                        time.time(),
                    ),
                )
                duplicate = False
            self.fault("before_receiver_commit", envelope)
        return Receipt(envelope.message_id, envelope.fingerprint, envelope.amount, duplicate)

    def snapshot(self) -> dict:
        self.db.execute("BEGIN")
        try:
            result = {
                "inbox_count": self.db.execute("SELECT COUNT(*) FROM inbox").fetchone()[0],
                "accounts": dict(self.db.execute("SELECT account,balance FROM receiver_accounts ORDER BY account")),
            }
            self.db.commit()
            return result
        except BaseException:
            self.db.rollback()
            raise


@dataclass(frozen=True)
class DeliveryLimits:
    lease_seconds: float = 10
    call_timeout_seconds: float = 5
    base_retry_seconds: float = 0.1
    max_batch: int = 64

    def __post_init__(self):
        for value in (self.lease_seconds, self.call_timeout_seconds, self.base_retry_seconds):
            if type(value) not in (int, float) or not math.isfinite(value) or not 0 < value <= 60:
                raise ValueError("delivery times must be finite numbers in (0,60]")
        if type(self.max_batch) is not int or not 1 <= self.max_batch <= 1024:
            raise ValueError("delivery batch must be 1..1024")


@dataclass(frozen=True)
class DeliveryLease:
    message: Envelope
    owner: str
    epoch: int
    attempt: int


class Dispatcher(SQLiteEndpoint):
    """Fenced, bounded at-least-once delivery. No sender transaction spans receiver I/O."""

    def __init__(
        self,
        sender_database: str | Path,
        receiver: Receiver,
        *,
        limits: DeliveryLimits | None = None,
        clock: Callable = time.time,
        timer: Callable = time.monotonic,
        fault: Callable = lambda *_: None,
    ):
        if not isinstance(receiver, Receiver) or (limits is not None and not isinstance(limits, DeliveryLimits)):
            raise ValueError("validated receiver and delivery limits required")
        if not all(callable(hook) for hook in (clock, timer, fault)):
            raise ValueError("delivery clock, timer and fault must be callable")
        sender = Path(durable_path(sender_database))
        target = Path(receiver.database)
        if sender == target or (sender.exists() and target.exists() and sender.samefile(target)):
            raise ValueError("sender and receiver must be two independently durable databases")
        super().__init__(sender)
        try:
            settings = self.db.execute("SELECT * FROM outbox_settings WHERE singleton=1").fetchone()
        except sqlite3.Error:
            self.close()
            raise ValueError("sender outbox has not been initialized") from None
        if settings is None:
            self.close()
            raise ValueError("sender outbox has not been initialized")
        self.receiver, self.limits = receiver, limits or DeliveryLimits()
        self.clock, self.timer, self.fault = clock, timer, fault

    def claim(self) -> DeliveryLease | None:
        with self.transaction():
            # Bound the exhausted/corrupt scan by the persisted admission ceiling.
            capacity = self.db.execute("SELECT capacity FROM outbox_settings WHERE singleton=1").fetchone()[0]
            for _ in range(capacity):
                row = self.db.execute(
                    """SELECT * FROM outbox WHERE next_attempt_at<=? AND
                     (status='pending' OR (status='inflight' AND lease_expires<=?))
                     ORDER BY created_at,message_id LIMIT 1""",
                    (self.clock(), self.clock()),
                ).fetchone()
                if row is None:
                    return None
                if row["attempts"] >= row["retry_ceiling"]:
                    self.db.execute(
                        "UPDATE outbox SET status='dead_letter',lease_owner=NULL,lease_expires=0,last_error='delivery_attempts_exhausted' WHERE message_id=?",
                        (row["message_id"],),
                    )
                    continue
                try:
                    message = Envelope.parse(row["payload_json"])
                    if message.message_id != row["message_id"] or message.fingerprint != row["payload_hash"]:
                        raise ValueError("payload binding mismatch")
                except ValueError:
                    self.db.execute(
                        "UPDATE outbox SET status='dead_letter',lease_owner=NULL,lease_expires=0,last_error='invalid_stored_payload' WHERE message_id=?",
                        (row["message_id"],),
                    )
                    continue
                token = DeliveryLease(message, uuid.uuid4().hex, row["lease_epoch"] + 1, row["attempts"] + 1)
                self.db.execute(
                    """UPDATE outbox SET status='inflight',attempts=?,lease_owner=?,lease_epoch=?,lease_expires=?
                                   WHERE message_id=?""",
                    (
                        token.attempt,
                        token.owner,
                        token.epoch,
                        self.clock() + self.limits.lease_seconds,
                        message.message_id,
                    ),
                )
                return token
        return None

    def _owned(self, token: DeliveryLease) -> bool:
        row = self.db.execute(
            "SELECT status,lease_owner,lease_epoch,lease_expires,payload_hash,payload_json FROM outbox WHERE message_id=?",
            (token.message.message_id,),
        ).fetchone()
        return bool(
            row
            and row["status"] == "inflight"
            and row["lease_owner"] == token.owner
            and row["lease_epoch"] == token.epoch
            and row["lease_expires"] > self.clock()
            and row["payload_hash"] == token.message.fingerprint
            and row["payload_json"] == token.message.payload
        )

    def acknowledge(self, token: DeliveryLease, receipt: Receipt) -> bool:
        message = token.message
        if (
            not isinstance(receipt, Receipt)
            or receipt.message_id != message.message_id
            or receipt.payload_hash != message.fingerprint
            or type(receipt.amount) is not int
            or receipt.amount != message.amount
            or type(receipt.duplicate) is not bool
        ):
            raise PayloadConflict("unbound receiver receipt")
        with self.transaction():
            if not self._owned(token):
                return False
            self.db.execute(
                """UPDATE outbox SET status='delivered',delivered_at=?,last_error=NULL,lease_owner=NULL,lease_expires=0
                               WHERE message_id=?""",
                (self.clock(), message.message_id),
            )
            return True

    def _negative_ack(self, token: DeliveryLease, code: str, permanent: bool = False) -> str:
        with self.transaction():
            if not self._owned(token):
                return "lease_lost"
            ceiling = self.db.execute(
                "SELECT retry_ceiling FROM outbox WHERE message_id=?", (token.message.message_id,)
            ).fetchone()[0]
            terminal = permanent or token.attempt >= ceiling
            status = "dead_letter" if terminal else "pending"
            delay = min(60, self.limits.base_retry_seconds * 2 ** (token.attempt - 1))
            self.db.execute(
                """UPDATE outbox SET status=?,next_attempt_at=?,last_error=?,lease_owner=NULL,lease_expires=0
                               WHERE message_id=?""",
                (status, self.clock() + delay, code, token.message.message_id),
            )
            return status

    def dispatch_one(self) -> dict:
        token = self.claim()
        if token is None:
            return {"status": "idle", "delivered": False}
        started = self.timer()
        base = {"message_id": token.message.message_id, "attempt": token.attempt, "lease_epoch": token.epoch}
        try:
            self.fault("before_send", token)
            with self.transaction():
                if not self._owned(token):
                    return {**base, "status": "lease_lost", "delivered": False}
            self.fault("after_send_gate", token)
            receipt = self.receiver.receive(token.message)
            self.fault("after_receiver_commit", token)
            if self.timer() - started >= self.limits.call_timeout_seconds:
                raise DeliveryTimeout("receiver returned after the cooperative call deadline")
            self.fault("before_sender_ack", token)
            accepted = self.acknowledge(token, receipt)
        except CrashInjected:
            raise
        except PayloadConflict:
            return {
                **base,
                "status": self._negative_ack(token, "payload_conflict", True),
                "delivered": False,
                "error": "payload_conflict",
            }
        except (ReceiverUnavailable, sqlite3.Error) as exc:
            code = "delivery_timeout" if isinstance(exc, DeliveryTimeout) else "receiver_unavailable"
            return {**base, "status": self._negative_ack(token, code), "delivered": False, "error": code}
        except Exception as exc:
            return {
                **base,
                "status": self._negative_ack(token, "delivery_error"),
                "delivered": False,
                "error": "delivery_error",
                "error_type": type(exc).__name__,
            }
        # No retry handler may rewrite a delivered sender row after its ACK commits.
        try:
            self.fault("after_sender_ack", token)
        except CrashInjected:
            raise
        except Exception as exc:
            return {
                **base,
                "status": "post_ack_observer_error",
                "delivered": accepted,
                "error_type": type(exc).__name__,
            }
        return {
            **base,
            "status": "delivered" if accepted else "lease_lost",
            "delivered": accepted,
            "receiver_duplicate": receipt.duplicate,
        }

    def drain(self) -> dict:
        outcomes = []
        for _ in range(self.limits.max_batch):
            result = self.dispatch_one()
            if result["status"] == "idle":
                break
            outcomes.append(result)
        return {"attempts_this_batch": len(outcomes), "outcomes": outcomes, "outbox": outbox_snapshot(self.db)}
