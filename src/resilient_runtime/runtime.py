"""Atomic SQLite effects, idempotency records, checkpoints, leases and retries.

The guarantee applies only to effects inside this database transaction. External
HTTP services need their own idempotency contract or an outbox/inbox protocol.
"""

from __future__ import annotations
from contextlib import contextmanager
from dataclasses import asdict, dataclass
import hashlib
import json
import math
import re
import sqlite3
import time
from typing import Callable
import uuid

VERSION = "local-runtime-v2"
IDENTIFIER = re.compile(r"[A-Za-z0-9_-]{1,64}\Z")
TERMINAL = {"completed", "cancelled", "failed"}


class CrashInjected(RuntimeError):
    """Simulates abrupt process failure; callers must open a new Runtime."""


class TransientToolError(RuntimeError):
    pass


class ToolTimeout(TransientToolError):
    pass


class LeaseLost(RuntimeError):
    """A stale, expired, or cancelled worker cannot commit or renew ownership."""


class RunDeadline(RuntimeError):
    pass


class ExecutionCancelled(RuntimeError):
    pass


class IdempotencyConflict(RuntimeError):
    pass


@dataclass(frozen=True)
class Lease:
    workflow_id: str
    owner: str
    epoch: int
    expires_at: float


@dataclass(frozen=True)
class Action:
    action_id: str
    tool: str = "credit_demo_account"
    account: str = "demo"
    amount: int = 1

    def __post_init__(self):
        if not all(type(x) is str and IDENTIFIER.fullmatch(x) for x in (self.action_id, self.tool, self.account)):
            raise ValueError("action identifiers must match [A-Za-z0-9_-]{1,64}")
        if type(self.amount) is not int or not 1 <= self.amount <= 1_000_000:
            raise ValueError("amount must be an integer between 1 and 1000000 fictional units")

    @property
    def fingerprint(self):
        return hashlib.sha256(json.dumps(asdict(self), sort_keys=True).encode()).hexdigest()


@dataclass(frozen=True)
class Limits:
    max_attempts: int = 3
    max_actions: int = 32
    action_timeout_seconds: float = 1.0
    run_timeout_seconds: float = 5.0
    base_backoff_seconds: float = 0.1
    lease_seconds: float = 10.0

    def __post_init__(self):
        if type(self.max_attempts) is not int or not 1 <= self.max_attempts <= 10:
            raise ValueError("max_attempts must be between 1 and 10")
        if type(self.max_actions) is not int or not 1 <= self.max_actions <= 128:
            raise ValueError("max_actions must be between 1 and 128")
        for value in (self.action_timeout_seconds, self.run_timeout_seconds, self.base_backoff_seconds):
            if type(value) not in (float, int) or not math.isfinite(value) or not 0 <= value <= 60:
                raise ValueError("timeouts and backoff must be between 0 and 60 seconds")
        if (
            type(self.lease_seconds) not in (float, int)
            or not math.isfinite(self.lease_seconds)
            or not 0.01 <= self.lease_seconds <= 300
        ):
            raise ValueError("lease_seconds must be between 0.01 and 300 seconds")


def no_fault(stage: str, action: Action, attempt: int):
    return None


class Runtime:
    """Single-connection worker. Use one Runtime instance per worker/thread.

    Clock is a wall clock for persisted lease/retry timestamps. Timer is
    monotonic for execution budgets. Fault/backoff hooks are trusted code.
    """

    SUPPORTED_TOOLS = frozenset({"credit_demo_account"})

    def __init__(
        self,
        database: str,
        *,
        limits: Limits | None = None,
        allowed_tools: frozenset[str] = frozenset({"credit_demo_account"}),
        clock: Callable[[], float] = time.time,
        timer: Callable[[], float] = time.monotonic,
        fault: Callable[[str, Action, int], None] = no_fault,
        backoff: Callable[[int, float], float] | None = None,
    ):
        if not isinstance(allowed_tools, (set, frozenset)) or not allowed_tools <= self.SUPPORTED_TOOLS:
            raise ValueError("only explicitly supported fictional tools can be permitted")
        if limits is not None and not isinstance(limits, Limits):
            raise ValueError("limits must be a validated Limits instance")
        if not all(callable(hook) for hook in (clock, timer, fault)) or (backoff is not None and not callable(backoff)):
            raise ValueError("clock, timer, fault and optional backoff must be callable")
        self.limits = limits or Limits()
        self.allowed_tools = frozenset(allowed_tools)
        self.clock, self.timer, self.fault = clock, timer, fault
        self.backoff = backoff or (lambda attempt, base: min(60.0, base * (2 ** (attempt - 1))))
        self.db = sqlite3.connect(database, timeout=2.0, isolation_level=None)
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA foreign_keys=ON")
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.executescript("""
            CREATE TABLE IF NOT EXISTS workflows (
                workflow_id TEXT PRIMARY KEY, fingerprint TEXT NOT NULL,
                status TEXT NOT NULL, checkpoint INTEGER NOT NULL DEFAULT 0,
                next_run_at REAL NOT NULL DEFAULT 0, last_error TEXT,
                lease_owner TEXT, lease_expires REAL NOT NULL DEFAULT 0,
                lease_epoch INTEGER NOT NULL DEFAULT 0,
                max_attempts INTEGER NOT NULL DEFAULT 3
            );
            CREATE TABLE IF NOT EXISTS steps (
                workflow_id TEXT NOT NULL REFERENCES workflows(workflow_id),
                position INTEGER NOT NULL, action_json TEXT NOT NULL,
                attempts INTEGER NOT NULL DEFAULT 0, status TEXT NOT NULL DEFAULT 'pending',
                PRIMARY KEY(workflow_id, position)
            );
            CREATE TABLE IF NOT EXISTS accounts (account TEXT PRIMARY KEY, balance INTEGER NOT NULL);
            CREATE TABLE IF NOT EXISTS ledger (
                idempotency_key TEXT PRIMARY KEY, workflow_id TEXT NOT NULL,
                position INTEGER NOT NULL, action_hash TEXT NOT NULL, result_json TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS events (
                event_id INTEGER PRIMARY KEY AUTOINCREMENT, workflow_id TEXT NOT NULL,
                timestamp REAL NOT NULL, kind TEXT NOT NULL, metadata_json TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS events_workflow ON events(workflow_id,event_id);
        """)
        for column, definition in (
            ("lease_epoch", "INTEGER NOT NULL DEFAULT 0"),
            ("max_attempts", "INTEGER NOT NULL DEFAULT 3"),
        ):
            if column not in {row[1] for row in self.db.execute("PRAGMA table_info(workflows)")}:
                try:
                    self.db.execute(f"ALTER TABLE workflows ADD COLUMN {column} {definition}")
                except sqlite3.OperationalError:
                    if column not in {row[1] for row in self.db.execute("PRAGMA table_info(workflows)")}:
                        raise

    def close(self):
        self.db.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    @contextmanager
    def transaction(self):
        self.db.execute("BEGIN IMMEDIATE")
        try:
            yield
        except BaseException:
            self.db.rollback()
            raise
        else:
            try:
                self.db.commit()
            except BaseException:
                self.db.rollback()
                raise

    def _event(self, workflow_id, kind, **metadata):
        self.db.execute(
            "INSERT INTO events(workflow_id,timestamp,kind,metadata_json) VALUES(?,?,?,?)",
            (workflow_id, self.clock(), kind, json.dumps(metadata, sort_keys=True)),
        )

    def _workflow(self, workflow_id):
        if type(workflow_id) is not str or not IDENTIFIER.fullmatch(workflow_id):
            raise ValueError("invalid workflow identifier")
        row = self.db.execute("SELECT * FROM workflows WHERE workflow_id=?", (workflow_id,)).fetchone()
        if row is None:
            raise KeyError("workflow_not_found")
        return row

    def submit(self, workflow_id: str, actions: list[Action]) -> dict:
        if type(workflow_id) is not str or not IDENTIFIER.fullmatch(workflow_id):
            raise ValueError("invalid workflow identifier")
        if (
            not isinstance(actions, (list, tuple))
            or not 1 <= len(actions) <= self.limits.max_actions
            or not all(isinstance(a, Action) for a in actions)
        ):
            raise ValueError("plan must contain between 1 and max_actions validated Action objects")
        if len({a.action_id for a in actions}) != len(actions):
            raise ValueError("action_id values must be unique within a workflow")
        plan = json.dumps([asdict(a) for a in actions], sort_keys=True)
        fingerprint = hashlib.sha256(plan.encode()).hexdigest()
        with self.transaction():
            previous = self.db.execute(
                "SELECT fingerprint FROM workflows WHERE workflow_id=?", (workflow_id,)
            ).fetchone()
            if previous is not None:
                if previous["fingerprint"] != fingerprint:
                    raise ValueError("workflow_id_conflict: a different plan already uses this identifier")
            else:
                self._admit(workflow_id, actions)
                self.db.execute(
                    "INSERT INTO workflows(workflow_id,fingerprint,status,max_attempts) VALUES(?,?,'pending',?)",
                    (workflow_id, fingerprint, self.limits.max_attempts),
                )
                self.db.executemany(
                    "INSERT INTO steps(workflow_id,position,action_json) VALUES(?,?,?)",
                    [(workflow_id, i, json.dumps(asdict(a), sort_keys=True)) for i, a in enumerate(actions)],
                )
                self._on_submit(workflow_id, actions)
                self._event(workflow_id, "submitted", action_count=len(actions), plan_hash=fingerprint, version=VERSION)
        return self.snapshot(workflow_id)

    def _admit(self, workflow_id: str, actions: list[Action]) -> None:
        """Trusted subclass admission check inside the submission transaction."""

    def _on_submit(self, workflow_id: str, actions: list[Action]) -> None:
        """Trusted subclass durable reservations inside the submission transaction."""

    def _expected_result(self, workflow_id: str, action: Action) -> dict:
        return {"credited_units": action.amount}

    def _apply_effect(self, workflow_id: str, position: int, action: Action) -> dict:
        self.db.execute(
            "INSERT INTO accounts(account,balance) VALUES(?,?) ON CONFLICT(account) DO UPDATE SET balance=balance+excluded.balance",
            (action.account, action.amount),
        )
        return self._expected_result(workflow_id, action)

    def cancel(self, workflow_id: str) -> dict:
        with self.transaction():
            row = self._workflow(workflow_id)
            if row["status"] not in TERMINAL:
                self.db.execute(
                    "UPDATE workflows SET status='cancelled',lease_owner=NULL,lease_expires=0,last_error='cancelled' WHERE workflow_id=?",
                    (workflow_id,),
                )
                self.db.execute(
                    "UPDATE steps SET status='cancelled' WHERE workflow_id=? AND status!='completed'", (workflow_id,)
                )
                self._event(workflow_id, "cancelled", checkpoint=row["checkpoint"])
        return self.snapshot(workflow_id)

    def snapshot(self, workflow_id: str, stop_reason: str | None = None) -> dict:
        # Every SELECT in the exported snapshot sees the same WAL read view.
        if not self.db.in_transaction:
            self.db.execute("BEGIN")
            try:
                result = self.snapshot(workflow_id, stop_reason)
                self.db.commit()
                return result
            except BaseException:
                self.db.rollback()
                raise
        row = self._workflow(workflow_id)
        steps = self.db.execute(
            "SELECT position,attempts,status FROM steps WHERE workflow_id=? ORDER BY position", (workflow_id,)
        ).fetchall()
        event_count = self.db.execute("SELECT COUNT(*) FROM events WHERE workflow_id=?", (workflow_id,)).fetchone()[0]
        events = list(
            reversed(
                self.db.execute(
                    "SELECT event_id,timestamp,kind,metadata_json FROM events WHERE workflow_id=? ORDER BY event_id DESC LIMIT 1000",
                    (workflow_id,),
                ).fetchall()
            )
        )
        return {
            "workflow_id": workflow_id,
            "runtime_version": VERSION,
            "status": row["status"],
            "checkpoint": row["checkpoint"],
            "next_run_at": row["next_run_at"],
            "lease_epoch": row["lease_epoch"],
            "lease_expires": row["lease_expires"],
            "lease_active": bool(row["lease_owner"] and row["lease_expires"] > self.clock()),
            "max_attempts": row["max_attempts"],
            "event_count": event_count,
            "trace_truncated": event_count > len(events),
            "stop_reason": stop_reason or row["last_error"] or row["status"],
            "steps": [dict(s) for s in steps],
            "ledger_count": self.db.execute(
                "SELECT COUNT(*) FROM ledger WHERE workflow_id=?", (workflow_id,)
            ).fetchone()[0],
            "accounts": dict(self.db.execute("SELECT account,balance FROM accounts ORDER BY account").fetchall()),
            "events": [
                {
                    "event_id": e["event_id"],
                    "timestamp": e["timestamp"],
                    "kind": e["kind"],
                    **json.loads(e["metadata_json"]),
                }
                for e in events
            ],
        }

    def acquire(self, workflow_id: str) -> Lease | None:
        """Claim a due workflow. A monotonically increasing epoch fences old owners."""
        with self.transaction():
            row = self._workflow(workflow_id)
            now = self.clock()
            if (
                row["status"] in TERMINAL
                or row["next_run_at"] > now
                or (row["lease_owner"] and row["lease_expires"] > now)
            ):
                return None
            token = Lease(workflow_id, uuid.uuid4().hex, row["lease_epoch"] + 1, now + self.limits.lease_seconds)
            self.db.execute(
                "UPDATE workflows SET lease_owner=?,lease_expires=?,lease_epoch=? WHERE workflow_id=?",
                (token.owner, token.expires_at, token.epoch, workflow_id),
            )
            self._event(workflow_id, "lease_acquired", checkpoint=row["checkpoint"], lease_epoch=token.epoch)
            return token

    def audit(self, workflow_id: str) -> dict:
        """Independently check persisted workflow/ledger invariants in one read view."""
        if not self.db.in_transaction:
            self.db.execute("BEGIN")
            try:
                result = self.audit(workflow_id)
                self.db.commit()
                return result
            except BaseException:
                self.db.rollback()
                raise
        row = self._workflow(workflow_id)
        steps = self.db.execute("SELECT * FROM steps WHERE workflow_id=? ORDER BY position", (workflow_id,)).fetchall()
        ledger = self.db.execute(
            "SELECT * FROM ledger WHERE workflow_id=? ORDER BY position", (workflow_id,)
        ).fetchall()
        violations = []
        checkpoint = row["checkpoint"]
        if not 0 <= checkpoint <= len(steps):
            violations.append("checkpoint_range")
        if [step["position"] for step in steps] != list(range(len(steps))):
            violations.append("step_positions")
        if len(ledger) != checkpoint:
            violations.append("ledger_checkpoint_count")
        by_position = {entry["position"]: entry for entry in ledger}
        if len(by_position) != len(ledger) or any(position >= checkpoint or position < 0 for position in by_position):
            violations.append("ledger_positions")
        for step in steps:
            position = step["position"]
            if not 0 <= step["attempts"] <= row["max_attempts"]:
                violations.append(f"attempt_budget:{position}")
            if (step["status"] == "completed") != (position < checkpoint):
                violations.append(f"step_checkpoint_status:{position}")
            if position < checkpoint:
                try:
                    action = Action(**json.loads(step["action_json"]))
                except (ValueError, TypeError):
                    violations.append(f"invalid_action:{position}")
                    continue
                entry = by_position.get(position)
                if entry is None:
                    violations.append(f"missing_ledger:{position}")
                elif (
                    entry["action_hash"] != action.fingerprint
                    or entry["idempotency_key"] != f"{workflow_id}:{action.action_id}"
                ):
                    violations.append(f"ledger_fingerprint:{position}")
                else:
                    try:
                        result = json.loads(entry["result_json"])
                    except (ValueError, TypeError):
                        result = None
                    if result != self._expected_result(workflow_id, action):
                        violations.append(f"ledger_result:{position}")
        if row["status"] == "completed" and checkpoint != len(steps):
            violations.append("completed_checkpoint")
        return {
            "workflow_id": workflow_id,
            "valid": not violations,
            "violations": violations,
            "step_count": len(steps),
            "ledger_count": len(ledger),
            "checkpoint": checkpoint,
            "scope": "workflow checkpoint, step statuses, retry ceiling, and local ledger binding; not external effects or global account conservation",
        }

    def _assert_lease(self, token: Lease):
        row = self._workflow(token.workflow_id)
        if (
            row["status"] in TERMINAL
            or row["lease_owner"] != token.owner
            or row["lease_epoch"] != token.epoch
            or row["lease_expires"] <= self.clock()
        ):
            raise LeaseLost("lease_lost")
        return row

    def heartbeat(self, token: Lease) -> Lease:
        """Extend an unexpired current lease; never resurrect an expired owner."""
        with self.transaction():
            self._assert_lease(token)
            expires = self.clock() + self.limits.lease_seconds
            self.db.execute(
                "UPDATE workflows SET lease_expires=? WHERE workflow_id=? AND lease_owner=? AND lease_epoch=?",
                (expires, token.workflow_id, token.owner, token.epoch),
            )
            self._event(token.workflow_id, "lease_renewed", lease_epoch=token.epoch)
            return Lease(token.workflow_id, token.owner, token.epoch, expires)

    def release(self, token: Lease) -> bool:
        with self.transaction():
            changed = self.db.execute(
                "UPDATE workflows SET lease_owner=NULL,lease_expires=0 WHERE workflow_id=? AND lease_owner=? AND lease_epoch=?",
                (token.workflow_id, token.owner, token.epoch),
            ).rowcount
            return bool(changed)

    def _execution_gate(self, token: Lease, started: float, action_started: float, cancelled: Callable[[], bool]):
        if cancelled():
            raise ExecutionCancelled()
        self._assert_lease(token)
        if self.timer() - action_started >= self.limits.action_timeout_seconds:
            raise ToolTimeout("action deadline exceeded")
        if self.timer() - started >= self.limits.run_timeout_seconds:
            raise RunDeadline()

    def _stop_pending(self, token: Lease, position: int, code: str) -> dict:
        with self.transaction():
            try:
                self._assert_lease(token)
            except LeaseLost:
                return self.snapshot(token.workflow_id, "lease_lost")
            self.db.execute(
                "UPDATE steps SET status='pending' WHERE workflow_id=? AND position=? AND status!='completed'",
                (token.workflow_id, position),
            )
            self.db.execute(
                "UPDATE workflows SET status='pending',last_error=?,lease_owner=NULL,lease_expires=0 WHERE workflow_id=?",
                (code, token.workflow_id),
            )
            self._event(token.workflow_id, "run_stopped", position=position, code=code, lease_epoch=token.epoch)
        return self.snapshot(token.workflow_id, code)

    def _fail(self, workflow_id, code, position):
        self.db.execute(
            "UPDATE workflows SET status='failed',last_error=?,lease_owner=NULL,lease_expires=0 WHERE workflow_id=?",
            (code, workflow_id),
        )
        self.db.execute("UPDATE steps SET status='failed' WHERE workflow_id=? AND position=?", (workflow_id, position))
        self._event(workflow_id, "failed", position=position, error_code=code)

    def run(
        self, workflow_id: str, *, cancelled: Callable[[], bool] = lambda: False, lease: Lease | None = None
    ) -> dict:
        started = self.timer()
        if lease is not None and (not isinstance(lease, Lease) or lease.workflow_id != workflow_id):
            raise ValueError("lease must belong to the requested workflow")
        token = lease if lease is not None else self.acquire(workflow_id)
        if token is None:
            result = self.snapshot(workflow_id)
            if result["status"] not in TERMINAL:
                result["stop_reason"] = "backoff_wait" if result["next_run_at"] > self.clock() else "worker_busy"
            return result
        for _ in range(self.limits.max_actions):
            try:
                if cancelled():
                    return self.cancel(workflow_id)
                if self.timer() - started >= self.limits.run_timeout_seconds:
                    self.release(token)
                    return self.snapshot(workflow_id, "run_deadline")
                with self.transaction():
                    row = self._workflow(workflow_id)
                    if row["status"] in TERMINAL:
                        return self.snapshot(workflow_id)
                    self._assert_lease(token)
                    position = row["checkpoint"]
                    step = self.db.execute(
                        "SELECT * FROM steps WHERE workflow_id=? AND position=?", (workflow_id, position)
                    ).fetchone()
                    if step is None:
                        self._fail(workflow_id, "invalid_checkpoint", position)
                        return self.snapshot(workflow_id)
                    action = Action(**json.loads(step["action_json"]))
                    if action.tool not in self.allowed_tools:
                        self._fail(workflow_id, "tool_denied", position)
                        return self.snapshot(workflow_id)
                    if step["attempts"] >= min(row["max_attempts"], self.limits.max_attempts):
                        self._fail(workflow_id, "retry_exhausted", position)
                        return self.snapshot(workflow_id)
                    attempt = step["attempts"] + 1
                    # The attempt commits separately, so repeated crashes consume budget.
                    self.db.execute(
                        "UPDATE steps SET attempts=?,status='running' WHERE workflow_id=? AND position=?",
                        (attempt, workflow_id, position),
                    )
                    self.db.execute(
                        "UPDATE workflows SET lease_expires=?,status='running' WHERE workflow_id=?",
                        (self.clock() + self.limits.lease_seconds, workflow_id),
                    )
                    self._event(
                        workflow_id,
                        "attempt_started",
                        position=position,
                        attempt=attempt,
                        action_hash=action.fingerprint,
                        tool=action.tool,
                        lease_epoch=token.epoch,
                    )
            except LeaseLost:
                return self.snapshot(workflow_id, "lease_lost")
            except Exception as exc:
                # Callback failures before reservation cannot leave a stranded lease.
                with self.transaction():
                    try:
                        self._assert_lease(token)
                    except LeaseLost:
                        return self.snapshot(workflow_id, "lease_lost")
                    self._fail(workflow_id, "control_error", self._workflow(workflow_id)["checkpoint"])
                    self._event(workflow_id, "error_classified", error_type=type(exc).__name__)
                return self.snapshot(workflow_id)

            action_started = self.timer()
            try:
                # An explicit nontransactional crash/takeover boundary for the fault lab.
                self.fault("after_attempt_reserved", action, attempt)
                with self.transaction():
                    row = self._workflow(workflow_id)
                    if row["status"] in TERMINAL:
                        return self.snapshot(workflow_id)
                    self._assert_lease(token)
                    self.fault("before_action", action, attempt)
                    self._execution_gate(token, started, action_started, cancelled)
                    key = f"{workflow_id}:{action.action_id}"
                    existing = self.db.execute("SELECT * FROM ledger WHERE idempotency_key=?", (key,)).fetchone()
                    if existing:
                        if existing["action_hash"] != action.fingerprint:
                            raise IdempotencyConflict()
                        self._event(workflow_id, "duplicate_suppressed", position=position, lease_epoch=token.epoch)
                    else:
                        effect_result = self._apply_effect(workflow_id, position, action)
                        self.db.execute(
                            "INSERT INTO ledger(idempotency_key,workflow_id,position,action_hash,result_json) VALUES(?,?,?,?,?)",
                            (
                                key,
                                workflow_id,
                                position,
                                action.fingerprint,
                                json.dumps(effect_result),
                            ),
                        )
                    count = self.db.execute(
                        "SELECT COUNT(*) FROM steps WHERE workflow_id=?", (workflow_id,)
                    ).fetchone()[0]
                    complete = position + 1 == count
                    self.db.execute(
                        "UPDATE steps SET status='completed' WHERE workflow_id=? AND position=?",
                        (workflow_id, position),
                    )
                    self.db.execute(
                        "UPDATE workflows SET checkpoint=?,status=?,next_run_at=0,last_error=NULL WHERE workflow_id=?",
                        (position + 1, "completed" if complete else "pending", workflow_id),
                    )
                    self._event(
                        workflow_id,
                        "action_committed",
                        position=position,
                        attempt=attempt,
                        action_hash=action.fingerprint,
                        duplicate=bool(existing),
                        lease_epoch=token.epoch,
                    )
                    self.fault("before_commit", action, attempt)
                    # The workflow may already say completed inside this uncommitted
                    # transaction. Validate ownership directly before publishing it.
                    lease_row = self._workflow(workflow_id)
                    if cancelled():
                        raise ExecutionCancelled()
                    if (
                        lease_row["lease_owner"] != token.owner
                        or lease_row["lease_epoch"] != token.epoch
                        or lease_row["lease_expires"] <= self.clock()
                    ):
                        raise LeaseLost("lease_lost")
                    if self.timer() - action_started >= self.limits.action_timeout_seconds:
                        raise ToolTimeout("action deadline exceeded before commit")
                    if self.timer() - started >= self.limits.run_timeout_seconds:
                        raise RunDeadline()
                    self.db.execute(
                        "UPDATE workflows SET lease_owner=?,lease_expires=? WHERE workflow_id=?",
                        (
                            None if complete else token.owner,
                            0 if complete else self.clock() + self.limits.lease_seconds,
                            workflow_id,
                        ),
                    )
            except CrashInjected:
                # Before commit: rollback; after reservation: retain attempt and lease.
                raise
            except ExecutionCancelled:
                return self.cancel(workflow_id)
            except LeaseLost:
                self.release(token)
                return self.snapshot(workflow_id, "lease_lost")
            except RunDeadline:
                return self._stop_pending(token, position, "run_deadline")
            except (TransientToolError, IdempotencyConflict) as exc:
                code = (
                    "tool_timeout"
                    if isinstance(exc, ToolTimeout)
                    else "idempotency_conflict"
                    if isinstance(exc, IdempotencyConflict)
                    else "transient_tool_error"
                )
                with self.transaction():
                    try:
                        current = self._assert_lease(token)
                    except LeaseLost:
                        return self.snapshot(workflow_id, "lease_lost")
                    if attempt >= min(current["max_attempts"], self.limits.max_attempts) or isinstance(
                        exc, IdempotencyConflict
                    ):
                        self._fail(
                            workflow_id,
                            "idempotency_conflict" if isinstance(exc, IdempotencyConflict) else "retry_exhausted",
                            position,
                        )
                    else:
                        try:
                            delay = self.backoff(attempt, self.limits.base_backoff_seconds)
                            if type(delay) not in (float, int) or not math.isfinite(delay) or not 0 <= delay <= 60:
                                raise ValueError("invalid backoff")
                        except Exception as backoff_error:
                            self._fail(workflow_id, "backoff_policy_error", position)
                            self._event(workflow_id, "error_classified", error_type=type(backoff_error).__name__)
                        else:
                            try:
                                self._assert_lease(token)
                            except LeaseLost:
                                return self.snapshot(workflow_id, "lease_lost")
                            self.db.execute(
                                "UPDATE steps SET status='pending' WHERE workflow_id=? AND position=?",
                                (workflow_id, position),
                            )
                            self.db.execute(
                                "UPDATE workflows SET status='waiting',next_run_at=?,last_error=?,lease_owner=NULL,lease_expires=0 WHERE workflow_id=?",
                                (self.clock() + delay, code, workflow_id),
                            )
                            self._event(
                                workflow_id,
                                "retry_scheduled",
                                position=position,
                                attempt=attempt,
                                error_code=code,
                                delay_seconds=delay,
                                lease_epoch=token.epoch,
                            )
                return self.snapshot(workflow_id)
            except Exception as exc:
                with self.transaction():
                    try:
                        self._assert_lease(token)
                    except LeaseLost:
                        return self.snapshot(workflow_id, "lease_lost")
                    self._fail(workflow_id, "tool_error", position)
                    self._event(workflow_id, "error_classified", error_type=type(exc).__name__)
                return self.snapshot(workflow_id)

            # A committed action cannot be retried or downgraded because an
            # observer failed after COMMIT. Keep that failure out of the retry path.
            try:
                self.fault("after_commit", action, attempt)
            except CrashInjected:
                raise
            except Exception as exc:
                with self.transaction():
                    self._event(
                        workflow_id,
                        "post_commit_hook_error",
                        position=position,
                        error_type=type(exc).__name__,
                        lease_epoch=token.epoch,
                    )
                    self.db.execute(
                        "UPDATE workflows SET lease_owner=NULL,lease_expires=0 WHERE workflow_id=? AND lease_owner=? AND lease_epoch=?",
                        (workflow_id, token.owner, token.epoch),
                    )
                return self.snapshot(workflow_id, "post_commit_hook_error")
            if complete:
                return self.snapshot(workflow_id)
        self.release(token)
        return self.snapshot(workflow_id)
