# Transaction and lease regressions

This review inspected the complete runtime, schema, CLI, tests, fault fixtures, and public guarantees. The initial implementation had 18 passing tests and 19 passing fixture scenarios, yet important state-machine defects remained. Green tests did not establish correctness at the missing boundaries.

| Reproduced issue | Before review | Implemented correction and proof |
| --- | --- | --- |
| Deadline checked only before effect | A `before_commit` hook advancing time by two seconds still committed credits under a one-second action limit. | Final pre-commit action/run/lease gates roll back the provisional effect, ledger, cursor, and commit event. Dedicated tests separately verify each cutoff. |
| Late cancellation | Callback cancellation requested in `before_commit` still published the first credit. | Recheck callback immediately before commit; cancel after rollback, retaining only earlier commits. |
| Post-commit observer error retried a committed action | A transient `after_commit` exception marked an already credited step failed under a one-attempt limit. | Isolate observation from transactional execution. Preserve the completed step and ledger; return `post_commit_hook_error` and resume from the next cursor. |
| Retry ceiling existed only in worker configuration | Restarting with a higher `Limits.max_attempts` widened the original workflow's budget. | Persist the ceiling at submission and enforce the minimum of persisted/operator ceilings. A restart with ten attempts cannot widen a workflow submitted with one. |
| Stale-owner coordination lacked an explicit public fencing contract | Ownership was an opaque per-run ID, without an epoch or renewable token API. | Add monotonic persisted epochs, `Lease`, acquire/heartbeat/release, and guarded execution with a supplied token. Old tokens cannot resurrect or clear replacement ownership. Test takeover after attempt reservation. |
| Invalid retry policy stranded ownership | A NaN/throwing backoff hook escaped its error handler, leaving a running workflow and lease. | Validate finite numeric delays; fail as `backoff_policy_error` and clear ownership without exporting the hook message. |
| Cancellation callback exception leaked lease | A callback failure occurred before the execution error boundary. | Classify as `control_error`, release ownership, and retain a stable public error code. |
| Inconsistent snapshots | Independent SELECTs could combine an old cursor with a newly committed ledger/balance. | Enclose snapshot queries in a single WAL read transaction. A test commits through another connection between SELECTs and verifies one consistent older view. |
| Mutable capability input | Mutating the application's original set could widen an existing worker's tool permissions. | Copy it into a `frozenset`; a regression verifies later mutation cannot grant the tool. |
| ValueError misclassification | Any ValueError from a trusted hook was called an idempotency conflict. | Use a distinct internal conflict exception; unrelated ValueErrors become `tool_error`. |
| Commit failure cleanup | Failure during `commit()` did not explicitly roll back the connection. | Roll back commit exceptions; an injected storage failure verifies zero published account rows and successful later reuse. |
| Unbounded event export | Repeated lease/heartbeat events could grow a single JSON snapshot indefinitely. | Preserve all database history, export newest 1,000 events, expose `event_count` and `trace_truncated`, and index workflow event lookups. |

## Substantive additions

- **Fenced lease API:** acquisition increments a durable epoch; heartbeats require an unexpired current owner; stale releases are no-ops. This is a local coordination mechanism, not an authentication protocol.
- **Independent integrity audit:** compare checkpoint position with completed steps and ledger cardinality, enforce durable retry ceilings, verify ledger position/key/fingerprint/result bindings, and report violations. Tests corrupt state to prove the audit can fail.
- **Real process-death experiments:** two fresh Python child processes terminate via `os._exit(23)` at the pre-/post-commit boundary. Recovery uses a new connection and verifies the expected attempts, balances, cursor, and ledger rows. These are abrupt process exits, not hardware power-loss tests.
- **Additional evaluation regimes:** actual four-worker contention, deterministic writer/read interleaving, replaced-worker rejection, late commit gates, and immutable retry ceilings.

## Executed verification

From this standalone project directory:

```bash
python -m pip install -e .
python -m unittest discover -s verification -v
python -m resilient_runtime demo --output replays/demo.json
python -m resilient_runtime evaluate --output replays/evaluation.json
ruff check src tests
ruff format --check src tests
```

Measured after review: **41/41 unittest methods passed; 31/31 fault-evaluation cases passed**. The evaluation includes 12 added review scenarios, including real concurrent threads and two abrupt child-process exits. No model weights, provider calls, or paid resources were used. The default state remains fictional local SQLite account credits. Executed artifacts are checked in under `replays/`; their timestamps and event interleavings are observations, not deterministic performance baselines.

## Limits that remain explicit

The effect, ledger, and checkpoint share one SQLite transaction. Remote services do not share it; this project does **not** establish production exactly-once execution. A real remote tool needs transport deadlines, provider idempotency, reconciliation, or an outbox/inbox protocol.

Hooks and callbacks are trusted synchronous code. Cooperative checks cannot terminate a hung hook; lock waits and commit syscalls can outlast configured budgets. Database cancellation serializes behind a writer, so it cannot revoke a prior committed effect. Wall-clock drift between hosts is not addressed. Each worker must own a separate connection; mixed-version workers and distributed databases are unsupported.

The additive legacy schema upgrade fills missing retry ceilings with the old default of three; it cannot reconstruct a custom ceiling that an older version never persisted. Fresh v2 submissions record their actual ceiling. Operators should migrate old custom workflows explicitly rather than infer that lost policy.

Snapshots include local balances and trace metadata. Event export is capped, but on-disk retention, global account conservation auditing, tenant isolation, authentication, backups, and corruption repair are outside this lab. The independent audit checks workflow/ledger metadata and detects the documented corruptions; it is not a database security or availability proof.
