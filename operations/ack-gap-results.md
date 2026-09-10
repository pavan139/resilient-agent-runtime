# Receiver commit and lost acknowledgement

The two-store protocol covers the gap where **receiver COMMIT can succeed while the sender never records its ACK**.

## Working implementation

- `outbox.py`: workflow-integrated enqueue action, durable admission reservations, stable sender namespace, canonical payload-bound envelopes, independent receiver inbox/effect transaction, fenced delivery claims/ACKs, bounded retry scheduling, dead-letter states, and sender/outbox audit binding.
- `outbox_experiments.py`: twelve executable failure scenarios using separate real SQLite files, concurrent threads, restarted connections, controlled clocks, and one actual child-process exit.
- `outbox-demo` / `outbox-evaluate`: standalone JSON CLI paths, included in repository CI and cold-wheel verification. Existing local runtime commands/tests remain available.

## Reproduce

```bash
python -m pip install -e . -r requirements-dev.txt
python -m unittest discover -s verification -v
python -m resilient_runtime outbox-demo --output replays/outbox-demo.json
python -m resilient_runtime outbox-evaluate --output replays/outbox-evaluation.json
python -m resilient_runtime evaluate --output artifacts/local-evaluation.json
python dev/check_runtime.py --require-lint --wheel
```

No model/provider calls, pretrained downloads, or paid services are used. The protocol is implemented with Python's standard library and SQLite, not a simulated success table.

## Actual results

**12/12 two-database experiment cases passed.** This is an invariant-test denominator, not a delivery-success or availability percentage.

| Experiment | Observed invariant |
| --- | --- |
| Sender pre-commit crash | Zero outbox/ledger/cursor advance; recovery commits one intent |
| Receiver pre-commit crash | Zero inbox records and zero effect; retry applies one credit |
| Receiver commit then sender ACK loss | Two delivery attempts; one inbox entry and one seven-unit effect |
| Receiver outage and endpoint restart | Due-time backoff respected; second attempt delivers |
| Eight concurrent duplicate deliveries | Seven duplicate receipts; one inbox entry and seven units |
| Payload conflict | Same identity/new amount rejected; original seven-unit effect unchanged |
| Stale dispatcher | Old epoch cannot ACK; replacement ACK succeeds |
| Concurrent admission with capacity one | Exactly one of two pending workflows admitted |
| Retry exhaustion | Exactly two attempts, dead letter, zero receiver effects |
| Last-attempt ACK loss | Sender dead letter while receiver retains one effect; uncertainty preserved |
| Real child-process death | Exit code 29 after receiver commit; restart retry is a duplicate and ACKs successfully |
| Independent sender namespaces | Identical workflow/action IDs from two senders create two inbox records, fourteen units |

The full suite contains **64 passing unittest methods**: the previous 41 plus 23 outbox/inbox tests. Tests additionally verify overlapping dispatchers racing lease expiry, a genuinely closed receiver connection reopened as a fresh endpoint, no sender transaction held during receiver execution, corrupted stored payload rejection, receipt binding, strict limits, namespace durability, and same-database rejection. The original local evaluation remains **31/31**. Executed artifacts are under `replays/`; IDs, timestamps, and thread interleavings can vary while the stated invariants remain reproducible.

## Limits and tradeoffs

At-least-once retry plus receiver deduplication is not universal distributed exactly-once execution. It assumes durable retained inbox state, stable identities/payloads, continued scheduling, and a receiver that eventually becomes available before retry exhaustion. A dead letter does not prove no effect occurred. No automatic reconciliation service is implemented.

Both databases are real and independently committed; transport is an in-process adapter. No HTTP/TCP behavior, broker throughput, power-loss durability, production financial processing, global ordering, or multi-region behavior is claimed. Leases fence sender state, not remote rollback. Dead letters count against capacity; historical disk retention needs an operator policy. See [outbox-protocol.md](outbox-protocol.md) for the complete transaction/failure matrix and complexity discussion.

Current source identity, test counts, and package-build outcomes are recorded in [verification JSON](../reports/verification.json).
