# Fault scenarios and expected durable state

Run the two suites separately:

```sh
python -m resilient_runtime evaluate --output artifacts/local-faults.json
python -m resilient_runtime outbox-evaluate --output artifacts/delivery-faults.json
```

The [local replay](../replays/evaluation.json) contains 31 scenarios around attempt reservation, commit, lease expiry, cancellation, retry, and restart. Effects, ledger, checkpoint, and commit event share one database transaction; attempts are durable before that transaction.

The [delivery replay](../replays/outbox-evaluation.json) contains 12 scenarios using separate sender and receiver stores. It includes eight concurrent duplicate deliveries, admission races, payload conflicts, outage/restart, stale ACKs, and an actual process exit after receiver commit. [ACK-gap results](ack-gap-results.md) gives the observed counts and state transitions.

The denominators count invariant cases, not delivery reliability under production traffic. Retry exhaustion and uncertain sender outcomes are expected cases and remain in the report. Manual-clock cases avoid sleeping; real threads and child processes exercise SQLite independently. IDs, timestamps, and scheduling order may change between runs.

For a new fault, record the injection stage and the sender/receiver state before and after recovery. Check lease epochs and payload hashes as well as status strings. Do not infer global account conservation, power-loss durability, or remote API behavior from the local checks.
