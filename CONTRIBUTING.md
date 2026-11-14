# Working on transaction boundaries

Name the failure boundary before changing the runtime: attempt reservation, local effect commit, receiver commit, or sender acknowledgement. A test should state which rows and balances survive a crash at that boundary.

Use a separate SQLite connection for each worker. Keep the local ledger/checkpoint transaction and receiver inbox/effect transaction distinct. A receiver error can occur after an effect committed, so retry logic must preserve the same identity and payload. Do not turn an uncertain ACK into a claim of no effect.

Run `python -m unittest discover -s verification -v`, then `python dev/check_runtime.py --require-lint --wheel`. The suite contains real child-process exits and concurrent deliveries; keep their timeouts bounded and their cleanup deterministic.

When adding a fault case, include its expected durable rows, attempt count, lease epoch, and restart outcome. The [ACK-gap results](operations/ack-gap-results.md) show how the current protocol records duplicates and uncertainty. Raw hook exception messages must remain out of exported state.

Changing retry ceilings, admission reservations, or cancellation semantics needs a corresponding update to the [outbox protocol](operations/outbox-protocol.md). Keep example balances fictional and document any new transport assumptions.
