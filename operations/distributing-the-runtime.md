# Distributing the runtime

A source release should include the workflow and receiver code, the failure fixtures, and the replay reports that support its transaction claims. The SQLite files produced by a run are temporary state, not release artifacts.

```sh
python -m pip install -e . -r requirements-dev.txt
python dev/check_runtime.py --require-lint --wheel
python -m resilient_runtime outbox-evaluate --output artifacts/delivery-cases.json
```

Before sharing changes, inspect both a successful delivery and the final-attempt ACK-loss case. The latter must retain a committed receiver effect even when the sender ends with a dead letter. A passing test count cannot substitute for that protocol distinction.

The package wheel needs to contain the outbox modules as well as the local runtime. The verifier checks the installed CLI outside the checkout. CI calls the same relocated entry point and uploads `reports/verification.json`; a local report is not a remote CI run.

Document receiver inbox retention, sender identity lifetime, queue capacity, retry ceilings, and any transport added by an application. The current transport is in-process. Network authentication, reconciliation procedures, retention automation, and service operation remain application responsibilities.
