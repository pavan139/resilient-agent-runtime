# Verified runtime build

[The verification report](../reports/verification.json) records the current source digest, executed tests, environment, and build stages. It exercises both local and outbox commands from an installed wheel outside the checkout.

Behavior checks are under `verification/`; the verifier entry point is `dev/check_runtime.py`. [Failure regressions](failure-regressions.md) identifies the assertions behind the package checks. [Fault scenarios](fault-scenarios.md) separates local transaction cases from the independently committed receiver protocol.
