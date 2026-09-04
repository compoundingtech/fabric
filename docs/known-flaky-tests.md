# Known flaky tests

There are no known flaky tests as of 2026-09-04. Treat every test failure as a
real failure. Do not rerun a failed job as a substitute for diagnosis.

A test can enter this list only after a failure is reproduced without the
suspected change. A test that passes a few hundred attempts has the wrong
failure hypothesis. Record the attempted count and the load shape before a test
is classified as flaky.

## Retired classifications

Three daemon-slice tests were listed here on 2026-08-30. The list attributed
all three to CI load before that common cause was proved.

`a_peer_not_permitted_for_a_service_cannot_reach_it` failed in CI on 2026-08-29
before the later tunnel and mux recovery repairs. It passed 300 of 300 attempts
on unchanged main under eight-way and ten-way process load on 2026-09-04.

`exec_expose_reconnect_keeps_child_bound_to_tunnel_session` also failed in CI
on 2026-08-29 before those repairs. It passed 300 of 300 attempts on unchanged
main under twelve-way process load on 2026-09-04.

`production_status_exposes_exact_inbound_scan_ledger` failed on current sync
code on 2026-09-03. One file write caused both its watcher and its explicit
reload to send valid inbound transactions. The assertion expected one guarded
transaction and observed two. The test now gives each phase an exact trigger:
two explicit no-op reloads must add exactly two no-op transactions, and one
watcher mutation must add exactly one guarded transaction.

These results reject the shared short-deadline theory. If either transport test
fails again, diagnose the new failure instead of assigning the old label.
