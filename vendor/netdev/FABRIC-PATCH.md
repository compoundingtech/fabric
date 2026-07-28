# Fabric patch to netdev 0.45.0

This directory vendors the MIT-licensed `netdev` 0.45.0 crate unchanged except
for `src/os/macos/wifi.rs`.

The macOS Wi-Fi transmit-rate lookup is optional metadata, but upstream performs
it through an unbounded synchronous CoreWLAN XPC request during interface
enumeration. Since iroh enumerates interfaces before Fabric binds its control
socket, a wedged CoreWLAN service can otherwise block daemon startup forever.

Fabric runs one bounded probe per interface, returns `None` after 250 ms, and
keeps an in-flight/cache record so network refreshes cannot accumulate blocked
workers. The focused unit tests live beside the patch.

Tracked as https://github.com/compoundingtech/fabric/issues/20. Remove this
vendor patch when upstream provides equivalent bounded behavior.
