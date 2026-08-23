## Boundary

**An agent merges its own fabric pull request only when both of these hold:**

1. The reviewer has approved **this** pull request. Not a blanket authorisation,
   and not an approval of earlier work. If the reviewer has not approved this
   one, do not merge it.
2. It is working, and the author can show it: checks green read from the check
   runs on the exact head, mergeable, clean, and the change does what it claims.

Both bind. One without the other is not enough.

**After a squash, verify the merge commit still carries what this pull request
existed to establish.** A squash rewrites the message, so the thing the change
was for is the thing most likely to be dropped by the act of landing it.

This applies to fabric. In any other repository the authoring agent reports
ready and stops.

Delete this section only if a human authored the change.

## What this changes

<!-- The fault first, then the fix. Say what breaks without it. -->

## What was verified, and what was not

<!--
Name the platform. A pass on the platform you can see is not a claim about the
one you cannot.

If a test guards a regression, say that you ran it against the broken code and
watched it fail. A test never seen failing is not evidence.
-->
