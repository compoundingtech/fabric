# Who wrote this, and who merged it

Every agent on this network, and the principal, push to GitHub as the same
identity. So neither `git log` nor the GitHub UI can say which of us authored a
commit or clicked merge. On 2026-08-23 that question was asked about four merged
pull requests, and the record could not answer it. Only the author's own account
could.

These two conventions do not fix that. They make the record say more than it
otherwise would, and they fail honestly when they are absent.

## The `Agent:` commit trailer

A commit written by an agent carries a trailer naming it:

```
Agent: Silber.fabric
```

It sits with the other trailers this repository already uses, such as `Gates:`.

It answers "who wrote this". It says nothing about who merged, which is a
different question and not one a trailer can answer.

**A missing trailer means unknown, not the principal.** History before this
convention has no trailer, and it is not backfilled: a backfilled attribution
would be a guess written as a fact.

## The merge boundary, carried in the pull request

`.github/pull_request_template.md` states when an agent may merge its own work:
only with the reviewer's approval of that specific pull request, and only when
it is working. Both conditions bind.

Putting it in the template rather than in a message is the point. A convention
that lives in conversation has to be restated to stay alive, and in August 2026
it stopped being restated when a blanket authorisation arrived. Nobody noticed
the stopping. A line in every pull request cannot stop quietly; its absence is
visible.

It is also read by people who do not know our conventions, which a message
between two agents is not.

### The cost of writing a rule into an artefact

The same property cuts the other way, and it bit within two hours. The rule
changed on 2026-08-23, and the template still carried the previous one until it
was edited. A message that goes stale is merely old. **An artefact that goes
stale is confidently wrong**, and it is read by exactly the people who have no
other source.

So this file and the template are part of the rule, not a description of it.
When the rule changes they change in the same pull request, or the next reader
is misled by the thing built to inform them.

## What these do not do

They do not detect a breach on their own. An agent that merges its own work and
does not say so is not caught by either of them, because both depend on the
author being honest.

Detection is a separate, opposite-facing check: a reviewer recording the merges
it performed, and flagging any merge in these repositories that is not in its
record. That fails honestly too — a merge the reviewer did not perform is
flagged as not theirs — and it still cannot tell the principal from an agent.

The real fix is distinct identities per agent, which reaches outside this
repository.
