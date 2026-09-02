# Git remotes over Fabric

Status: proposed on 2026-09-02. No code is implemented by this plan.

## Outcome

Git uses a Fabric peer as a remote without GitHub, SSH, or a public listener.

The remote URL is `fabric://<peer>/<remote>`. The peer is a local Fabric name or a NodeID.

The remote name is a logical name. A caller never supplies a filesystem path to another machine.

The host grants read and write access separately. Every grant names one repository and one immutable peer NodeID.

Peer and remote URL segments use `A-Z`, `a-z`, `0-9`, `.`, `_`, and `-`. Each segment is 64 bytes or less.

Version 1 rejects empty segments, dot segments, percent encoding, user information, ports, queries, and fragments.

Example:

```sh
# On the machine that owns the repository.
fabric git share mandat /path/to/mandat
fabric git grant mandat hetz --read-write
fabric git grant mandat friend --read

# On another trusted machine.
git remote add hetz fabric://hetz/mandat
git fetch hetz
git push hetz HEAD:refs/heads/my-work
```

## Transport choice

Fabric installs `git-remote-fabric` as a link to the `fabric` binary.

Git invokes that helper for every `fabric://` URL. The helper advertises only the standard `connect` capability.

The helper accepts `git-upload-pack` for reads and `git-receive-pack` for writes. It rejects every other service.

The helper carries Git's native full-duplex protocol. Fabric does not list refs, create packs, or interpret Git packets.

The host starts `git upload-pack` or `git receive-pack` with direct arguments. Fabric never invokes a shell.

This design keeps shallow clones, signed pushes, atomic pushes, hooks, object formats, and Git protocol version 2 in Git.

Git documents the helper `connect` capability as a full-duplex path to these two server processes.

On 2026-09-02, Git 2.38.1 invoked `git-remote-fabric` for a test `fabric://` URL.

The command stopped only because that helper does not exist yet. This measurement confirms the selected URL path on the oldest current test host.

## Repository configuration

Fabric stores peer trust, service permissions, and local Git remote declarations in one `peers.toml` file.

Fabric is an allow list. A peer gets exactly the listed services and no other service.

```toml
[[peers]]
id = "<peer-node-id>"
name = "friend"
allow = ["shell", "git/mandat/read", "git/mandat/write"]

[[git_remotes]]
name = "mandat"
path = "/absolute/path/to/mandat/.git"
```

The top-level `[[git_remotes]]` entry stores the host-local path. The matching peer `allow` entry stores each grant.

`fabric git share` asks Git for the repository directory. It stores the canonical absolute directory.

A share starts with no grants. Creating a share alone gives no peer access.

The share command says that no peer has access yet. It prints the exact `fabric git grant` command as the next step.

The grant stays inside the peer entry that contains the immutable NodeID. A local peer rename cannot transfer a grant.

Read and write are independent. A write-only automation peer can receive a push without gaining clone access.

`--read-write` is a convenience that adds both grants. It does not create a third permission.

Granting write permits Git to update refs and run host-controlled receive hooks. The command states that effect before it saves the grant.

A write session advertises ref names and object IDs because Git requires them for a push. It does not permit object fetches.

A write grant covers every ref that the repository accepts. Git configuration and receive hooks remain the branch-policy authority.

The first command set is:

```text
fabric git share <remote> <repository>
fabric git unshare <remote>
fabric git grant <remote> <peer> --read|--write|--read-write
fabric git revoke <remote> <peer> --read|--write|--all
fabric git ls
fabric git status
```

`fabric git ls` shows the stored path, repository type, and effective peer names for each grant.

`fabric git status` also checks Git, the helper link, the daemon, and the configuration file.

## ACL rules

Git permissions use these exact service names:

- `git/<remote>/read`
- `git/<remote>/write`

The remote segment follows the URL segment rules in this plan. Fabric reserves the complete `git/` service namespace.

`fabric expose` rejects a protocol that starts with `git/`. An exposure cannot collide with a repository permission.

Every incoming operation must pass these checks in this order:

1. The NodeID is present in the host's `peers.toml`.
2. The peer's `allow` list contains the exact remote and operation permission.
3. The named top-level `[[git_remotes]]` entry resolves to a host-local repository path.

Every peer has an explicit `allow` list before this work starts. The baseline release migrates all fleet files first.

An absent `allow` field means an empty list. It grants no ordinary service and no Git operation.

`fabric peers` and `fabric doctor` identify a peer with no grants. A denial names the missing grant instead of a network fault.

Only an exact `git/<remote>/<operation>` item permits that Git operation. Read and write never imply each other.

A peer's pty, shell, or exec permission gives no Git access. A Git grant gives no access to those services.

The command validates the remote and peer before it changes the file. It then saves the complete `PeerBook` atomically.

`fabric git revoke` removes only the requested exact items. It does not change ordinary service permissions.

`fabric remove` removes its grants because those grants are part of the removed peer entry. Re-adding the NodeID restores no grant.

`fabric git unshare` removes the declaration and all matching peer permissions in one atomic save.

`fabric git share` refuses to rebind an existing name. An operator must unshare it first, which also removes the old grants.

An unshare or revoke affects new Git sessions. This matches the current Fabric ACL behavior for live tunnels.

## Wire protocol

The daemon advertises one built-in ALPN, `fabric/git/1`.

The helper sends a bounded request with the remote name, the operation, and a valid `GIT_PROTOCOL` value.

The ALPN gate first requires a trusted NodeID. It does not treat the Git ALPN as an ordinary service permission.

After it reads the bounded request, the Git handler checks the exact qualified permission in the peer entry.

The host resolves the logical name through its local configuration. A peer-supplied value never becomes a path or command.

The host checks the exact qualified permission before it starts Git. A denied push cannot run a hook or change a ref.

The host replies `ready` only after the Git child starts. The helper then tells Git that the connection is ready.

Client input flows to the Git child's standard input without whole-pack buffering.

The host frames standard output and standard error as separate channels. The helper writes them to Git and the terminal respectively.

Frames have fixed size limits and bounded queues. A slow receiver applies backpressure instead of growing memory.

The host sets `FABRIC_PEER`, `FABRIC_GIT_REMOTE`, and `FABRIC_GIT_ACCESS` for repository hooks.

The host forwards only a bounded, valid Git protocol request. It does not forward other caller environment variables.

The child uses `kill_on_drop`. A client disconnect terminates the Git child and releases its session permit.

The daemon permits eight Git sessions in total and four from one peer. An excess request gets a clear busy response.

The feature adds no timer, scan, watcher, or idle network work. With no Git operation, it has no recurring cost.

## Failure contract

The helper writes helper control output only to Git. It writes all human text to standard error.

| Condition | Required result |
| --- | --- |
| The local daemon is down | Say that Fabric is not running and exit nonzero. |
| The local daemon is old | Say that the running daemon lacks Git remote support. |
| The remote daemon is old | Say that the peer does not support Git remotes. |
| The peer gives no answer in 10 seconds | Name the peer as unreachable and exit nonzero. Do not retry forever. |
| The remote is absent or hidden by the ACL | Say that the peer did not permit the requested access. |
| The peer has no grants | Say that the peer has no grants and name the required Git grant. |
| The peer has read but a push starts | Name the denied write access and print the host-side grant command. |
| The stored path is unavailable | Say that the granted remote is unavailable on the peer. |
| The Git child cannot start | Name the host-side Git failure. |
| Git rejects a ref or hook | Carry Git's native rejection unchanged. |
| A session limit is full | Say that the Git service is busy and ask the caller to retry. |
| A live connection breaks | End the Git command with an error. A new Git command negotiates again. |

The denial for an unknown remote matches the denial for an ungranted remote. A peer cannot enumerate share names.

A denial has this form: `hetz did not grant write access to Git remote "mandat"`.

It then says: `on hetz, run: fabric git grant mandat <requester> --write`.

The host supplies its local requester label when one exists. Otherwise, it supplies the immutable requester NodeID.

The same instruction is safe for a hidden name. The grant command refuses if the host has not shared that remote.

The host log keeps the precise local cause. It does not write repository content or Git packet data.

## Install and compatibility

`install.sh` creates `git-remote-fabric` beside `fabric` as an idempotent relative link.

`fabric update` preserves or repairs the same link after it replaces the binary.

`fabric git install-helper` repairs a manual installation. It refuses to replace an unrelated file.

The release archive remains one binary. The link always runs the same version as `fabric`.

An old peer does not advertise `fabric/git/1`. The helper reports unsupported instead of unreachable.

The strict baseline release must run on every machine before an operator adds a Git declaration.

The Git release preserves all peer and Git fields on every read and write.

## Implementation sequence

### Pull request 1: repository declarations and ACL

Extend `PeerBook` with Git remote declarations, qualified permissions, validation, and the `fabric git` management commands.

Add exact Git checks. Keep repository access explicit for every peer.

Make all `peers.toml` changes atomic. Reject the reserved `git/` namespace in generic exposures.

This pull request exposes no network service and starts no Git process.

### Pull request 2: helper, wire service, and proof

Add the helper mode, `fabric/git/1`, the bounded session protocol, and the Git child runner.

Add the install link, status checks, README instructions, skill instructions, and the change log entry.

Merge only after the real Git tests and the ACL denial tests pass.

## Verification

Each behavioral test must fail before its implementation lands.

1. Confirm installed Git invokes `git-remote-fabric` for a `fabric://` URL.
2. Test the helper command transcript for `capabilities` and both `connect` services.
3. Property-test ACL decisions across peers, remotes, operations, omitted lists, empty lists, and peer renames.
4. Prove that pty, shell, and exec grants do not grant any Git remote.
5. Start two temporary Fabric nodes and two temporary Git repositories.
6. Clone through `fabric://` with a read grant and compare the exact commit and object hashes.
7. Deny a push from the read-only peer and prove that no remote ref or hook marker changed.
8. Grant write, push a new ref, and compare the exact remote ref and commit hash.
9. Revoke write, retry the push, and prove the denial names the ACL instead of the network.
10. Follow the printed grant command and prove that the same denied Git command now works.
11. Prove that the grant changes no ordinary access and denies every other Git remote.
12. Round-trip mixed peer and Git declarations through every command that saves `peers.toml`.
13. Ask for an unknown remote and prove its client message matches the hidden-remote message.
14. Hold eight sessions, refuse the ninth, and prove echo and shell still answer.
15. Disconnect during a transfer and prove the Git child and every permit leave.
16. Test a peer without Git support, an unreachable peer, and the 10-second bound as three different results.
17. Transfer a pack larger than every frame and prove the bounded path does not retain the pack.
18. Test the installer, updater, manual repair, and refusal to replace an unrelated helper file.
19. Run the full local suite, Nix build, macOS job, and deterministic Linux job.
20. Prove that `fabric peers`, `fabric doctor`, and a denied connection clearly report a peer with no grants.

The live proof uses a temporary bare repository first. It then uses a non-bare test repository and keeps Git's native push rules.

The proof records the transfer size and time window. It compares final Git hashes instead of trusting command exit codes.

## Release and fleet changes

The two pull requests can merge after their specific reviews. A merge changes no running daemon.

Before a release, ask `Silber.cos` which build the fleet runs and obtain the current release gate.

Do not change a live Git share or ACL during deployment. Nathan must approve that separate data-access change.

After approval, deploy to two owned peers before adding Git declarations. Run the temporary proof before any friend receives a grant.

## Rejected designs

Generic `fabric expose --exec` grants a command, not one repository operation. It cannot enforce this ACL safely.

An `ext::` Git URL works but exposes transport commands in every remote URL. It also loses the required `fabric://` interface.

An SSH-compatible command would copy SSH's command parsing and path risks. Fabric already has authenticated peer identity.

Putting repository paths in URLs lets a remote peer select host paths. Logical names remove that authority.

Treating all trusted peers as writers makes a new share public to every trusted friend. Every repository grant stays explicit.

A separate Git configuration file conflicts with the one-file configuration goal. Git declarations and grants stay in `peers.toml`.

Reimplementing pack or ref logic would duplicate Git. The helper `connect` path preserves Git as the protocol authority.

## Primary references

- [Git remote helpers](https://git-scm.com/docs/gitremote-helpers)
- [Git upload-pack](https://git-scm.com/docs/git-upload-pack)
- [Git receive-pack](https://git-scm.com/docs/git-receive-pack)
- [Git protocol version 2](https://git-scm.com/docs/gitprotocol-v2)
