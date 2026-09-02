//! What is wrong with this machine, in words a stranger can act on.
//!
//! # Three verdicts, and why the third exists
//!
//! `Ok`, `Problem`, and `Unknown`. The third is the important one. A doctor is
//! trusted INSTEAD of investigating, so a check that says `ok` when it could not
//! look is worse than one that admits it — the person stops looking and the
//! fault stays. Four separate checks did exactly that this week, and every one
//! of them erred toward "fine".
//!
//! So `Unknown` is first class and it counts as needing attention.
//!
//! # `Setup` is not a fault
//!
//! A machine that installed fabric five minutes ago has nothing wrong with it.
//! Reporting a wall of red on the first run teaches a person to ignore the tool,
//! and this is the page a stranger meets first.
//!
//! The discriminator is not a softer verdict, it is a different question:
//! **absence of EVERYTHING is setup, absence of one thing among many is a
//! problem.** A machine with no key, no peers and no service has never been
//! configured. A machine with a key and peers but no service was configured and
//! something is missing.
//!
//! The exit code stays honest either way — an unconfigured machine is not ready,
//! and a script asking "is this ready" gets told no. Only the words change, and
//! the words are what a person reads.
//!
//! # It never fixes anything
//!
//! It says what is wrong and what to type. A tool that changes things is a
//! different tool with a different risk profile.

use std::path::{Path, PathBuf};

/// How a single check came out.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Verdict {
    Ok,
    /// Not done yet on a machine that has never been configured.
    Setup,
    Problem,
    /// The check could not establish an answer. Counts as needing attention,
    /// because a guess in the reassuring direction is the failure this tool
    /// exists to prevent.
    Unknown,
}

impl Verdict {
    pub fn needs_attention(self) -> bool {
        !matches!(self, Verdict::Ok)
    }

    pub fn token(self) -> &'static str {
        match self {
            Verdict::Ok => "ok",
            Verdict::Setup => "setup",
            Verdict::Problem => "problem",
            Verdict::Unknown => "unknown",
        }
    }
}

/// One line of the report.
#[derive(Debug, Clone)]
pub struct Finding {
    pub check: String,
    pub verdict: Verdict,
    /// What is true, in a sentence.
    pub detail: String,
    /// What to type next. Present whenever there is something to do.
    pub action: Option<String>,
}

impl Finding {
    fn new(check: &str, verdict: Verdict, detail: impl Into<String>) -> Self {
        Self {
            check: check.to_string(),
            verdict,
            detail: detail.into(),
            action: None,
        }
    }

    fn with_action(mut self, action: impl Into<String>) -> Self {
        self.action = Some(action.into());
        self
    }
}

/// What a peer looks like from here.
#[derive(Debug, Clone)]
pub struct PeerFact {
    pub label: String,
    pub has_address: bool,
    /// Whether this peer has at least one incoming service grant here.
    /// `None` means the peer configuration could not be read.
    pub has_grants: Option<bool>,
    /// `None` when the check could not run at all.
    pub reachable: Option<bool>,
    /// What the peer reports as its version, if it could be asked.
    pub version: Option<String>,
    /// Why it could not be asked. A count of peers you could not reach is not
    /// something a stranger can act on; the name and the reason are.
    pub version_error: Option<String>,
}

/// What one sync entry looks like.
#[derive(Debug, Clone)]
pub struct SyncFact {
    pub name: String,
    pub folder: PathBuf,
    pub folder_exists: bool,
    pub drift_clean: bool,
    /// Paths the last scan saw but could not read as syncable files.
    pub scan_issues: Vec<(String, String)>,
    /// Peers this entry has stopped syncing with, and why.
    pub stopped: Vec<(String, String)>,
}

/// What the certificate authority looks like.
#[derive(Debug, Clone)]
pub struct CaFact {
    pub present: bool,
    /// `None` when trust could not be determined.
    pub installed: Option<bool>,
    /// The key's permission bits, when it exists.
    pub key_mode: Option<u32>,
    /// The sync entry whose folder contains the key, if any. This is the one
    /// that must never happen.
    pub key_inside_sync_entry: Option<String>,
}

/// Everything the checks reason about. Gathered by IO, then handed to pure code.
#[derive(Debug, Clone)]
pub struct Facts {
    pub has_identity: bool,
    /// Does an OS service apply to this home at all?
    ///
    /// `service install` is prod-only, so for a `--home` that is not the default
    /// state root there is no unit to look for. The unit path is per-USER, not
    /// per-home, so without this a fresh `--home` on a configured machine sees
    /// the prod plist and reports itself as configured. That turned the whole
    /// first-run report red, which is the one thing this design was for.
    pub manages_service: bool,
    pub daemon_running: bool,
    /// Whether the managed service will start on boot, not merely whether its
    /// unit file exists. Presence is not enablement.
    pub service: ServiceEnablement,
    pub own_version: String,
    pub peers: Vec<PeerFact>,
    pub syncs: Vec<SyncFact>,
    pub ca: CaFact,
}

impl Facts {
    /// Has this machine ever been configured?
    ///
    /// No identity, no peers, and no service. Anything else means somebody set
    /// something up, so a gap is a fault rather than a next step.
    fn never_configured(&self) -> bool {
        !self.has_identity
            && self.peers.is_empty()
            && !(self.manages_service
                && matches!(
                    self.service,
                    ServiceEnablement::Enabled | ServiceEnablement::PresentNotEnabled
                ))
    }
}

/// Turn facts into findings. Pure, so every outcome is testable.
pub fn diagnose(facts: &Facts) -> Vec<Finding> {
    let fresh = facts.never_configured();
    let mut out = Vec::new();

    out.push(if facts.has_identity {
        Finding::new("identity", Verdict::Ok, "this machine has a key")
    } else {
        Finding::new(
            "identity",
            if fresh { Verdict::Setup } else { Verdict::Problem },
            "this machine has no key, so no peer can recognise it",
        )
        .with_action("fabric key generate")
    });

    out.push(if !facts.manages_service {
        Finding::new(
            "service",
            Verdict::Ok,
            "this home is not the managed one, so no OS service applies to it",
        )
    } else {
        match facts.service {
        ServiceEnablement::Enabled => {
            Finding::new("service", Verdict::Ok, "installed, enabled, and will start on boot")
        }
        // The unit file exists but the manager will not start it on boot. This
        // is NOT the same as "installed": a reboot leaves no daemon. It reads
        // differently from "not installed" because the repair differs.
        ServiceEnablement::PresentNotEnabled => Finding::new(
            "service",
            if fresh { Verdict::Setup } else { Verdict::Problem },
            "the service is installed but not enabled, so it will not start after a reboot",
        )
        .with_action("fabric service install"),
        ServiceEnablement::NotInstalled => Finding::new(
            "service",
            if fresh { Verdict::Setup } else { Verdict::Problem },
            "fabric is not installed as a service, so it will not come back after a reboot",
        )
        .with_action("fabric service install"),
        ServiceEnablement::Unknown => Finding::new(
            "service",
            Verdict::Unknown,
            "could not tell whether fabric is enabled as a service",
        ),
        }
    });

    out.push(if facts.daemon_running {
        Finding::new("daemon", Verdict::Ok, "running and answering")
    } else if fresh {
        Finding::new("daemon", Verdict::Setup, "not running yet").with_action("fabric up")
    } else {
        Finding::new(
            "daemon",
            Verdict::Problem,
            "not running, so nothing on this machine is reachable by a peer",
        )
        .with_action("fabric up")
    });

    if facts.peers.is_empty() {
        out.push(
            Finding::new(
                "peers",
                if fresh { Verdict::Setup } else { Verdict::Problem },
                "no peers are trusted, so there is nobody to reach",
            )
            .with_action("fabric add <their node id> <a name for them>"),
        );
    } else {
        for peer in &facts.peers {
            out.push(peer_finding(peer));
        }
        out.extend(version_findings(facts));
    }

    for sync in &facts.syncs {
        out.extend(sync_findings(sync));
    }

    out.extend(ca_findings(&facts.ca));
    out
}

fn peer_finding(peer: &PeerFact) -> Finding {
    let label = &peer.label;
    if peer.has_grants == Some(false) {
        return Finding::new(
            "peer",
            Verdict::Problem,
            format!("{label} has no grants on this machine, so it cannot reach any service"),
        )
        .with_action(format!(
            "edit peers.toml and add the required services to {label}'s allow list"
        ));
    }
    match peer.reachable {
        Some(true) => Finding::new("peer", Verdict::Ok, format!("{label} is reachable")),
        Some(false) if !peer.has_address => Finding::new(
            "peer",
            Verdict::Problem,
            format!("{label} is trusted but fabric does not know where it is"),
        )
        .with_action(format!(
            "fabric add <its node id> {label} --addr-json '<its addr, from `fabric addr` there>'"
        )),
        Some(false) => Finding::new(
            "peer",
            Verdict::Problem,
            format!("{label} is trusted and has an address, but is not answering"),
        )
        .with_action(format!(
            "check that fabric is running there: fabric exec {label} -- fabric --version"
        )),
        None => Finding::new(
            "peer",
            Verdict::Unknown,
            format!("could not test whether {label} is reachable"),
        ),
    }
}

/// A fleet on mixed builds runs the degraded path and says nothing about it.
///
/// This check exists because that cost two days: one old peer among three was
/// producing 99.996% of the wire traffic, and every counter read healthy.
fn version_findings(facts: &Facts) -> Vec<Finding> {
    let mut behind: Vec<&PeerFact> = Vec::new();
    let mut unknown: Vec<&PeerFact> = Vec::new();
    for peer in &facts.peers {
        match &peer.version {
            Some(version) if version != &facts.own_version => behind.push(peer),
            Some(_) => {}
            // A peer that is simply not reachable is already reported by its own
            // check; saying it twice is noise, not information.
            None if peer.reachable == Some(false) => {}
            None => unknown.push(peer),
        }
    }
    let mut out = Vec::new();
    if !behind.is_empty() {
        let names: Vec<&str> = behind.iter().map(|p| p.label.as_str()).collect();
        out.push(
            Finding::new(
                "versions",
                Verdict::Problem,
                format!(
                    "{} is on a different build from this machine ({}). A mixed fleet \
                     falls back to sending whole manifests, which is slow and silent",
                    names.join(", "),
                    facts.own_version
                ),
            )
            .with_action(format!("fabric exec {} -- fabric update", names[0])),
        );
    } else if !unknown.is_empty() {
        // Say what IS known as well. On the fleet's first real run this
        // reported one unknown peer and nothing else, so a reader could not
        // tell "one of three" from "one of one" — and the reassuring half is
        // the half that says how much of the fleet was actually checked.
        let known = facts.peers.len() - unknown.len();
        if known > 0 {
            out.push(Finding::new(
                "versions",
                Verdict::Ok,
                format!(
                    "{known} of {} peers answered, and all of them are on {}",
                    facts.peers.len(),
                    facts.own_version
                ),
            ));
        }
        for peer in unknown {
            let reason = peer
                .version_error
                .clone()
                .unwrap_or_else(|| "it did not answer".to_string());
            let mut finding = Finding::new(
                "versions",
                Verdict::Unknown,
                format!(
                    "could not ask {} which build it is on: {reason}. A peer on an \
                     older build makes the whole fleet send whole manifests, and \
                     nothing else reports that",
                    peer.label
                ),
            );
            if reason.contains("exec is disabled") {
                finding = finding.with_action(format!(
                    "on {}, run: fabric service install --allow-exec",
                    peer.label
                ));
            }
            out.push(finding);
        }
    } else {
        out.push(Finding::new(
            "versions",
            Verdict::Ok,
            format!("every peer is on {}", facts.own_version),
        ));
    }
    out
}

fn sync_findings(sync: &SyncFact) -> Vec<Finding> {
    let name = &sync.name;
    let mut out = Vec::new();

    if !sync.folder_exists {
        // Deliberately silent in the engine: a folder that vanished must not be
        // read as "everything was deleted". Correct, and invisible, which is
        // exactly why it belongs here.
        out.push(
            Finding::new(
                "sync",
                Verdict::Problem,
                format!(
                    "{name} points at {} , which does not exist. fabric will not \
                     replicate anything for this entry and will not complain, \
                     because a folder that disappears must never be read as a \
                     mass delete",
                    sync.folder.display()
                ),
            )
            .with_action(format!(
                "create {} , or fix the folder in syncs.toml",
                sync.folder.display()
            )),
        );
        return out;
    }

    for (path, reason) in &sync.scan_issues {
        let finding = if reason == "too-large" {
            Finding::new(
                "sync",
                Verdict::Problem,
                format!(
                    "{name} cannot sync {path}: the file exceeds 512 MiB. fabric still treats it as present"
                ),
            )
            .with_action(format!(
                "reduce {path} to 512 MiB or less, or exclude it from {name}"
            ))
        } else {
            Finding::new(
                "sync",
                Verdict::Unknown,
                format!(
                    "{name} cannot read {path} as a syncable file. fabric will not treat it as deleted"
                ),
            )
            .with_action(format!("make {path} a readable regular file, or exclude it from {name}"))
        };
        out.push(finding);
    }

    for (peer, reason) in &sync.stopped {
        // Denied waits for a person; unreachable waits for the network. Telling
        // them apart is the difference between a chore and weather.
        let finding = if reason == "unknown" && peer == "*" {
            Finding::new(
                "sync",
                Verdict::Problem,
                format!(
                    "{name} selects every trusted peer and this machine has no trusted \
                     peer, so it is syncing with nobody"
                ),
            )
            .with_action("fabric add <nodeid> <name> for at least one peer")
        } else if reason == "unknown" {
            Finding::new(
                "sync",
                Verdict::Problem,
                format!(
                    "{name} names a peer {peer} that is not in peers.toml, so it is \
                     syncing with nobody by that name. This will not fix itself"
                ),
            )
            .with_action(format!(
                "fix the name in syncs.toml, or `fabric add <nodeid> {peer}`"
            ))
        } else if reason == "denied" {
            Finding::new(
                "sync",
                Verdict::Problem,
                format!(
                    "{name} has stopped syncing with {peer}: it is not permitted to. \
                     This will not fix itself"
                ),
            )
            .with_action(format!(
                "on {peer}, add `sync` to this machine's allow list in peers.toml"
            ))
        } else if reason == "missing-entry" {
            Finding::new(
                "sync",
                Verdict::Problem,
                format!(
                    "{name} has stopped syncing with {peer}: {peer} has no local sync entry named {name}"
                ),
            )
            .with_action(format!(
                "on {peer}, add {name} to syncs.toml or fix the shared entry name"
            ))
        } else if reason == "too-large" {
            Finding::new(
                "sync",
                Verdict::Problem,
                format!("{name} has stopped syncing with {peer}: sync content exceeds 512 MiB"),
            )
            .with_action(format!(
                "reduce the large file to 512 MiB or less, or exclude it from {name}"
            ))
        } else {
            Finding::new(
                "sync",
                Verdict::Problem,
                format!("{name} has stopped syncing with {peer}: {peer} is unreachable"),
            )
            .with_action(format!("this usually clears when {peer} comes back"))
        };
        out.push(finding);
    }

    if !sync.drift_clean {
        out.push(Finding::new(
            "sync",
            Verdict::Problem,
            format!("{name} has drifted: what is on disk does not match what fabric recorded"),
        ));
    }

    if sync.stopped.is_empty() && sync.scan_issues.is_empty() && sync.drift_clean {
        out.push(Finding::new(
            "sync",
            Verdict::Ok,
            format!("{name} is clean and syncing with every peer"),
        ));
    }
    out
}

fn ca_findings(ca: &CaFact) -> Vec<Finding> {
    let mut out = Vec::new();
    if !ca.present {
        // NOT a problem, and not a setup step either. Installing trust nobody
        // needs is the wrong advice, and today nothing needs it.
        out.push(Finding::new(
            "ca",
            Verdict::Ok,
            "no certificate authority, which is normal. One is only needed for \
             https on a fabric name or for a listener a phone has to reach",
        ));
        return out;
    }

    // The one that must never happen.
    if let Some(entry) = &ca.key_inside_sync_entry {
        out.push(
            Finding::new(
                "ca",
                Verdict::Problem,
                format!(
                    "THE AUTHORITY'S PRIVATE KEY IS INSIDE THE SYNCED FOLDER FOR \
                     {entry}. It will be copied to every peer, and any machine \
                     that receives it can sign certificates this one trusts"
                ),
            )
            .with_action(
                "stop that entry or narrow its include, then delete the key and run \
                 `fabric ca init` again",
            ),
        );
    }

    match ca.key_mode {
        Some(mode) if mode & 0o077 != 0 => {
            out.push(
                Finding::new(
                    "ca",
                    Verdict::Problem,
                    format!(
                        "the authority's private key is mode {mode:o}, so somebody \
                         other than its owner can read it and sign certificates \
                         this machine trusts"
                    ),
                )
                .with_action("chmod 600 the key, or delete it and run `fabric ca init` again"),
            );
        }
        Some(_) => {}
        None => out.push(Finding::new(
            "ca",
            Verdict::Unknown,
            "could not read the authority key's permissions",
        )),
    }

    out.push(match ca.installed {
        // Present but not trusted is the normal resting state.
        Some(false) => Finding::new(
            "ca",
            Verdict::Ok,
            "an authority exists and this machine does not trust it yet, which is \
             fine until something needs a certificate",
        ),
        Some(true) => Finding::new(
            "ca",
            Verdict::Ok,
            "an authority exists and this machine trusts it",
        ),
        None => Finding::new(
            "ca",
            Verdict::Unknown,
            "could not tell whether this machine trusts the authority",
        ),
    });
    out
}

/// How the whole run came out.
pub fn exit_code(findings: &[Finding]) -> i32 {
    if findings.iter().any(|f| f.verdict.needs_attention()) {
        1
    } else {
        0
    }
}

/// The opening line, which is different on a machine that is simply new.
pub fn opening(facts: &Facts) -> Option<String> {
    if facts.never_configured() {
        Some(
            "fabric is not set up on this machine yet. Nothing is wrong; this is \
             where every machine starts. The steps marked `setup` below are what \
             is left to do."
                .to_string(),
        )
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn configured() -> Facts {
        Facts {
            has_identity: true,
            manages_service: true,
            daemon_running: true,
            service: ServiceEnablement::Enabled,
            own_version: "0.2.0+abc".to_string(),
            peers: vec![PeerFact {
                label: "hetz".to_string(),
                has_address: true,
                has_grants: Some(true),
                reachable: Some(true),
                version: Some("0.2.0+abc".to_string()),
                version_error: None,
            }],
            syncs: vec![SyncFact {
                name: "bus".to_string(),
                folder: PathBuf::from("/tmp/bus"),
                folder_exists: true,
                drift_clean: true,
                scan_issues: Vec::new(),
                stopped: Vec::new(),
            }],
            ca: CaFact {
                present: false,
                installed: None,
                key_mode: None,
                key_inside_sync_entry: None,
            },
        }
    }

    fn find<'a>(findings: &'a [Finding], check: &str) -> Vec<&'a Finding> {
        findings.iter().filter(|f| f.check == check).collect()
    }

    #[test]
    fn a_healthy_machine_reports_nothing_to_do() {
        let findings = diagnose(&configured());
        let attention: Vec<&Finding> = findings
            .iter()
            .filter(|f| f.verdict.needs_attention())
            .collect();
        assert!(
            attention.is_empty(),
            "a healthy machine reported work to do: {attention:?}"
        );
        assert_eq!(exit_code(&findings), 0);
        assert!(opening(&configured()).is_none());
    }

    #[test]
    fn a_peer_with_no_grants_is_an_actionable_problem() {
        let mut facts = configured();
        facts.peers[0].has_grants = Some(false);

        let findings = diagnose(&facts);
        let peer = find(&findings, "peer")[0];

        assert_eq!(peer.verdict, Verdict::Problem);
        assert!(peer.detail.contains("no grants"), "wrong detail: {}", peer.detail);
        assert!(
            peer.action
                .as_deref()
                .is_some_and(|action| action.contains("peers.toml")),
            "the finding did not say where to add a grant: {:?}",
            peer.action
        );
    }

    /// The first run. Nothing is wrong, and the words have to say so, or a
    /// person learns to ignore the tool at the only moment it had their
    /// attention.
    #[test]
    fn a_brand_new_machine_reads_as_setup_not_as_broken() {
        let facts = Facts {
            has_identity: false,
            manages_service: true,
            daemon_running: false,
            service: ServiceEnablement::NotInstalled,
            own_version: "0.2.0+abc".to_string(),
            peers: Vec::new(),
            syncs: Vec::new(),
            ca: CaFact {
                present: false,
                installed: None,
                key_mode: None,
                key_inside_sync_entry: None,
            },
        };
        let findings = diagnose(&facts);
        assert!(
            findings
                .iter()
                .all(|f| matches!(f.verdict, Verdict::Setup | Verdict::Ok)),
            "a new machine reported problems: {:?}",
            findings
                .iter()
                .filter(|f| f.verdict == Verdict::Problem)
                .collect::<Vec<_>>()
        );
        assert!(
            opening(&facts).is_some_and(|line| line.contains("Nothing is wrong")),
            "the first run does not say that nothing is wrong"
        );
        // Still not READY, and a script asking that deserves the truth.
        assert_eq!(exit_code(&findings), 1);
    }

    /// The unit path is per-USER, not per-home, so a fresh `--home` on a
    /// configured machine finds the prod plist.
    ///
    /// This is the case my first fresh-machine test could not produce: it set
    /// the facts by hand and the real gatherer could not reach that combination
    /// here. The binary reported three problems on an empty home, which is the
    /// exact first-run wall of red the design exists to avoid. Assert against
    /// the combination the world actually hands us.
    #[test]
    fn a_fresh_unmanaged_home_beside_an_installed_service_is_still_setup() {
        let facts = Facts {
            has_identity: false,
            // Not the prod home, so no OS service applies to it...
            manages_service: false,
            // ...but the unit is on disk for the prod home, and this is exactly
            // what the gatherer sees.
            service: ServiceEnablement::Enabled,
            daemon_running: false,
            own_version: "0.2.0+abc".to_string(),
            peers: Vec::new(),
            syncs: Vec::new(),
            ca: CaFact {
                present: false,
                installed: None,
                key_mode: None,
                key_inside_sync_entry: None,
            },
        };
        let findings = diagnose(&facts);
        assert!(
            findings
                .iter()
                .all(|f| matches!(f.verdict, Verdict::Setup | Verdict::Ok)),
            "a fresh home reported problems: {:?}",
            findings
                .iter()
                .filter(|f| f.verdict == Verdict::Problem)
                .collect::<Vec<_>>()
        );
        assert!(opening(&facts).is_some(), "it did not open by saying it is new");
    }

    /// The same gap on a machine that WAS configured is a fault, not a step.
    #[test]
    fn the_same_gap_on_a_configured_machine_is_a_problem() {
        let mut facts = configured();
        facts.service = ServiceEnablement::NotInstalled;
        let findings = diagnose(&facts);
        let service = find(&findings, "service");
        assert_eq!(service[0].verdict, Verdict::Problem);
        assert!(service[0].action.is_some(), "no action was offered");
    }

    /// A check that could not look must never say ok.
    #[test]
    fn a_check_that_could_not_look_says_unknown_and_counts() {
        let mut facts = configured();
        facts.service = ServiceEnablement::Unknown;
        facts.peers[0].reachable = None;
        facts.ca.present = true;
        facts.ca.installed = None;
        facts.ca.key_mode = Some(0o600);

        let findings = diagnose(&facts);
        for check in ["service", "peer", "ca"] {
            assert!(
                find(&findings, check)
                    .iter()
                    .any(|f| f.verdict == Verdict::Unknown),
                "{check} reported something other than unknown when it could not look"
            );
        }
        assert_eq!(exit_code(&findings), 1, "unknown must count as attention");
    }

    /// Finding 3 of the 2026-08-29 review. A peer named in `syncs.toml` that is
    /// not in `peers.toml` is a third kind of stopped: not a refusal, not the
    /// network, a name nobody answers to. It needs a person to fix a file, and
    /// it must not be reported as "unreachable ... clears when it comes back",
    /// because it never will.
    #[test]
    fn a_sync_peer_that_resolves_to_nobody_is_named_as_such() {
        let mut facts = configured();
        facts.syncs[0].stopped = vec![("hetzner".to_string(), "unknown".to_string())];
        let findings = diagnose(&facts);
        let syncs = find(&findings, "sync");
        let unknown = syncs
            .iter()
            .find(|f| f.detail.contains("hetzner"))
            .expect("no finding for the unknown peer");
        assert_eq!(unknown.verdict, Verdict::Problem);
        assert!(
            unknown.detail.contains("peers.toml") && !unknown.detail.contains("unreachable"),
            "an unknown peer was described as a network fault: {}",
            unknown.detail
        );
        assert!(
            unknown
                .action
                .as_deref()
                .is_some_and(|a| a.contains("syncs.toml") || a.contains("fabric add")),
            "the action must point at the file to fix: {:?}",
            unknown.action
        );
        assert!(
            !syncs.iter().any(|f| f.detail.contains("syncing with every peer")),
            "an entry syncing with nobody was still called clean and syncing with every peer"
        );

        // The wildcard form: nothing trusted at all.
        facts.syncs[0].stopped = vec![("*".to_string(), "unknown".to_string())];
        let findings = diagnose(&facts);
        let syncs = find(&findings, "sync");
        assert!(
            syncs
                .iter()
                .any(|f| f.verdict == Verdict::Problem && f.detail.contains("no trusted peer")),
            "a wildcard with no peers was not named: {:?}",
            syncs.iter().map(|f| &f.detail).collect::<Vec<_>>()
        );
    }

    /// Finding 12: doctor must ask a peer its version over the home it was
    /// invoked with, or on a non-default home it asks the wrong daemon and
    /// reports `unknown peer` for a peer that is fine.
    #[test]
    fn peer_version_carries_the_home_flag_before_exec() {
        let home = FabricHome::new(std::path::Path::new("/srv/fabric"));
        let argv = peer_version_argv(&home, "hetz");
        let home_at = argv.iter().position(|a| a == "--home").expect("no --home flag");
        assert_eq!(argv.get(home_at + 1).map(String::as_str), Some("/srv/fabric"));
        let exec_at = argv.iter().position(|a| a == "exec").expect("no exec verb");
        assert!(
            home_at < exec_at,
            "--home must come before the exec subcommand or clap rejects it: {argv:?}"
        );
        // And it still runs the version query it describes.
        let dashes = argv.iter().position(|a| a == "--").expect("no argv separator");
        assert_eq!(&argv[dashes + 1..], ["fabric".to_string(), "--version".to_string()]);
        assert_eq!(argv.get(exec_at + 1).map(String::as_str), Some("hetz"));
    }

    /// Finding 10: a unit file left in place by a `disable` is NOT "installed
    /// and managed by the OS". It will not start after a reboot, and it reads
    /// as a problem with a different repair from "never installed".
    #[test]
    fn a_present_but_not_enabled_service_is_a_problem_not_ok() {
        let mut facts = configured();
        facts.service = ServiceEnablement::PresentNotEnabled;
        let findings = diagnose(&facts);
        let service = find(&findings, "service");
        assert!(
            service
                .iter()
                .any(|f| f.verdict == Verdict::Problem && f.detail.contains("not enabled")),
            "a disabled-but-present service did not read as a problem: {:?}",
            service.iter().map(|f| (&f.verdict, &f.detail)).collect::<Vec<_>>()
        );
        assert!(
            !service.iter().any(|f| f.detail.contains("installed, enabled")),
            "a disabled service was still called enabled"
        );

        // And the healthy case still reads Ok, so the test is about the state
        // and not about the check being broken.
        facts.service = ServiceEnablement::Enabled;
        let findings = diagnose(&facts);
        assert!(
            find(&findings, "service")
                .iter()
                .any(|f| f.verdict == Verdict::Ok && f.detail.contains("will start on boot"))
        );
    }

    #[test]
    fn a_denied_sync_and_an_unreachable_one_read_differently() {
        let mut facts = configured();
        facts.syncs[0].stopped = vec![
            ("droppy".to_string(), "denied".to_string()),
            ("hetz".to_string(), "unreachable".to_string()),
        ];
        let findings = diagnose(&facts);
        let syncs = find(&findings, "sync");
        let denied = syncs
            .iter()
            .find(|f| f.detail.contains("droppy"))
            .expect("no finding for the denied peer");
        let unreachable = syncs
            .iter()
            .find(|f| f.detail.contains("hetz"))
            .expect("no finding for the unreachable peer");

        assert!(
            denied.detail.contains("will not fix itself"),
            "a denied sync did not say a person has to act: {}",
            denied.detail
        );
        assert!(
            denied.action.as_deref().is_some_and(|a| a.contains("peers.toml")),
            "a denied sync did not say what to edit"
        );
        assert!(
            unreachable
                .action
                .as_deref()
                .is_some_and(|a| a.contains("comes back")),
            "an unreachable peer was reported as a chore rather than weather"
        );
    }

    #[test]
    fn local_sync_faults_do_not_read_as_unreachable() {
        let mut facts = configured();
        facts.syncs[0].stopped = vec![
            ("droppy".to_string(), "missing-entry".to_string()),
            ("hetz".to_string(), "too-large".to_string()),
        ];
        let findings = diagnose(&facts);
        let syncs = find(&findings, "sync");

        for peer in ["droppy", "hetz"] {
            let finding = syncs
                .iter()
                .find(|finding| finding.detail.contains(peer))
                .expect("no finding for the local sync fault");
            assert!(!finding.detail.contains("unreachable"));
            assert!(
                !finding
                    .action
                    .as_deref()
                    .unwrap_or_default()
                    .contains("comes back")
            );
        }
        assert!(
            syncs
                .iter()
                .find(|finding| finding.detail.contains("droppy"))
                .unwrap()
                .action
                .as_deref()
                .is_some_and(|action| action.contains("syncs.toml"))
        );
        assert!(
            syncs
                .iter()
                .find(|finding| finding.detail.contains("hetz"))
                .unwrap()
                .action
                .as_deref()
                .is_some_and(|action| action.contains("512 MiB"))
        );
    }

    #[test]
    fn an_oversized_local_path_is_named_and_is_not_called_clean() {
        let mut facts = configured();
        facts.syncs[0].scan_issues = vec![("archive.bin".into(), "too-large".into())];
        let findings = diagnose(&facts);
        let syncs = find(&findings, "sync");
        let issue = syncs
            .iter()
            .find(|finding| finding.detail.contains("archive.bin"))
            .expect("the scan issue did not name its path");
        assert!(issue.detail.contains("still treats it as present"));
        assert!(
            issue
                .action
                .as_deref()
                .is_some_and(|action| action.contains("512 MiB"))
        );
        assert!(
            !syncs
                .iter()
                .any(|finding| finding.detail.contains("clean and syncing"))
        );
    }

    /// The engine treats a missing folder as "wait", deliberately, so nothing
    /// else will ever mention it.
    #[test]
    fn a_sync_entry_pointing_at_nothing_is_reported() {
        let mut facts = configured();
        facts.syncs[0].folder_exists = false;
        let findings = diagnose(&facts);
        let sync = find(&findings, "sync");
        assert_eq!(sync[0].verdict, Verdict::Problem);
        assert!(
            sync[0].detail.contains("does not exist"),
            "the missing folder was not named: {}",
            sync[0].detail
        );
        assert!(
            sync[0].detail.contains("will not complain"),
            "it did not explain why nothing else told them"
        );
    }

    /// The worst failure available here, and nothing else would notice it.
    #[test]
    fn a_private_key_inside_a_synced_folder_is_reported_loudly() {
        let mut facts = configured();
        facts.ca = CaFact {
            present: true,
            installed: Some(false),
            key_mode: Some(0o600),
            key_inside_sync_entry: Some("st2-declarations-default".to_string()),
        };
        let findings = diagnose(&facts);
        let ca = find(&findings, "ca");
        let leaked = ca
            .iter()
            .find(|f| f.detail.contains("SYNCED FOLDER"))
            .expect("a key inside a synced folder was not reported");
        assert_eq!(leaked.verdict, Verdict::Problem);
        assert!(
            leaked.detail.contains("every peer"),
            "it did not say what the consequence is"
        );
        assert!(leaked.action.is_some(), "it did not say what to do");
    }

    #[test]
    fn a_key_readable_by_others_is_a_problem() {
        let mut facts = configured();
        facts.ca = CaFact {
            present: true,
            installed: Some(true),
            key_mode: Some(0o644),
            key_inside_sync_entry: None,
        };
        let findings = diagnose(&facts);
        assert!(
            find(&findings, "ca")
                .iter()
                .any(|f| f.verdict == Verdict::Problem && f.detail.contains("644")),
            "a world-readable authority key was not reported"
        );
    }

    /// An authority that exists and is not trusted is the resting state, not a
    /// prompt to install trust nobody needs.
    #[test]
    fn an_uninstalled_authority_is_not_a_problem() {
        let mut facts = configured();
        facts.ca = CaFact {
            present: true,
            installed: Some(false),
            key_mode: Some(0o600),
            key_inside_sync_entry: None,
        };
        let findings = diagnose(&facts);
        assert!(
            find(&findings, "ca").iter().all(|f| f.verdict == Verdict::Ok),
            "an uninstalled authority was reported as something to fix"
        );
    }

    /// A mixed fleet runs the degraded path and nothing else says so.
    /// "could not ask 1 peer" is a count. A count is not something a stranger
    /// can act on, and this check exists to be acted on.
    #[test]
    fn a_peer_that_could_not_be_asked_is_named_with_the_reason() {
        let mut facts = configured();
        facts.peers[0].version = None;
        facts.peers[0].version_error =
            Some("remote exec is disabled on this peer".to_string());

        let findings = diagnose(&facts);
        let versions = find(&findings, "versions");
        assert_eq!(versions[0].verdict, Verdict::Unknown);
        assert!(
            versions[0].detail.contains("hetz"),
            "the peer was not named: {}",
            versions[0].detail
        );
        assert!(
            versions[0].detail.contains("exec is disabled"),
            "the reason was not given: {}",
            versions[0].detail
        );
        assert!(
            versions[0]
                .action
                .as_deref()
                .is_some_and(|a| a.contains("--allow-exec")),
            "it did not say what to change"
        );
    }

    /// "one peer could not be asked" does not say one of how many.
    #[test]
    fn the_peers_that_did_answer_are_reported_too() {
        let mut facts = configured();
        facts.peers.push(PeerFact {
            label: "droppy".to_string(),
            has_address: true,
            has_grants: Some(true),
            reachable: Some(true),
            version: None,
            version_error: Some("remote exec is disabled".to_string()),
        });
        let findings = diagnose(&facts);
        let versions = find(&findings, "versions");
        assert!(
            versions
                .iter()
                .any(|f| f.verdict == Verdict::Ok && f.detail.contains("1 of 2")),
            "it did not say how much of the fleet it actually checked: {:?}",
            versions.iter().map(|f| &f.detail).collect::<Vec<_>>()
        );
        assert!(
            versions.iter().any(|f| f.verdict == Verdict::Unknown),
            "the unknown peer stopped being reported"
        );
    }

    /// An unreachable peer already has its own finding. Saying it twice under a
    /// second heading is noise wearing the shape of a second fault.
    #[test]
    fn an_unreachable_peer_is_not_reported_again_as_a_version_unknown() {
        let mut facts = configured();
        facts.peers[0].reachable = Some(false);
        facts.peers[0].version = None;
        facts.peers[0].version_error = None;

        let findings = diagnose(&facts);
        assert!(
            find(&findings, "versions")
                .iter()
                .all(|f| f.verdict == Verdict::Ok),
            "an unreachable peer was reported a second time as a version problem"
        );
    }

    #[test]
    fn a_peer_on_a_different_build_is_reported() {
        let mut facts = configured();
        facts.peers[0].version = Some("0.2.0+old".to_string());
        let findings = diagnose(&facts);
        let versions = find(&findings, "versions");
        assert_eq!(versions[0].verdict, Verdict::Problem);
        assert!(
            versions[0].detail.contains("whole manifests"),
            "it did not say what a mixed fleet costs: {}",
            versions[0].detail
        );
    }
}

// ---------------------------------------------------------------------------
// Gathering. Everything above this line is pure; everything below touches the
// world. The split is the point: it is what lets every verdict be tested at
// every outcome, including the ones that are painful to reproduce.
// ---------------------------------------------------------------------------

use anyhow::Result;

use crate::ca;
use crate::service::ServiceEnablement;
use crate::config::{FabricHome, PeerBook};
use crate::control::{ControlRequest, ControlResponse, PeerReachability, SyncEntryStatus};

/// Where the OS service definition would live on this platform.

/// Collect everything the checks reason about.
///
/// Anything that cannot be established becomes `None`, never a cheerful
/// default. That is the whole contract with the pure half: it may only report
/// `Ok` from a fact, so a fact it could not get must arrive as absence.
pub async fn gather<F, Fut>(home: &FabricHome, send_control: F) -> Facts
where
    F: Fn(ControlRequest) -> Fut,
    Fut: std::future::Future<Output = Result<ControlResponse>>,
{
    let has_identity = home.identity_path().exists();
    let manages_service = home.is_default_state_root();

    let service = crate::service::service_enablement();

    let mut own_version = env!("CARGO_PKG_VERSION").to_string();
    let mut daemon_running = false;
    let mut reachability: Vec<PeerReachability> = Vec::new();

    if let Ok(ControlResponse::ReachabilityStatus { version, peers, .. }) =
        send_control(ControlRequest::ReachabilityStatus).await
    {
        daemon_running = true;
        own_version = version;
        reachability = peers;
    }

    let peer_book = PeerBook::load(home).ok();
    let peers = reachability
        .iter()
        .map(|peer| {
            let label = peer.name.clone().unwrap_or_else(|| peer.id.clone());

            let (version, version_error) = if peer.reachable {
                match peer_version(home, &label) {
                    Ok(version) => (Some(version), None),
                    Err(why) => (None, Some(why)),
                }
            } else {
                (None, None)
            };
            PeerFact {
                version,
                version_error,
                // "no address" is how an unreachable peer with nowhere to dial
                // reports itself; the error text is the only signal we have.
                has_address: !peer
                    .error
                    .as_deref()
                    .is_some_and(|e| e.contains("no address") || e.contains("no addr")),
                has_grants: peer_book.as_ref().and_then(|book| {
                    book.peers()
                        .iter()
                        .find(|configured| configured.id.to_string() == peer.id)
                        .map(|configured| !configured.allow.is_empty())
                }),
                reachable: Some(peer.reachable),
                label,
            }
        })
        .collect::<Vec<_>>();

    let entries: Vec<SyncEntryStatus> = match send_control(ControlRequest::SyncStatus).await {
        Ok(ControlResponse::SyncStatus { entries }) => entries,
        // The daemon is the only thing that knows this. Without it we say
        // nothing about sync rather than guessing it is fine.
        _ => Vec::new(),
    };

    let syncs = entries
        .iter()
        .map(|entry| {
            let folder = PathBuf::from(&entry.folder);
            SyncFact {
                name: entry.name.clone(),
                folder_exists: folder.exists(),
                folder,
                drift_clean: entry.missing == 0
                    && entry.mismatched == 0
                    && entry.scan_issues.is_empty(),
                scan_issues: entry.scan_issues.clone(),
                stopped: entry.stopped_peers.clone(),
            }
        })
        .collect::<Vec<_>>();

    Facts {
        has_identity,
        manages_service,
        daemon_running,
        service,
        own_version,
        peers,
        ca: gather_ca(home, &syncs),
        syncs,
    }
}

/// Ask a peer which build it runs, over the path a person would use.
///
/// Deliberately the real command rather than something inferred locally: a
/// check that does not exercise the path it describes is how four status fields
/// reported fine this week having established nothing. `None` when it could not
/// be asked, which includes exec not being permitted.
/// The arguments doctor runs to ask a peer its version, over the SAME home it
/// was invoked with.
///
/// `--home` must be here. `fabric --home X doctor` asks about a peer only the X
/// daemon knows, but the child `exec` without `--home` talks to the DEFAULT
/// daemon, which reports `unknown peer`. FABRIC_HOME in the environment does
/// propagate to the child; the flag a person typed does not, unless doctor
/// carries it. Finding 12 of the 2026-08-29 review.
fn peer_version_argv(home: &FabricHome, label: &str) -> Vec<String> {
    vec![
        "--home".to_string(),
        home.root().display().to_string(),
        "exec".to_string(),
        label.to_string(),
        "--".to_string(),
        "fabric".to_string(),
        "--version".to_string(),
    ]
}

fn peer_version(home: &FabricHome, label: &str) -> std::result::Result<String, String> {
    let exe = std::env::current_exe().map_err(|e| e.to_string())?;
    let output = std::process::Command::new(exe)
        .args(peer_version_argv(home, label))
        .output()
        .map_err(|e| e.to_string())?;
    if !output.status.success() {
        // The refusal text is the useful part. `exec is disabled` is the common
        // one and it names something a person can change.
        let why = String::from_utf8_lossy(&output.stderr)
            .lines()
            .chain(String::from_utf8_lossy(&output.stdout).lines())
            .map(str::trim)
            .find(|line| !line.is_empty())
            .unwrap_or("the command failed")
            .to_string();
        return Err(why);
    }
    let text = String::from_utf8_lossy(&output.stdout).trim().to_string();
    // `fabric --version` prints `fabric 0.2.0+abc`; keep the version alone.
    match text.split_whitespace().next_back() {
        Some(version) if !version.is_empty() => Ok(version.to_string()),
        _ => Err("it answered with no version".to_string()),
    }
}

fn gather_ca(home: &FabricHome, syncs: &[SyncFact]) -> CaFact {
    let key_path = ca::ca_key_path(home);
    let present = ca::ca_cert_path(home).exists() || key_path.exists();
    if !present {
        return CaFact {
            present: false,
            installed: None,
            key_mode: None,
            key_inside_sync_entry: None,
        };
    }

    let key_mode = key_permissions(&key_path);

    // Canonicalise both sides. A symlinked or relative sync folder that reaches
    // the key still reaches it, and comparing the text of two paths would miss
    // that entirely.
    let key_real = key_path.canonicalize().ok();
    let key_inside_sync_entry = key_real.and_then(|key| {
        syncs
            .iter()
            .find(|sync| {
                sync.folder
                    .canonicalize()
                    .is_ok_and(|folder| key.starts_with(&folder))
            })
            .map(|sync| sync.name.clone())
    });

    CaFact {
        present: true,
        installed: ca::is_installed(home).ok(),
        key_mode,
        key_inside_sync_entry,
    }
}

#[cfg(unix)]
fn key_permissions(path: &Path) -> Option<u32> {
    use std::os::unix::fs::PermissionsExt;
    Some(std::fs::metadata(path).ok()?.permissions().mode() & 0o777)
}

#[cfg(not(unix))]
fn key_permissions(_path: &Path) -> Option<u32> {
    None
}

/// Print a report and return the exit code.
pub fn report(facts: &Facts, findings: &[Finding]) -> i32 {
    if let Some(line) = opening(facts) {
        println!("{line}");
        println!();
    }
    for finding in findings {
        println!("{:<8} {:<9} {}", finding.verdict.token(), finding.check, finding.detail);
        if let Some(action) = &finding.action {
            println!("{:<8} {:<9}   try: {action}", "", "");
        }
    }
    let code = exit_code(findings);
    println!();
    let count = findings.iter().filter(|f| f.verdict.needs_attention()).count();
    if code == 0 {
        println!("nothing to do.");
    } else if opening(facts).is_some() {
        // Say steps, not attention. The closing line is the last thing read and
        // it must not undo the framing the opening line just set.
        println!("{count} {} left.", plural(count, "step", "steps"));
    } else {
        println!(
            "{count} {} above {} attention.",
            plural(count, "thing", "things"),
            plural(count, "needs", "need")
        );
    }
    code
}

fn plural(count: usize, one: &'static str, many: &'static str) -> &'static str {
    if count == 1 { one } else { many }
}
