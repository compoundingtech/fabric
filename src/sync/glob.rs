//! A tiny dependency-free glob matcher for sync `include` filters.
//!
//! Patterns match against a normalized forward-slash relative path:
//! - `*` matches any run of characters within a single path segment (not `/`),
//! - `**` matches any run of characters including `/` (and, when written as
//!   `**/`, also matches zero leading directories),
//! - `?` matches exactly one non-`/` character,
//! - everything else is a literal.
//!
//! This is intentionally small — enough for `*.toml`, `**/*.toml`, and exact
//! names — not a full POSIX/git implementation. If richer matching is ever
//! needed, swap in `globset` behind this same `matches` function.

/// Match `pattern` against `text` (a normalized relative path).
pub fn matches(pattern: &str, text: &str) -> bool {
    matches_bytes(pattern.as_bytes(), text.as_bytes())
}

fn matches_bytes(p: &[u8], t: &[u8]) -> bool {
    let Some(&first) = p.first() else {
        return t.is_empty();
    };

    match first {
        b'*' if p.get(1) == Some(&b'*') => {
            // `**` — matches any run including `/`. An optional trailing `/` is
            // consumed so `**/foo` also matches a top-level `foo`.
            let mut rest = &p[2..];
            if rest.first() == Some(&b'/') {
                rest = &rest[1..];
            }
            (0..=t.len()).any(|i| matches_bytes(rest, &t[i..]))
        }
        b'*' => {
            // Single `*` — matches within one segment (never crosses `/`).
            let rest = &p[1..];
            let mut i = 0;
            loop {
                if matches_bytes(rest, &t[i..]) {
                    return true;
                }
                if i >= t.len() || t[i] == b'/' {
                    return false;
                }
                i += 1;
            }
        }
        b'?' => !t.is_empty() && t[0] != b'/' && matches_bytes(&p[1..], &t[1..]),
        c => !t.is_empty() && t[0] == c && matches_bytes(&p[1..], &t[1..]),
    }
}

/// True when `path` matches at least one of `patterns`.
pub fn matches_any(patterns: &[String], path: &str) -> bool {
    patterns.iter().any(|pattern| matches(pattern, path))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn star_matches_within_segment_only() {
        assert!(matches("*.toml", "agent.toml"));
        assert!(matches("*.toml", ".toml"));
        assert!(!matches("*.toml", "agent.txt"));
        // `*` does not cross a directory separator.
        assert!(!matches("*.toml", "sub/agent.toml"));
    }

    #[test]
    fn doublestar_crosses_segments() {
        assert!(matches("**/*.toml", "agent.toml"));
        assert!(matches("**/*.toml", "sub/agent.toml"));
        assert!(matches("**/*.toml", "a/b/c/agent.toml"));
        assert!(!matches("**/*.toml", "a/b/agent.txt"));
    }

    #[test]
    fn question_matches_one_non_slash() {
        assert!(matches("a?c", "abc"));
        assert!(!matches("a?c", "ac"));
        assert!(!matches("a?c", "a/c"));
    }

    #[test]
    fn literal_exact_match() {
        assert!(matches("catalog/index.toml", "catalog/index.toml"));
        assert!(!matches("catalog/index.toml", "catalog/other.toml"));
    }

    #[test]
    fn matches_any_ors_the_patterns() {
        let pats = vec!["*.toml".to_string(), "*.md".to_string()];
        assert!(matches_any(&pats, "a.toml"));
        assert!(matches_any(&pats, "b.md"));
        assert!(!matches_any(&pats, "c.txt"));
    }

    #[test]
    fn bare_doublestar_matches_everything() {
        assert!(matches("**", "anything/at/all.bin"));
        assert!(matches("**", ""));
    }
}
