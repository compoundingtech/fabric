use std::io::{self, IsTerminal};

use anyhow::Result;

/// Owns the caller terminal mode while an interactive Fabric client is active.
///
/// On Unix we preserve the exact termios structure rather than asking a library
/// to synthesize a generic "cooked" mode on exit. This matters when the caller
/// had non-default control characters or flags before starting Fabric.
pub struct TerminalModeGuard {
    #[cfg(unix)]
    fd: std::os::fd::RawFd,
    #[cfg(unix)]
    original: Option<libc::termios>,
    #[cfg(not(unix))]
    enabled: bool,
}

impl TerminalModeGuard {
    pub fn enable_if_terminal() -> Result<Self> {
        if !io::stdin().is_terminal() {
            return Ok(Self::disabled());
        }

        #[cfg(unix)]
        {
            let fd = libc::STDIN_FILENO;
            let original = read_termios(fd)?;
            let guard = Self {
                fd,
                original: Some(original),
            };
            guard.apply_raw()?;
            Ok(guard)
        }

        #[cfg(not(unix))]
        {
            crossterm::terminal::enable_raw_mode()?;
            Ok(Self { enabled: true })
        }
    }

    pub fn is_enabled(&self) -> bool {
        #[cfg(unix)]
        {
            self.original.is_some()
        }
        #[cfg(not(unix))]
        {
            self.enabled
        }
    }

    /// Restore the exact mode observed before Fabric entered raw mode.
    pub fn restore(&self) -> Result<()> {
        #[cfg(unix)]
        {
            if let Some(original) = self.original.as_ref() {
                set_termios(self.fd, original)?;
            }
        }
        #[cfg(not(unix))]
        {
            if self.enabled {
                crossterm::terminal::disable_raw_mode()?;
            }
        }
        Ok(())
    }

    /// Re-enter raw mode after a suspend/continue cycle.
    pub fn reenter_raw(&self) -> Result<()> {
        if self.is_enabled() {
            #[cfg(unix)]
            self.apply_raw()?;
            #[cfg(not(unix))]
            crossterm::terminal::enable_raw_mode()?;
        }
        Ok(())
    }

    fn disabled() -> Self {
        Self {
            #[cfg(unix)]
            fd: libc::STDIN_FILENO,
            #[cfg(unix)]
            original: None,
            #[cfg(not(unix))]
            enabled: false,
        }
    }

    #[cfg(unix)]
    fn apply_raw(&self) -> Result<()> {
        let Some(original) = self.original.as_ref() else {
            return Ok(());
        };
        // termios is plain C data. Copy it so the saved state remains immutable
        // across repeated suspend/continue cycles.
        let mut raw = unsafe { std::ptr::read(original) };
        unsafe {
            libc::cfmakeraw(&mut raw);
        }
        set_termios(self.fd, &raw)?;
        Ok(())
    }
}

impl Drop for TerminalModeGuard {
    fn drop(&mut self) {
        let _ = self.restore();
    }
}

/// Emulator state a session leaves behind in the bytes it writes: private
/// modes it set or reset, the application keypad, and text attributes.
///
/// A program on the far side that dies without cleaning up (its own process
/// killed, its transport gone) cannot restore what it set; the terminal in
/// front of the person stays in the alternate screen, keeps reporting mouse
/// movement as escape sequences, or hides the cursor. The bytes that did it all
/// passed through here, so this tracks exactly those, and `cleanup` undoes
/// exactly those: nothing is emitted for a session that set nothing.
#[derive(Debug, Default)]
pub struct TerminalState {
    parser: Parser,
    /// Private modes whose state differs from the emulator's default.
    modes: std::collections::BTreeSet<u16>,
    keypad_application: bool,
    attributes_set: bool,
}

/// Private modes worth tracking and whether the emulator's default is "set".
const PRIVATE_MODES: &[(u16, bool)] = &[
    (1, false),    // DECCKM: application cursor keys
    (6, false),    // DECOM: origin mode
    (7, true),     // DECAWM: auto-wrap
    (9, false),    // X10 mouse reporting
    (25, true),    // DECTCEM: cursor visible
    (47, false),   // alternate screen (legacy)
    (66, false),   // DECNKM: application keypad
    (1000, false), // mouse reporting: clicks
    (1001, false), // mouse reporting: highlight
    (1002, false), // mouse reporting: drag
    (1003, false), // mouse reporting: all motion
    (1004, false), // focus reporting
    (1005, false), // mouse encoding: UTF-8
    (1006, false), // mouse encoding: SGR
    (1015, false), // mouse encoding: urxvt
    (1016, false), // mouse encoding: SGR pixels
    (1047, false), // alternate screen
    (1049, false), // alternate screen with cursor save
    (2004, false), // bracketed paste
    (2026, false), // synchronized output
];

fn private_mode_default(mode: u16) -> Option<bool> {
    PRIVATE_MODES
        .iter()
        .find(|(known, _)| *known == mode)
        .map(|(_, default_set)| *default_set)
}

#[derive(Debug, Default)]
enum Parser {
    #[default]
    Ground,
    Escape,
    /// Inside a control sequence: parameter and intermediate bytes so far.
    Csi(Vec<u8>),
    /// Inside a string (OSC, DCS, APC, ...) that ends with BEL or ESC \.
    Text {
        escape: bool,
    },
}

/// Control sequences longer than this are not modes; stop collecting them.
const MAX_CSI_LEN: usize = 64;

impl TerminalState {
    pub fn new() -> Self {
        Self::default()
    }

    /// Feed bytes the session wrote to the terminal, in order. Sequences may be
    /// split across calls.
    pub fn observe(&mut self, bytes: &[u8]) {
        for &byte in bytes {
            self.observe_byte(byte);
        }
    }

    fn observe_byte(&mut self, byte: u8) {
        let parser = std::mem::take(&mut self.parser);
        self.parser = match parser {
            Parser::Ground => match byte {
                0x1b => Parser::Escape,
                _ => Parser::Ground,
            },
            Parser::Escape => match byte {
                b'[' => Parser::Csi(Vec::new()),
                b']' | b'P' | b'X' | b'^' | b'_' => Parser::Text { escape: false },
                b'=' => {
                    self.keypad_application = true;
                    Parser::Ground
                }
                b'>' => {
                    self.keypad_application = false;
                    Parser::Ground
                }
                b'c' => {
                    self.clear();
                    Parser::Ground
                }
                0x1b => Parser::Escape,
                _ => Parser::Ground,
            },
            Parser::Csi(mut collected) => match byte {
                0x1b => Parser::Escape,
                0x20..=0x3f => {
                    if collected.len() >= MAX_CSI_LEN {
                        Parser::Ground
                    } else {
                        collected.push(byte);
                        Parser::Csi(collected)
                    }
                }
                0x40..=0x7e => {
                    self.dispatch(&collected, byte);
                    Parser::Ground
                }
                // C0 controls inside a sequence execute and do not end it.
                _ => Parser::Csi(collected),
            },
            Parser::Text { escape } => match byte {
                0x07 => Parser::Ground,
                0x1b => Parser::Text { escape: true },
                b'\\' if escape => Parser::Ground,
                _ => Parser::Text { escape: false },
            },
        };
    }

    fn dispatch(&mut self, collected: &[u8], final_byte: u8) {
        match (collected.first(), final_byte) {
            (Some(b'?'), b'h') | (Some(b'?'), b'l') => {
                let set = final_byte == b'h';
                for mode in parse_params(&collected[1..]) {
                    self.private_mode(mode, set);
                }
            }
            (Some(b'!'), b'p') if collected.len() == 1 => self.clear(),
            (first, b'm') if first.is_none_or(|byte| byte.is_ascii_digit() || *byte == b';') => {
                self.attributes_set = parse_params(collected).into_iter().any(|param| param != 0);
            }
            _ => {}
        }
    }

    fn private_mode(&mut self, mode: u16, set: bool) {
        let Some(default_set) = private_mode_default(mode) else {
            return;
        };
        if set == default_set {
            self.modes.remove(&mode);
        } else {
            self.modes.insert(mode);
        }
    }

    /// True when nothing the session did is still in effect.
    pub fn is_clean(&self) -> bool {
        self.modes.is_empty() && !self.keypad_application && !self.attributes_set
    }

    /// The bytes that put the emulator back: one reset per mode still set,
    /// in an order that renders (synchronized output ends first, the alternate
    /// screen is left last). Empty when there is nothing to undo.
    pub fn cleanup(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        self.restore_mode(&mut bytes, 2026);
        for &(mode, _) in PRIVATE_MODES {
            if !matches!(mode, 2026 | 25 | 47 | 1047 | 1049) {
                self.restore_mode(&mut bytes, mode);
            }
        }
        if self.keypad_application {
            bytes.extend_from_slice(b"\x1b>");
        }
        if self.attributes_set {
            bytes.extend_from_slice(b"\x1b[0m");
        }
        self.restore_mode(&mut bytes, 25);
        self.restore_mode(&mut bytes, 1049);
        self.restore_mode(&mut bytes, 1047);
        self.restore_mode(&mut bytes, 47);
        bytes
    }

    fn restore_mode(&self, bytes: &mut Vec<u8>, mode: u16) {
        if !self.modes.contains(&mode) {
            return;
        }
        let default_set = private_mode_default(mode).unwrap_or(false);
        let final_byte = if default_set { 'h' } else { 'l' };
        bytes.extend_from_slice(format!("\x1b[?{mode}{final_byte}").as_bytes());
    }

    /// Forget everything: the terminal was reset, or a new session starts.
    pub fn clear(&mut self) {
        self.modes.clear();
        self.keypad_application = false;
        self.attributes_set = false;
    }
}

fn parse_params(bytes: &[u8]) -> Vec<u16> {
    bytes
        .split(|byte| *byte == b';')
        .map(|param| {
            std::str::from_utf8(param)
                .ok()
                .and_then(|text| text.parse::<u16>().ok())
                .unwrap_or(0)
        })
        .collect()
}

#[cfg(unix)]
fn read_termios(fd: std::os::fd::RawFd) -> io::Result<libc::termios> {
    let mut value = std::mem::MaybeUninit::<libc::termios>::uninit();
    let result = unsafe { libc::tcgetattr(fd, value.as_mut_ptr()) };
    if result == -1 {
        return Err(io::Error::last_os_error());
    }
    Ok(unsafe { value.assume_init() })
}

#[cfg(unix)]
fn set_termios(fd: std::os::fd::RawFd, value: &libc::termios) -> io::Result<()> {
    let result = unsafe { libc::tcsetattr(fd, libc::TCSANOW, value) };
    if result == -1 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{TerminalModeGuard, TerminalState};

    fn state_after(chunks: &[&[u8]]) -> TerminalState {
        let mut state = TerminalState::new();
        for chunk in chunks {
            state.observe(chunk);
        }
        state
    }

    #[test]
    fn a_session_that_set_nothing_has_nothing_to_clean_up() {
        let state = state_after(&[b"plain text\r\n\x1b[2J\x1b[H\x1b[K"]);
        assert!(state.is_clean());
        assert!(state.cleanup().is_empty());
    }

    #[test]
    fn modes_set_and_not_cleared_are_reset_in_a_rendering_order() {
        let state = state_after(&[
            b"\x1b[?1049h\x1b[?2004h\x1b[?1000h\x1b[?1006h\x1b[?1004h\x1b[?2026h\x1b[?25l\x1b=\x1b[1;31mtext",
        ]);
        assert!(!state.is_clean());
        assert_eq!(
            String::from_utf8(state.cleanup()).unwrap(),
            "\x1b[?2026l\x1b[?1000l\x1b[?1004l\x1b[?1006l\x1b[?2004l\x1b>\x1b[0m\x1b[?25h\x1b[?1049l"
        );
    }

    #[test]
    fn a_mode_the_session_cleared_itself_is_not_reset_again() {
        let state = state_after(&[b"\x1b[?1049h\x1b[?2004h", b"\x1b[?2004l\x1b[?1049l\x1b[0m"]);
        assert!(state.is_clean(), "{:?}", state.cleanup());
    }

    #[test]
    fn a_sequence_split_across_chunks_is_still_seen() {
        let state = state_after(&[b"\x1b", b"[?10", b"49;20", b"04h"]);
        assert_eq!(
            String::from_utf8(state.cleanup()).unwrap(),
            "\x1b[?2004l\x1b[?1049l"
        );
    }

    #[test]
    fn a_reset_of_a_default_set_mode_is_undone_by_setting_it() {
        let state = state_after(&[b"\x1b[?25l\x1b[?7l"]);
        assert_eq!(
            String::from_utf8(state.cleanup()).unwrap(),
            "\x1b[?7h\x1b[?25h"
        );
        let state = state_after(&[b"\x1b[?25l\x1b[?25h"]);
        assert!(state.is_clean());
    }

    #[test]
    fn a_string_body_does_not_look_like_a_mode() {
        // An OSC title containing what would be a mode sequence, ended by BEL
        // and by ESC \, and a DCS body likewise.
        let state = state_after(&[
            b"\x1b]0;\x1b[?1049h title\x07",
            b"\x1b]8;;\x1b[?2004h\x1b\\",
            b"\x1bPq\x1b[?1000h\x1b\\",
        ]);
        assert!(state.is_clean(), "{:?}", state.cleanup());
    }

    #[test]
    fn a_full_reset_by_the_session_clears_the_record() {
        let state = state_after(&[b"\x1b[?1049h\x1b[?1000h\x1b=\x1bc"]);
        assert!(state.is_clean());
        let state = state_after(&[b"\x1b[?1049h\x1b[!p"]);
        assert!(state.is_clean());
    }

    #[test]
    fn unknown_modes_and_overlong_sequences_are_ignored() {
        let mut long = b"\x1b[?".to_vec();
        long.extend(std::iter::repeat_n(b'1', 100));
        long.push(b'h');
        let state = state_after(&[b"\x1b[?9999h\x1b[?1234l", &long]);
        assert!(state.is_clean());
        // The parser is back on the ground: a mode after the junk is seen.
        let mut state = state;
        state.observe(b"\x1b[?1049h");
        assert!(!state.is_clean());
    }

    #[test]
    fn text_attributes_are_reset_only_when_left_set() {
        let state = state_after(&[b"\x1b[1mbold\x1b[m"]);
        assert!(state.is_clean());
        let state = state_after(&[b"\x1b[38;5;196mred"]);
        assert_eq!(state.cleanup(), b"\x1b[0m");
    }

    #[test]
    fn disabled_guard_is_idempotently_restorable() {
        let guard = TerminalModeGuard::enable_if_terminal().unwrap();
        if !guard.is_enabled() {
            guard.restore().unwrap();
            guard.reenter_raw().unwrap();
            guard.restore().unwrap();
        }
    }
}
