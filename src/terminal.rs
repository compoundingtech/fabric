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
    use super::TerminalModeGuard;

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
