use std::{process::Command, thread, time::Duration};

use fabric::{config::FabricHome, control::ControlRequest, daemon::send_control};
use tempfile::TempDir;

struct ChildGuard(std::process::Child);
impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[tokio::test]
async fn second_daemon_for_same_home_is_refused_by_lease() {
    let dir = TempDir::new().unwrap();
    let home = FabricHome::new(dir.path());
    let mut first = ChildGuard(
        Command::new(env!("CARGO_BIN_EXE_fabric"))
            .args(["--home", home.root().to_str().unwrap(), "daemon"])
            .spawn()
            .unwrap(),
    );
    let mut first_ready = false;
    for _ in 0..50 {
        if send_control(&home, ControlRequest::Status).await.is_ok() {
            first_ready = true;
            break;
        }
        thread::sleep(Duration::from_millis(100));
    }
    assert!(first_ready, "first daemon never became ready");
    let mut second = ChildGuard(
        Command::new(env!("CARGO_BIN_EXE_fabric"))
            .args(["--home", home.root().to_str().unwrap(), "daemon"])
            .spawn()
            .unwrap(),
    );
    thread::sleep(Duration::from_millis(500));
    assert!(
        second.0.try_wait().unwrap().is_some(),
        "second daemon unexpectedly started"
    );
    let _ = first.0.kill();
    let _ = first.0.wait();
    let _replacement = ChildGuard(
        Command::new(env!("CARGO_BIN_EXE_fabric"))
            .args(["--home", home.root().to_str().unwrap(), "daemon"])
            .spawn()
            .unwrap(),
    );
    for _ in 0..50 {
        if send_control(&home, ControlRequest::Status).await.is_ok() {
            break;
        }
        thread::sleep(Duration::from_millis(100));
    }
    assert!(
        send_control(&home, ControlRequest::Status).await.is_ok(),
        "replacement never became ready"
    );
    let mut duplicate = ChildGuard(
        Command::new(env!("CARGO_BIN_EXE_fabric"))
            .args(["--home", home.root().to_str().unwrap(), "daemon"])
            .spawn()
            .unwrap(),
    );
    thread::sleep(Duration::from_millis(500));
    assert!(
        duplicate.0.try_wait().unwrap().is_some(),
        "duplicate owner did not exit"
    );
}
