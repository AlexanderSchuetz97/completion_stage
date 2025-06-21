use completion_stage::{CompletionStage, GetTimeoutResult};
use std::thread;
use std::time::Duration;

#[test]
fn test() {
    let n = CompletionStage::new_async::<thread::Thread>(|| {
        thread::sleep(Duration::from_millis(2000));
    });
    assert!(!n.completed());
    matches!(
        n.get_timeout(Duration::from_millis(500)),
        GetTimeoutResult::TimedOut
    );
    _ = n.get_timeout(Duration::from_millis(1800)).unwrap();
}
