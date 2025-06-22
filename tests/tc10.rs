use completion_stage::{Completion, CompletionStage};
use std::thread;
use std::time::Duration;

#[test]
fn test() {
    let x = CompletionStage::new_completed_value(123i32);
    let y = x.then_apply_ref_async::<thread::Thread, _>(|t| {
        thread::sleep(Duration::from_millis(2000));
        assert_eq!(*t.unwrap(), 123i32);
        Completion::Value(std::thread::current().id())
    });

    assert!(!y.completed());
    assert_ne!(y.unwrap(), thread::current().id());
}
