use std::time::Duration;
use completion_stage::{Completion, CompletionStage};

#[test]
fn test() {
    let n = CompletionStage::new_completed_value(());
    let joined = n.borrow().unwrap();
    let v1 = n.then_apply(|comp| {
        assert_eq!(comp, Completion::DeadLock); //Because joined is borrowed, we cannot take it out.
        return Completion::Value(456)
    }).take().unwrap();
    assert_eq!(v1, 456);
    drop(joined);
    let v2 = n.then_apply(|comp| {
        assert_eq!(comp, Completion::Value(()));
        return Completion::Value(123)
    }).take().unwrap();
    assert_eq!(v2, 123);
}


#[test]
fn test2() {
    CompletionStage::new_async_with_executor((), || {
        let n = CompletionStage::new_completed_value(());
        let cl = n.clone();
        n.then_apply(move |comp| {
            assert!(cl.completed());
            assert!(cl.taken());
            
            assert!(matches!(comp, Completion::Value(_)));
            cl.then_apply_ref(|comp2| {
                assert!(matches!(comp2, Completion::Taken));
                return Completion::Value(());
            });
            return Completion::Value(())
        }).take().unwrap();
    }).get_timeout(Duration::from_millis(5000)).unwrap();
}
