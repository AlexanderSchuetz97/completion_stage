use completion_stage::CompletionStage;

#[test]
fn test() {
    let stage = CompletionStage::new();
    assert!(!stage.taken());
    let stage2 = stage.and_then_apply(|d| {
        assert_eq!(d, 123);
        456
    });
    assert!(stage.taken());
        
    assert!(!stage.completed());
    assert!(!stage2.completed());
    stage.complete_with_value(123);
    assert!(stage.completed());
    assert!(stage2.completed());
    assert_eq!(None, stage.take());
    assert_eq!(456, stage2.take().unwrap());
}