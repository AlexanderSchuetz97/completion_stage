use completion_stage::CompletionStage;

#[test]
fn test() {
    let stage = CompletionStage::new();
    assert!(!stage.completed());
    stage.complete_with_value(());
    assert!(stage.completed());
    stage.take().unwrap();
}
