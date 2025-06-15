use completion_stage::CompletionStage;


#[test]
fn test() {
    let ftr = CompletionStage::new_completed_value(());
    assert!(!ftr.borrowed_by_current_thread());
    let g1 = ftr.borrow().unwrap();
    assert!(ftr.borrowed_by_current_thread());
    let g2 = ftr.borrow().unwrap();
    assert!(ftr.borrowed_by_current_thread());
    drop(g1);
    assert!(ftr.borrowed_by_current_thread());
    drop(g2);
    assert!(!ftr.borrowed_by_current_thread())

}