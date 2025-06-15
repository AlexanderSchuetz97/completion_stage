use completion_stage::CompletionStage;
use std::thread;

#[test]
fn test() {
    let n = thread::spawn(|| {
        let ftr = CompletionStage::new_completed_value(());
        let g1 = ftr.borrow().unwrap();
        let _g2 = ftr.take().unwrap();
        drop(g1);
    });
    let tmp = n.join().unwrap_err();
    let msg = tmp.downcast::<&str>().unwrap();
    assert_eq!(*msg, "deadlock detected");
}

#[test]
fn test2() {
    let n = thread::spawn(|| {
        let ftr = CompletionStage::new_completed_value(());
        let g1 = ftr.borrow().unwrap();
        let g2 = ftr.borrow().unwrap();
        let _g3 = ftr.take().unwrap();
        drop(g2);
        drop(g1);
        
    });
    let tmp = n.join().unwrap_err();
    let msg = tmp.downcast::<&str>().unwrap();
    assert_eq!(*msg, "deadlock detected");
}