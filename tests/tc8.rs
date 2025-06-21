use std::thread;
use std::time::Duration;
use completion_stage::{Completion, CompletionStage};

#[test]
fn test() {
    let t = CompletionStage::new();
    let cl = t.clone();
    
    let n = CompletionStage::new();
    n.then_run_ref(|_| panic!("Beep"));
    n.then_run(move |comp| {
        cl.complete(comp);
    });
    
    let cl2 = n.clone();
    let g = thread::spawn(move || {
        cl2.complete_with_value(1234i32);
    });
    
    assert!(t.wait_timeout(Duration::from_millis(5000)));
   
    let e = g.join().unwrap_err();
    let dc = e.downcast_ref::<&str>().unwrap(); //We panicked
    assert_eq!(*dc, "Beep");
    
    let comp = t.get();
    assert_eq!(Completion::Panic, comp);
}