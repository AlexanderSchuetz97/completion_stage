use std::thread;
use completion_stage::CompletionStage;

#[test]
fn stack_blower() {
   let uw = thread::Builder::new().stack_size(8096).spawn(move || {
       let n = CompletionStage::new();
       let mut n2 = n.clone();
       for _ in 0..8096 {
           n2 = n2.and_then_apply(|v| v+1);
       }

       eprintln!("PRE STACK");
       n.complete_with_value(1i32);
       n2.unwrap()
   }).unwrap().join().unwrap();
    assert_eq!(uw, 123);

}
