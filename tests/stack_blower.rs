use completion_stage::CompletionStage;
use std::thread;
use std::time::Instant;

//Note: This may fail on some architectures, 4k o stack is not much.
//Intended for linux amd64
#[test]
fn stack_blower() {
    let uw = thread::Builder::new()
        .stack_size(4096)
        .spawn(move || {
            let n = CompletionStage::new();
            let mut n2 = n.clone();
            for _ in 0..8096 {
                n2 = n2.and_then_apply(|v| v + 1);
            }

            eprintln!("PRE STACK");
            let cnt = Instant::now();
            n.complete_with_value(1i32);
            let elapsed = cnt.elapsed();
            eprintln!("{}", elapsed.as_nanos()); //Poor mans benchmark

            n2.unwrap()
        })
        .unwrap()
        .join()
        .unwrap();
    assert_eq!(uw, 8097);
}

#[test]
fn stack_blower_ref() {
    let uw = thread::Builder::new()
        .stack_size(4096)
        .spawn(move || {
            let n = CompletionStage::new();
            let mut n2 = n.clone();
            for _ in 0..8096 {
                n2 = n2.and_then_apply_ref(|v| *v + 1);
            }

            eprintln!("PRE STACK");
            let cnt = Instant::now();
            n.complete_with_value(1i32);
            let elapsed = cnt.elapsed();
            eprintln!("{}", elapsed.as_nanos()); //Poor mans benchmark
            n2.unwrap()
        })
        .unwrap()
        .join()
        .unwrap();
    assert_eq!(uw, 8097);
}
