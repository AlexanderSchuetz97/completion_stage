use std::thread;
use std::time::Duration;
use completion_stage::CompletionStage;

fn main() {
    let result : String = CompletionStage::new_async::<thread::Thread>(|| {
        // Executed in a new virgin thread via thread::spawn, 
        // you can provide your own executor via the generic instead of 'thread::Thread' 
        // by implementing a simple trait for your executor.
        //
        // Do some background task here
        thread::sleep(Duration::from_secs(5));
        // 
        // eventually return the result.
        return 12345;
    }).and_then_apply(|intermediate| {
        // Executed in the same thread as above, 
        // or the main thread if the thread above is already finished, 
        // which is unlikely for this example
        return format!("The result is {intermediate}");
    }).unwrap();
    
    println!("{}", result);
}

