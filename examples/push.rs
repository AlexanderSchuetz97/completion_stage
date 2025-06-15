use std::thread;
use std::time::Duration;
use completion_stage::CompletionStage;

fn main() {
    let first_stage : CompletionStage<i32> = CompletionStage::new();
    {
        //The CompletionStage is reference counted so it can be sent/shared with any thread.
        let first_stage = first_stage.clone();
        thread::spawn(move || {
            thread::sleep(Duration::from_secs(5));

            //'Push' the value into the future and execute all further child stages right here!
            first_stage.complete_with_value(12345i32)
        });
    }

    let second_stage : CompletionStage<String> = first_stage.and_then_apply(|intermediate : i32| {
        // Executed in the same thread as above, 
        // or the main thread if the thread above is already finished, 
        // which is unlikely for this example
        return format!("The result is {intermediate}");
    });

    println!("{}", second_stage.unwrap());
}