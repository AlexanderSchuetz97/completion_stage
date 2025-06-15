# completion_stage
Push-based futures for Rust similar to Java's CompletionStage.

## Simple Example
```rust
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
```

## Motivation
In the rust language 'futures' are poll-based and require use of 'async/await' which hides the callback-based execution flow from the programmer 
as well as requiring complex execution engines and frameworks that are unsuitable for a lot of situations.

This crate aims to provide futures that require no async execution engine, 
nor do they attempt to hide callback-based control flow from you.

## What does push-based mean?
This example here illustrates what is meant by push-based, functionally it's completely identical to the example above.
```rust
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
```

## Panics
This crate does not catch any panic using panic::catch_unwind.
If any user code panic's then a "drop guard" ensures that all dependent stages will be completed and in turn have 
their user code invoked during unwinding with Completion::Panic. This means that if user code panics again during this unwinding, 
then the application will abort as it normally does on a panic during unwind.

The panic value itself is not caught and therefore not available to dependent stages, 
as rust panic's should not be used in lieu of exceptions.

If you compile your code with `panic=abort` then none of this matters to you.

## Use-Case
I intend to use this crate to "return" results to an opengl ui-thread.
The opengl ui "thread" will start background tasks when, for example, a "button" is pressed
but should not be blocked (as that would freeze the UI).

Currently, I have been using a lot of janky code that uses mpsc Channels and try_recv for this purpose,
but I intend to replace all such code with this crate.
