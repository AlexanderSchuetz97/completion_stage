use completion_stage::{CompletionStage, GetTimeoutResult, InfallibleExecutor};
use std::thread;
use std::time::Duration;

struct ShittyExecutor;

impl InfallibleExecutor for ShittyExecutor {
    fn execute(task: impl FnOnce() + Send + 'static) {
        task()
    }
}

#[test]
fn test() {
    let ftr = CompletionStage::<()>::new();
    let cl = ftr.clone();
    thread::spawn(move || {
        let stage = CompletionStage::new_completed_value(123);
        let stage_cl = stage.clone();
        stage.then_run_ref_async::<ShittyExecutor>(move |comp| {
            stage_cl.then_run_ref(|x| {
                assert_eq!(123, *x.unwrap());
            });

            assert_eq!(123, *comp.unwrap());
        });

        cl.complete_with_value(());
    });

    if let GetTimeoutResult::Value(_) = ftr.get_timeout(Duration::from_millis(2000)) {
        return;
    }

    panic!("failed");
}
