//! # completion_stage
//! Push-based futures for Rust similar to Java's CompletionStage.
//!
//! ## Simple Example
//! ```rust
//! use std::thread;
//! use std::time::Duration;
//! use completion_stage::CompletionStage;
//!
//! fn main() {
//!     let result : String = CompletionStage::new_async::<thread::Thread>(|| {
//!         // Executed in a new virgin thread via thread::spawn,
//!         // you can provide your own executor via the generic instead of 'thread::Thread'
//!         // by implementing a simple trait for your executor.
//!         //
//!         // Do some background task here
//!         thread::sleep(Duration::from_secs(5));
//!         //
//!         // eventually return the result.
//!         return 12345;
//!     }).and_then_apply(|intermediate| {
//!         // Executed in the same thread as above,
//!         // or the main thread if the thread above is already finished,
//!         // which is unlikely for this example
//!         return format!("The result is {intermediate}");
//!     }).unwrap();
//!
//!     println!("{}", result);
//! }
//! ```
#![allow(non_snake_case)]
#![allow(non_camel_case_types)]
#![deny(clippy::correctness)]
#![deny(
    clippy::perf,
    clippy::complexity,
    clippy::style,
    clippy::nursery,
    clippy::pedantic,
    clippy::clone_on_ref_ptr,
    clippy::decimal_literal_representation,
    clippy::float_cmp_const,
    clippy::missing_docs_in_private_items,
    clippy::multiple_inherent_impl,
    clippy::unwrap_used,
    clippy::cargo_common_metadata,
    clippy::used_underscore_binding
)]
use defer_heavy::defer;
use parking_lot::{
    Condvar, MappedRwLockReadGuard, Mutex, RwLock, RwLockReadGuard, RwLockWriteGuard,
};
use std::collections::{HashMap, VecDeque};
use std::fmt::{Debug, Formatter};
use std::io::Error;
use std::ops::Deref;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering::SeqCst;
use std::thread::ThreadId;
use std::time::{Duration, Instant};
use std::vec::IntoIter;
use std::{mem, thread};
use std::cell::RefCell;

#[derive(Debug)]
pub struct CompletionStage<T: Send + Sync + 'static>(Arc<CompletionStageInner<T>>);

impl<T: Send + Sync + 'static> Clone for CompletionStage<T> {
    fn clone(&self) -> Self {
        CompletionStage(self.0.clone())
    }
}

/// Enum that represents completion of the stage
#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub enum Completion<T> {
    Taken,
    Panic,
    DeadLock,
    Value(T),
}

impl<T> Completion<T> {
    pub fn unwrap(self) -> T {
        match self {
            Completion::Taken => panic!("unwrap called on Taken Completion"),
            Completion::Panic => panic!("unwrap called on Panic Completion"),
            Completion::DeadLock => panic!("unwrap called on DeadLock Completion"),
            Completion::Value(v) => v,
        }
    }

    pub fn some(self) -> Option<T> {
        match self {
            Completion::Value(v) => Some(v),
            _ => None,
        }
    }

    pub fn map<X>(self, func: impl FnOnce(T) -> X) -> Completion<X> {
        match self {
            Completion::Taken => Completion::Taken,
            Completion::Panic => Completion::Panic,
            Completion::DeadLock => Completion::DeadLock,
            Completion::Value(v) => Completion::Value(func(v)),
        }
    }
}

#[derive(Debug)]
enum CellValue<T> {
    None,
    Taken,
    Panic,
    DeadLock,
    Value(T),
}

impl<T> CellValue<T> {
    fn take(&mut self) -> CellValue<T> {
        match self {
            CellValue::None => CellValue::None,
            CellValue::Taken => CellValue::Taken,
            CellValue::Panic => CellValue::Panic,
            CellValue::DeadLock => CellValue::DeadLock,
            CellValue::Value(_) => mem::replace(self, CellValue::Taken),
        }
    }

    fn map_ref(guard: RwLockReadGuard<CellValue<T>>) -> CellValue<MappedRwLockReadGuard<T>> {
        match guard.deref() {
            CellValue::None => CellValue::None,
            CellValue::Taken => CellValue::Taken,
            CellValue::Panic => CellValue::Panic,
            CellValue::DeadLock => CellValue::DeadLock,
            CellValue::Value(_) => {
                CellValue::Value(RwLockReadGuard::map(guard, |grd| {
                    let CellValue::Value(value) = grd else {
                        //This is unreachable because we hold a lock on the cell the entire time, and we already know its Value.
                        unreachable!()
                    };

                    value
                }))
            }
        }
    }
}

enum Taker<T: Send + Sync> {
    None,
    Some(Box<dyn FnOnce(Completion<T>, &CompletionQueue) + Send>),
    Closed,
}

impl<T: Send + Sync> Debug for Taker<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Taker::None => f.write_str("None"),
            Taker::Some(_) => f.write_str("Some"),
            Taker::Closed => f.write_str("Closed"),
        }
    }
}

impl<T: Send + Sync> Taker<T> {
    fn is_none(&self) -> bool {
        matches!(self, Taker::None)
    }

    fn take(&mut self) -> Option<Box<dyn FnOnce(Completion<T>, &CompletionQueue) + Send>> {
        if let Taker::Some(fbox) = mem::replace(self, Taker::Closed) {
            return Some(fbox);
        }

        None
    }
}

type RefTask<T> = Box<dyn FnOnce(Completion<&T>, &CompletionQueue) + Send>;
fn rts_invoke_ref<T: Send + Sync>(
    ref_task_state: Vec<RefTask<T>>,
    data: &Completion<T>,
    q: &CompletionQueue,
) {
    let mut iter: IntoIter<RefTask<T>> = ref_task_state.into_iter();
    while let Some(task) = iter.next() {
        defer! {
            rts_unwind_panic_all(&mut iter);
        }
        task(
            match data {
                Completion::Taken => Completion::Taken,
                Completion::Panic => Completion::Panic,
                Completion::DeadLock => Completion::DeadLock,
                Completion::Value(val_ref) => Completion::Value(val_ref),
            },
            q,
        );
    }
}
fn rts_invoke<T: Send + Sync>(
    ref_task_state: Vec<RefTask<T>>,
    data: Completion<&T>,
    q: &CompletionQueue,
) {
    let mut iter: IntoIter<RefTask<T>> = ref_task_state.into_iter();
    while let Some(task) = iter.next() {
        defer! {
            rts_unwind_panic_all(&mut iter);
        }
        task(data, q);
    }
}

fn rts_unwind_panic_all<T: Send + Sync>(iter: &mut IntoIter<RefTask<T>>) {
    if std::thread::panicking() {
        //TODO figure out a way that this doesnt suck
        let mut q = CompletionQueue::default();
        while let Some(task) = iter.next() {
            task(Completion::Panic, &mut q);
        }
        q.run();
    }
}

#[derive(Debug)]
struct CompletionStageInner<T: Send + Sync> {
    completed: AtomicBool,
    thread_borrow_counts: Mutex<HashMap<ThreadId, usize>>,
    cell: RwLock<CellValue<T>>,
    tasks: Mutex<CompleteStageInnerTasks<T>>,
    cell_cond: Condvar,
}

struct CompleteStageInnerTasks<T: Send + Sync> {
    ref_task_state: Option<Vec<Box<dyn FnOnce(Completion<&T>, &CompletionQueue) + Send>>>,
    taker: Taker<T>,
}

impl<T: Send + Sync> Debug for CompleteStageInnerTasks<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CompleteStageInnerTasks")
            .field("ref_task_state", &self.ref_task_state.as_ref().map(|_| ()))
            .field("taker", &self.taker)
            .finish()
    }
}

fn handle_supplier_panic<T: Send + Sync>(inner: &Arc<CompletionStageInner<T>>) {
    if std::thread::panicking() {
        complete_inner::<T>(inner, Completion::Panic);
    }
}

fn complete_inner<T: Send + Sync>(
    inner: &Arc<CompletionStageInner<T>>,
    data: Completion<T>,
) -> Option<Completion<T>> {
    let mut q = CompletionQueue::default();
    let r = complete_inner_queue(inner, data, &mut q);
    q.run();
    r
}

/// Helper struct that wraps a dequeue which is used to shallow the stack when calling child stages.
#[derive(Default)]
struct CompletionQueue(RefCell<VecDeque<Box<dyn FnOnce(&CompletionQueue)>>>);

impl CompletionQueue {
    fn push(&self, task: impl FnOnce(&CompletionQueue) + 'static) {
        self.0.borrow_mut().push_back(Box::new(task));
    }
    fn run(&self) {
        loop {
            let mut brw = self.0.borrow_mut();
            let Some(t) = brw.pop_front() else {
              return;
            };
            drop(brw);
            t(self);
        }
    }
}

impl Drop for CompletionQueue {
    fn drop(&mut self) {
        #[cfg(debug_assertions)]
        if !std::thread::panicking() {
            //We should only get here if we are panicking
            debug_assert!(self.0.borrow().is_empty());
        }
        self.run()
    }
}

fn complete_inner_queue<T: Send + Sync>(
    inner: &Arc<CompletionStageInner<T>>,
    data: Completion<T>,
    queue: &CompletionQueue,
) -> Option<Completion<T>> {
    if inner.completed.load(SeqCst) {
        return Some(data);
    }

    let mut inner_state = inner.tasks.lock();
    let mut write_guard = inner.cell.write();
    if inner.completed.swap(true, SeqCst) {
        return Some(data);
    }

    let rts = inner_state
        .ref_task_state
        .take()
        .expect("ref_task_state was none on uncompleted cell");

    if let Some(grd) = inner_state.taker.take() {
        *write_guard = CellValue::Taken;
        drop(inner_state);
        drop(write_guard);
        rts_invoke_ref(rts, &data, queue);
        grd(data.into(), queue);
        return None;
    }

    *write_guard = match data {
        Completion::Taken => CellValue::Taken,
        Completion::Panic => CellValue::Panic,
        Completion::DeadLock => CellValue::DeadLock,
        Completion::Value(data) => CellValue::Value(data),
    };

    drop(inner_state);

    let read_guard = RwLockWriteGuard::downgrade(write_guard);

    inner.cell_cond.notify_all();

    match read_guard.deref() {
        //We set this to a different value just above and held the lock the entire time.
        CellValue::None => unreachable!("read_ref is none"),
        CellValue::Value(val_ref) => rts_invoke(rts, Completion::Value(val_ref), queue),
        CellValue::DeadLock => rts_invoke(rts, Completion::DeadLock, queue),
        CellValue::Taken => rts_invoke(rts, Completion::Taken, queue),
        CellValue::Panic => rts_invoke(rts, Completion::Panic, queue),
    }

    None
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub enum GetTimeoutResult<T> {
    /// The call would have needed to block longer than intended.
    TimedOut,
    /// Either a parent stage completed with a `DeadLock` or
    /// The current thread currently borrows the completed value of the stage.
    /// The call would have therefore resulted in a deadlock
    DeadLock,
    /// The value was already taken out of the completed stage
    Taken,
    /// The stage could not be completed due to a panic.
    Panic,
    /// The completed value from the stage.
    Value(T),
}

impl<T> GetTimeoutResult<T> {
    pub fn unwrap(self) -> T {
        match self {
            GetTimeoutResult::TimedOut => panic!("unwrap called on TimedOut"),
            GetTimeoutResult::Taken => panic!("unwrap called on Taken"),
            GetTimeoutResult::Panic => panic!("unwrap called on Panic"),
            GetTimeoutResult::DeadLock => panic!("unwrap called on DeadLock"),
            GetTimeoutResult::Value(v) => v,
        }
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub enum TryGetResult<T> {
    WouldBlock,
    Taken,
    Panic,
    Some(T),
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub enum TryResult<T> {
    Taken,
    WouldBlock,
    Value(T),
}

#[derive(Debug)]
pub struct RefGuard<'a, T: Send + Sync>(MappedRwLockReadGuard<'a, T>, Arc<CompletionStageInner<T>>);

impl<'a, T: Send + Sync> RefGuard<'a, T> {
    fn new(guard: MappedRwLockReadGuard<'a, T>, inner: Arc<CompletionStageInner<T>>) -> Self {
        let tid = thread::current().id();
        let mut g2 = inner.thread_borrow_counts.lock();
        if let Some(mt) = g2.get_mut(&tid) {
            assert!(*mt > 0);
            *mt += 1;
            drop(g2);
            return Self(guard, inner);
        }
        g2.insert(tid, 1);
        drop(g2);
        Self(guard, inner)
    }
}

impl<T: Send + Sync> Deref for RefGuard<'_, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        self.0.deref()
    }
}

impl<T: Send + Sync> Drop for RefGuard<'_, T> {
    fn drop(&mut self) {
        let tid = thread::current().id();
        let mut g2 = self.1.thread_borrow_counts.lock();
        let Some(mt) = g2.get_mut(&tid) else {
            panic!("tid not found in deadlock_detector map during drop");
        };
        assert!(*mt > 0);
        *mt -= 1;
        if *mt == 0 {
            g2.remove(&tid);
        }
    }
}

impl<T: Send + Sync + 'static> CompletionStage<T> {
    pub fn new() -> Self {
        Self(Arc::new(CompletionStageInner {
            completed: Default::default(),
            thread_borrow_counts: Default::default(),
            cell: RwLock::new(CellValue::None),
            tasks: Mutex::new(CompleteStageInnerTasks {
                ref_task_state: Some(Vec::new()),
                taker: Taker::None,
            }),
            cell_cond: Condvar::new(),
        }))
    }

    pub fn new_completed(compl: Completion<T>) -> Self {
        match compl {
            Completion::Taken => Self::new_taken(),
            Completion::Panic => Self::new_panicked(),
            Completion::DeadLock => Self::new_deadlocked(),
            Completion::Value(val) => Self::new_completed_value(val),
        }
    }

    pub fn new_completed_value(data: T) -> Self {
        Self(Arc::new(CompletionStageInner {
            completed: AtomicBool::new(true),
            thread_borrow_counts: Default::default(),
            cell: RwLock::new(CellValue::Value(data)),
            tasks: Mutex::new(CompleteStageInnerTasks {
                ref_task_state: None,
                taker: Taker::Closed,
            }),
            cell_cond: Condvar::new(),
        }))
    }

    pub fn new_taken() -> Self {
        Self(Arc::new(CompletionStageInner {
            completed: AtomicBool::new(true),
            thread_borrow_counts: Default::default(),
            cell: RwLock::new(CellValue::Taken),
            tasks: Mutex::new(CompleteStageInnerTasks {
                ref_task_state: None,
                taker: Taker::Closed,
            }),
            cell_cond: Condvar::new(),
        }))
    }

    pub fn new_panicked() -> Self {
        Self(Arc::new(CompletionStageInner {
            completed: AtomicBool::new(true),
            thread_borrow_counts: Default::default(),
            cell: RwLock::new(CellValue::Panic),
            tasks: Mutex::new(CompleteStageInnerTasks {
                ref_task_state: None,
                taker: Taker::Closed,
            }),
            cell_cond: Condvar::new(),
        }))
    }

    pub fn new_deadlocked() -> Self {
        Self(Arc::new(CompletionStageInner {
            completed: AtomicBool::new(true),
            thread_borrow_counts: Default::default(),
            cell: RwLock::new(CellValue::DeadLock),
            tasks: Mutex::new(CompleteStageInnerTasks {
                ref_task_state: None,
                taker: Taker::Closed,
            }),
            cell_cond: Condvar::new(),
        }))
    }

    ///
    /// Create a new stage that completes when the given executor has finished executing the task.
    ///
    /// # Errors
    /// The executor may return an error to refuse execution. This error is propagated.
    ///
    /// # Example
    /// ```rust
    /// use std::{io, thread};
    /// use completion_stage::CompletionStage;
    ///
    /// fn example() {
    ///         let stage: io::Result<CompletionStage<_>> = CompletionStage::new_async_with_error::<thread::Builder, _>(|| {
    ///             //DO work here
    ///         });
    ///
    ///         //Handle your error, probably via '?' operator!
    ///         let result = stage.expect("Os failed to spawn a thread")
    ///             //Wait for work to complete.
    ///             .and_then_apply(|intermediary_result| {
    ///                 //DO more work here
    ///             })
    ///             .take();
    /// }
    /// ```
    ///
    pub fn new_async_with_error<E: FallibleExecutor<R>, R>(
        supplier: impl FnOnce() -> T + Send + 'static,
    ) -> Result<Self, R> {
        let next_stage = CompletionStage::new();
        let next_inner = next_stage.0.clone();
        E::execute(move || {
            defer! {
                handle_supplier_panic(&next_inner)
            }
            complete_inner(&next_inner, Completion::Value(supplier()));
        })?;
        Ok(next_stage)
    }

    ///
    /// Create a new stage that completes when the given executor has finished executing the task.
    ///
    /// # Errors
    /// The executor may return an error to refuse execution. This error is propagated.
    ///
    pub fn new_async_with_executor<E: InstancedInfallibleExecutor>(
        executor: E,
        supplier: impl FnOnce() -> T + Send + 'static,
    ) -> Self {
        let next_stage = CompletionStage::new();
        let next_inner = next_stage.0.clone();
        executor.execute(move || {
            defer! {
                handle_supplier_panic(&next_inner)
            }
            complete_inner(&next_inner, Completion::Value(supplier()));
        });
        next_stage
    }

    ///
    /// Create a new stage that completes when the given executor has finished executing the task.
    ///
    /// # Errors
    /// The executor may return an error to refuse execution. This error is propagated.
    ///
    pub fn new_async_with_executor_and_error<E: InstancedFallibleExecutor<R>, R>(
        executor: E,
        supplier: impl FnOnce() -> T + Send + 'static,
    ) -> Result<Self, R> {
        let next_stage = CompletionStage::new();
        let next_inner = next_stage.0.clone();
        executor.execute(move || {
            defer! {
                handle_supplier_panic(&next_inner)
            }
            complete_inner(&next_inner, Completion::Value(supplier()));
        })?;
        Ok(next_stage)
    }

    ///
    /// Create a new stage that completes when the given executor has finished executing the task.
    ///
    pub fn new_async<E: InfallibleExecutor>(supplier: impl FnOnce() -> T + Send + 'static) -> Self {
        let next_stage = CompletionStage::new();
        let next_inner = next_stage.0.clone();
        E::execute(move || {
            defer! {
                handle_supplier_panic(&next_inner)
            }
            complete_inner(&next_inner, Completion::Value(supplier()));
        });
        next_stage
    }

    /// Complete this stage with the given value.
    /// # Returns
    /// - None if the call completed the stage.
    /// - Some with the input if the stage was already completed.
    ///
    /// # Triggers Execution
    /// This function may trigger or fully perform execution of child stages in the current thread before it returns.
    ///
    /// # Panics
    /// If a child stage panics
    ///
    pub fn complete_with_value(&self, data: T) -> Option<T> {
        if let Some(Completion::Value(v)) = complete_inner(&self.0, Completion::Value(data)) {
            return Some(v);
        }

        None
    }

    /// Complete this stage with the given completion.
    /// # Returns
    /// - None if the call completed the stage.
    /// - Some with the input if the stage was already completed.
    ///
    /// # Triggers Execution
    /// This function may trigger or fully perform execution of child stages in the current thread before it returns.
    ///
    /// # Panics
    /// If a child stage panics
    ///
    pub fn complete(&self, data: Completion<T>) -> Option<Completion<T>> {
        complete_inner(&self.0, data)
    }

    pub(crate) fn complete_internal(
        &self,
        data: Completion<T>,
        q: &CompletionQueue,
    ) -> Option<Completion<T>> {
        complete_inner_queue(&self.0, data, q)
    }

    ///
    /// Crates a child stage that will complete after this stage is completed, and an additional task that transforms the intermediary result is completed.
    ///
    /// # Order of execution
    /// The closure is executed before any closure that takes ownership of the value.
    /// The implementation does not guarantee any particular order of execution in relation to other tasks that only consume the reference to the value.
    ///
    /// # Triggers Execution
    /// If the stage is already completed, the closure is executed immediately in the current thread.
    /// If the stage is not yet completed, then the closure is executed in the thread that will complete the stage.
    ///
    /// # Panics
    /// if the closure is executed immediately and it panics.
    ///
    /// # Unwinding
    /// The closure might be called during unwinding if the completion parameter to the close is `Completion::Panic`.
    /// Beware of the usual limitations that apply during unwinding. (double panic -> abort!)
    /// This is only relevant if unwinding is enabled.
    ///
    pub fn then_apply_ref<X: Send + Sync>(
        &self,
        func: impl FnOnce(Completion<&T>) -> Completion<X> + Send + 'static,
    ) -> CompletionStage<X> {
        let next_stage = CompletionStage::new();
        let next_inner = next_stage.0.clone();

        self.then_run_ref_internal(move |completion, q| {
            let r = {
                defer! {
                    handle_supplier_panic(&next_inner)
                }
                func(completion)
            };

            q.push(move |q| {
                complete_inner_queue(&next_inner, r, q);
            });
        });

        next_stage
    }

    pub fn then_apply_ref_async<E: InfallibleExecutor, X: Send + Sync>(
        &self,
        func: impl FnOnce(Completion<&T>) -> Completion<X> + Send + 'static,
    ) -> CompletionStage<X> {
        let stage = CompletionStage::new();
        let stage_inner = stage.0.clone();
        self.then_run_ref_async::<E>(move |completion| {
            defer! {
                handle_supplier_panic(&stage_inner);
            }

            complete_inner(&stage_inner, func(completion));
        });
        stage
    }

    pub fn then_apply_ref_async_with_executor<X: Send + Sync>(
        &self,
        executor: impl InstancedInfallibleExecutor + Send + 'static,
        func: impl FnOnce(Completion<&T>) -> Completion<X> + Send + 'static,
    ) -> CompletionStage<X> {
        let stage = CompletionStage::new();
        let stage_inner = stage.0.clone();
        self.then_run_ref_async_with_executor(executor, move |completion| {
            defer! {
                handle_supplier_panic(&stage_inner);
            }

            complete_inner(&stage_inner, func(completion));
        });
        stage
    }

    pub fn then_apply_ref_async_with_error<E: FallibleExecutor<R>, X: Send + Sync, R>(
        &self,
        func: impl FnOnce(Result<Completion<&T>, R>) -> Completion<X> + Send + 'static,
    ) -> CompletionStage<X> {
        let stage = CompletionStage::new();
        let stage_inner = stage.0.clone();
        self.then_run_ref_async_with_error::<E, R>(move |completion| {
            defer! {
                handle_supplier_panic(&stage_inner);
            }

            complete_inner(&stage_inner, func(completion));
        });
        stage
    }

    pub fn then_apply_ref_async_with_executor_and_error<X: Send + Sync, R>(
        &self,
        executor: impl InstancedFallibleExecutor<R> + Send + 'static,
        func: impl FnOnce(Result<Completion<&T>, R>) -> Completion<X> + Send + 'static,
    ) -> CompletionStage<X> {
        let stage = CompletionStage::new();
        let stage_inner = stage.0.clone();
        self.then_run_ref_async_with_executor_and_error(executor, move |completion| {
            defer! {
                handle_supplier_panic(&stage_inner);
            }

            complete_inner(&stage_inner, func(completion));
        });
        stage
    }

    pub fn then_apply_async<E: InfallibleExecutor, X: Send + Sync>(
        &self,
        func: impl FnOnce(Completion<T>) -> Completion<X> + Send + 'static,
    ) -> CompletionStage<X> {
        let stage = CompletionStage::new();
        let stage_inner = stage.0.clone();
        self.then_run_async::<E>(move |completion| {
            defer! {
                handle_supplier_panic(&stage_inner);
            }

            complete_inner(&stage_inner, func(completion));
        });
        stage
    }

    pub fn then_apply_async_with_executor<X: Send + Sync>(
        &self,
        executor: impl InstancedInfallibleExecutor + Send + 'static,
        func: impl FnOnce(Completion<T>) -> Completion<X> + Send + 'static,
    ) -> CompletionStage<X> {
        let stage = CompletionStage::new();
        let stage_inner = stage.0.clone();
        self.then_run_async_with_executor(executor, move |completion| {
            defer! {
                handle_supplier_panic(&stage_inner);
            }

            complete_inner(&stage_inner, func(completion));
        });
        stage
    }

    pub fn then_apply_async_with_error<E: FallibleExecutor<R>, X: Send + Sync, R>(
        &self,
        func: impl FnOnce(Result<Completion<T>, R>) -> Completion<X> + Send + 'static,
    ) -> CompletionStage<X> {
        let stage = CompletionStage::new();
        let stage_inner = stage.0.clone();
        self.then_run_async_with_error::<E, R>(move |completion| {
            defer! {
                handle_supplier_panic(&stage_inner);
            }

            complete_inner(&stage_inner, func(completion));
        });
        stage
    }

    pub fn then_apply_async_with_executor_and_error<X: Send + Sync, R>(
        &self,
        executor: impl InstancedFallibleExecutor<R> + Send + 'static,
        func: impl FnOnce(Result<Completion<T>, R>) -> Completion<X> + Send + 'static,
    ) -> CompletionStage<X> {
        let stage = CompletionStage::new();
        let stage_inner = stage.0.clone();
        self.then_run_async_with_executor_and_error(executor, move |completion| {
            defer! {
                handle_supplier_panic(&stage_inner);
            }

            complete_inner(&stage_inner, func(completion));
        });
        stage
    }

    ///
    /// Crates a child stage that will complete after this stage is completed, and an additional task that transforms the intermediary result is completed.
    ///
    /// # Order of execution
    /// Since the task closure takes ownership of the value it will only be executed after all
    /// other consumers that only consume the 'ref' to the value have been executed.
    /// It doesn't matter if these ref consumers were added before or after
    /// this function returns.
    ///
    /// # State of self after this call
    /// Since closure takes the ownership of the value 'self' will always complete to taken after this method is called.
    /// It is impossible to take the value out of 'self' after this fn returns.
    ///
    /// # Triggers Execution
    /// If the stage is already completed, the closure is executed immediately in the current thread.
    /// If the stage is not yet completed, then the closure is executed in the thread that will complete the stage.
    ///
    /// # Panics
    /// if the closure is executed immediately and it panics.
    ///
    /// # Unwinding
    /// The closure might be called during unwinding if the completion parameter to the close is `Completion::Panic`.
    /// Beware of the usual limitations that apply during unwinding. (double panic -> abort!)
    /// This is only relevant if unwinding is enabled.
    ///
    pub fn then_apply<X: Send + Sync>(
        &self,
        func: impl FnOnce(Completion<T>) -> Completion<X> + Send + 'static,
    ) -> CompletionStage<X> {
        let next_stage = CompletionStage::new();
        let next_inner = next_stage.0.clone();
        self.then_run_internal(move |completion, q| {
            defer! {
                handle_supplier_panic(&next_inner)
            }
            complete_inner_queue(&next_inner, func(completion), q);
        });

        next_stage
    }

    pub fn then_run_async<E: InfallibleExecutor>(
        &self,
        func: impl FnOnce(Completion<T>) + Send + 'static,
    ) -> &Self {
        if self.borrowed_by_current_thread() {
            let scl = self.clone();
            E::execute(move || {
                scl.then_run(move |completion| {
                    func(completion);
                });
            });
            return self;
        }
        self.then_run(move |completion| {
            E::execute(move || {
                func(completion);
            });
        })
    }

    pub fn then_run_async_with_executor(
        &self,
        executor: impl InstancedInfallibleExecutor + Send + 'static,
        func: impl FnOnce(Completion<T>) + Send + 'static,
    ) -> &Self {
        if self.borrowed_by_current_thread() {
            let scl = self.clone();
            executor.execute(move || {
                scl.then_run(move |completion| {
                    func(completion);
                });
            });
            return self;
        }
        self.then_run(move |completion| {
            executor.execute(move || {
                func(completion);
            });
        })
    }

    pub fn then_run_async_with_error<E: FallibleExecutor<R>, R>(
        &self,
        func: impl FnOnce(Result<Completion<T>, R>) + Send + 'static,
    ) -> &Self {
        if self.borrowed_by_current_thread() {
            let scl = self.clone();
            let ar = Arc::new(Mutex::new(Some(func)));
            let cl = ar.clone();
            if let Err(e) = E::execute(move || {
                let Some(f) = cl.lock().take() else {
                    //TODO should we ignore this buggy implementation of InstancedFallibleExecutor?
                    panic!("executor.execute called the closure and returned an error");
                };
                scl.then_run(move |completion| {
                    f(Ok(completion));
                });
            }) {
                let Some(f) = ar.lock().take() else {
                    //TODO should we ignore this buggy implementation of InstancedFallibleExecutor?
                    panic!("executor.execute called the closure and returned an error");
                };
                f(Err(e));
            };

            return self;
        }

        self.then_run(move |completion| {
            let ar = Arc::new(Mutex::new(Some(func)));
            let cl = ar.clone();
            if let Err(e) = E::execute(move || {
                let Some(f) = cl.lock().take() else {
                    //TODO should we ignore this buggy implementation of InstancedFallibleExecutor?
                    panic!("executor.execute called the closure and returned an error");
                };
                f(Ok(completion));
            }) {
                let Some(f) = ar.lock().take() else {
                    //TODO should we ignore this buggy implementation of InstancedFallibleExecutor?
                    panic!("executor.execute called the closure and returned an error");
                };
                f(Err(e));
            };
        })
    }

    pub fn then_run_async_with_executor_and_error<R>(
        &self,
        executor: impl InstancedFallibleExecutor<R> + Send + 'static,
        func: impl FnOnce(Result<Completion<T>, R>) + Send + 'static,
    ) -> &Self {
        self.then_run(move |completion| {
            let ar = Arc::new(Mutex::new(Some(func)));
            let cl = ar.clone();
            if let Err(e) = executor.execute(move || {
                let Some(f) = cl.lock().take() else {
                    //TODO should we ignore this buggy implementation of InstancedFallibleExecutor?
                    panic!("executor.execute called the closure and returned an error");
                };
                f(Ok(completion));
            }) {
                let Some(f) = ar.lock().take() else {
                    //TODO should we ignore this buggy implementation of InstancedFallibleExecutor?
                    panic!("executor.execute called the closure and returned an error");
                };
                f(Err(e));
            };
        })
    }

    pub fn then_run_ref_async<E: InfallibleExecutor>(
        &self,
        func: impl FnOnce(Completion<&T>) + Send + 'static,
    ) -> &Self {
        let scl = self.clone();
        self.then_run_ref_internal(|_, _| {
            E::execute(move || {
                match scl.try_get_ref() {
                    GetTimeoutResult::TimedOut => unreachable!("then_run_ref_async TimedOut"),
                    GetTimeoutResult::Taken => func(Completion::Taken),
                    GetTimeoutResult::Panic => func(Completion::Panic),
                    GetTimeoutResult::DeadLock => func(Completion::DeadLock),
                    GetTimeoutResult::Value(data) => func(Completion::Value(data.deref())),
                };
            })
        })
    }

    pub fn then_run_ref_async_with_executor(
        &self,
        executor: impl InstancedInfallibleExecutor + Send + 'static,
        func: impl FnOnce(Completion<&T>) + Send + 'static,
    ) -> &Self {
        let scl = self.clone();
        self.then_run_ref_internal(move |_, _| {
            executor.execute(move || {
                match scl.try_get_ref() {
                    GetTimeoutResult::TimedOut => {
                        unreachable!("then_run_ref_async_with_executor TimedOut")
                    }
                    GetTimeoutResult::Taken => func(Completion::Taken),
                    GetTimeoutResult::Panic => func(Completion::Panic),
                    GetTimeoutResult::DeadLock => func(Completion::DeadLock),
                    GetTimeoutResult::Value(data) => func(Completion::Value(data.deref())),
                };
            });
        })
    }

    pub fn then_run_ref_async_with_error<E: FallibleExecutor<R>, R>(
        &self,
        func: impl FnOnce(Result<Completion<&T>, R>) + Send + 'static,
    ) -> &Self {
        let scl = self.clone();
        self.then_run_ref_internal(move |_, _| {
            let ar = Arc::new(Mutex::new(Some(func)));
            let cl = ar.clone();
            if let Err(e) = E::execute(move || {
                let Some(f) = cl.lock().take() else {
                    //TODO should we ignore this buggy implementation of InstancedFallibleExecutor?
                    panic!("executor::execute called the closure and returned an error");
                };
                match scl.try_get_ref() {
                    GetTimeoutResult::TimedOut => unreachable!(),
                    GetTimeoutResult::Taken => f(Ok(Completion::Taken)),
                    GetTimeoutResult::Panic => f(Ok(Completion::Panic)),
                    GetTimeoutResult::DeadLock => f(Ok(Completion::DeadLock)),
                    GetTimeoutResult::Value(data) => f(Ok(Completion::Value(data.deref()))),
                };
            }) {
                let Some(f) = ar.lock().take() else {
                    //TODO should we ignore this buggy implementation of InstancedFallibleExecutor?
                    panic!("executor::execute called the closure and returned an error");
                };
                f(Err(e));
            };
        })
    }

    pub fn then_run_ref_async_with_executor_and_error<R>(
        &self,
        executor: impl InstancedFallibleExecutor<R> + Send + 'static,
        func: impl FnOnce(Result<Completion<&T>, R>) + Send + 'static,
    ) -> &Self {
        let scl = self.clone();
        self.then_run_ref_internal(move |_, _| {
            let ar = Arc::new(Mutex::new(Some(func)));
            let cl = ar.clone();
            if let Err(e) = executor.execute(move || {
                let Some(f) = cl.lock().take() else {
                    //TODO should we ignore this buggy implementation of InstancedFallibleExecutor?
                    panic!("executor.execute called the closure and returned an error");
                };
                match scl.try_get_ref() {
                    GetTimeoutResult::TimedOut => unreachable!(),
                    GetTimeoutResult::Taken => f(Ok(Completion::Taken)),
                    GetTimeoutResult::Panic => f(Ok(Completion::Panic)),
                    GetTimeoutResult::DeadLock => f(Ok(Completion::DeadLock)),
                    GetTimeoutResult::Value(data) => f(Ok(Completion::Value(data.deref()))),
                };
            }) {
                let Some(f) = ar.lock().take() else {
                    //TODO should we ignore this buggy implementation of InstancedFallibleExecutor?
                    panic!("executor.execute called the closure and returned an error");
                };
                f(Err(e));
            };
        })
    }

    pub fn then_run_ref(&self, func: impl FnOnce(Completion<&T>) + Send + 'static) -> &Self {
        self.then_run_ref_internal(move |comp, _| {
            func(comp);
        })
    }

    pub(crate) fn then_run_ref_internal(
        &self,
        func: impl FnOnce(Completion<&T>, &CompletionQueue) + Send + 'static,
    ) -> &Self {
        let mut locked = self.0.tasks.lock();
        let guard = self.0.cell.read_recursive();
        match guard.deref() {
            CellValue::None => (),
            CellValue::Panic => {
                drop(locked);
                let mut q = CompletionQueue::default();
                func(Completion::Panic, &mut q);
                q.run();
                return self;
            }
            CellValue::Taken => {
                drop(locked);
                let mut q = CompletionQueue::default();
                func(Completion::Taken, &mut q);
                q.run();
                return self;
            }
            CellValue::DeadLock => {
                drop(locked);
                let mut q = CompletionQueue::default();
                func(Completion::DeadLock, &mut q);
                q.run();
                return self;
            }
            CellValue::Value(data) => {
                drop(locked);
                let mut q = CompletionQueue::default();
                func(Completion::Value(data), &mut q);
                q.run();
                return self;
            }
        }
        drop(guard);

        let task: Box<dyn FnOnce(Completion<&T>, &CompletionQueue) + Send> = Box::new(func);

        let Some(rts) = locked.ref_task_state.as_mut() else {
            panic!("ref_task_state is none even tho cell was empty.");
        };

        rts.push(task);
        self
    }

    pub fn then_complete<F: From<T> + Sync + Send + 'static>(&self, stage: CompletionStage<F>) {
        self.then_run_internal(move |comp, q| {
            complete_inner_queue(&stage.0, comp.map(F::from), q);
        });
    }

    /// Executes the given closure when this stage completes.
    /// # Thread of execution
    /// if the stage is already completed or completion is immediately imminent, then
    /// the closure is executed in the current thread.
    /// if the stage is not yet completed then it is executed in the thread that completes the stage.
    ///
    ///
    pub fn then_run(&self, func: impl FnOnce(Completion<T>) + Send + 'static) -> &Self {
        self.then_run_internal(|comp, _| {
            func(comp);
        })
    }

    pub(crate) fn then_run_internal_queue(
        &self,
        func: impl FnOnce(Completion<T>, &CompletionQueue) + Send + 'static,
        q: &CompletionQueue,
    ) -> &Self {
        if self.borrowed_by_current_thread() {
            //Alternatively, we could enqueue the task so its ran when we drop the guard, however
            //that would cause the drop to block if another thread also borrows. No one expects that.
            func(Completion::DeadLock, q);
            return self;
        }
        let mut locked = self.0.tasks.lock();
        let taken = self.0.cell.write().take();
        match taken {
            CellValue::None => (),
            CellValue::Panic => {
                drop(locked);
                func(Completion::Panic, q);
                return self;
            }
            CellValue::Taken => {
                drop(locked);
                func(Completion::Taken, q);
                return self;
            }
            CellValue::DeadLock => {
                drop(locked);
                func(Completion::DeadLock, q);
                return self;
            }
            CellValue::Value(data) => {
                drop(locked);
                func(Completion::Value(data), q);
                return self;
            }
        }

        if !locked.taker.is_none() {
            let task: RefTask<T> = Box::new(move |comp, q| match comp {
                Completion::Taken | Completion::Value(_) => func(Completion::Taken, q),
                Completion::Panic => func(Completion::Panic, q),
                Completion::DeadLock => func(Completion::DeadLock, q),
            });

            let Some(ref_task_state) = locked.ref_task_state.as_mut() else {
                panic!("ref_task_state is none even tho cell was empty.");
            };
            ref_task_state.push(task);
            return self;
        }

        locked.taker = Taker::Some(Box::new(move |comp, q| {
            q.push(move |q| {
                func(comp, q);
            });
        }));
        self
    }

    pub(crate) fn then_run_internal(
        &self,
        func: impl FnOnce(Completion<T>, &CompletionQueue) + Send + 'static,
    ) -> &Self {
        if self.borrowed_by_current_thread() {
            //Alternatively, we could enqueue the task so its ran when we drop the guard, however
            //that would cause the drop to block if another thread also borrows. No one expects that.
            let mut q = CompletionQueue::default();
            func(Completion::DeadLock, &mut q);
            q.run();
            return self;
        }
        let mut locked = self.0.tasks.lock();
        let taken = self.0.cell.write().take();
        match taken {
            CellValue::None => (),
            CellValue::Panic => {
                drop(locked);
                let mut q = CompletionQueue::default();
                func(Completion::Panic, &mut q);
                q.run();
                return self;
            }
            CellValue::Taken => {
                drop(locked);
                let mut q = CompletionQueue::default();
                func(Completion::Taken, &mut q);
                q.run();
                return self;
            }
            CellValue::DeadLock => {
                drop(locked);
                let mut q = CompletionQueue::default();
                func(Completion::DeadLock, &mut q);
                q.run();
                return self;
            }
            CellValue::Value(data) => {
                drop(locked);
                let mut q = CompletionQueue::default();
                func(Completion::Value(data), &mut q);
                q.run();
                return self;
            }
        }

        if !locked.taker.is_none() {
            let task: RefTask<T> = Box::new(move |comp, q| match comp {
                Completion::Taken | Completion::Value(_) => func(Completion::Taken, q),
                Completion::Panic => func(Completion::Panic, q),
                Completion::DeadLock => func(Completion::DeadLock, q),
            });

            let Some(ref_task_state) = locked.ref_task_state.as_mut() else {
                panic!("ref_task_state is none even tho cell was empty.");
            };
            ref_task_state.push(task);
            return self;
        }

        locked.taker = Taker::Some(Box::new(move |comp, q| {
            q.push(move |q| {
                func(comp, q);
            });
        }));
        self
    }

    ///
    /// Crates a child stage that will complete after this stage is completed, and an additional task that transforms the intermediary result is completed.
    /// The closure is only executed if the stage is completed with a value. Should the stage be completed with `Panic` or `Taken` then the closure is never executed.
    ///
    /// # Order of execution
    /// Since the closure takes ownership of the value it will only be executed after all
    /// other consumers that only consume the 'ref' to the value have been executed.
    /// It doesn't matter if these ref consumers were added before or after
    /// this function returns.
    ///
    /// # State of self after this call
    /// Since closure takes the ownership of the value 'self' will always complete to taken after this method is called.
    /// It is impossible to take the value out of 'self' after this fn returns.
    ///
    /// # Thread of execution
    /// If the stage is already completed, the closure is executed in the current thread.
    /// If the stage is not yet completed, then the closure is executed in the thread that will complete the stage.
    ///
    /// # Panics
    /// if the closure is executed immediately and it panics.
    ///
    pub fn and_then_apply<X: Send + Sync>(
        &self,
        func: impl FnOnce(T) -> X + Send + 'static,
    ) -> CompletionStage<X> {
        self.then_apply(|comp| comp.map(func))
    }

    pub fn and_then_apply_async<E: InfallibleExecutor, X: Send + Sync>(
        &self,
        func: impl FnOnce(T) -> X + Send + 'static,
    ) -> CompletionStage<X> {
        self.then_apply_async::<E, X>(|comp| comp.map(func))
    }

    pub fn and_then_apply_async_with_error<E: FallibleExecutor<R>, X: Send + Sync, R>(
        &self,
        func: impl FnOnce(Result<T, R>) -> X + Send + 'static,
    ) -> CompletionStage<X> {
        self.then_apply_async_with_error::<E, X, R>(|comp| match comp {
            Ok(comp) => comp.map(|v| func(Ok(v))),
            Err(e) => Completion::Value(func(Err(e))),
        })
    }

    pub fn and_then_apply_async_with_executor<X: Send + Sync>(
        &self,
        executor: impl InstancedInfallibleExecutor + Send + 'static,
        func: impl FnOnce(T) -> X + Send + 'static,
    ) -> CompletionStage<X> {
        self.then_apply_async_with_executor(executor, |comp| comp.map(func))
    }

    pub fn and_then_apply_async_with_executor_and_error<X: Send + Sync, R>(
        &self,
        executor: impl InstancedFallibleExecutor<R> + Send + 'static,
        func: impl FnOnce(Result<T, R>) -> X + Send + 'static,
    ) -> CompletionStage<X> {
        self.then_apply_async_with_executor_and_error(executor, |comp| match comp {
            Ok(comp) => comp.map(|v| func(Ok(v))),
            Err(e) => Completion::Value(func(Err(e))),
        })
    }

    ///
    /// Crates a child stage that will complete after this stage is completed, and an additional task that transforms the intermediary result is completed.
    /// The closure is only executed if the stage is completed with a value. Should the stage be completed with `Panic` or `Taken` then the closure is never executed.
    ///
    /// # Order of execution
    /// The closure is executed before any closure that takes ownership of the value.
    /// The implementation does not guarantee any particular order of execution in relation to other tasks that only consume the reference to the value.
    ///
    /// # Triggers Execution
    /// If the stage is already completed, the closure is executed immediately in the current thread.
    /// If the stage is not yet completed, then the closure is executed in the thread that will complete the stage.
    ///
    /// # Panics
    /// if the closure is executed immediately and it panics.
    ///
    pub fn and_then_apply_ref<X: Send + Sync>(
        &self,
        func: impl FnOnce(&T) -> X + Send + 'static,
    ) -> CompletionStage<X> {
        self.then_apply_ref(|comp| comp.map(func))
    }

    pub fn and_then_apply_ref_async<E: InfallibleExecutor, X: Send + Sync>(
        &self,
        func: impl FnOnce(&T) -> X + Send + 'static,
    ) -> CompletionStage<X> {
        self.then_apply_ref_async::<E, X>(|comp| comp.map(func))
    }

    pub fn and_then_apply_ref_async_with_error<E: FallibleExecutor<R>, X: Send + Sync, R>(
        &self,
        func: impl FnOnce(Result<&T, R>) -> X + Send + 'static,
    ) -> CompletionStage<X> {
        self.then_apply_ref_async_with_error::<E, X, R>(|comp| match comp {
            Ok(comp) => comp.map(|v| func(Ok(v))),
            Err(e) => Completion::Value(func(Err(e))),
        })
    }

    pub fn and_then_apply_ref_async_with_executor<X: Send + Sync>(
        &self,
        executor: impl InstancedInfallibleExecutor + Send + 'static,
        func: impl FnOnce(&T) -> X + Send + 'static,
    ) -> CompletionStage<X> {
        self.then_apply_ref_async_with_executor(executor, |comp| comp.map(func))
    }

    pub fn and_then_apply_ref_async_with_executor_and_error<X: Send + Sync, R>(
        &self,
        executor: impl InstancedFallibleExecutor<R> + Send + 'static,
        func: impl FnOnce(Result<&T, R>) -> X + Send + 'static,
    ) -> CompletionStage<X> {
        self.then_apply_ref_async_with_executor_and_error(executor, |comp| match comp {
            Ok(comp) => comp.map(|v| func(Ok(v))),
            Err(e) => Completion::Value(func(Err(e))),
        })
    }

    /// Borrow a refence to the value of the stage without panicking.
    ///
    /// This function may block to wait for the stage to finish completing roughly for the given Duration.
    ///
    /// # Panics
    /// This function never panics.
    ///
    /// # Returns
    /// - `GetTimeoutResult::TimedOut` if blocking for longer would have been required.
    /// - `GetTimeoutResult::Value` if the value was successfully taken out of the stage.
    /// - `GetTimeoutResult::Panic` if the supplier of the stage panicked.
    /// - `GetTimeoutResult::Taken` if the stage already had its value taken previously or by another thread.
    ///
    #[must_use]
    pub fn get_ref_timeout(&self, timeout: Duration) -> GetTimeoutResult<RefGuard<'_, T>> {
        if timeout.is_zero() {
            return self.try_get_ref();
        }

        self.get_ref_until(Instant::now() + timeout)
    }

    /// Take the value out of the stage without panicking.
    ///
    /// This function may block to wait for the stage to finish completing roughly for the given duration.
    ///
    /// # Panics
    /// This function never panics.
    ///
    /// # Returns
    /// - `GetTimeoutResult::TimedOut` if blocking for longer would have been required.
    /// - `GetTimeoutResult::Value` if the value was successfully taken out of the stage.
    /// - `GetTimeoutResult::Panic` if the supplier of the stage panicked.
    /// - `GetTimeoutResult::Taken` if the stage already had its value taken previously or by another thread.
    /// - `GetTimeoutResult::DeadLock` if the current thread has the value in the stage borrowed.
    ///
    #[must_use]
    pub fn get_timeout(&self, timeout: Duration) -> GetTimeoutResult<T> {
        if timeout.is_zero() {
            return self.try_get();
        }

        self.get_until(Instant::now() + timeout)
    }

    /// Borrows a refence to the value of the stage without panicking.
    ///
    /// This function may block to wait for the stage to finish completing roughly until the given Instant has arrived.
    ///
    /// # Panics
    /// This function never panics.
    ///
    /// # Returns
    /// - `GetTimeoutResult::TimedOut` if blocking for longer would have been required.
    /// - `GetTimeoutResult::Value` if the value was successfully taken out of the stage.
    /// - `GetTimeoutResult::Panic` if the supplier of the stage panicked.
    /// - `GetTimeoutResult::DeadLock` if the supplier of the stage deadlocked.
    /// - `GetTimeoutResult::Taken` if the stage already had its value taken previously or by another thread.
    ///
    #[must_use]
    pub fn get_ref_until(&self, until: Instant) -> GetTimeoutResult<RefGuard<'_, T>> {
        if self.0.completed.load(SeqCst) {
            match CellValue::map_ref(self.0.cell.read_recursive()) {
                CellValue::None => (),
                CellValue::Panic => return GetTimeoutResult::Panic,
                CellValue::Taken => return GetTimeoutResult::Taken,
                CellValue::DeadLock => return GetTimeoutResult::DeadLock,
                CellValue::Value(dta) => {
                    return GetTimeoutResult::Value(RefGuard::new(dta, Arc::clone(&self.0)));
                }
            }
        }

        let mut locked = self.0.tasks.lock();
        loop {
            match CellValue::map_ref(self.0.cell.read_recursive()) {
                CellValue::None => (),
                CellValue::Panic => return GetTimeoutResult::Panic,
                CellValue::Taken => return GetTimeoutResult::Taken,
                CellValue::DeadLock => return GetTimeoutResult::DeadLock,
                CellValue::Value(dta) => {
                    return GetTimeoutResult::Value(RefGuard::new(dta, Arc::clone(&self.0)));
                }
            }
            if self.0.cell_cond.wait_until(&mut locked, until).timed_out() {
                return GetTimeoutResult::TimedOut;
            }
        }
    }

    /// Take the value out of the stage without panicking.
    ///
    /// This function may block to wait for the stage to finish completing roughly until the given Instant has arrived.
    ///
    /// # Panics
    /// This function never panics.
    ///
    /// # Returns
    /// - `GetTimeoutResult::TimedOut` if blocking for longer would have been required.
    /// - `GetTimeoutResult::Value` if the value was successfully taken out of the stage.
    /// - `GetTimeoutResult::Panic` if the supplier of the stage panicked.
    /// - `GetTimeoutResult::Taken` if the stage already had its value taken previously or by another thread.
    /// - `GetTimeoutResult::DeadLock` if the current thread has the value in the stage borrowed.
    ///
    #[must_use]
    pub fn get_until(&self, until: Instant) -> GetTimeoutResult<T> {
        if self.borrowed_by_current_thread() {
            return GetTimeoutResult::DeadLock;
        }

        if self.0.completed.load(SeqCst) {
            let taken = self.0.cell.write().take();
            match taken {
                CellValue::None => (),
                CellValue::Panic => return GetTimeoutResult::Panic,
                CellValue::Taken => return GetTimeoutResult::Taken,
                CellValue::DeadLock => return GetTimeoutResult::DeadLock,
                CellValue::Value(dta) => return GetTimeoutResult::Value(dta),
            }
        }

        let mut locked = self.0.tasks.lock();
        loop {
            let taken = self.0.cell.write().take();
            match taken {
                CellValue::None => (),
                CellValue::Panic => return GetTimeoutResult::Panic,
                CellValue::Taken => return GetTimeoutResult::Taken,
                CellValue::DeadLock => return GetTimeoutResult::DeadLock,
                CellValue::Value(dta) => return GetTimeoutResult::Value(dta),
            }
            if self.0.cell_cond.wait_until(&mut locked, until).timed_out() {
                return GetTimeoutResult::TimedOut;
            }
        }
    }

    /// Block until the stage is completed and then borrow the stage's value without panicking or blocking for a significant amount of time.
    ///
    /// This function will not block to wait for the completion of the stage.
    /// It may block for an insignificant amount of time if completion of the stage is known to be immediately imminent.
    ///
    ///
    /// # Returns
    /// - `GetTimeoutResult::TimedOut` if blocking would have been required
    /// - `GetTimeoutResult::Value` if the value was successfully taken out of the stage.
    /// - `GetTimeoutResult::Panic` if the supplier of the stage panicked.
    /// - `GetTimeoutResult::Taken` if a reference to the stages was successfully created.
    ///
    #[must_use]
    pub fn try_get_ref(&self) -> GetTimeoutResult<RefGuard<'_, T>> {
        if !self.0.completed.load(SeqCst) {
            return GetTimeoutResult::TimedOut;
        }
        match CellValue::map_ref(self.0.cell.read_recursive()) {
            CellValue::None => GetTimeoutResult::TimedOut,
            CellValue::Panic => GetTimeoutResult::Panic,
            CellValue::Taken => GetTimeoutResult::Taken,
            CellValue::DeadLock => GetTimeoutResult::DeadLock,
            CellValue::Value(dta) => {
                GetTimeoutResult::Value(RefGuard::new(dta, Arc::clone(&self.0)))
            }
        }
    }

    /// Block until the stage is completed and then borrow the stage's value without panicking.
    ///
    /// # Panics
    /// This function never panics.
    ///
    /// # Returns
    /// - `GetRefResult::Value` if the value was successfully taken out of the stage.
    /// - `GetRefResult::Panic` if the supplier of the stage panicked.
    /// - `GetRefResult::Taken` if a reference to the stages was successfully created.
    /// - `GetRefResult::DeadLock` if the supplier of the stage deadlocked.
    ///
    #[must_use]
    pub fn get_ref(&self) -> Completion<RefGuard<'_, T>> {
        if self.0.completed.load(SeqCst) {
            match CellValue::map_ref(self.0.cell.read_recursive()) {
                CellValue::None => (),
                CellValue::Panic => return Completion::Panic,
                CellValue::Taken => return Completion::Taken,
                CellValue::DeadLock => return Completion::DeadLock,
                CellValue::Value(guard) => {
                    return Completion::Value(RefGuard::new(guard, Arc::clone(&self.0)));
                }
            }
        }

        let mut locked = self.0.tasks.lock();
        loop {
            match CellValue::map_ref(self.0.cell.read_recursive()) {
                CellValue::None => (),
                CellValue::Panic => return Completion::Panic,
                CellValue::Taken => return Completion::Taken,
                CellValue::DeadLock => return Completion::DeadLock,
                CellValue::Value(guard) => {
                    return Completion::Value(RefGuard::new(guard, Arc::clone(&self.0)));
                }
            }
            self.0.cell_cond.wait(&mut locked);
        }
    }

    /// Take the value out of the stage without blocking for significant amounts of time or panicking.
    ///
    /// # Panics
    /// This function never panics.
    ///
    /// # Returns
    /// - `GetTimeoutResult::TimedOut` if blocking would have been required
    /// - `GetTimeoutResult::Value` if the value was successfully taken out of the stage.
    /// - `GetTimeoutResult::Panic` if the supplier of the stage panicked.
    /// - `GetTimeoutResult::Taken` if the stage already had its value taken previously or by another thread.
    /// - `GetTimeoutResult::DeadLock` if the current thread has the value in the stage borrowed or a supplier deadlocked.
    ///
    #[must_use]
    pub fn try_get(&self) -> GetTimeoutResult<T> {
        if !self.0.completed.load(SeqCst) {
            return GetTimeoutResult::TimedOut;
        }

        if self.borrowed_by_current_thread() {
            return GetTimeoutResult::DeadLock;
        }

        let taken = self.0.cell.write().take();
        match taken {
            CellValue::None => GetTimeoutResult::TimedOut,
            CellValue::Panic => GetTimeoutResult::Panic,
            CellValue::Taken => GetTimeoutResult::Taken,
            CellValue::DeadLock => GetTimeoutResult::DeadLock,
            CellValue::Value(dta) => GetTimeoutResult::Value(dta),
        }
    }

    /// Blocks until the stage is completed and takes the value ouf of the stage without panicking
    ///
    /// # Panics
    /// This function never panics.
    ///
    /// # Returns
    /// - `Completion::Value` if the value was successfully taken out of the stage.
    /// - `Completion::Panic` if the supplier of the stage panicked.
    /// - `Completion::Taken` if the stage already had its value taken previously or by another thread.
    /// - `Completion::WouldDeadlock` if the current thread has the value in the stage borrowed.
    ///
    #[must_use]
    pub fn get(&self) -> Completion<T> {
        if self.borrowed_by_current_thread() {
            return Completion::DeadLock;
        }

        if self.0.completed.load(SeqCst) {
            let taken = self.0.cell.write().take();
            match taken {
                CellValue::None => (),
                CellValue::Panic => return Completion::Panic,
                CellValue::Taken => return Completion::Taken,
                CellValue::DeadLock => return Completion::DeadLock,
                CellValue::Value(dta) => return Completion::Value(dta),
            }
        }

        let mut locked = self.0.tasks.lock();
        loop {
            let taken = self.0.cell.write().take();
            match taken {
                CellValue::None => (),
                CellValue::Panic => return Completion::Panic,
                CellValue::Taken => return Completion::Taken,
                CellValue::DeadLock => return Completion::DeadLock,
                CellValue::Value(dta) => return Completion::Value(dta),
            }
            self.0.cell_cond.wait(&mut locked);
        }
    }

    ///
    /// Attempts to return a reference to the result of the stage without blocking for a significant amount of time.
    /// This function may still block for an insignificant amount of time when completion of the sage is imminent.
    ///
    /// If the stage is yet completed and completion is not immediately imminent,
    /// then this function returns `TryResult::WouldBlock`
    ///
    /// # Panics
    /// if the supplier of the stage either panicked or deadlocked.
    ///
    #[must_use]
    pub fn try_borrow(&self) -> TryResult<RefGuard<'_, T>> {
        if !self.0.completed.load(SeqCst) {
            return TryResult::WouldBlock;
        }

        match CellValue::map_ref(self.0.cell.read_recursive()) {
            CellValue::None => TryResult::WouldBlock,
            CellValue::Taken => TryResult::Taken,
            CellValue::Panic => panic!("supplier panicked"),
            CellValue::DeadLock => panic!("stage deadlocked"),
            CellValue::Value(guard) => TryResult::Value(RefGuard::new(guard, Arc::clone(&self.0))),
        }
    }

    ///
    /// Blocks until stage resolves and returns a reference to the result of the stage.
    ///
    /// # Panics
    /// if the supplier of this stage panics or a deadlock occurred.
    ///
    /// # None
    /// if the stage has been completed and ownership of the value was taken out of the stage.
    ///
    /// # Some
    /// if the stage has been completed and a `ReadLock` on a non-taken value was obtained.
    /// The `ReadLock` is relinquished once the returned guard is dropped.
    /// While the `ReadLock` is held, the value cannot be taken out of the stage.
    ///
    #[must_use]
    pub fn borrow(&self) -> Option<RefGuard<'_, T>> {
        if self.0.completed.load(SeqCst) {
            match CellValue::map_ref(self.0.cell.read_recursive()) {
                CellValue::None => (),
                CellValue::Taken => return None,
                CellValue::Panic => panic!("supplier panicked"),
                CellValue::DeadLock => panic!("stage deadlocked"),
                CellValue::Value(guard) => return Some(RefGuard::new(guard, Arc::clone(&self.0))),
            }
        }

        let mut locked = self.0.tasks.lock();
        loop {
            match CellValue::map_ref(self.0.cell.read_recursive()) {
                CellValue::None => (),
                CellValue::Taken => return None,
                CellValue::Panic => panic!("supplier panicked"),
                CellValue::DeadLock => panic!("stage deadlocked"),
                CellValue::Value(guard) => return Some(RefGuard::new(guard, Arc::clone(&self.0))),
            }
            self.0.cell_cond.wait(&mut locked);
        }
    }

    ///
    /// Takes the value from the stage if it has been completed.
    ///
    /// # Panics
    /// - if the supplier of this stage panics
    /// - if the current thread also has the result of the stage borrowed, causing a deadlock.
    ///
    /// # Returns
    /// `TryResult::WouldBlock` - if the stage has not yet completed.
    /// `TryResult::Taken` - if the value has already been taken out of the stage
    /// `TryResult::Value` - if the value was successfully taken out of the stage
    ///
    ///
    #[must_use]
    pub fn try_take(&self) -> TryResult<T> {
        if !self.0.completed.load(SeqCst) {
            return TryResult::WouldBlock;
        }

        assert!(!self.borrowed_by_current_thread());

        let taken = self.0.cell.write().take();

        match taken {
            CellValue::None => TryResult::WouldBlock,
            CellValue::Taken => TryResult::Taken,
            CellValue::Panic => panic!("supplier panicked"),
            CellValue::DeadLock => panic!("stage deadlocked"),
            CellValue::Value(v) => TryResult::Value(v),
        }
    }

    /// This function tries to take the value from the stage and place it into the given mutable reference.
    /// The returned Value in the `TryResult` is not the value from the stage but rather the value
    /// that was stored in the reference previously.
    #[must_use]
    pub fn try_take_into(&self, into: &mut T) -> TryResult<T> {
        match self.try_take() {
            TryResult::Value(v) => TryResult::Value(mem::replace(into, v)),
            o => o,
        }
    }

    ///
    /// Same as calling `self.take().unwrap()`
    ///
    /// # Panics
    /// whenever `self.take()` returns none
    ///
    #[must_use]
    pub fn unwrap(&self) -> T {
        self.take().expect("value already taken from stage")
    }

    /// Blocks until the stage is complete and takes the value out of the stage.
    ///
    /// # Panics
    /// - if the supplier of this stage panics
    /// - if the current thread also has the result of the stage borrowed, causing a deadlock.
    ///
    /// # Returns
    /// - Some - if the value was taken from the stage
    /// - None - if the value was already taken by a previous call or another thread.
    ///
    #[must_use]
    pub fn take(&self) -> Option<T> {
        assert!(!self.borrowed_by_current_thread(), "deadlock detected");

        if self.0.completed.load(SeqCst) {
            let taken = self.0.cell.write().take();
            match taken {
                CellValue::None => (),
                CellValue::Panic => panic!("supplier panicked"),
                CellValue::Taken => return None,
                CellValue::DeadLock => panic!("stage deadlocked"),
                CellValue::Value(dta) => return Some(dta),
            }
        }

        let mut locked = self.0.tasks.lock();
        loop {
            let taken = self.0.cell.write().take();
            match taken {
                CellValue::None => (),
                CellValue::Panic => panic!("supplier panicked"),
                CellValue::Taken => return None,
                CellValue::DeadLock => panic!("stage deadlocked"),
                CellValue::Value(dta) => return Some(dta),
            }
            self.0.cell_cond.wait(&mut locked);
        }
    }

    /// Returns true if the value has already been taken out of the stage or if the value will be taken immediately by a child stage once the stage completed.
    #[must_use]
    pub fn taken(&self) -> bool {
        if self.borrowed_by_current_thread() {
            return false;
        }

        let guard = self.0.cell.read_recursive();
        if matches!(&*guard, CellValue::Taken) {
            return true;
        }
        drop(guard);

        let locked = self.0.tasks.lock();
        if matches!(locked.taker, Taker::Some(_)) {
            return true;
        }
        drop(locked);

        false
    }

    /// Returns true if the stage is either completed or completion is immediately imminent
    #[must_use]
    pub fn completed(&self) -> bool {
        self.0.completed.load(SeqCst)
    }

    /// Returns true if the current thread currently borrows the result of the stage.
    #[must_use]
    pub fn borrowed_by_current_thread(&self) -> bool {
        self.0
            .thread_borrow_counts
            .lock()
            .contains_key(&std::thread::current().id())
    }

    pub fn compose<Y>(&self, func: impl FnOnce(&Self) -> Y) -> &Self {
        _ = func(self);
        self
    }

    pub fn then_compose<X: Sync + Send>(
        &self,
        func: impl FnOnce(Completion<T>) -> CompletionStage<X> + 'static + Send,
    ) -> CompletionStage<X> {
        let new_stage = CompletionStage::new();
        let ncl = new_stage.clone();
        self.then_run_internal(move |comp, q| {
            q.push(move |q| {
                func(comp).then_run_internal_queue(
                    move |comp, q| match comp {
                        Completion::Taken => _ = ncl.complete_internal(Completion::Taken, q),
                        Completion::Panic => _ = ncl.complete_internal(Completion::Panic, q),
                        Completion::Value(v) => _ = ncl.complete_internal(Completion::Value(v), q),
                        Completion::DeadLock => _ = ncl.complete_internal(Completion::DeadLock, q),
                    },
                    q,
                );
            });
        });

        new_stage
    }

    pub fn then_compose_async<E: InfallibleExecutor, X: Sync + Send>(
        &self,
        func: impl FnOnce(Completion<T>) -> CompletionStage<X> + 'static + Send,
    ) -> CompletionStage<X> {
        let new_stage = CompletionStage::new();
        let ncl = new_stage.clone();
        self.then_run_internal(move |comp, q| {
            q.push(move |_| {
                func(comp).then_run_async::<E>(move |comp| match comp {
                    Completion::Taken => _ = ncl.complete(Completion::Taken),
                    Completion::Panic => _ = ncl.complete(Completion::Panic),
                    Completion::Value(v) => _ = ncl.complete_with_value(v),
                    Completion::DeadLock => _ = ncl.complete(Completion::DeadLock),
                });
            });
        });

        new_stage
    }

    pub fn then_compose_async_with_executor<X: Sync + Send>(
        &self,
        executor: impl InstancedInfallibleExecutor + Send + 'static,
        func: impl FnOnce(Completion<T>) -> CompletionStage<X> + 'static + Send,
    ) -> CompletionStage<X> {
        let new_stage = CompletionStage::new();
        let ncl = new_stage.clone();
        self.then_run_async_with_executor(executor, move |comp| {
            func(comp).then_complete(ncl);
        });

        new_stage
    }

    pub fn then_compose_async_with_error<E: FallibleExecutor<R>, X: Sync + Send, R>(
        &self,
        func: impl FnOnce(Result<Completion<T>, R>) -> CompletionStage<X> + 'static + Send,
    ) -> CompletionStage<X> {
        let new_stage = CompletionStage::new();
        let ncl = new_stage.clone();
        self.then_run_async_with_error::<E, R>(move |comp| {
            func(comp).then_complete(ncl);
        });

        new_stage
    }

    pub fn then_compose_async_with_executor_and_error<X: Sync + Send, R>(
        &self,
        executor: impl InstancedFallibleExecutor<R> + Send + 'static,
        func: impl FnOnce(Result<Completion<T>, R>) -> CompletionStage<X> + 'static + Send,
    ) -> CompletionStage<X> {
        let new_stage = CompletionStage::new();
        let ncl = new_stage.clone();
        self.then_run_async_with_executor_and_error(executor, move |comp| {
            func(comp).then_complete(ncl);
        });

        new_stage
    }

    pub fn then_combine<X: Send + Sync, Y: Send + Sync>(
        &self,
        other: &CompletionStage<X>,
        func: impl FnOnce(Completion<T>, Completion<X>) -> Completion<Y> + 'static + Send,
    ) -> CompletionStage<Y> {
        let stage = CompletionStage::new();
        let scl = stage.clone();
        let ocl = other.clone();
        self.then_run_internal(move |comp, q| {
            q.push(move |q| {
                ocl.then_run_internal_queue(
                    move |comp2, q| {
                        q.push(move |q| {
                            scl.complete_internal(func(comp, comp2), q);
                        })
                    },
                    q,
                );
            });
        });

        stage
    }

    /// Returns a completion stage that completes when any of the given stages completes.
    #[must_use]
    pub fn any_of(&self, any: &[Self]) -> Self {
        let new_stage = Self::new();
        for stage in any {
            if new_stage.completed() {
                return new_stage;
            }

            if stage.completed() {
                new_stage.complete(stage.get());
                return new_stage;
            }

            stage.then_complete(new_stage.clone());
        }

        new_stage
    }

    /// Returns a completion stage that completes when all given stages are completed.
    ///
    /// The returned stage completes with a Vec that has the same order as the input slice.
    #[must_use]
    pub fn all_of(&self, all: &[Self]) -> CompletionStage<Vec<Completion<T>>> {
        let new_stage = CompletionStage::new();
        let all_len = all.len();
        let result = Arc::new(Mutex::new(Vec::<Completion<T>>::new()));
        for stage in all {
            let cp = Arc::clone(&result);
            let scl = new_stage.clone();
            stage.then_run_internal(move |comp, q| {
                q.push(move |q| {
                    let mut guard = cp.lock();
                    guard.push(match comp {
                        Completion::Taken => Completion::Taken,
                        Completion::Panic => Completion::Panic,
                        Completion::Value(v) => Completion::Value(v),
                        Completion::DeadLock => Completion::DeadLock,
                    });

                    if guard.len() == all_len {
                        scl.complete_internal(Completion::Value(mem::take(&mut *guard)), q);
                    }
                });
            });
        }

        new_stage
    }
}

/// Static Executor that can only fail to execute something by panicking
pub trait InfallibleExecutor {
    fn execute(task: impl FnOnce() + Send + 'static);
}

/// Static Executor that can fail with a given error when trying to execute something.
/// Error refers to an OS error like "I cannot spawn more threads" rather than any error the task may produce
pub trait FallibleExecutor<R> {
    /// Execute the given or schedule it for execution
    ///
    /// # Errors
    /// If execution of the task could not be started because of, for example, os' limits on threads.
    fn execute(task: impl FnOnce() + Send + 'static) -> Result<(), R>;
}

/// Executor that can only fail to execute something by panicking
pub trait InstancedInfallibleExecutor {
    /// Execute the given or schedule it for execution
    fn execute(self, task: impl FnOnce() + Send + 'static);
}

/// Executor that can fail with a given error when trying to execute something.
/// Error refers to an OS error like "I cannot spawn more threads" rather than any error the task may produce
pub trait InstancedFallibleExecutor<R> {
    /// Execute the given or schedule it for execution
    ///
    /// # Errors
    /// If execution of the task could not be started because of, for example, os' limits on threads.
    fn execute(self, task: impl FnOnce() + Send + 'static) -> Result<(), R>;
}

impl InfallibleExecutor for thread::Thread {
    fn execute(task: impl FnOnce() + Send + 'static) {
        thread::spawn(task);
    }
}

impl FallibleExecutor<Error> for thread::Builder {
    fn execute(task: impl FnOnce() + Send + 'static) -> Result<(), Error> {
        Self::new().spawn(task).map(|_| ())
    }
}

impl InstancedFallibleExecutor<Error> for thread::Builder {
    fn execute(self, task: impl FnOnce() + Send + 'static) -> Result<(), Error> {
        self.spawn(task).map(|_| ())
    }
}

impl InstancedInfallibleExecutor for () {
    fn execute(self, task: impl FnOnce() + Send + 'static) {
        thread::spawn(task);
    }
}

impl InfallibleExecutor for () {
    fn execute(task: impl FnOnce() + Send + 'static) {
        thread::spawn(task);
    }
}
