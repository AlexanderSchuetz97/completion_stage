use defer_heavy::defer;
use parking_lot::{Condvar, MappedRwLockReadGuard, Mutex, RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::io::Error;
use std::ops::{Deref, DerefMut};
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering::SeqCst;
use std::sync::Arc;
use std::thread::ThreadId;
use std::time::{Duration, Instant};
use std::{mem, thread};


#[derive(Debug)]
pub struct CompletionStage<T: Send+Sync+'static>(Arc<CompletionStageInner<T>>);

impl<T: Send+Sync+'static> Clone for CompletionStage<T> {
    fn clone(&self) -> Self {
        CompletionStage(self.0.clone())
    }
}

/// Completion that did not take the value out of the stage.
#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub enum Completion<T> {
    Taken,
    Panic,
    Value(T)
}

impl<T> Completion<T> {
    pub fn unwrap(self) -> T {
        match self {
            Completion::Taken => panic!("unwrap called on Taken Completion"),
            Completion::Panic => panic!("unwrap called on Panic Completion"),
            Completion::Value(v) => v
        }
    }

    pub fn some(self) -> Option<T> {
        match self {
            Completion::Value(v) => Some(v),
            _ => None,
        }
    }
}

impl<T> From<Completion<T>> for TakenCompletion<T> {
    fn from(value: Completion<T>) -> Self {
        match value {
            Completion::Taken => TakenCompletion::Taken,
            Completion::Panic => TakenCompletion::Panic,
            Completion::Value(v) => TakenCompletion::Value(v)
        }
    }
}

/// Completion that took the value out of the stage.
/// This type of completion may fail due to deadlocks.
#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub enum TakenCompletion<T> {
    Taken,
    Panic,
    Value(T),

    /// The implementation was able to detect a deadlock, this can only occur
    /// if a custom executor uses or re-uses a thread which also
    /// borrows the result of the stage. An executor which uses virgin threads can never cause a deadlock.
    DeadLock
}

impl<T> TakenCompletion<T> {
    pub fn unwrap(self) -> T {
        match self {
            TakenCompletion::Taken => panic!("unwrap called on Taken TakenCompletion"),
            TakenCompletion::Panic => panic!("unwrap called on Panic TakenCompletion"),
            TakenCompletion::DeadLock => panic!("unwrap called on Deadlock TakenCompletion"),
            TakenCompletion::Value(v) => v
        }
    }

    pub fn some(self) -> Option<T> {
        match self {
            TakenCompletion::Value(v) => Some(v),
            _ => None,
        }
    }
}

impl<T> TryFrom<TakenCompletion<T>> for Completion<T> {
    type Error = ();

    fn try_from(value: TakenCompletion<T>) -> Result<Self, Self::Error> {
        match value {
            TakenCompletion::Taken => Ok(Completion::Taken),
            TakenCompletion::Panic => Ok(Completion::Panic),
            TakenCompletion::Value(v) => Ok(Completion::Value(v)),
            TakenCompletion::DeadLock => Err(())
        }
    }
}



#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub enum ExecutorCompletion<T, E> {
    Taken,
    Panic,
    Value(T),
    Err(E),
}

impl<T, E> ExecutorCompletion<T, E> {
    pub fn unwrap(self) -> T {
        match self {
            ExecutorCompletion::Taken => panic!("unwrap called on Taken ExecutorCompletion"),
            ExecutorCompletion::Panic => panic!("unwrap called on Panic ExecutorCompletion"),
            ExecutorCompletion::Err(_) => panic!("unwrap called on Err ExecutorCompletion"),
            ExecutorCompletion::Value(v) => v
        }
    }

    pub fn unwrap_err(self) -> E {
        match self {
            ExecutorCompletion::Taken => panic!("unwrap_err called on Taken ExecutorCompletion"),
            ExecutorCompletion::Panic => panic!("unwrap_err called on Panic ExecutorCompletion"),
            ExecutorCompletion::Err(e) => e,
            ExecutorCompletion::Value(_) => panic!("unwrap_err called on Value ExecutorCompletion"),
        }
    }

    pub fn ok(self) -> Option<T> {
        match self {
            ExecutorCompletion::Value(v) => Some(v),
            _ => None,
        }
    }

    pub fn err(self) -> Option<E> {
        match self {
            ExecutorCompletion::Err(v) => Some(v),
            _ => None,
        }
    }
}


impl<T, E> From<Completion<T>> for ExecutorCompletion<T, E> {
    fn from(value: Completion<T>) -> Self {
        match value {
            Completion::Taken => ExecutorCompletion::Taken,
            Completion::Panic => ExecutorCompletion::Panic,
            Completion::Value(value) => ExecutorCompletion::Value(value),
        }
    }
}

impl<T, E> TryFrom<ExecutorCompletion<T, E>> for Completion<T> {
    type Error = E;

    fn try_from(value: ExecutorCompletion<T, E>) -> Result<Self, Self::Error> {
        match value {
            ExecutorCompletion::Taken => Ok(Completion::Taken),
            ExecutorCompletion::Panic => Ok(Completion::Panic),
            ExecutorCompletion::Value(value) => Ok(Completion::Value(value)),
            ExecutorCompletion::Err(e) => Err(e)
        }
    }
}


#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub enum TakenExecutorCompletion<T, E> {
    Taken,
    Panic,

    /// The implementation was able to detect a deadlock, this can only occur
    /// if a custom executor uses or re-uses a thread which also
    /// borrows the result of the stage. An executor which uses virgin threads can never cause a deadlock.
    DeadLock,
    Value(T),

    /// The executor refused to execute the task.
    /// Note: If the completion function is called with this result,
    /// then it is not called on a thread managed by the executor.
    Err(E),
}

impl<T, E> TakenExecutorCompletion<T, E> {
    pub fn unwrap(self) -> T {
        match self {
            TakenExecutorCompletion::Taken => panic!("unwrap called on Taken TakenExecutorCompletion"),
            TakenExecutorCompletion::Panic => panic!("unwrap called on Panic TakenExecutorCompletion"),
            TakenExecutorCompletion::Err(_) => panic!("unwrap called on Err TakenExecutorCompletion"),
            TakenExecutorCompletion::DeadLock => panic!("unwrap called on Deadlock TakenExecutorCompletion"),
            TakenExecutorCompletion::Value(v) => v
        }
    }

    pub fn unwrap_err(self) -> E {
        match self {
            TakenExecutorCompletion::Taken => panic!("unwrap_err called on Taken TakenExecutorCompletion"),
            TakenExecutorCompletion::Panic => panic!("unwrap_err called on Panic TakenExecutorCompletion"),
            TakenExecutorCompletion::Err(e) => e,
            TakenExecutorCompletion::DeadLock => panic!("unwrap_err called on Deadlock TakenExecutorCompletion"),
            TakenExecutorCompletion::Value(_) => panic!("unwrap_err called on Value TakenExecutorCompletion"),
        }
    }

    pub fn ok(self) -> Option<T> {
        match self {
            TakenExecutorCompletion::Value(v) => Some(v),
            _ => None,
        }
    }

    pub fn err(self) -> Option<E> {
        match self {
            TakenExecutorCompletion::Err(v) => Some(v),
            _ => None,
        }
    }
}

impl<T, E> TryFrom<TakenExecutorCompletion<T, E>> for TakenCompletion<T> {
    type Error = E;

    fn try_from(value: TakenExecutorCompletion<T, E>) -> Result<Self, Self::Error> {
        match value {
            TakenExecutorCompletion::Taken => Ok(TakenCompletion::Taken),
            TakenExecutorCompletion::Panic => Ok(TakenCompletion::Panic),
            TakenExecutorCompletion::Value(value) => Ok(TakenCompletion::Value(value)),
            TakenExecutorCompletion::DeadLock => Ok(TakenCompletion::DeadLock),
            TakenExecutorCompletion::Err(e) => Err(e)
        }
    }
}

impl<T, E> From<TakenCompletion<T>> for TakenExecutorCompletion<T, E> {
    fn from(value: TakenCompletion<T>) -> Self {
        match value {
            TakenCompletion::Taken => TakenExecutorCompletion::Taken,
            TakenCompletion::Panic => TakenExecutorCompletion::Panic,
            TakenCompletion::Value(value) => TakenExecutorCompletion::Value(value),
            TakenCompletion::DeadLock => TakenExecutorCompletion::DeadLock,
        }
    }
}





#[derive(Debug)]
enum CellValue<T> {
    None,
    Taken,
    Panic,
    Value(T)
}

impl<T> CellValue<T> {
    fn take(&mut self) -> CellValue<T> {
        match self {
            CellValue::None => CellValue::None,
            CellValue::Taken => CellValue::Taken,
            CellValue::Panic => CellValue::Panic,
            CellValue::Value(_) => {
                mem::replace(self, CellValue::Taken)
            }
        }
    }

    fn map_ref(guard: RwLockReadGuard<CellValue<T>>) -> CellValue<MappedRwLockReadGuard<T>> {
        match guard.deref() {
            CellValue::None => CellValue::None,
            CellValue::Taken => CellValue::Taken,
            CellValue::Panic => CellValue::Panic,
            CellValue::Value(_) => {
                CellValue::Value(RwLockReadGuard::map(guard, |grd| {
                    let CellValue::Value(value) = grd else  {
                        //TODO can I use try map to not have this here?
                        //Should be fine but ehh.
                        unreachable!()
                    };

                    value
                }))
            }
        }
    }
}

enum Taker<T: Send+Sync> {
    None,
    Some(Box<dyn FnOnce(TakenCompletion<T>)+Send>),
    Closed,
}

impl<T: Send+Sync> Debug for Taker<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Taker::None => f.write_str("None"),
            Taker::Some(_) => f.write_str("Some"),
            Taker::Closed => f.write_str("Closed"),
        }
    }
}

impl<T: Send+Sync> Taker<T> {

    fn is_none(&self) -> bool {
        matches!(self, Taker::None)
    }
    
    fn take(&mut self) -> Option<Box<dyn FnOnce(TakenCompletion<T>)+Send>> {
        if let Taker::Some(fbox) = mem::replace(self, Taker::Closed) {
            return Some(fbox);
        }
        
        None
        
    }
}
#[derive(Debug)]
enum RefTaskState<T: Send+Sync> {
    Closed,
    Empty,
    Node(TaskStackNode<T>)
}

impl<T: Send+Sync> RefTaskState<T> {
    fn invoke_ref(self, data: &Completion<T>) {
        match self {
            RefTaskState::Closed => panic!("ref_task_state closed in complete even tho cell was not empty."),
            RefTaskState::Empty => (),
            RefTaskState::Node(mut node) => {
                let Some(task) = node.task.take() else {
                    panic!("node.task.take() is none");
                };


                Self::invoke_next(data, &mut node, task);

                while let Some(next) = node.next.take() {
                    node.next = next.next;
                    let Some(task) = node.task.take() else {
                        panic!("node.task.take() is none");
                    };

                    Self::invoke_next(data, &mut node, task);
                }
            }
        }
    }

    fn invoke(self, data: Completion<&T>) {
        match self {
            RefTaskState::Closed => panic!("ref_task_state closed in complete even tho cell was not empty."),
            RefTaskState::Empty => (),
            RefTaskState::Node(mut node) => {
                let Some(task) = node.task.take() else {
                    panic!("node.task.take() is none");
                };

                Self::invoke_next_ref(data, &mut node, task);

                while let Some(next) = node.next.take() {
                    node.next = next.next;
                    let Some(task) = node.task.take() else {
                        panic!("node.task.take() is none");
                    };

                    Self::invoke_next_ref(data, &mut node, task);
                }
            }
        }
    }

    fn unwind_panic_all(node: &mut TaskStackNode<T>) {
        if std::thread::panicking() {
            while let Some(next) = node.next.take() {
                node.next = next.next;
                let Some(task) = node.task.take() else {
                    panic!("node.task.take() is none");
                };

                task(Completion::Panic);
            }
        }
    }

    fn invoke_next(data: &Completion<T>, node: &mut TaskStackNode<T>, task: Box<dyn FnOnce(Completion<&T>) + Send>) {
        defer! {
            Self::unwind_panic_all(node);
        }
        task(match data {
            Completion::Taken => Completion::Taken,
            Completion::Panic => Completion::Panic,
            Completion::Value(val_ref) => Completion::Value(val_ref)
        });
    }

    fn invoke_next_ref(data: Completion<&T>, node: &mut TaskStackNode<T>, task: Box<dyn FnOnce(Completion<&T>) + Send>) {
        defer! {
            Self::unwind_panic_all(node);
        }
        task(data);
    }
}

#[derive(Debug)]
struct CompletionStageInner<T: Send+Sync> {
    completed: AtomicBool,
    thread_borrow_counts: Mutex<HashMap<ThreadId, usize>>,
    cell: RwLock<CellValue<T>>,
    tasks: Mutex<CompleteStageInnerTasks<T>>,
    cell_cond: Condvar,
}

#[derive(Debug)]
struct CompleteStageInnerTasks<T: Send+Sync> {
    ref_task_state: RefTaskState<T>,
    taker: Taker<T>,
}

fn handle_supplier_panic<T: Send+Sync>(inner: &Arc<CompletionStageInner<T>>) {
    if std::thread::panicking() {
        complete_inner::<T>(inner, Completion::Panic);
    }
}

fn complete_inner<T: Send+Sync>(inner: &Arc<CompletionStageInner<T>>, data: Completion<T>) -> Option<Completion<T>> {
    if inner.completed.load(SeqCst) {
        return Some(data);
    }

    let mut inner_state = inner.tasks.lock();
    let mut write_guard = inner.cell.write();
    if inner.completed.swap(true, SeqCst) {
        return Some(data);
    }

    let rts = mem::replace(&mut inner_state.ref_task_state, RefTaskState::Closed);

    if let Some(grd) = inner_state.taker.take() {
        *write_guard = CellValue::Taken;
        drop(inner_state);
        drop(write_guard);
        rts.invoke_ref(&data);
        grd(data.into());
        return None;
    }

    *write_guard = match data {
        Completion::Taken => CellValue::Taken,
        Completion::Panic => CellValue::Panic,
        Completion::Value(data) => CellValue::Value(data),
    };

    drop(inner_state);

    let read_guard = RwLockWriteGuard::downgrade(write_guard);

    inner.cell_cond.notify_all();

    match read_guard.deref() {
        CellValue::None =>  panic!("read_ref is none"),
        CellValue::Taken => rts.invoke(Completion::Taken),
        CellValue::Panic => rts.invoke(Completion::Panic),
        CellValue::Value(val_ref) => rts.invoke(Completion::Value(val_ref)),
    }


    None
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub enum GetTimeoutResult<T> {
    /// The call would have needed to block longer than intended.
    TimedOut,
    /// The current thread currently borrows the completed value of the stage.
    /// The call would have therefore resulted in a deadlock
    WouldDeadlock,
    /// The value was already taken out of the completed stage
    Taken,
    /// The stage could not be completed due to a panic.
    Panic,
    /// The completed value from the stage.
    Value(T)
}

impl<T> GetTimeoutResult<T> {
    pub fn unwrap(self) -> T {
        match self {
            GetTimeoutResult::TimedOut => panic!("unwrap called on TimedOut"),
            GetTimeoutResult::WouldDeadlock => panic!("unwrap called on WouldDeadlock"),
            GetTimeoutResult::Taken => panic!("unwrap called on Taken"),
            GetTimeoutResult::Panic => panic!("unwrap called on Panic"),
            GetTimeoutResult::Value(v) => v,
        }
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub enum GetRefTimeoutResult<T> {
    /// The call would have needed to block longer than intended.
    TimedOut,
    /// The value was already taken out of the completed stage
    Taken,
    /// The stage could not be completed due to a panic.
    Panic,
    /// The completed value from the stage.
    Value(T)
}

impl<T> GetRefTimeoutResult<T> {
    pub fn unwrap(self) -> T {
        match self {
            Self::TimedOut => panic!("unwrap called on TimedOut"),
            Self::Taken => panic!("unwrap called on Taken"),
            Self::Panic => panic!("unwrap called on Panic"),
            Self::Value(v) => v,
        }
    }
}


#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub enum TryGetResult<T> {
    WouldBlock,
    Taken,
    Panic,
    Some(T)
}



#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub enum GetRefResult<T> {
    Taken,
    Panic,
    Value(T)
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub enum TryResult<T> {
    Taken,
    WouldBlock,
    Value(T)
}


#[derive(Debug)]
pub struct RefGuard<'a, T: Send+Sync>(MappedRwLockReadGuard<'a, T>, Arc<CompletionStageInner<T>>);

impl<'a, T: Send+Sync> RefGuard<'a, T> {
    fn new(guard: MappedRwLockReadGuard<'a, T>, inner: Arc<CompletionStageInner<T>>) -> Self {
        let tid = thread::current().id();
        let mut g2 = inner.thread_borrow_counts.lock();
        if let Some(mt) = g2.get_mut(&tid) {
            assert!(*mt > 0);
            *mt +=1;
            drop(g2);
            return Self(guard, inner);
        }
        g2.insert(tid, 1);
        drop(g2);
        Self(guard, inner)
    }
}

impl<'a, T: Send+Sync> Deref for RefGuard<'a, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        self.0.deref()
    }
}

impl<'a, T: Send+Sync> Drop for RefGuard<'a, T> {
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

struct TaskStackNode<T: Send+Sync> {
    next: Option<Box<TaskStackNode<T>>>,
    task: Option<Box<dyn FnOnce(Completion<&T>) + Send>>,
}

impl<T: Send+Sync> Debug for TaskStackNode<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str("TaskStackNode")
    }
}

impl<T: Send+Sync + 'static> CompletionStage<T> {
    pub fn new() -> Self {
        Self(Arc::new(CompletionStageInner {
            completed: Default::default(),
            thread_borrow_counts: Default::default(),
            cell: RwLock::new(CellValue::None),
            tasks: Mutex::new(CompleteStageInnerTasks {
                ref_task_state: RefTaskState::Empty,
                taker: Taker::None,
            }),
            cell_cond: Condvar::new(),
        }))
    }

    pub fn new_completed(compl: Completion<T>) -> Self {
        match compl {
            Completion::Taken => Self::new_taken(),
            Completion::Panic => Self::new_panicked(),
            Completion::Value(val) => Self::new_completed_value(val),
        }
    }

    pub fn new_completed_value(data: T) -> Self {
        Self(Arc::new(CompletionStageInner {
            completed: AtomicBool::new(true),
            thread_borrow_counts: Default::default(),
            cell: RwLock::new(CellValue::Value(data)),
            tasks: Mutex::new(CompleteStageInnerTasks {
                ref_task_state: RefTaskState::Closed,
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
                ref_task_state: RefTaskState::Closed,
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
                ref_task_state: RefTaskState::Closed,
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
    ///             .and_then_apply_async(|intermediary_result| {
    ///                 //DO more work here
    ///             })
    ///             .take();
    /// }
    /// ```
    ///
    pub fn new_async_with_error<E: FallibleExecutor<R>, R>(supplier: impl FnOnce()->T + Send+'static) -> Result<Self, R> {
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
    pub fn new_async_with_executor<E: InstancedInfallibleExecutor>(executor: E, supplier: impl FnOnce()->T + Send+'static) -> Self {
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
    pub fn new_async_with_executor_and_error<E: InstancedFallibleExecutor<R>, R>(executor: E, supplier: impl FnOnce()->T + Send+'static) -> Result<Self, R> {
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
    pub fn new_async<E: InfallibleExecutor>(supplier: impl FnOnce()->T + Send+'static) -> Self {
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
    pub fn then_apply_ref<X: Send+Sync>(&self, func: impl FnOnce(Completion<&T>) -> Completion<X> + Send+'static) -> CompletionStage<X> {
        let next_stage = CompletionStage::new();
        let next_inner = next_stage.0.clone();

        self.then_run_ref(move |completion| {
            defer! {
                handle_supplier_panic(&next_inner)
            }
            complete_inner(&next_inner, func(completion));
        });

        next_stage
    }

    pub fn then_apply_ref_async<E: InfallibleExecutor, X: Send+Sync>(&self, func: impl FnOnce(Completion<&T>) -> Completion<X> + Send+'static) -> CompletionStage<X> {
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

    pub fn then_apply_ref_async_with_executor<X: Send+Sync>(&self, executor: impl InstancedInfallibleExecutor+Send+'static, func: impl FnOnce(Completion<&T>) -> Completion<X> + Send+'static) -> CompletionStage<X> {
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

    pub fn then_apply_ref_async_with_error<E: FallibleExecutor<R>, X: Send+Sync, R>(&self, func: impl FnOnce(ExecutorCompletion<&T, R>) -> Completion<X> + Send+'static) -> CompletionStage<X> {
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

    pub fn then_apply_ref_async_with_executor_and_error<X: Send+Sync, R>(&self, executor: impl InstancedFallibleExecutor<R>+Send+'static, func: impl FnOnce(ExecutorCompletion<&T, R>) -> Completion<X> + Send+'static) -> CompletionStage<X> {
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

    pub fn then_apply_async<E: InfallibleExecutor, X: Send+Sync>(&self, func: impl FnOnce(TakenCompletion<T>) -> Completion<X> + Send+'static) -> CompletionStage<X> {
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

    pub fn then_apply_async_with_executor<X: Send+Sync>(&self, executor: impl InstancedInfallibleExecutor+Send+'static, func: impl FnOnce(TakenCompletion<T>) -> Completion<X> + Send+'static) -> CompletionStage<X> {
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

    pub fn then_apply_async_with_error<E: FallibleExecutor<R>, X: Send+Sync, R>(&self, func: impl FnOnce(TakenExecutorCompletion<T, R>) -> Completion<X> + Send+'static) -> CompletionStage<X> {
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

    pub fn then_apply_async_with_executor_and_error<X: Send+Sync, R>(&self, executor: impl InstancedFallibleExecutor<R>+Send+'static, func: impl FnOnce(TakenExecutorCompletion<T, R>) -> Completion<X> + Send+'static) -> CompletionStage<X> {
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
    pub fn then_apply<X: Send+Sync>(&self, func: impl FnOnce(TakenCompletion<T>) -> Completion<X> + Send+'static) -> CompletionStage<X> {
        let next_stage = CompletionStage::new();
        let next_inner = next_stage.0.clone();
        self.then_run(move |completion| {
            defer! {
                handle_supplier_panic(&next_inner)
            }
            complete_inner(&next_inner, func(completion));
        });

        next_stage
    }

    pub fn then_run_async<E: InfallibleExecutor>(&self, func: impl FnOnce(TakenCompletion<T>) +Send+'static) -> &Self {
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

    pub fn then_run_async_with_executor(&self, executor: impl InstancedInfallibleExecutor+Send+'static, func: impl FnOnce(TakenCompletion<T>) +Send+'static) -> &Self {
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

    pub fn then_run_async_with_error<E: FallibleExecutor<R>, R>(&self, func: impl FnOnce(TakenExecutorCompletion<T, R>) +Send+'static) -> &Self {
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
                    f(completion.into());
                });
            }) {
                let Some(f) = ar.lock().take() else {
                    //TODO should we ignore this buggy implementation of InstancedFallibleExecutor?
                    panic!("executor.execute called the closure and returned an error");
                };
                f(TakenExecutorCompletion::Err(e));
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
                f(completion.into());
            }) {
                let Some(f) = ar.lock().take() else {
                    //TODO should we ignore this buggy implementation of InstancedFallibleExecutor?
                    panic!("executor.execute called the closure and returned an error");
                };
                f(TakenExecutorCompletion::Err(e));
            };
        })
    }

    pub fn then_run_async_with_executor_and_error<R>(&self, executor: impl InstancedFallibleExecutor<R>+Send+'static, func: impl FnOnce(TakenExecutorCompletion<T, R>) +Send+'static) -> &Self {
        self.then_run(move |completion| {
            let ar = Arc::new(Mutex::new(Some(func)));
            let cl = ar.clone();
            if let Err(e) = executor.execute(move || {
                let Some(f) = cl.lock().take() else {
                    //TODO should we ignore this buggy implementation of InstancedFallibleExecutor?
                    panic!("executor.execute called the closure and returned an error");
                };
                f(completion.into());
            }) {
                let Some(f) = ar.lock().take() else {
                    //TODO should we ignore this buggy implementation of InstancedFallibleExecutor?
                    panic!("executor.execute called the closure and returned an error");
                };
                f(TakenExecutorCompletion::Err(e));
            };
        })
    }

    pub fn then_run_ref_async<E: InfallibleExecutor>(&self, func: impl FnOnce(Completion<&T>) +Send+'static) -> &Self {
        let scl = self.clone();
        self.then_run_ref(|_| {
            E::execute(move || {
                match scl.try_get_ref() {
                    GetRefTimeoutResult::TimedOut => unreachable!(),
                    GetRefTimeoutResult::Taken => func(Completion::Taken),
                    GetRefTimeoutResult::Panic => func(Completion::Panic),
                    GetRefTimeoutResult::Value(data) => func(Completion::Value(data.deref()))
                };
            })
        })

    }


    pub fn then_run_ref_async_with_executor(&self, executor: impl InstancedInfallibleExecutor+Send+'static, func: impl FnOnce(Completion<&T>) +Send+'static) -> &Self {
        let scl = self.clone();
        self.then_run_ref(move |_| {
            executor.execute(move || {
                match scl.try_get_ref() {
                    GetRefTimeoutResult::TimedOut => unreachable!(),
                    GetRefTimeoutResult::Taken => func(Completion::Taken),
                    GetRefTimeoutResult::Panic => func(Completion::Panic),
                    GetRefTimeoutResult::Value(data) => func(Completion::Value(data.deref()))
                };
            });
        })
    }

    pub fn then_run_ref_async_with_error<E: FallibleExecutor<R>, R>(&self, func: impl FnOnce(ExecutorCompletion<&T, R>) +Send+'static) -> &Self {
        let scl = self.clone();
        self.then_run_ref(move |_| {
            let ar = Arc::new(Mutex::new(Some(func)));
            let cl = ar.clone();
            if let Err(e) = E::execute(move || {
                let Some(f) = cl.lock().take() else {
                    //TODO should we ignore this buggy implementation of InstancedFallibleExecutor?
                    panic!("executor::execute called the closure and returned an error");
                };
                match scl.try_get_ref() {
                    GetRefTimeoutResult::TimedOut => unreachable!(),
                    GetRefTimeoutResult::Taken => f(ExecutorCompletion::Taken),
                    GetRefTimeoutResult::Panic => f(ExecutorCompletion::Panic),
                    GetRefTimeoutResult::Value(data) => f(ExecutorCompletion::Value(data.deref()))
                };
            }) {
                let Some(f) = ar.lock().take() else {
                    //TODO should we ignore this buggy implementation of InstancedFallibleExecutor?
                    panic!("executor::execute called the closure and returned an error");
                };
                f(ExecutorCompletion::Err(e));
            };
        })
    }

    pub fn then_run_ref_async_with_executor_and_error<R>(&self, executor: impl InstancedFallibleExecutor<R>+Send+'static, func: impl FnOnce(ExecutorCompletion<&T, R>) +Send+'static) -> &Self {
        let scl = self.clone();
        self.then_run_ref(move |_| {
            let ar = Arc::new(Mutex::new(Some(func)));
            let cl = ar.clone();
            if let Err(e) = executor.execute(move || {
                let Some(f) = cl.lock().take() else {
                    //TODO should we ignore this buggy implementation of InstancedFallibleExecutor?
                    panic!("executor.execute called the closure and returned an error");
                };
                match scl.try_get_ref() {
                    GetRefTimeoutResult::TimedOut => unreachable!(),
                    GetRefTimeoutResult::Taken => f(ExecutorCompletion::Taken),
                    GetRefTimeoutResult::Panic => f(ExecutorCompletion::Panic),
                    GetRefTimeoutResult::Value(data) => f(ExecutorCompletion::Value(data.deref())),
                };
            }) {
                let Some(f) = ar.lock().take() else {
                    //TODO should we ignore this buggy implementation of InstancedFallibleExecutor?
                    panic!("executor.execute called the closure and returned an error");
                };
                f(ExecutorCompletion::Err(e));
            };
        })
    }

    pub fn then_run_ref(&self, func: impl FnOnce(Completion<&T>) +Send+'static) -> &Self {
        let mut locked = self.0.tasks.lock();
        let guard = self.0.cell.read_recursive();
        match guard.deref() {
            CellValue::None => (),
            CellValue::Panic => {
                drop(locked);
                func(Completion::Panic);
                return self;
            },
            CellValue::Taken => {
                drop(locked);
                func(Completion::Taken);
                return self;
            },
            CellValue::Value(data) => {
                drop(locked);
                func(Completion::Value(data));
                return self;
            },
        }
        drop(guard);

        let task : Option<Box<dyn FnOnce(Completion<&T>) + Send>> = Some(Box::new(func));

        match &mut locked.ref_task_state {
            RefTaskState::Closed => panic!("ref_task_state closed even tho cell was not empty."),
            RefTaskState::Empty => {
                locked.ref_task_state = RefTaskState::Node(TaskStackNode {
                    next: None,
                    task,
                });
            }
            RefTaskState::Node(node) => {
                node.next = Some(Box::new(TaskStackNode {
                    next: node.next.take(),
                    task,
                }));
            }
        }

        self
    }

    pub fn then_run(&self, func: impl FnOnce(TakenCompletion<T>) +Send+'static) -> &Self {
        if self.borrowed_by_current_thread() {
            //Alternatively, we could enqueue the task so its ran when we drop the guard, however
            //that would cause the drop to block if another thread also borrows. No one expects that.
            func(TakenCompletion::DeadLock);
            return self;
        }
        let mut locked = self.0.tasks.lock();
        let taken = self.0.cell.write().take();
        match taken {
            CellValue::None => (),
            CellValue::Panic => {
                drop(locked);
                func(TakenCompletion::Panic);
                return self
            },
            CellValue::Taken => {
                drop(locked);
                func(TakenCompletion::Taken);
                return self
            },
            CellValue::Value(data) =>  {
                drop(locked);
                func(TakenCompletion::Value(data));
                return self
            },
        }

        if !locked.taker.is_none() {
            let task : Option<Box<dyn FnOnce(Completion<&T>) + Send>> = Some(Box::new(move |comp| {
                _= match comp {
                    Completion::Taken | Completion::Value(_) => func(TakenCompletion::Taken),
                    Completion::Panic => func(TakenCompletion::Panic),
                }
            }));

            match &mut locked.ref_task_state {
                RefTaskState::Closed => panic!("ref_task_state closed even tho cell was not empty."),
                RefTaskState::Empty => {
                    locked.ref_task_state = RefTaskState::Node(TaskStackNode {
                        next: None,
                        task,
                    });
                }
                RefTaskState::Node(node) => {
                    node.next = Some(Box::new(TaskStackNode {
                        next: node.next.take(),
                        task,
                    }));
                }
            }
            return self;
        }

        locked.taker = Taker::Some(Box::new(func));
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
    pub fn and_then_apply<X: Send+Sync>(&self, func: impl FnOnce(T) -> X + Send+'static) -> CompletionStage<X> {
        self.then_apply(|comp| {
            match comp {
                TakenCompletion::Value(value) => Completion::Value(func(value)),
                TakenCompletion::Taken => Completion::Taken,
                TakenCompletion::Panic => Completion::Panic,
                TakenCompletion::DeadLock => Completion::Panic, //I think this is reasonable.
            }
        })
    }

    pub fn and_then_apply_async<E: InfallibleExecutor, X: Send+Sync>(&self, func: impl FnOnce(T) -> X + Send+'static) -> CompletionStage<X> {
        self.then_apply_async::<E, X>(|comp| {
            match comp {
                TakenCompletion::Value(value) => Completion::Value(func(value)),
                TakenCompletion::Taken => Completion::Taken,
                TakenCompletion::Panic => Completion::Panic,
                TakenCompletion::DeadLock => Completion::Panic, //I think this is reasonable.
            }
        })
    }

    pub fn and_then_apply_async_with_error<E: FallibleExecutor<R>, X: Send+Sync, R>(&self, func: impl FnOnce(Result<T, R>) -> X + Send+'static) -> CompletionStage<X> {
        self.then_apply_async_with_error::<E, X, R>(|comp| {
            match comp {
                TakenExecutorCompletion::Value(value) => Completion::Value(func(Ok(value))),
                TakenExecutorCompletion::Err(e) => Completion::Value(func(Err(e))),
                TakenExecutorCompletion::Taken => Completion::Taken,
                TakenExecutorCompletion::Panic => Completion::Panic,
                TakenExecutorCompletion::DeadLock => Completion::Panic, //I think this is reasonable.

            }
        })
    }

    pub fn and_then_apply_async_with_executor<X: Send+Sync>(&self, executor: impl InstancedInfallibleExecutor+Send+'static,  func: impl FnOnce(T) -> X + Send+'static) -> CompletionStage<X> {
        self.then_apply_async_with_executor(executor, |comp| {
            match comp {
                TakenCompletion::Value(value) => Completion::Value(func(value)),
                TakenCompletion::Taken => Completion::Taken,
                TakenCompletion::Panic => Completion::Panic,
                TakenCompletion::DeadLock => Completion::Panic, //I think this is reasonable.

            }
        })
    }

    pub fn and_then_apply_async_with_executor_and_error<X: Send+Sync, R>(&self, executor: impl InstancedFallibleExecutor<R>+Send+'static,  func: impl FnOnce(Result<T, R>) -> X + Send+'static) -> CompletionStage<X> {
        self.then_apply_async_with_executor_and_error(executor, |comp| {
            match comp {
                TakenExecutorCompletion::Value(value) => Completion::Value(func(Ok(value))),
                TakenExecutorCompletion::Err(e) => Completion::Value(func(Err(e))),
                TakenExecutorCompletion::Taken => Completion::Taken,
                TakenExecutorCompletion::Panic => Completion::Panic,
                TakenExecutorCompletion::DeadLock => Completion::Panic, //I think this is reasonable.
            }
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
    pub fn and_then_apply_ref<X: Send+Sync>(&self, func: impl FnOnce(&T) -> X + Send+'static) -> CompletionStage<X> {
        self.then_apply_ref(|comp| {
            match comp {
                Completion::Value(value) => Completion::Value(func(value)),
                Completion::Taken => Completion::Taken,
                Completion::Panic => Completion::Panic
            }
        })
    }

    pub fn and_then_apply_ref_async<E: InfallibleExecutor, X: Send+Sync>(&self, func: impl FnOnce(&T) -> X + Send+'static) -> CompletionStage<X> {
        self.then_apply_ref_async::<E, X>(|comp| {
            match comp {
                Completion::Value(value) => Completion::Value(func(value)),
                Completion::Taken => Completion::Taken,
                Completion::Panic => Completion::Panic,
            }
        })
    }

    pub fn and_then_apply_ref_async_with_error<E: FallibleExecutor<R>, X: Send+Sync, R>(&self, func: impl FnOnce(Result<&T, R>) -> X + Send+'static) -> CompletionStage<X> {
        self.then_apply_ref_async_with_error::<E, X, R>(|comp| {
            match comp {
                ExecutorCompletion::Value(value) => Completion::Value(func(Ok(value))),
                ExecutorCompletion::Err(e) => Completion::Value(func(Err(e))),
                ExecutorCompletion::Taken => Completion::Taken,
                ExecutorCompletion::Panic => Completion::Panic,
            }
        })
    }

    pub fn and_then_apply_ref_async_with_executor<X: Send+Sync>(&self, executor: impl InstancedInfallibleExecutor+Send+'static,  func: impl FnOnce(&T) -> X + Send+'static) -> CompletionStage<X> {
        self.then_apply_ref_async_with_executor(executor, |comp| {
            match comp {
                Completion::Value(value) => Completion::Value(func(value)),
                Completion::Taken => Completion::Taken,
                Completion::Panic => Completion::Panic,
            }
        })
    }

    pub fn and_then_apply_ref_async_with_executor_and_error<X: Send+Sync, R>(&self, executor: impl InstancedFallibleExecutor<R>+Send+'static,  func: impl FnOnce(Result<&T, R>) -> X + Send+'static) -> CompletionStage<X> {
        self.then_apply_ref_async_with_executor_and_error(executor, |comp| {
            match comp {
                ExecutorCompletion::Value(value) => Completion::Value(func(Ok(value))),
                ExecutorCompletion::Err(e) => Completion::Value(func(Err(e))),
                ExecutorCompletion::Taken => Completion::Taken,
                ExecutorCompletion::Panic => Completion::Panic,
            }
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
    /// - GetTimeoutRefResult::TimedOut if blocking for longer would have been required.
    /// - GetTimeoutRefResult::Value if the value was successfully taken out of the stage.
    /// - GetTimeoutRefResult::Panic if the supplier of the stage panicked.
    /// - GetTimeoutRefResult::Taken if the stage already had its value taken previously or by another thread.
    ///
    pub fn get_ref_timeout(&self, timeout: Duration) -> GetRefTimeoutResult<RefGuard<'_, T>> {
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
    /// - GetTimeoutResult::TimedOut if blocking for longer would have been required.
    /// - GetTimeoutResult::Value if the value was successfully taken out of the stage.
    /// - GetTimeoutResult::Panic if the supplier of the stage panicked.
    /// - GetTimeoutResult::Taken if the stage already had its value taken previously or by another thread.
    /// - GetTimeoutResult::WouldDeadlock if the current thread has the value in the stage borrowed.
    ///
    pub fn get_timeout(&self, timeout: Duration) -> GetTimeoutResult<T> {
        if timeout.is_zero() {
            return self.try_get();
        }
        
        self.get_until(Instant::now() + timeout)
    }

    /// Borrow a refence to the value of the stage without panicking.
    ///
    /// This function may block to wait for the stage to finish completing roughly until the given Instant has arrived.
    ///
    /// # Panics
    /// This function never panics.
    ///
    /// # Returns
    /// - GetTimeoutRefResult::TimedOut if blocking for longer would have been required.
    /// - GetTimeoutRefResult::Value if the value was successfully taken out of the stage.
    /// - GetTimeoutRefResult::Panic if the supplier of the stage panicked.
    /// - GetTimeoutRefResult::Taken if the stage already had its value taken previously or by another thread.
    ///
    pub fn get_ref_until(&self, until: Instant) -> GetRefTimeoutResult<RefGuard<'_, T>> {
        if self.0.completed.load(SeqCst) {
            match CellValue::map_ref(self.0.cell.read_recursive()) {
                CellValue::None => (),
                CellValue::Panic => return GetRefTimeoutResult::Panic,
                CellValue::Taken => return GetRefTimeoutResult::Taken,
                CellValue::Value(dta) => return GetRefTimeoutResult::Value(RefGuard::new(dta, self.0.clone())),
            }
        }

        let mut locked = self.0.tasks.lock();
        loop {
            match CellValue::map_ref(self.0.cell.read_recursive()) {
                CellValue::None => (),
                CellValue::Panic => return GetRefTimeoutResult::Panic,
                CellValue::Taken => return GetRefTimeoutResult::Taken,
                CellValue::Value(dta) => return GetRefTimeoutResult::Value(RefGuard::new(dta, self.0.clone())),
            }
            if self.0.cell_cond.wait_until(&mut locked, until).timed_out() {
                return GetRefTimeoutResult::TimedOut;
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
    /// - GetTimeoutResult::TimedOut if blocking for longer would have been required.
    /// - GetTimeoutResult::Value if the value was successfully taken out of the stage.
    /// - GetTimeoutResult::Panic if the supplier of the stage panicked.
    /// - GetTimeoutResult::Taken if the stage already had its value taken previously or by another thread.
    /// - GetTimeoutResult::WouldDeadlock if the current thread has the value in the stage borrowed.
    ///
    pub fn get_until(&self, until: Instant) -> GetTimeoutResult<T> {
        if self.borrowed_by_current_thread() {
            return GetTimeoutResult::WouldDeadlock;
        }

        if self.0.completed.load(SeqCst) {
            let taken = self.0.cell.write().take();
            match taken {
                CellValue::None => (),
                CellValue::Panic => return GetTimeoutResult::Panic,
                CellValue::Taken => return GetTimeoutResult::Taken,
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
    /// - GetTimeoutRefResult::TimedOut if blocking would have been required
    /// - GetTimeoutRefResult::Value if the value was successfully taken out of the stage.
    /// - GetTimeoutRefResult::Panic if the supplier of the stage panicked.
    /// - GetTimeoutRefResult::Taken if a reference to the stages was successfully created.
    ///
    pub fn try_get_ref(&self) -> GetRefTimeoutResult<RefGuard<'_, T>> {
        if !self.0.completed.load(SeqCst) {
            return GetRefTimeoutResult::TimedOut
        }
        match CellValue::map_ref(self.0.cell.read_recursive()) {
            CellValue::None => GetRefTimeoutResult::TimedOut,
            CellValue::Panic => GetRefTimeoutResult::Panic,
            CellValue::Taken => GetRefTimeoutResult::Taken,
            CellValue::Value(dta) => GetRefTimeoutResult::Value(RefGuard::new(dta, self.0.clone())),
        }
    }

    /// Block until the stage is completed and then borrow the stage's value without panicking.
    ///
    /// # Panics
    /// This function never panics.
    ///
    /// # Returns
    /// - GetRefResult::Value if the value was successfully taken out of the stage.
    /// - GetRefResult::Panic if the supplier of the stage panicked.
    /// - GetRefResult::Taken if a reference to the stages was successfully created.
    ///
    pub fn get_ref(&self) -> GetRefResult<RefGuard<'_, T>> {
        if self.0.completed.load(SeqCst) {
            match CellValue::map_ref(self.0.cell.read_recursive()) {
                CellValue::None => (),
                CellValue::Panic => return GetRefResult::Panic,
                CellValue::Taken => return GetRefResult::Taken,
                CellValue::Value(guard) => return GetRefResult::Value(RefGuard::new(guard, self.0.clone())),
            }
        }

        let mut locked = self.0.tasks.lock();
        loop {
            match CellValue::map_ref(self.0.cell.read_recursive()) {
                CellValue::None => (),
                CellValue::Panic => return GetRefResult::Panic,
                CellValue::Taken => return GetRefResult::Taken,
                CellValue::Value(guard) => return GetRefResult::Value(RefGuard::new(guard, self.0.clone())),
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
    /// - GetTimeoutResult::TimedOut if blocking would have been required
    /// - GetTimeoutResult::Value if the value was successfully taken out of the stage.
    /// - GetTimeoutResult::Panic if the supplier of the stage panicked.
    /// - GetTimeoutResult::Taken if the stage already had its value taken previously or by another thread.
    /// - GetTimeoutResult::WouldDeadlock if the current thread has the value in the stage borrowed.
    ///
    pub fn try_get(&self) -> GetTimeoutResult<T> {
        if !self.0.completed.load(SeqCst) {
            return GetTimeoutResult::TimedOut;
        }

        if self.borrowed_by_current_thread() {
            return GetTimeoutResult::WouldDeadlock;
        }

        let taken = self.0.cell.write().take();
        match taken {
            CellValue::None => GetTimeoutResult::TimedOut,
            CellValue::Panic => GetTimeoutResult::Panic,
            CellValue::Taken => GetTimeoutResult::Taken,
            CellValue::Value(dta) => GetTimeoutResult::Value(dta),
        }
    }

    /// Blocks until the stage is completed and takes the value ouf of the stage without panicking
    ///
    /// # Panics
    /// This function never panics.
    ///
    /// # Returns
    /// - TakenCompletion::Value if the value was successfully taken out of the stage.
    /// - TakenCompletion::Panic if the supplier of the stage panicked.
    /// - TakenCompletion::Taken if the stage already had its value taken previously or by another thread.
    /// - TakenCompletion::WouldDeadlock if the current thread has the value in the stage borrowed.
    ///
    pub fn get(&self) -> TakenCompletion<T> {
        if self.borrowed_by_current_thread() {
           return TakenCompletion::DeadLock;
        }

        if self.0.completed.load(SeqCst) {
            let taken = self.0.cell.write().take();
            match taken {
                CellValue::None => (),
                CellValue::Panic => return TakenCompletion::Panic,
                CellValue::Taken => return TakenCompletion::Taken,
                CellValue::Value(dta) => return TakenCompletion::Value(dta),
            }
        }

        let mut locked = self.0.tasks.lock();
        loop {
            let taken = self.0.cell.write().take();
            match taken {
                CellValue::None => (),
                CellValue::Panic => return TakenCompletion::Panic,
                CellValue::Taken => return TakenCompletion::Taken,
                CellValue::Value(dta) => return TakenCompletion::Value(dta),
            }
            self.0.cell_cond.wait(&mut locked);
        }
    }
    
    /// 
    /// Attempts to return a reference to the result of the stage without blocking for a significant amount of time.
    /// This function may still block for an insignificant amount of time when completion of the sage is imminent.
    /// 
    /// If the stage is yet completed and completion is not immediately imminent, 
    /// then this function returns TryResult::WouldBlock
    /// 
    pub fn try_borrow(&self) -> TryResult<RefGuard<'_, T>> {
        if !self.0.completed.load(SeqCst) {
            return TryResult::WouldBlock;
        }

        match CellValue::map_ref(self.0.cell.read_recursive()) {
            CellValue::None => TryResult::WouldBlock,
            CellValue::Taken => TryResult::Taken,
            CellValue::Panic => panic!("supplier panicked"),
            CellValue::Value(guard) => TryResult::Value(RefGuard::new(guard, self.0.clone())),
        }
    }

    ///
    /// Blocks until stage resolves and returns a reference to the result of the stage.
    /// 
    /// # Panics
    /// if the supplier of this stage panics
    /// 
    /// # None
    /// if the stage has been completed and ownership of the value was taken out of the stage.
    /// 
    /// # Some
    /// if the stage has been completed and a ReadLock on a non-taken value was obtained.
    /// The ReadLock is relinquished once the returned guard is dropped.
    /// While the ReadLock is held, the value cannot be taken out of the stage.
    /// 
    pub fn borrow(&self) -> Option<RefGuard<'_, T>> {
        if self.0.completed.load(SeqCst) {
            match CellValue::map_ref(self.0.cell.read_recursive()) {
                CellValue::None => (),
                CellValue::Taken => return None,
                CellValue::Panic => panic!("supplier panicked"),
                CellValue::Value(guard) => return Some(RefGuard::new(guard, self.0.clone())),
            }
        }

        let mut locked = self.0.tasks.lock();
        loop {
            match CellValue::map_ref(self.0.cell.read_recursive()) {
                CellValue::None => (),
                CellValue::Taken => return None,
                CellValue::Panic => panic!("supplier panicked"),
                CellValue::Value(guard) => return Some(RefGuard::new(guard, self.0.clone())),
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
    /// TryResult::WouldBlock - if the stage has not yet completed.
    /// TryResult::Taken - if the value has already been taken out of the stage
    /// TryResult::Value - if the value was successfully taken out of the stage
    ///
    ///
    pub fn try_take(&self) -> TryResult<T> {
        if !self.0.completed.load(SeqCst) {
            return TryResult::WouldBlock;
        }

        if self.borrowed_by_current_thread() {
            panic!("deadlock detected");
        }

        let taken = self.0.cell.write().take();
        
        match taken {
            CellValue::None => TryResult::WouldBlock,
            CellValue::Taken => TryResult::Taken,
            CellValue::Panic => panic!("supplier panicked"),
            CellValue::Value(v) => TryResult::Value(v),
        }
    }

    /// This function tries to take the value from the stage and place it into the given mutable reference.
    /// The returned Value in the TryResult is not the value from the stage but rather the value
    /// that was stored in the reference previously.
    pub fn try_take_into(&self, into: &mut T) -> TryResult<T> {
        match self.try_take() {
            TryResult::Value(v) => TryResult::Value(mem::replace(into, v)),
            o => o,
        }
    }

    ///
    /// Same as calling take().unwrap()
    ///
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
    pub fn take(&self) -> Option<T> {
        if self.borrowed_by_current_thread() {
            panic!("deadlock detected");
        }

        if self.0.completed.load(SeqCst) {
            let taken = self.0.cell.write().take();
            match taken {
                CellValue::None => (),
                CellValue::Panic => panic!("supplier panicked"),
                CellValue::Taken => return None,
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
                CellValue::Value(dta) => return Some(dta),
            }
            self.0.cell_cond.wait(&mut locked);
        }
    }



    /// Returns true if the value has already been taken out of the stage or if the value will be taken immediately by a child stage once the stage completed.
    pub fn taken(&self) -> bool {
        if self.borrowed_by_current_thread() {
            return false;
        }

        let guard = self.0.cell.read_recursive();
        if matches!(guard.deref(), CellValue::Taken) {
            return true;
        }
        drop(guard);


        let locked = self.0.tasks.lock();
        if matches!(locked.taker, Taker::Some(_)) {
            return true;
        }

        false
    }

    /// Returns true if the stage is either completed or completion is immediately imminent
    pub fn completed(&self) -> bool {
        self.0.completed.load(SeqCst)
    }

    pub fn borrowed_by_current_thread(&self) -> bool {
        self.0.thread_borrow_counts.lock().contains_key(&std::thread::current().id())
    }

    pub fn compose<Y>(&self, func: impl FnOnce(&CompletionStage<T>) -> Y) -> &Self {
        _= func(self);
        self
    }

    pub fn then_compose<X: Sync+Send>(&self, func: impl FnOnce(TakenCompletion<T>) -> CompletionStage<X>+'static+Send) -> CompletionStage<X> {
        let new_stage = CompletionStage::new();
        let ncl = new_stage.clone();
        self.then_run(move |comp| {
            func(comp).then_run(move |comp| {
                match comp {
                    TakenCompletion::Taken => _= ncl.complete(Completion::Taken),
                    TakenCompletion::Panic => _= ncl.complete(Completion::Panic),
                    TakenCompletion::Value(v) => _= ncl.complete_with_value(v),
                    TakenCompletion::DeadLock => _= ncl.complete(Completion::Panic), //Should be unreachable!
                }
            });
        });

        new_stage
    }

    pub fn then_compose_async<E: InfallibleExecutor, X: Sync+Send>(&self, func: impl FnOnce(TakenCompletion<T>) -> CompletionStage<X>+'static+Send) -> CompletionStage<X> {
        let new_stage = CompletionStage::new();
        let ncl = new_stage.clone();
        self.then_run(move |comp| {
            func(comp).then_run_async::<E>(move |comp| {
                match comp {
                    TakenCompletion::Taken => _= ncl.complete(Completion::Taken),
                    TakenCompletion::Panic => _= ncl.complete(Completion::Panic),
                    TakenCompletion::Value(v) => _= ncl.complete_with_value(v),
                    TakenCompletion::DeadLock => _= ncl.complete(Completion::Panic), //Should be unreachable!
                }
            });
        });

        new_stage
    }

    pub fn then_compose_async_with_executor<X: Sync+Send>(&self, executor: impl InstancedInfallibleExecutor+Send+'static, func: impl FnOnce(TakenCompletion<T>) -> CompletionStage<X>+'static+Send) -> CompletionStage<X> {
        let new_stage = CompletionStage::new();
        let ncl = new_stage.clone();
        self.then_run_async_with_executor(executor, move |comp| {
            func(comp).then_run(move |comp| {
                match comp {
                    TakenCompletion::Taken => _= ncl.complete(Completion::Taken),
                    TakenCompletion::Panic => _= ncl.complete(Completion::Panic),
                    TakenCompletion::Value(v) => _= ncl.complete_with_value(v),
                    TakenCompletion::DeadLock => _= ncl.complete(Completion::Panic), //Should be unreachable!
                }
            });
        });

        new_stage
    }

    pub fn then_compose_async_with_error<E: FallibleExecutor<R>, X: Sync+Send, R>(&self, func: impl FnOnce(TakenExecutorCompletion<T, R>) -> CompletionStage<X>+'static+Send) -> CompletionStage<X> {
        let new_stage = CompletionStage::new();
        let ncl = new_stage.clone();
        self.then_run_async_with_error::<E, R>(move |comp| {
            func(comp).then_run(move |comp| {
                match comp {
                    TakenCompletion::Taken => _= ncl.complete(Completion::Taken),
                    TakenCompletion::Panic => _= ncl.complete(Completion::Panic),
                    TakenCompletion::Value(v) => _= ncl.complete_with_value(v),
                    TakenCompletion::DeadLock => _= ncl.complete(Completion::Panic), //Should be unreachable!
                }
            });
        });

        new_stage
    }

    pub fn then_compose_async_with_executor_and_error<X: Sync+Send, R>(&self, executor: impl InstancedFallibleExecutor<R>+Send+'static, func: impl FnOnce(TakenExecutorCompletion<T, R>) -> CompletionStage<X>+'static+Send) -> CompletionStage<X> {
        let new_stage = CompletionStage::new();
        let ncl = new_stage.clone();
        self.then_run_async_with_executor_and_error(executor, move |comp| {
            func(comp).then_run(move |comp| {
                match comp {
                    TakenCompletion::Taken => _= ncl.complete(Completion::Taken),
                    TakenCompletion::Panic => _= ncl.complete(Completion::Panic),
                    TakenCompletion::Value(v) => _= ncl.complete_with_value(v),
                    TakenCompletion::DeadLock => _= ncl.complete(Completion::Panic), //Should be unreachable!
                }
            });
        });

        new_stage
    }

    pub fn then_combine<X: Send+Sync, Y: Send+Sync>(&self, other: &CompletionStage<X>, func: impl FnOnce(TakenCompletion<T>, TakenCompletion<X>) -> Completion<Y>+'static+Send) -> CompletionStage<Y> {
        let stage = CompletionStage::new();
        let scl = stage.clone();
        let ocl = other.clone();
        self.then_run(move |comp| {
            ocl.then_run(move |comp2| {
                scl.complete(func(comp, comp2));
            });
        });

        stage
    }

    pub fn any_of(&self, any: &[CompletionStage<T>]) -> CompletionStage<T> {
        let new_stage = CompletionStage::new();
        for stage in any {
            if new_stage.completed() {
                return new_stage;
            }
            
            if stage.completed() {
                new_stage.complete(stage.get().try_into().unwrap_or(Completion::Panic));
                return new_stage;
            }

            let scl = new_stage.clone();
            stage.then_run(move |comp| {
                scl.complete(comp.try_into().unwrap_or(Completion::Panic));
            });
        }

        new_stage
    }

    pub fn all_of(&self, all: &[CompletionStage<T>]) -> CompletionStage<Vec<Completion<T>>> {
        let new_stage = CompletionStage::new();
        let all_len = all.len();
        let result = Arc::new(Mutex::new(Vec::<Completion<T>>::new()));
        for stage in all {
            let cp = result.clone();
            let scl = new_stage.clone();
            stage.then_run(move |comp| {
                let mut guard = cp.lock();
                guard.push(match comp {
                    TakenCompletion::Taken => Completion::Taken,
                    TakenCompletion::Panic => Completion::Panic,
                    TakenCompletion::Value(v) => Completion::Value(v),
                    TakenCompletion::DeadLock => Completion::Panic,
                });

                if guard.len() == all_len {
                    scl.complete_with_value(mem::take(guard.deref_mut()));
                }
            });
        }

        new_stage
    }


}

/// Static Executor that can only fail to execute something by panicking
pub trait InfallibleExecutor {
    fn execute(task: impl FnOnce() + Send+'static);
}

/// Static Executor that can fail with a given error when trying to execute something.
/// Error refers to an OS error like "I cannot spawn more threads" rather than any error the task may produce
pub trait FallibleExecutor<R> {
    fn execute(task: impl FnOnce() + Send+'static) -> Result<(), R>;
}

/// Executor that can only fail to execute something by panicking
pub trait InstancedInfallibleExecutor {
    fn execute(self, task: impl FnOnce() + Send+'static);
}

/// Executor that can fail with a given error when trying to execute something.
/// Error refers to an OS error like "I cannot spawn more threads" rather than any error the task may produce
pub trait InstancedFallibleExecutor<R>: Send {
    fn execute(self, task: impl FnOnce() + Send+'static) -> Result<(), R>;
}

impl InfallibleExecutor for thread::Thread {
    fn execute(task: impl FnOnce() + Send + 'static) {
        thread::spawn(task);
    }
}

impl FallibleExecutor<Error> for thread::Builder {
    fn execute(task: impl FnOnce() + Send + 'static) -> Result<(), Error> {
        thread::Builder::new().spawn(task).map(|_| ())
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