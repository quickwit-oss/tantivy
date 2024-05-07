#[cfg(feature = "quickwit")]
use futures_util::{future::Either, FutureExt};
use rayon::{ThreadPool, ThreadPoolBuilder};

use crate::TantivyError;

/// Search executor whether search request are single thread or multithread.
///
/// We don't expose Rayon thread pool directly here for several reasons.
///
/// First dependency hell. It is not a good idea to expose the
/// API of a dependency, knowing it might conflict with a different version
/// used by the client. Second, we may stop using rayon in the future.
pub enum Executor {
    /// Single thread variant of an Executor
    SingleThread,
    /// Thread pool variant of an Executor
    ThreadPool(ThreadPool),
}

impl Executor {
    /// Creates an Executor that performs all task in the caller thread.
    pub fn single_thread() -> Executor {
        Executor::SingleThread
    }

    /// Creates an Executor that dispatches the tasks in a thread pool.
    pub fn multi_thread(num_threads: usize, prefix: &'static str) -> crate::Result<Executor> {
        let pool = ThreadPoolBuilder::new()
            .num_threads(num_threads)
            .thread_name(move |num| format!("{prefix}{num}"))
            .build()?;
        Ok(Executor::ThreadPool(pool))
    }

    /// Perform a map in the thread pool.
    ///
    /// Regardless of the executor (`SingleThread` or `ThreadPool`), panics in the task
    /// will propagate to the caller.
    pub fn map<
        A: Send,
        R: Send,
        AIterator: Iterator<Item = A>,
        F: Sized + Sync + Fn(A) -> crate::Result<R>,
    >(
        &self,
        f: F,
        args: AIterator,
    ) -> crate::Result<Vec<R>> {
        match self {
            Executor::SingleThread => args.map(f).collect::<crate::Result<_>>(),
            Executor::ThreadPool(pool) => {
                let args: Vec<A> = args.collect();
                let num_fruits = args.len();
                let fruit_receiver = {
                    let (fruit_sender, fruit_receiver) = crossbeam_channel::unbounded();
                    pool.scope(|scope| {
                        for (idx, arg) in args.into_iter().enumerate() {
                            // We name references for f and fruit_sender_ref because we do not
                            // want these two to be moved into the closure.
                            let f_ref = &f;
                            let fruit_sender_ref = &fruit_sender;
                            scope.spawn(move |_| {
                                let fruit = f_ref(arg);
                                if let Err(err) = fruit_sender_ref.send((idx, fruit)) {
                                    error!(
                                        "Failed to send search task. It probably means all search \
                                         threads have panicked. {:?}",
                                        err
                                    );
                                }
                            });
                        }
                    });
                    fruit_receiver
                    // This ends the scope of fruit_sender.
                    // This is important as it makes it possible for the fruit_receiver iteration to
                    // terminate.
                };
                let mut result_placeholders: Vec<Option<R>> =
                    std::iter::repeat_with(|| None).take(num_fruits).collect();
                for (pos, fruit_res) in fruit_receiver {
                    let fruit = fruit_res?;
                    result_placeholders[pos] = Some(fruit);
                }
                let results: Vec<R> = result_placeholders.into_iter().flatten().collect();
                if results.len() != num_fruits {
                    return Err(TantivyError::InternalError(
                        "One of the mapped execution failed.".to_string(),
                    ));
                }
                Ok(results)
            }
        }
    }

    /// Spawn a task on the pool, returning a future completing on task success.
    ///
    /// If the task panic, returns `Err(())`.
    #[cfg(feature = "quickwit")]
    pub fn spawn_blocking<T: Send + 'static>(
        &self,
        cpu_intensive_task: impl FnOnce() -> T + Send + 'static,
    ) -> impl std::future::Future<Output = Result<T, ()>> {
        match self {
            Executor::SingleThread => Either::Left(std::future::ready(Ok(cpu_intensive_task()))),
            Executor::ThreadPool(pool) => {
                let (sender, receiver) = oneshot_with_sentinel::channel();
                pool.spawn(|| {
                    if sender.is_closed() {
                        return;
                    }
                    let task_result = cpu_intensive_task();
                    let _ = sender.send(task_result);
                });

                let res = receiver.map(|res| res.map_err(|_| ()));
                Either::Right(res)
            }
        }
    }
}

#[cfg(feature = "quickwit")]
mod oneshot_with_sentinel {
    use std::pin::Pin;
    use std::sync::Arc;
    use std::task::{Context, Poll};
    // TODO get ride of this if oneshot ever gains a is_closed()

    pub struct SenderWithSentinel<T> {
        tx: oneshot::Sender<T>,
        guard: Arc<()>,
    }

    pub struct ReceiverWithSentinel<T> {
        rx: oneshot::Receiver<T>,
        _guard: Arc<()>,
    }

    pub fn channel<T>() -> (SenderWithSentinel<T>, ReceiverWithSentinel<T>) {
        let (tx, rx) = oneshot::channel();
        let guard = Arc::new(());
        (
            SenderWithSentinel {
                tx,
                guard: guard.clone(),
            },
            ReceiverWithSentinel { rx, _guard: guard },
        )
    }

    impl<T> SenderWithSentinel<T> {
        pub fn send(self, message: T) -> Result<(), oneshot::SendError<T>> {
            self.tx.send(message)
        }

        pub fn is_closed(&self) -> bool {
            Arc::strong_count(&self.guard) == 1
        }
    }

    impl<T> std::future::Future for ReceiverWithSentinel<T> {
        type Output = Result<T, oneshot::RecvError>;

        fn poll(mut self: Pin<&mut Self>, ctx: &mut Context<'_>) -> Poll<Self::Output> {
            Pin::new(&mut self.rx).poll(ctx)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Executor;

    #[test]
    #[should_panic(expected = "panic should propagate")]
    fn test_panic_propagates_single_thread() {
        let _result: Vec<usize> = Executor::single_thread()
            .map(
                |_| {
                    panic!("panic should propagate");
                },
                vec![0].into_iter(),
            )
            .unwrap();
    }

    #[test]
    #[should_panic] //< unfortunately the panic message is not propagated
    fn test_panic_propagates_multi_thread() {
        let _result: Vec<usize> = Executor::multi_thread(1, "search-test")
            .unwrap()
            .map(
                |_| {
                    panic!("panic should propagate");
                },
                vec![0].into_iter(),
            )
            .unwrap();
    }

    #[test]
    fn test_map_singlethread() {
        let result: Vec<usize> = Executor::single_thread()
            .map(|i| Ok(i * 2), 0..1_000)
            .unwrap();
        assert_eq!(result.len(), 1_000);
        for i in 0..1_000 {
            assert_eq!(result[i], i * 2);
        }
    }

    #[test]
    fn test_map_multithread() {
        let result: Vec<usize> = Executor::multi_thread(3, "search-test")
            .unwrap()
            .map(|i| Ok(i * 2), 0..10)
            .unwrap();
        assert_eq!(result.len(), 10);
        for i in 0..10 {
            assert_eq!(result[i], i * 2);
        }
    }

    #[cfg(feature = "quickwit")]
    #[test]
    fn test_cancel_cpu_intensive_tasks() {
        use std::sync::atomic::{AtomicU64, Ordering};
        use std::sync::Arc;
        use std::time::Duration;

        let counter: Arc<AtomicU64> = Default::default();
        let mut futures = Vec::new();
        let executor = Executor::multi_thread(3, "search-test").unwrap();
        for _ in 0..1_000 {
            let counter_clone = counter.clone();
            let fut = executor.spawn_blocking(move || {
                std::thread::sleep(Duration::from_millis(4));
                counter_clone.fetch_add(1, Ordering::SeqCst)
            });
            futures.push(fut);
        }
        std::thread::sleep(Duration::from_millis(5));
        // The first few num_cores tasks should run, but the other should get cancelled.
        drop(futures);
        while Arc::strong_count(&counter) > 1 {
            std::thread::sleep(Duration::from_millis(10));
        }
        // with ideal timing, we expect the result to always be 6, but as long as we run some, and
        // cancelled most, the test is a success
        assert!(counter.load(Ordering::SeqCst) > 0);
        assert!(counter.load(Ordering::SeqCst) < 50);
    }
}
