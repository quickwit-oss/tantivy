use crossbeam::channel;
use rayon::{ThreadPool, ThreadPoolBuilder};

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
            .thread_name(move |num| format!("{}{}", prefix, num))
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
                let args_with_indices: Vec<(usize, A)> = args.enumerate().collect();
                let num_fruits = args_with_indices.len();
                let fruit_receiver = {
                    let (fruit_sender, fruit_receiver) = channel::unbounded();
                    pool.scope(|scope| {
                        for arg_with_idx in args_with_indices {
                            scope.spawn(|_| {
                                let (idx, arg) = arg_with_idx;
                                let fruit = f(arg);
                                if let Err(err) = fruit_sender.send((idx, fruit)) {
                                    error!("Failed to send search task. It probably means all search threads have panicked. {:?}", err);
                                }
                            });
                        }
                    });
                    fruit_receiver
                    // This ends the scope of fruit_sender.
                    // This is important as it makes it possible for the fruit_receiver iteration to
                    // terminate.
                };
                // This is lame, but safe.
                let mut results_with_position = Vec::with_capacity(num_fruits);
                for (pos, fruit_res) in fruit_receiver {
                    let fruit = fruit_res?;
                    results_with_position.push((pos, fruit));
                }
                results_with_position.sort_by_key(|(pos, _)| *pos);
                assert_eq!(results_with_position.len(), num_fruits);
                Ok(results_with_position
                    .into_iter()
                    .map(|(_, fruit)| fruit)
                    .collect::<Vec<_>>())
            }
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
}
