use Result;
use scoped_pool::{Pool, ThreadConfig};
use crossbeam::channel;

/// Search executor whether search request are single thread or multithread.
///
/// We don't expose Rayon thread pool directly here for several reasons.
///
/// First dependency hell. It is not a good idea to expose the
/// API of a dependency, knowing it might conflict with a different version
/// used by the client. Second, we may stop using rayon in the future.
pub enum Executor {
    SingleThread,
    ThreadPool(Pool),
}

impl Executor {
    /// Creates an Executor that performs all task in the caller thread.
    pub fn single_thread() -> Executor {
        Executor::SingleThread
    }

    // Creates an Executor that dispatches the tasks in a thread pool.
    pub fn multi_thread(num_threads: usize, prefix: &'static str) -> Executor {
        let thread_config = ThreadConfig::new().prefix(prefix);
        let pool = Pool::with_thread_config(num_threads, thread_config);
        Executor::ThreadPool(pool)
    }

    // Perform a map in the thread pool.
    //
    // Regardless of the executor (`SingleThread` or `ThreadPool`), panics in the task
    // will propagate to the caller.
    pub fn map_unsorted<A: Send, R: Send, AIterator: Iterator<Item=A>, F: Sized + Sync + Fn(A) -> Result<R>>(&self, f: F, args: AIterator) -> Result<Vec<R>> {
        match self {
            Executor::SingleThread => {
                args.map(f).collect::<Result<_>>()
            }
            Executor::ThreadPool(pool) => {
                let fruit_receiver = {
                    let (fruit_sender, fruit_receiver) = channel::unbounded();
                    pool.scoped(|scope| {
                        for arg in args {
                            scope.execute(|| {
                                let fruit = f(arg);
                                if let Err(err) = fruit_sender.send(fruit) {
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
                fruit_receiver
                    .into_iter()
                    .collect::<Result<_>>()
            }
        }
    }
}

#[cfg(test)]
mod tests {

    use super::Executor;

    #[test]
    #[should_panic(expected="panic should propagate")]
    fn test_panic_propagates_single_thread() {
        let _result: Vec<usize> = Executor::single_thread().map_unsorted(|_| {panic!("panic should propagate"); }, vec![0].into_iter()).unwrap();
    }

    #[test]
    #[should_panic] //< unfortunately the panic message is not propagated
    fn test_panic_propagates_multi_thread() {
        let _result: Vec<usize> = Executor::multi_thread(1, "search-test")
            .map_unsorted(|_| {panic!("panic should propagate"); }, vec![0].into_iter()).unwrap();
    }

    #[test]
    fn test_map_singlethread() {
        let result: Vec<usize> = Executor::single_thread()
            .map_unsorted(|i| { Ok(i * 2) }, 0..1_000).unwrap();
        assert_eq!(result.len(), 1_000);
        for i in 0..1_000 {
            assert_eq!(result[i], i * 2);
        }
    }

    #[test]
    fn test_map_multithread() {
        let mut result: Vec<usize> = Executor::multi_thread(3, "search-test")
            .map_unsorted(|i| Ok(i * 2), 0..10).unwrap();
        assert_eq!(result.len(), 10);
        result.sort();
        for i in 0..10 {
            assert_eq!(result[i], i * 2);
        }
    }

}