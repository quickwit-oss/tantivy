use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex, OnceLock};

use lru::LruCache;

use super::{CompileError, CompiledFn};
use crate::ast::UntypedExpr;
use crate::types::VarType;

/// The outcome of compiling one key, shared by every caller of that key.
type CompilationResult = Result<Arc<CompiledFn>, CompileError>;

/// A per-key cell, written once by whichever caller compiles the expression.
///
/// Initializing it is what serializes concurrent compilations of the same key.
type CompilationSlot = Arc<OnceLock<CompilationResult>>;

/// A bounded, thread-safe cache of JIT-compiled expressions.
/// The cache is cheap to clone: every clone shares one set of entries.
/// The cache does not allocate on creation. Allocation happens on the first usage.
#[derive(Clone)]
pub struct ExprCompilationCache {
    inner: Arc<Mutex<ExprCompilationCacheInner>>,
}

struct ExprCompilationCacheInner {
    capacity: usize,
    // We use Option here to lazily allocate on the first insertion.
    entries: Option<LruCache<ExprCacheKey, CompilationSlot>>,
}

impl ExprCompilationCacheInner {
    fn entries(&mut self) -> Option<&mut LruCache<ExprCacheKey, CompilationSlot>> {
        if self.entries.is_none() {
            let non_zero_capacity = NonZeroUsize::new(self.capacity)?;
            self.entries = Some(LruCache::new(non_zero_capacity));
        }
        self.entries.as_mut()
    }
}

/// Identifies a compilation: an expression plus the types it was compiled for.
#[derive(PartialEq, Eq, Hash)]
struct ExprCacheKey {
    expr: String,
    /// The variable types, sorted by variable name so the key does not depend
    /// on the caller's `HashMap` iteration order.
    var_types: Box<[(String, VarType)]>,
}

impl ExprCacheKey {
    fn new(untyped_expr: &UntypedExpr, var_types: &HashMap<&str, VarType>) -> ExprCacheKey {
        let mut sorted_var_types: Vec<(String, VarType)> = Vec::with_capacity(var_types.len());
        for (variable_name, var_type) in var_types {
            sorted_var_types.push((variable_name.to_string(), *var_type));
        }
        sorted_var_types.sort_unstable();
        ExprCacheKey {
            expr: untyped_expr.to_string(),
            var_types: sorted_var_types.into_boxed_slice(),
        }
    }
}

impl ExprCompilationCache {
    /// A capacity of 0 means disabled.
    pub fn with_capacity(capacity: usize) -> ExprCompilationCache {
        ExprCompilationCache {
            inner: Arc::new(Mutex::new(ExprCompilationCacheInner {
                capacity,
                entries: None,
            })),
        }
    }

    /// Creates a cache that memoizes nothing and allocates nothing.
    pub fn disabled() -> ExprCompilationCache {
        ExprCompilationCache::with_capacity(0)
    }

    /// Returns false for a cache that memoizes nothing.
    pub fn is_enabled(&self) -> bool {
        self.inner.lock().unwrap().capacity > 0
    }

    /// Returns the number of compiled expressions currently retained.
    pub fn len(&self) -> usize {
        let mut inner_guard = self.inner.lock().unwrap();
        let Some(entries) = inner_guard.entries() else {
            return 0;
        };
        entries.len()
    }

    /// Returns true if the cache retains no compiled expression.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns the expression compiled for `var_types`, compiling it on a miss.
    ///
    /// Concurrent callers asking for the same expression and types block until
    /// the first of them is done, so an expression is normally compiled once.
    /// An expression evicted while a compilation is in flight may be compiled
    /// again by a later caller.
    ///
    /// A compilation failure is cached like a success and handed to later
    /// callers.
    pub fn compile(
        &self,
        untyped_expr: &UntypedExpr,
        var_types: &HashMap<&str, VarType>,
    ) -> Result<Arc<CompiledFn>, CompileError> {
        if let Some(slot) = self.slot_opt(untyped_expr, var_types) {
            // Initializing the cell ensures we cannot have two threads compiling
            // the same function at the same time.
            slot.get_or_init(|| super::compile(untyped_expr, var_types))
                .clone()
        } else {
            // no caching
            super::compile(untyped_expr, var_types)
        }
    }

    fn slot_opt(
        &self,
        untyped_expr: &UntypedExpr,
        var_types: &HashMap<&str, VarType>,
    ) -> Option<CompilationSlot> {
        // That function does take the lock but only does trivial things that
        // cannot panick before releasing it.
        let mut inner_guard = self.inner.lock().unwrap();
        let entries = inner_guard.entries()?;
        let key = ExprCacheKey::new(untyped_expr, var_types);
        let slot: CompilationSlot = entries.get_or_insert(key, CompilationSlot::default).clone();
        Some(slot)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Barrier;

    use super::*;
    use crate::ast::Function;

    #[test]
    fn test_cache_hit_returns_the_same_compiled_fn() {
        let cache = ExprCompilationCache::with_capacity(64);
        let untyped_expr = UntypedExpr::variable("flag");
        let variable_types = HashMap::from([("flag", VarType::Bool)]);
        let first = cache.compile(&untyped_expr, &variable_types).unwrap();
        let second = cache.compile(&untyped_expr, &variable_types).unwrap();
        assert!(Arc::ptr_eq(&first, &second));
        assert_eq!(cache.len(), 1);
    }

    #[test]
    fn test_var_types_are_part_of_the_key() {
        let cache = ExprCompilationCache::with_capacity(64);
        let untyped_expr = UntypedExpr::variable("value");

        let as_u64 = cache
            .compile(&untyped_expr, &HashMap::from([("value", VarType::U64)]))
            .unwrap();
        let as_i64 = cache
            .compile(&untyped_expr, &HashMap::from([("value", VarType::I64)]))
            .unwrap();

        assert!(!Arc::ptr_eq(&as_u64, &as_i64));
        assert_eq!(as_u64.result_type(), VarType::U64);
        assert_eq!(as_i64.result_type(), VarType::I64);
        assert_eq!(cache.len(), 2);
    }

    #[test]
    fn test_var_types_order_is_not_part_of_the_key() {
        let cache = ExprCompilationCache::with_capacity(16);
        let untyped_expr = Function::Add
            .call(vec![
                UntypedExpr::variable("left"),
                UntypedExpr::variable("right"),
            ])
            .unwrap();

        let first = cache
            .compile(
                &untyped_expr,
                &HashMap::from([("left", VarType::U64), ("right", VarType::U64)]),
            )
            .unwrap();
        let second = cache
            .compile(
                &untyped_expr,
                &HashMap::from([("right", VarType::U64), ("left", VarType::U64)]),
            )
            .unwrap();

        assert!(Arc::ptr_eq(&first, &second));
        assert_eq!(cache.len(), 1);
    }

    #[test]
    fn test_equal_expressions_built_separately_share_an_entry() {
        let cache = ExprCompilationCache::with_capacity(64);
        let variable_types = HashMap::from([("value", VarType::U64)]);
        let make_expr = || {
            Function::Add
                .call(vec![
                    UntypedExpr::variable("value"),
                    UntypedExpr::literal(1u64),
                ])
                .unwrap()
        };

        let first = cache.compile(&make_expr(), &variable_types).unwrap();
        let second = cache.compile(&make_expr(), &variable_types).unwrap();

        assert!(Arc::ptr_eq(&first, &second));
        assert_eq!(cache.len(), 1);
    }

    #[test]
    fn test_least_recently_used_entry_is_evicted() {
        let cache = ExprCompilationCache::with_capacity(1);
        let variable_types = HashMap::from([("flag", VarType::Bool)]);
        let first_expr = UntypedExpr::variable("flag");
        let second_expr = Function::Not
            .call(vec![UntypedExpr::variable("flag")])
            .unwrap();

        let first = cache.compile(&first_expr, &variable_types).unwrap();
        assert_eq!(cache.len(), 1);
        cache.compile(&second_expr, &variable_types).unwrap();
        assert_eq!(cache.len(), 1);
        let first_again = cache.compile(&first_expr, &variable_types).unwrap();

        assert!(!Arc::ptr_eq(&first, &first_again));
        assert_eq!(cache.len(), 1);
    }

    #[test]
    fn test_disabled_cache_memoizes_nothing() {
        let cache = ExprCompilationCache::disabled();
        let untyped_expr = UntypedExpr::variable("flag");
        let variable_types = HashMap::from([("flag", VarType::Bool)]);

        let first = cache.compile(&untyped_expr, &variable_types).unwrap();
        let second = cache.compile(&untyped_expr, &variable_types).unwrap();

        assert!(!cache.is_enabled());
        assert!(!Arc::ptr_eq(&first, &second));
        assert_eq!(cache.len(), 0);
        assert!(cache.is_empty());
    }

    #[test]
    fn test_capacity_0_means_disabled() {
        assert!(ExprCompilationCache::with_capacity(1).is_enabled());
        assert!(!ExprCompilationCache::with_capacity(0).is_enabled());
        assert!(!ExprCompilationCache::disabled().is_enabled());
    }

    #[test]
    fn test_compilation_failures_are_memoized() {
        let cache = ExprCompilationCache::with_capacity(16);
        // `(` is not a valid regular expression, which is rejected at compile time.
        let untyped_expr = crate::ast::deserialize(r#"(REGEXP_EXTRACT "a" "*(" 1u64)"#).unwrap();
        let variable_types = HashMap::new();
        assert!(cache.is_empty());
        assert!(cache.compile(&untyped_expr, &variable_types).is_err());
        assert_eq!(cache.len(), 1);
    }

    #[test]
    fn test_concurrent_callers_compile_once() {
        const NUM_THREADS: usize = 8;

        let cache = ExprCompilationCache::with_capacity(NUM_THREADS);
        let barrier = Barrier::new(NUM_THREADS);
        let untyped_expr = UntypedExpr::variable("flag");

        let compiled_fns: Vec<Arc<CompiledFn>> = std::thread::scope(|scope| {
            let handles: Vec<_> = (0..NUM_THREADS)
                .map(|_| {
                    let cache = cache.clone();
                    let untyped_expr = &untyped_expr;
                    let barrier = &barrier;
                    scope.spawn(move || {
                        let variable_types = HashMap::from([("flag", VarType::Bool)]);
                        barrier.wait();
                        cache.compile(untyped_expr, &variable_types).unwrap()
                    })
                })
                .collect();
            handles
                .into_iter()
                .map(|handle| handle.join().unwrap())
                .collect()
        });

        // Identical pointers can only come from a single compilation.
        for compiled_fn in &compiled_fns {
            assert!(Arc::ptr_eq(&compiled_fns[0], compiled_fn));
        }
        assert_eq!(cache.len(), 1);
    }
}
