use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use analyzer::BoxedAnalyzer;
use analyzer::Analyzer;
use analyzer::box_analyzer;
use analyzer::SimpleTokenizer;
use analyzer::JapaneseTokenizer;
use analyzer::RemoveLongFilter;
use analyzer::LowerCaser;
use analyzer::Stemmer;


#[derive(Clone)]
pub struct AnalyzerManager {
    analyzers: Arc< RwLock<HashMap<String, Box<BoxedAnalyzer> >> >
}

impl AnalyzerManager {
    pub fn register<A>(&self, analyzer_name: &str, analyzer: A) 
        where A: 'static + Send + Sync + for <'a> Analyzer<'a> {
        let boxed_analyzer = box_analyzer(analyzer);
        self.analyzers
            .write()
            .expect("Acquiring the lock should never fail")
            .insert(analyzer_name.to_string(), boxed_analyzer);
    }

    pub fn get(&self, analyzer_name: &str) -> Option<Box<BoxedAnalyzer>> {
        self.analyzers
            .read()
            .expect("Acquiring the lock should never fail")
            .get(analyzer_name)
            .map(|boxed_analyzer| {
              boxed_analyzer.boxed_clone()  
            })
    }
}

impl Default for AnalyzerManager {
    /// Creates an `AnalyzerManager` prepopulated with
    /// the default analyzers of `tantivy`.
    /// - simple
    /// - en_stem
    /// - jp
    fn default() -> AnalyzerManager {
        let manager = AnalyzerManager {
            analyzers: Arc::new(RwLock::new(HashMap::new()))
        };
        manager.register("default",
            SimpleTokenizer
                .filter(RemoveLongFilter::limit(40))
                .filter(LowerCaser)
        );
        manager.register("en_stem",
            SimpleTokenizer
                .filter(RemoveLongFilter::limit(40))
                .filter(LowerCaser)
                .filter(Stemmer::new())
        );
        manager.register("ja",
            JapaneseTokenizer
                .filter(RemoveLongFilter::limit(40))
        );
        manager
    }
}