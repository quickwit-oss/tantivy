mod index;
mod serve;
mod new;
mod merge;
mod bench;

pub use self::new::run_new_cli;
pub use self::index::run_index_cli;
pub use self::serve::run_serve_cli;
pub use self::bench::run_bench_cli;

// pub mod writer;
// pub mod searcher;
// pub mod index;
// pub mod merger;

// mod segment_serializer;
// mod segment_writer;
// mod segment_reader;
// mod segment_id;
// mod segment_component;

// pub use self::segment_component::SegmentComponent;
// pub use self::segment_id::SegmentId;
// pub use self::segment_reader::SegmentReader;