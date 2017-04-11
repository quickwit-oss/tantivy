mod fstmap;
mod skip;
pub mod stacker;
mod stream_dictionary;


//pub use self::fstmap::FstMapBuilder as TermDictionaryBuilder;
//pub use self::fstmap::FstMap as TermDictionary;


pub use self::stream_dictionary::StreamDictionaryBuilder as TermDictionaryBuilder;
pub use self::stream_dictionary::StreamDictionary as TermDictionary;
pub use self::stream_dictionary::StreamDictionaryStreamer as TermDictionaryStreamer;

pub use self::skip::{SkipListBuilder, SkipList};
