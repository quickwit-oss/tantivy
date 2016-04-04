use std::io;
use core::reader::SegmentReader;
use core::index::Segment;
use core::schema::DocId;
use core::index::SerializableSegment;
use core::codec::SegmentSerializer;
use core::postings::PostingsSerializer;
use std::collections::BinaryHeap;
use core::serialize::BinarySerializable;
use core::fstmap::FstMapIter;
use std::cmp::Ordering;
use std::cmp::Ord;


pub struct StreamUnion<'a, V: 'static + BinarySerializable + Ord + Clone> {
    streams: Vec<FstMapIter<'a, V>>,
    heap: BinaryHeap<(&'a [u8], usize, V)>,
    //heap: BinaryHeap<(&'a [u8], usize)>,
    // heap: BinaryHeap<usize>,
}

impl<'a, V: 'static + Ord + BinarySerializable + Clone> StreamUnion<'a, V> {

    pub fn open(mut streams: Vec<FstMapIter<'a, V>>) -> StreamUnion<'a, V> {
        let mut heap = BinaryHeap::new();
        {
            let streams_it = streams.iter_mut();
            loop {
                match streams_it {

                    Some(fst_map_it) => {
                    }
                    None => {
                        break;
                    }
                }
            }
        }
        let (k, v) = streams.iter_mut().next().unwrap().next().unwrap();
        // for (i, stream) in streams.iter_mut().enumerate() {
        //     match stream.next() {
        //         Some(kv) => {
        //             let (key, val): (&'a [u8], V) = kv;
        //             //heap.push((key.clone(), i.clone(), val.clone()));
        //             // let c: &'a [u8] = key;
        //             // heap.push((key.clone(), i.clone()));
        //             //heap.push(i.clone());
        //             // heap.push(i.clone());
        //         },
        //         None => {},
        //     }
        // }
        StreamUnion {
            streams: streams,
            heap: heap,
        }
    }

    pub fn next(&mut self) -> Option<(&[u8], V)> {
        // match self.heap.pop() {
        //     Some((k, i, v)) => {
        //         let mut vals = Vec::new();
        //         match self.streams[i].next() {
        //             Some((k_next,v_next)) => {
        //                 self.heap.push((k_next, i, v_next));
        //             },
        //             None => {}
        //         }
        //
        //     },
        //     None => None,
        // }
        None
    }
}



struct IndexMerger {
    readers: Vec<SegmentReader>,
    offsets: Vec<DocId>,
}

impl IndexMerger {
    pub fn open(segments: &Vec<Segment>) -> io::Result<IndexMerger> {
        let mut readers = Vec::new();
        let mut offsets = Vec::new();
        for segment in segments.iter() {
            let reader = try!(SegmentReader::open(segment.clone()));
            offsets.push(reader.max_doc());
            readers.push(reader);
        }
        Ok(IndexMerger {
            readers: readers,
            offsets: offsets,
        })
    }

    fn write_postings(&self, postings_serializer: &mut PostingsSerializer) -> io::Result<()> {
        for reader in self.readers.iter() {
            let term_infos = reader.term_infos();
            let term_stream = term_infos.stream();

        }
        Ok(())
    }
}

impl SerializableSegment for IndexMerger {
    fn write(&self, mut serializer: SegmentSerializer) -> io::Result<()> {
        try!(self.write_postings(serializer.get_postings_serializer()));
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use core::schema;
    use core::schema::Document;
    use core::index::Index;
    use super::IndexMerger;

    #[test]
    fn test_index_merger() {
        let mut schema = schema::Schema::new();
        let text_fieldtype = schema::TextOptions::new().set_tokenized_indexed();
        let text_field = schema.add_text_field("text", &text_fieldtype);
        let index = Index::create_in_ram(schema);

        let merger;
        {
            {
                // writing the segment
                let mut index_writer = index.writer_with_num_threads(1).unwrap();
                {
                    let mut doc = Document::new();
                    doc.set(&text_field, "af b");
                    index_writer.add_document(doc).unwrap();
                }
                {
                    let mut doc = Document::new();
                    doc.set(&text_field, "a b c");
                    index_writer.add_document(doc).unwrap();
                }
                {
                    let mut doc = Document::new();
                    doc.set(&text_field, "a b c d");
                    index_writer.add_document(doc).unwrap();
                }
                index_writer.wait();
            }

            {
                // writing the segment
                let mut index_writer = index.writer_with_num_threads(1).unwrap();
                {
                    let mut doc = Document::new();
                    doc.set(&text_field, "af b");
                    index_writer.add_document(doc).unwrap();
                }
                {
                    let mut doc = Document::new();
                    doc.set(&text_field, "a d c");
                    index_writer.add_document(doc).unwrap();
                }
                {
                    let mut doc = Document::new();
                    doc.set(&text_field, "a b c g");
                    index_writer.add_document(doc).unwrap();
                }
                index_writer.wait();
            }
        }
        let segments = index.segments();
        merger = IndexMerger::open(&segments).unwrap();


    }
}
