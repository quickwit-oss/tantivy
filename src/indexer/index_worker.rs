use chan;
use schema::Document;
use Index;
use indexer::SegmentWriter;
use std::thread::{JoinHandle, self};


pub struct IndexWorker {
    // schema: Schema,
    // document_sender: chan::Sender<Document>,
    handle: JoinHandle<()>
}

impl IndexWorker {
    
    
    
    pub fn wait(self,) -> thread::Result<()> {
        self.handle.join()
    }
    
    
    pub fn spawn(mut index: Index,
           document_receiver: chan::Receiver<Document>) -> IndexWorker {
        
        let schema = index.schema();
        
        let thread_handle = thread::spawn(move || {

            let mut docs_remaining = true;
            while docs_remaining {
                
                // the first document is handled separately in order to
                // avoid creating a new segment if there are no documents. 
                let mut doc: Document;
                {
                    match document_receiver.recv() {
                        Some(doc_) => { 
                            doc = doc_;
                        }
                        None => {
                            return;
                        }
                    }
                }
                
                let segment = index.new_segment();
                let mut segment_writer = SegmentWriter::for_segment(segment.clone(), &schema).unwrap();
                segment_writer.add_document(&doc, &schema).unwrap();
                
                for _ in 0..100_000 {
                    {
                        match document_receiver.recv() {
                            Some(doc_) => {
                                doc = doc_
                            }
                            None => {
                                docs_remaining = false;
                                break;
                            }
                        }
                    }
                    segment_writer.add_document(&doc, &schema).unwrap();
                }
                segment_writer.finalize().unwrap();
                index.publish_segment(&segment).unwrap();
            }
        });
        
        IndexWorker {
            handle: thread_handle,
        }
            
    }
    

}