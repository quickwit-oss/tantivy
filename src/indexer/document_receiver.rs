use DocId;

pub trait DocumentReceiver {
    fn receive(&mut self, doc: DocId);
}