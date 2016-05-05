


pub trait Recorder {
    fn new() -> Self;
    fn record_position(&mut self, position: u32);
    fn close_doc(&mut self,);
    fn get_tf_and_posdeltas(&self, i: usize) -> (u32, &[u32]);
}


const EMPTY_ARRAY: [u32; 0] = [];

pub struct NothingRecorder;

impl Recorder for NothingRecorder {
    fn new() -> Self {
        NothingRecorder
    }
    
    fn record_position(&mut self, _position: u32) {
    }
    
    fn close_doc(&mut self,) {
    }
    
    fn get_tf_and_posdeltas(&self, _: usize) -> (u32, &[u32]) {
        (0u32, &EMPTY_ARRAY)
    }
}



pub struct TermFrequencyRecorder {
    term_freqs: Vec<u32>,
    current_tf: u32,
}

impl Recorder for TermFrequencyRecorder {
    fn new() -> Self {
        TermFrequencyRecorder {
            term_freqs: Vec::new(),
            current_tf: 0u32,
        }
    }
    
    fn record_position(&mut self, _position: u32) {
        self.current_tf += 1;
    }
    
    fn close_doc(&mut self,) {
        assert!(self.current_tf > 0);
        self.term_freqs.push(self.current_tf);
        self.current_tf = 0;
    }
    
    fn get_tf_and_posdeltas(&self, i: usize) -> (u32, &[u32]) {
        (self.term_freqs[i], &EMPTY_ARRAY)
    }
}



pub struct TFAndPositionRecorder {
    cumulated_tfs: Vec<u32>,
    positions: Vec<u32>,
    cumulated_tf: u32,
    current_pos: u32,
}

impl Recorder for TFAndPositionRecorder {
    fn new() -> Self {
        TFAndPositionRecorder {
            cumulated_tfs: vec!(0u32),
            cumulated_tf: 0u32,
            positions: Vec::new(),
            current_pos: 0u32,
        }
    }
    
    fn record_position(&mut self, position: u32) {
        self.cumulated_tf += 1;
        self.positions.push(position - self.current_pos);
        self.current_pos = position;
    }
    
    fn close_doc(&mut self,) {
        self.cumulated_tfs.push(self.cumulated_tf);
    }
    
    fn get_tf_and_posdeltas(&self, i: usize) -> (u32, &[u32]) {
        let tf = self.cumulated_tfs[i+1] - self.cumulated_tfs[i];
        let pos_idx = self.cumulated_tfs[i] as usize;
        let posdeltas = &self.positions[pos_idx..pos_idx + tf as usize];
        (tf, posdeltas)
    }
}


