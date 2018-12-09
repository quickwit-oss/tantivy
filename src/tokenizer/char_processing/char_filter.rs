extern crate aho_corasick;
use self::aho_corasick::{AcAutomaton, Automaton};
use std::mem;

use super::{OffsetIncrements, OffsetIncrementsBuilder};

pub trait CharMogrifier {
    fn process_text(&mut self, text: &str, dest: &mut String, correction: &mut OffsetIncrementsBuilder);
}

pub struct CharFilter {
    text: String,
    buffer: String,
    mogrifiers: Vec<Box<CharMogrifier>>
}

impl CharFilter {

    fn process_text(&mut self, text: &str) {
        self.text.clear();
        self.text.push_str(text);
        self.buffer.clear();
        let mut offset_increment_builder = OffsetIncrements::builder();
//        self.offsets_translator.reset(text);
        for mogrifier in &mut self.mogrifiers {
            mogrifier.process_text(&self.text,
                                   &mut self.buffer,
                                   &mut offset_increment_builder);
            mem::swap(&mut self.text, &mut self.buffer);
            offset_increment_builder.new_layer();
        }
    }
}


pub struct SubstringReplacer<'a> {
    automaton: AcAutomaton<&'a str>,
    replacements: Vec<&'a str>
}


impl SubstringReplacer<'static> {
    fn new(from_tos: Vec<(&'static str, &'static str)>) -> SubstringReplacer<'static> {
        let from_ptns: Vec<&'static str> = from_tos
            .iter()
            .map(|(from_str, _)| *from_str)
            .collect();
        let to_strs: Vec<&'static str> = from_tos
            .iter()
            .map(|(_, to_str)| *to_str)
            .collect();
        let automaton = AcAutomaton::new(from_ptns);
        SubstringReplacer {
            automaton,
            replacements: to_strs
        }
    }
}
impl<'a> CharMogrifier for SubstringReplacer<'a> {
    // correction is an array that goes from old_offset -> new_offset.
    // correction len is `text.len() + 1`
    fn process_text(&mut self, text: &str, dest: &mut String, correction: &mut OffsetIncrementsBuilder) {
        let mut start = 0;
        for m in self.automaton.find(text) {
            dest.push_str(&text[start..m.start]);
            let replacement = self.replacements[m.pati];
            let previous_len = m.end - m.start;
            correction.register_inc(m.end, (replacement.len() as isize) - (previous_len as isize));
            dest.push_str(replacement);
            start = m.end;
        }
        dest.push_str(&text[start..]);
    }
}


// lowercasing
// Unicode normalization*
// '
// accent simplification
