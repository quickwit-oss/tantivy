use std::borrow::Cow;
use std::fmt;
use std::ops::RangeInclusive;
use std::sync::Arc;

/// Characters that do not count towards a term's length when an automaton query's expansion is
/// capped (see
/// [`AutomatonWeight::set_zero_width_chars`](crate::query::AutomatonWeight::set_zero_width_chars)).
///
/// This is meant for marks a tokenizer keeps inside terms, such as an apostrophe or a combining
/// accent, that should not make a term "longer" than the same word without them. Under a cap,
/// terms that differ only in these characters also share one expansion slot.
///
/// Terms are read as UTF-8. Bytes that are not part of a valid UTF-8 character are never
/// zero-width. Cloning is cheap.
///
/// ```rust
/// use tantivy::query::ZeroWidthChars;
///
/// // An apostrophe and the combining diacritical marks.
/// let zero_width_chars = ZeroWidthChars::from_chars(['\''])
///     .with_range('\u{300}'..='\u{36F}');
/// assert!(zero_width_chars.contains('\''));
/// assert!(zero_width_chars.contains('\u{301}'));
/// assert!(!zero_width_chars.contains('e'));
/// ```
#[derive(Clone, Default, PartialEq, Eq)]
pub struct ZeroWidthChars {
    ascii: [u64; 2],
    // Sorted, non-overlapping and non-adjacent ranges of non-ASCII chars.
    non_ascii: Arc<[(char, char)]>,
}

impl fmt::Debug for ZeroWidthChars {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let ascii = (0u8..0x80)
            .filter(|&byte| self.ascii[(byte >> 6) as usize] & (1u64 << (byte & 63)) != 0)
            .map(|byte| (char::from(byte), char::from(byte)));
        let mut list = f.debug_list();
        for (start, end) in ascii.chain(self.non_ascii.iter().copied()) {
            if start == end {
                list.entry(&start);
            } else {
                list.entry(&(start..=end));
            }
        }
        list.finish()
    }
}

impl ZeroWidthChars {
    /// Makes the given characters zero-width.
    pub fn from_chars(chars: impl IntoIterator<Item = char>) -> ZeroWidthChars {
        ZeroWidthChars::default().with_ranges(chars.into_iter().map(|c| c..=c))
    }

    /// Makes the characters of the given inclusive range zero-width too. An empty range, such as
    /// `'b'..='a'`, adds nothing.
    pub fn with_range(self, range: RangeInclusive<char>) -> ZeroWidthChars {
        self.with_ranges([range])
    }

    /// Makes the characters of the given inclusive ranges zero-width too. Empty ranges add
    /// nothing.
    pub fn with_ranges(
        mut self,
        ranges: impl IntoIterator<Item = RangeInclusive<char>>,
    ) -> ZeroWidthChars {
        let mut non_ascii: Vec<(char, char)> = self.non_ascii.to_vec();
        for range in ranges {
            let (start, end) = range.into_inner();
            if start > end {
                continue;
            }
            for byte in (start as u32)..=(end as u32).min(0x7F) {
                self.ascii[(byte >> 6) as usize] |= 1u64 << (byte & 63);
            }
            if end as u32 > 0x7F {
                non_ascii.push((start.max('\u{80}'), end));
            }
        }
        non_ascii.sort_unstable();
        let mut merged: Vec<(char, char)> = Vec::with_capacity(non_ascii.len());
        for (start, end) in non_ascii {
            if let Some(last) = merged.last_mut() {
                if start as u32 <= last.1 as u32 + 1 {
                    last.1 = last.1.max(end);
                    continue;
                }
            }
            merged.push((start, end));
        }
        self.non_ascii = merged.into();
        self
    }

    /// Returns true if no character is zero-width.
    pub fn is_empty(&self) -> bool {
        self.ascii == [0; 2] && self.non_ascii.is_empty()
    }

    /// Returns true if `c` is zero-width.
    pub fn contains(&self, c: char) -> bool {
        if c.is_ascii() {
            return self.contains_ascii(c as u8);
        }
        let ranges = &self.non_ascii[..];
        let after = ranges.partition_point(|&(start, _)| start <= c);
        after > 0 && c <= ranges[after - 1].1
    }

    fn contains_ascii(&self, byte: u8) -> bool {
        (self.ascii[(byte >> 6) as usize] >> (byte & 63)) & 1 == 1
    }

    /// The number of characters of `term` that are not zero-width.
    ///
    /// In bytes that are not valid UTF-8, every byte that is not a UTF-8 continuation byte counts
    /// as one character, so that without zero-width characters this is the number of bytes that
    /// are not continuation bytes.
    pub(crate) fn num_chars(&self, term: &[u8]) -> usize {
        if self.non_ascii.is_empty() {
            return term
                .iter()
                .filter(|&&byte| {
                    !is_continuation_byte(byte) && !self.contains_zero_width_byte(byte)
                })
                .count();
        }
        term.utf8_chunks()
            .map(|chunk| {
                let valid_chars = chunk.valid().chars().filter(|&c| !self.contains(c)).count();
                let invalid_chars = chunk
                    .invalid()
                    .iter()
                    .filter(|&&byte| !is_continuation_byte(byte))
                    .count();
                valid_chars + invalid_chars
            })
            .sum()
    }

    /// `term` without its zero-width characters.
    pub(crate) fn strip<'a>(&self, term: &'a [u8]) -> Cow<'a, [u8]> {
        if self.non_ascii.is_empty() {
            if !term.iter().any(|&byte| self.contains_zero_width_byte(byte)) {
                return Cow::Borrowed(term);
            }
            return Cow::Owned(
                term.iter()
                    .copied()
                    .filter(|&byte| !self.contains_zero_width_byte(byte))
                    .collect(),
            );
        }
        let has_zero_width = term
            .utf8_chunks()
            .any(|chunk| chunk.valid().chars().any(|c| self.contains(c)));
        if !has_zero_width {
            return Cow::Borrowed(term);
        }
        let mut stripped = Vec::with_capacity(term.len());
        let mut buffer = [0u8; 4];
        for chunk in term.utf8_chunks() {
            for c in chunk.valid().chars().filter(|&c| !self.contains(c)) {
                stripped.extend_from_slice(c.encode_utf8(&mut buffer).as_bytes());
            }
            stripped.extend_from_slice(chunk.invalid());
        }
        Cow::Owned(stripped)
    }

    /// Whether `byte` is a zero-width ASCII character. Never true for a byte of a multi-byte
    /// character.
    fn contains_zero_width_byte(&self, byte: u8) -> bool {
        byte.is_ascii() && self.contains_ascii(byte)
    }
}

fn is_continuation_byte(byte: u8) -> bool {
    byte & 0xC0 == 0x80
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::ZeroWidthChars;

    #[test]
    fn test_zero_width_chars_contains() {
        let none = ZeroWidthChars::default();
        assert!(none.is_empty());
        assert!(!none.contains('\''));

        let zero_width_chars = ZeroWidthChars::from_chars(['\'', '"', '\u{2019}']).with_ranges([
            '\u{300}'..='\u{36F}',
            '\u{200B}'..='\u{200D}',
            'b'..='a',
        ]);
        assert!(!zero_width_chars.is_empty());
        for c in [
            '\'', '"', '\u{2019}', '\u{300}', '\u{301}', '\u{36F}', '\u{200C}',
        ] {
            assert!(zero_width_chars.contains(c), "{c:?}");
        }
        for c in [
            'a', 'b', '\u{2FF}', '\u{370}', '\u{e9}', '\u{200E}', '\u{2018}',
        ] {
            assert!(!zero_width_chars.contains(c), "{c:?}");
        }
    }

    #[test]
    fn test_zero_width_chars_ranges_are_merged() {
        let zero_width_chars = ZeroWidthChars::from_chars(['\u{302}', '\u{300}']).with_ranges([
            '\u{301}'..='\u{301}',
            '\u{310}'..='\u{320}',
            '\u{315}'..='\u{330}',
        ]);
        assert_eq!(
            &zero_width_chars.non_ascii[..],
            &[('\u{300}', '\u{302}'), ('\u{310}', '\u{330}')]
        );
        // A range over ASCII and non-ASCII chars.
        let zero_width_chars = ZeroWidthChars::default().with_range('~'..='\u{82}');
        assert!(zero_width_chars.contains('~'));
        assert!(zero_width_chars.contains('\u{7F}'));
        assert!(zero_width_chars.contains('\u{81}'));
        assert_eq!(&zero_width_chars.non_ascii[..], &[('\u{80}', '\u{82}')]);
    }

    #[test]
    fn test_zero_width_chars_num_chars_and_strip() {
        let apostrophe = ZeroWidthChars::from_chars(['\'']);
        assert_eq!(apostrophe.num_chars(b"it's"), 3);
        assert_eq!(apostrophe.strip(b"it's").as_ref(), b"its");
        assert_eq!(apostrophe.num_chars("caf\u{e9}".as_bytes()), 4);

        let marks = ZeroWidthChars::from_chars(['\'']).with_range('\u{300}'..='\u{36F}');
        let term = "e\u{301}t'e\u{e9}";
        assert_eq!(marks.num_chars(term.as_bytes()), 4);
        assert_eq!(
            marks.strip(term.as_bytes()).as_ref(),
            "ete\u{e9}".as_bytes()
        );
        assert!(matches!(marks.strip(b"abc"), std::borrow::Cow::Borrowed(_)));
        assert_eq!(marks.num_chars("\u{301}".as_bytes()), 0);

        // Bytes that are not valid UTF-8 are never zero-width, and are kept by `strip`.
        assert_eq!(marks.num_chars(&[b'a', 0xCC]), 2);
        assert_eq!(marks.strip(&[0xCC, 0x81, 0xCC]).as_ref(), &[0xCC]);
        assert_eq!(marks.num_chars(&[0x80, 0xCC, 0x81, 0xFF]), 1);
    }

    proptest! {
        #[test]
        fn test_strip_removes_zero_width_chars(term in "[a\u{e9}'\u{301}\u{302}]{0,8}") {
            let marks = '\u{300}'..='\u{36F}';
            let zero_width_chars = ZeroWidthChars::from_chars(['\'']).with_range(marks.clone());
            let expected: String = term
                .chars()
                .filter(|&c| c != '\'' && !marks.contains(&c))
                .collect();
            let stripped = zero_width_chars.strip(term.as_bytes());
            prop_assert_eq!(stripped.as_ref(), expected.as_bytes());
            let num_chars = zero_width_chars.num_chars(term.as_bytes());
            prop_assert_eq!(num_chars, expected.chars().count());
        }
    }
}
