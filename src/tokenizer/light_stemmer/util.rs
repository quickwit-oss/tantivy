//! Helpers shared by the ported stemmers, from Lucene's `StemmerUtil`.

/// Returns `true` if the first `len` characters of `s` end with `suffix`.
///
/// This is Lucene's `StemmerUtil.endsWith`. Lucene compares UTF-16 code units; every language
/// handled by these stemmers is confined to the Basic Multilingual Plane, so comparing [`char`]s
/// is equivalent.
pub(crate) fn ends_with(s: &[char], len: usize, suffix: &str) -> bool {
    let mut i = len;
    for c in suffix.chars().rev() {
        if i == 0 {
            return false;
        }
        i -= 1;
        if s[i] != c {
            return false;
        }
    }
    true
}

/// Returns `true` if the first `len` characters of `s` start with `prefix`.
///
/// Lucene's `StemmerUtil.startsWith`.
pub(crate) fn starts_with(s: &[char], len: usize, prefix: &str) -> bool {
    for (i, c) in prefix.chars().enumerate() {
        if i == len || s[i] != c {
            return false;
        }
    }
    true
}

/// Deletes `n` characters at `pos`, shifting the remainder left, and returns the new length.
///
/// Lucene's `StemmerUtil.deleteN`.
pub(crate) fn delete_n(s: &mut [char], pos: usize, len: usize, n: usize) -> usize {
    debug_assert!(pos + n <= len);
    s.copy_within(pos + n..len, pos);
    len - n
}

#[cfg(test)]
mod tests {
    use super::{delete_n, ends_with, starts_with};

    #[test]
    fn test_ends_with() {
        let s: Vec<char> = "абвгд".chars().collect();
        assert!(ends_with(&s, s.len(), "гд"));
        assert!(ends_with(&s, s.len(), "абвгд"));
        assert!(ends_with(&s, s.len(), ""));
        assert!(!ends_with(&s, s.len(), "вг"));
        // a suffix longer than the inspected prefix never matches
        assert!(!ends_with(&s, 2, "абв"));
        // `len` shortens the window, as Lucene's explicit length argument does
        assert!(ends_with(&s, 3, "бв"));
    }

    #[test]
    fn test_starts_with() {
        let s: Vec<char> = "abcde".chars().collect();
        assert!(starts_with(&s, s.len(), "abc"));
        assert!(starts_with(&s, s.len(), ""));
        assert!(!starts_with(&s, s.len(), "bcd"));
        // a prefix longer than the inspected window never matches
        assert!(!starts_with(&s, 2, "abc"));
    }

    #[test]
    fn test_delete_n() {
        let mut s: Vec<char> = "najlepsi".chars().collect();
        let len = delete_n(&mut s, 0, 8, 3);
        assert_eq!(len, 5);
        assert_eq!(s[..len].iter().collect::<String>(), "lepsi");
    }
}
