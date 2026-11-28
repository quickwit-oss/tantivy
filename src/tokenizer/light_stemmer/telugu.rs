//! Stemmer for Telugu.
//!
//! Ported from Lucene's `org.apache.lucene.analysis.te.TeluguStemmer`.

use super::util::ends_with;

/// Stems a Telugu word in place, returning the new length.
pub(super) fn stem(s: &[char], len: usize) -> usize {
    // 4
    if len > 5 && (ends_with(s, len, "ళ్ళు") || ends_with(s, len, "డ్లు")) {
        return len - 4;
    }

    // 2 (upstream lists "డు" twice in this block; the duplicate check is a no-op)
    if len > 3
        && (ends_with(s, len, "డు")
            || ends_with(s, len, "ము")
            || ends_with(s, len, "వు")
            || ends_with(s, len, "లు")
            || ends_with(s, len, "ని")
            || ends_with(s, len, "ను")
            || ends_with(s, len, "చే")
            || ends_with(s, len, "కై")
            || ends_with(s, len, "లో")
            || ends_with(s, len, "ది")
            || ends_with(s, len, "కి")
            || ends_with(s, len, "సు")
            || ends_with(s, len, "వై")
            || ends_with(s, len, "పై"))
    {
        return len - 2;
    }

    // 1
    if len > 2
        && (ends_with(s, len, "ి")
            || ends_with(s, len, "ీ")
            || ends_with(s, len, "ు")
            || ends_with(s, len, "ూ")
            || ends_with(s, len, "ె")
            || ends_with(s, len, "ే")
            || ends_with(s, len, "ొ")
            || ends_with(s, len, "ో")
            || ends_with(s, len, "ా"))
    {
        return len - 1;
    }

    len
}
