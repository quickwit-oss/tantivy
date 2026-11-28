//! Light stemmer for Sorani Kurdish.
//!
//! Ported from Lucene's `org.apache.lucene.analysis.ckb.SoraniStemmer`.

use super::util::ends_with;

/// Stems a Sorani word, returning the new length.
// The if/else ladder is kept one-for-one with Lucene's so that it can be diffed against
// upstream. Several arms do strip the same number of characters as a neighbour -- they are
// distinct grammatical cases with different length guards -- so collapsing them would lose the
// correspondence without changing behaviour.
#[allow(clippy::if_same_then_else)]
pub(super) fn stem(s: &[char], mut len: usize) -> usize {
    // postposition
    if len > 5 && ends_with(s, len, "دا") {
        len -= 2;
    } else if len > 4 && ends_with(s, len, "نا") {
        // Upstream removes a single character here for a two-character suffix. Kept verbatim so
        // this port matches Lucene's behaviour, and its test vectors, exactly.
        len -= 1;
    } else if len > 6 && ends_with(s, len, "ەوە") {
        len -= 3;
    }

    // possessive pronoun
    if len > 6 && (ends_with(s, len, "مان") || ends_with(s, len, "یان") || ends_with(s, len, "تان"))
    {
        len -= 3;
    }

    // indefinite singular ezafe
    if len > 6 && ends_with(s, len, "ێکی") {
        len - 3
    } else if len > 7 && ends_with(s, len, "یەکی") {
        len - 4
    }
    // indefinite singular
    else if len > 5 && ends_with(s, len, "ێک") {
        len - 2
    } else if len > 6 && ends_with(s, len, "یەک") {
        len - 3
    }
    // definite singular
    else if len > 6 && ends_with(s, len, "ەکە") {
        len - 3
    } else if len > 5 && ends_with(s, len, "کە") {
        len - 2
    }
    // definite plural
    else if len > 7 && ends_with(s, len, "ەکان") {
        len - 4
    } else if len > 6 && ends_with(s, len, "کان") {
        len - 3
    }
    // indefinite plural ezafe
    else if len > 7 && ends_with(s, len, "یانی") {
        len - 4
    } else if len > 6 && ends_with(s, len, "انی") {
        len - 3
    }
    // indefinite plural
    else if len > 6 && ends_with(s, len, "یان") {
        len - 3
    } else if len > 5 && ends_with(s, len, "ان") {
        len - 2
    }
    // demonstrative plural
    else if len > 7 && ends_with(s, len, "یانە") {
        len - 4
    } else if len > 6 && ends_with(s, len, "انە") {
        len - 3
    }
    // demonstrative singular
    else if len > 5 && (ends_with(s, len, "ایە") || ends_with(s, len, "ەیە")) {
        len - 2
    } else if len > 4 && ends_with(s, len, "ە") {
        len - 1
    }
    // absolute singular ezafe
    else if len > 4 && ends_with(s, len, "ی") {
        len - 1
    } else {
        len
    }
}
