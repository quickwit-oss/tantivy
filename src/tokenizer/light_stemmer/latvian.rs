//! Light stemmer for Latvian.
//!
//! Ported from Lucene's `org.apache.lucene.analysis.lv.LatvianStemmer`.
//!
//! This is a light version of the algorithm in Karlis Kreslin's PhD thesis *A stemming algorithm
//! for Latvian*, with the following modifications:
//!
//! - Only explicitly stems noun and adjective morphology.
//! - Stricter length/vowel checks for the resulting stems (verb etc. suffix stripping is removed).
//! - Removes only the primary inflectional suffixes: case and number for nouns; case, number,
//!   gender and definitiveness for adjectives.
//! - Palatalization is only handled when a declension II, V, VI noun suffix is removed.

use super::util::ends_with;

struct Affix {
    /// The suffix to strip.
    affix: &'static str,
    /// Length of `affix` in characters.
    len: usize,
    /// Vowel count the remaining stem must exceed.
    vc: usize,
    /// Whether to apply the palatalization rules after stripping.
    palatalizes: bool,
}

#[rustfmt::skip]
static AFFIXES: &[Affix] = &[
    Affix { affix: "ajiem", len: 5, vc: 3, palatalizes: false },
    Affix { affix: "ajai",  len: 4, vc: 3, palatalizes: false },
    Affix { affix: "ajam",  len: 4, vc: 2, palatalizes: false },
    Affix { affix: "ajām",  len: 4, vc: 2, palatalizes: false },
    Affix { affix: "ajos",  len: 4, vc: 2, palatalizes: false },
    Affix { affix: "ajās",  len: 4, vc: 2, palatalizes: false },
    Affix { affix: "iem",   len: 3, vc: 2, palatalizes: true },
    Affix { affix: "ajā",   len: 3, vc: 2, palatalizes: false },
    Affix { affix: "ais",   len: 3, vc: 2, palatalizes: false },
    Affix { affix: "ai",    len: 2, vc: 2, palatalizes: false },
    Affix { affix: "ei",    len: 2, vc: 2, palatalizes: false },
    Affix { affix: "ām",    len: 2, vc: 1, palatalizes: false },
    Affix { affix: "am",    len: 2, vc: 1, palatalizes: false },
    Affix { affix: "ēm",    len: 2, vc: 1, palatalizes: false },
    Affix { affix: "īm",    len: 2, vc: 1, palatalizes: false },
    Affix { affix: "im",    len: 2, vc: 1, palatalizes: false },
    Affix { affix: "um",    len: 2, vc: 1, palatalizes: false },
    Affix { affix: "us",    len: 2, vc: 1, palatalizes: true },
    Affix { affix: "as",    len: 2, vc: 1, palatalizes: false },
    Affix { affix: "ās",    len: 2, vc: 1, palatalizes: false },
    Affix { affix: "es",    len: 2, vc: 1, palatalizes: false },
    Affix { affix: "os",    len: 2, vc: 1, palatalizes: true },
    Affix { affix: "ij",    len: 2, vc: 1, palatalizes: false },
    Affix { affix: "īs",    len: 2, vc: 1, palatalizes: false },
    Affix { affix: "ēs",    len: 2, vc: 1, palatalizes: false },
    Affix { affix: "is",    len: 2, vc: 1, palatalizes: false },
    Affix { affix: "ie",    len: 2, vc: 1, palatalizes: false },
    Affix { affix: "u",     len: 1, vc: 1, palatalizes: true },
    Affix { affix: "a",     len: 1, vc: 1, palatalizes: true },
    Affix { affix: "i",     len: 1, vc: 1, palatalizes: true },
    Affix { affix: "e",     len: 1, vc: 1, palatalizes: false },
    Affix { affix: "ā",     len: 1, vc: 1, palatalizes: false },
    Affix { affix: "ē",     len: 1, vc: 1, palatalizes: false },
    Affix { affix: "ī",     len: 1, vc: 1, palatalizes: false },
    Affix { affix: "ū",     len: 1, vc: 1, palatalizes: false },
    Affix { affix: "o",     len: 1, vc: 1, palatalizes: false },
    Affix { affix: "s",     len: 1, vc: 0, palatalizes: false },
    Affix { affix: "š",     len: 1, vc: 0, palatalizes: false },
];

/// Stems a Latvian word in place, returning the new length.
///
/// `s` must be the full word: [`unpalatalize`] inspects the character just past the shortened
/// length, and the `kš` rule grows the length back by one.
pub(super) fn stem(s: &mut [char], len: usize) -> usize {
    let num_vowels = num_vowels(s, len);

    for affix in AFFIXES {
        if num_vowels > affix.vc && len >= affix.len + 3 && ends_with(s, len, affix.affix) {
            let len = len - affix.len;
            return if affix.palatalizes {
                unpalatalize(s, len)
            } else {
                len
            };
        }
    }

    len
}

/// Most cases are handled except for the ambiguous ones:
///
/// - `s` -> `š`
/// - `t` -> `š`
/// - `d` -> `ž`
/// - `z` -> `ž`
fn unpalatalize(s: &mut [char], mut len: usize) -> usize {
    // We check the character removed: if it is -u then it is 2, 5 or 6 gen. pl., and these two
    // can only apply then.
    if s[len] == 'u' {
        // kš -> kst
        if ends_with(s, len, "kš") {
            len += 1;
            s[len - 2] = 's';
            s[len - 1] = 't';
            return len;
        }
        // ņņ -> nn
        if ends_with(s, len, "ņņ") {
            s[len - 2] = 'n';
            s[len - 1] = 'n';
            return len;
        }
    }

    // otherwise all other rules
    if ends_with(s, len, "pj")
        || ends_with(s, len, "bj")
        || ends_with(s, len, "mj")
        || ends_with(s, len, "vj")
    {
        // labial consonant
        len - 1
    } else if ends_with(s, len, "šņ") {
        s[len - 2] = 's';
        s[len - 1] = 'n';
        len
    } else if ends_with(s, len, "žņ") {
        s[len - 2] = 'z';
        s[len - 1] = 'n';
        len
    } else if ends_with(s, len, "šļ") {
        s[len - 2] = 's';
        s[len - 1] = 'l';
        len
    } else if ends_with(s, len, "žļ") {
        s[len - 2] = 'z';
        s[len - 1] = 'l';
        len
    } else if ends_with(s, len, "ļņ") {
        s[len - 2] = 'l';
        s[len - 1] = 'n';
        len
    } else if ends_with(s, len, "ļļ") {
        s[len - 2] = 'l';
        s[len - 1] = 'l';
        len
    } else if s[len - 1] == 'č' {
        s[len - 1] = 'c';
        len
    } else if s[len - 1] == 'ļ' {
        s[len - 1] = 'l';
        len
    } else if s[len - 1] == 'ņ' {
        s[len - 1] = 'n';
        len
    } else {
        len
    }
}

/// Counts the vowels in the string. At least one is always required in the remaining stem for it
/// to be accepted.
fn num_vowels(s: &[char], len: usize) -> usize {
    s[..len]
        .iter()
        .filter(|c| matches!(c, 'a' | 'e' | 'i' | 'o' | 'u' | 'ā' | 'ī' | 'ē' | 'ū'))
        .count()
}
