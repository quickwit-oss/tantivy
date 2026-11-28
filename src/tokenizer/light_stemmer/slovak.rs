//! Light stemmer for Slovak.
//!
//! Ported from Wikimedia's `SlovakStemmer`, in
//! `opensearch-extra-analysis-slovak` of <https://github.com/wikimedia/search-extra>, which is in
//! production on Slovak Wikipedia search. Unlike the other stemmers here it is not Lucene's, but
//! it is built from Lucene's Czech light stemmer plus Slovak suffix data, and is licensed:
//!
//! - implementation: Apache License 2.0, the Wikimedia Foundation, derived from Lucene's
//!   `CzechStemmer`;
//! - Slovak-specific suffix information: MIT, Copyright (c) 2015 Marek Šuppa, adapted from
//!   <https://github.com/mrshu/stemm-sk/>.
//!
//! Both descend from Dolamic & Savoy, *Indexing and stemming approaches for the Czech language*.
//!
//! Input is expected to be lowercased, with diacritics intact.

use super::util::{delete_n, ends_with, starts_with};

/// Stems a Slovak word in place, returning the new length.
pub(super) fn stem(s: &mut [char], len: usize) -> usize {
    let len = remove_case(s, len);
    let len = remove_possessives(s, len);
    remove_prefixes(s, len)
}

fn remove_prefixes(s: &mut [char], len: usize) -> usize {
    if len > 5 && starts_with(s, len, "naj") {
        return delete_n(s, 0, len, 3);
    }
    len
}

fn remove_case(s: &mut [char], len: usize) -> usize {
    if len > 7 && ends_with(s, len, "atoch") {
        return len - 5;
    }

    if len > 6 && ends_with(s, len, "aťom") {
        return palatalize(s, len - 3);
    }

    if len > 5 {
        if ends_with(s, len, "och")
            || ends_with(s, len, "ich")
            || ends_with(s, len, "ích")
            || ends_with(s, len, "ého")
            || ends_with(s, len, "ami")
            || ends_with(s, len, "emi")
            || ends_with(s, len, "ému")
            || ends_with(s, len, "ete")
            || ends_with(s, len, "eti")
            || ends_with(s, len, "iho")
            || ends_with(s, len, "ího")
            || ends_with(s, len, "ími")
            || ends_with(s, len, "imu")
            || ends_with(s, len, "aťa")
        {
            return palatalize(s, len - 2);
        }
        if ends_with(s, len, "ách")
            || ends_with(s, len, "ata")
            || ends_with(s, len, "aty")
            || ends_with(s, len, "ých")
            || ends_with(s, len, "ové")
            || ends_with(s, len, "ovi")
            || ends_with(s, len, "ými")
        {
            return len - 3;
        }
    }

    if len > 4 {
        if ends_with(s, len, "om") {
            return palatalize(s, len - 1);
        }
        if ends_with(s, len, "es") || ends_with(s, len, "ém") || ends_with(s, len, "ím") {
            return palatalize(s, len - 2);
        }
        if ends_with(s, len, "úm")
            || ends_with(s, len, "at")
            || ends_with(s, len, "ám")
            || ends_with(s, len, "os")
            || ends_with(s, len, "us")
            || ends_with(s, len, "ým")
            || ends_with(s, len, "mi")
            || ends_with(s, len, "ou")
            || ends_with(s, len, "ej")
        {
            return len - 2;
        }
    }

    if len > 3 {
        match s[len - 1] {
            'e' | 'i' | 'í' => return palatalize(s, len),
            'ú' | 'y' | 'a' | 'o' | 'á' | 'é' | 'ý' => return len - 1,
            _ => {}
        }
    }

    len
}

fn remove_possessives(s: &mut [char], len: usize) -> usize {
    if len > 5 {
        if ends_with(s, len, "ov") {
            return len - 2;
        }
        if ends_with(s, len, "in") {
            return palatalize(s, len - 1);
        }
    }
    len
}

fn palatalize(s: &mut [char], len: usize) -> usize {
    // Upstream asserts `len > 3` here, but `remove_case` can legitimately reach this with
    // exactly 3: an `-es` ending on a five-character word calls `palatalize(s, len - 2)`.
    // Java disables assertions unless `-ea` is passed, so upstream never trips it. Three is
    // the real lower bound, and the rules below only index within it.
    debug_assert!(len >= 3);

    if ends_with(s, len, "ci")
        || ends_with(s, len, "ce")
        || ends_with(s, len, "či")
        || ends_with(s, len, "če")
    {
        // [cč][ie] -> k
        s[len - 2] = 'k';
    } else if ends_with(s, len, "zi")
        || ends_with(s, len, "ze")
        || ends_with(s, len, "ži")
        || ends_with(s, len, "že")
    {
        // [zž][ie] -> h
        s[len - 2] = 'h';
    } else if ends_with(s, len, "čte") || ends_with(s, len, "čti") || ends_with(s, len, "čtí") {
        // čt[eií] -> ck
        s[len - 3] = 'c';
        s[len - 2] = 'k';
    } else if ends_with(s, len, "šte") || ends_with(s, len, "šti") || ends_with(s, len, "ští") {
        // št[eií] -> sk
        s[len - 3] = 's';
        s[len - 2] = 'k';
    }

    len - 1
}
