//! Light stemmer for Bulgarian.
//!
//! Ported from Lucene's `org.apache.lucene.analysis.bg.BulgarianStemmer`, which implements the
//! algorithm described in *Searching Strategies for the Bulgarian Language*,
//! <http://members.unine.ch/jacques.savoy/Papers/BUIR.pdf>.

use super::util::ends_with;

/// Stems a Bulgarian word in place, returning the new length.
///
/// Unlike the purely truncating stemmers, this one also rewrites characters within `s`.
pub(super) fn stem(s: &mut [char], mut len: usize) -> usize {
    if len < 4 {
        // do not stem
        return len;
    }

    if len > 5 && ends_with(s, len, "ища") {
        return len - 3;
    }

    len = remove_article(s, len);
    len = remove_plural(s, len);

    if len > 3 {
        if ends_with(s, len, "я") {
            len -= 1;
        }
        if ends_with(s, len, "а") || ends_with(s, len, "о") || ends_with(s, len, "е") {
            len -= 1;
        }
    }

    // The rule to rewrite ен -> н is duplicated in the paper. In the perl implementation the
    // paper references this is fixed, and it is fixed here as well.
    if len > 4 && ends_with(s, len, "ен") {
        s[len - 2] = 'н'; // replace with н
        len -= 1;
    }

    if len > 5 && s[len - 2] == 'ъ' {
        s[len - 2] = s[len - 1]; // replace ъN with N
        len -= 1;
    }

    len
}

/// Mainly removes the definite article.
fn remove_article(s: &[char], len: usize) -> usize {
    if len > 6 && ends_with(s, len, "ият") {
        return len - 3;
    }

    if len > 5
        && (ends_with(s, len, "ът")
            || ends_with(s, len, "то")
            || ends_with(s, len, "те")
            || ends_with(s, len, "та")
            || ends_with(s, len, "ия"))
    {
        return len - 2;
    }

    if len > 4 && ends_with(s, len, "ят") {
        return len - 2;
    }

    len
}

fn remove_plural(s: &mut [char], len: usize) -> usize {
    if len > 6 {
        if ends_with(s, len, "овци") {
            return len - 3; // replace with о
        }
        if ends_with(s, len, "ове") {
            return len - 3;
        }
        if ends_with(s, len, "еве") {
            s[len - 3] = 'й'; // replace with й
            return len - 2;
        }
    }

    if len > 5 {
        if ends_with(s, len, "ища") {
            return len - 3;
        }
        if ends_with(s, len, "та") {
            return len - 2;
        }
        if ends_with(s, len, "ци") {
            s[len - 2] = 'к'; // replace with к
            return len - 1;
        }
        if ends_with(s, len, "зи") {
            s[len - 2] = 'г'; // replace with г
            return len - 1;
        }
        if s[len - 3] == 'е' && s[len - 1] == 'и' {
            s[len - 3] = 'я'; // replace е with я, remove и
            return len - 1;
        }
    }

    if len > 4 {
        if ends_with(s, len, "си") {
            s[len - 2] = 'х'; // replace with х
            return len - 1;
        }
        if ends_with(s, len, "и") {
            return len - 1;
        }
    }

    len
}
