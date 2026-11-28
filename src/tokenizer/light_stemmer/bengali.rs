//! Stemmer for Bengali.
//!
//! Ported from Lucene's `org.apache.lucene.analysis.bn.BengaliStemmer`.
//!
//! The algorithm is based on the report in: *Natural Language Processing in an Indian Language
//! (Bengali)-I: Verb Phrase Analysis*, P Sengupta and B B Chaudhuri. A few of the criteria are
//! taken from <http://members.unine.ch/jacques.savoy/clef/BengaliStemmerLight.java.txt>.
//!
//! The suffix tables are generated from the Lucene source rather than transcribed: Bengali
//! `\u{9df}` has a canonically equivalent decomposed form (`\u{9af}\u{9bc}`), and substituting it
//! silently stops these suffixes from ever matching Lucene's (precomposed) input.

use super::util::ends_with;

/// Stems a Bengali word, returning the new length.
pub(super) fn stem(s: &[char], len: usize) -> usize {
    // 8
    if len > 9
        && (ends_with(s, len, "িয়াছিলাম")
            || ends_with(s, len, "িতেছিলাম")
            || ends_with(s, len, "িতেছিলেন")
            || ends_with(s, len, "ইতেছিলেন")
            || ends_with(s, len, "িয়াছিলেন")
            || ends_with(s, len, "ইয়াছিলেন"))
    {
        return len - 8;
    }
    // 7
    if len > 8
        && (ends_with(s, len, "িতেছিলি")
            || ends_with(s, len, "িতেছিলে")
            || ends_with(s, len, "িয়াছিলা")
            || ends_with(s, len, "িয়াছিলে")
            || ends_with(s, len, "িতেছিলা")
            || ends_with(s, len, "িয়াছিলি")
            || ends_with(s, len, "য়েদেরকে"))
    {
        return len - 7;
    }
    // 6
    if len > 7
        && (ends_with(s, len, "িতেছিস")
            || ends_with(s, len, "িতেছেন")
            || ends_with(s, len, "িয়াছিস")
            || ends_with(s, len, "িয়াছেন")
            || ends_with(s, len, "েছিলাম")
            || ends_with(s, len, "েছিলেন")
            || ends_with(s, len, "েদেরকে"))
    {
        return len - 6;
    }
    // 5
    if len > 6
        && (ends_with(s, len, "িতেছি")
            || ends_with(s, len, "িতেছা")
            || ends_with(s, len, "িতেছে")
            || ends_with(s, len, "ছিলাম")
            || ends_with(s, len, "ছিলেন")
            || ends_with(s, len, "িয়াছি")
            || ends_with(s, len, "িয়াছা")
            || ends_with(s, len, "িয়াছে")
            || ends_with(s, len, "েছিলে")
            || ends_with(s, len, "েছিলা")
            || ends_with(s, len, "য়েদের")
            || ends_with(s, len, "দেরকে"))
    {
        return len - 5;
    }
    // 4
    if len > 5
        && (ends_with(s, len, "িলাম")
            || ends_with(s, len, "িলেন")
            || ends_with(s, len, "িতাম")
            || ends_with(s, len, "িতেন")
            || ends_with(s, len, "িবেন")
            || ends_with(s, len, "ছিলি")
            || ends_with(s, len, "ছিলে")
            || ends_with(s, len, "ছিলা")
            || ends_with(s, len, "তেছে")
            || ends_with(s, len, "িতেছ")
            || ends_with(s, len, "খানা")
            || ends_with(s, len, "খানি")
            || ends_with(s, len, "গুলো")
            || ends_with(s, len, "গুলি")
            || ends_with(s, len, "য়েরা")
            || ends_with(s, len, "েদের"))
    {
        return len - 4;
    }
    // 3
    if len > 4
        && (ends_with(s, len, "লাম")
            || ends_with(s, len, "িলি")
            || ends_with(s, len, "ইলি")
            || ends_with(s, len, "িলে")
            || ends_with(s, len, "ইলে")
            || ends_with(s, len, "লেন")
            || ends_with(s, len, "িলা")
            || ends_with(s, len, "ইলা")
            || ends_with(s, len, "তাম")
            || ends_with(s, len, "িতি")
            || ends_with(s, len, "ইতি")
            || ends_with(s, len, "িতে")
            || ends_with(s, len, "ইতে")
            || ends_with(s, len, "তেন")
            || ends_with(s, len, "িতা")
            || ends_with(s, len, "িবা")
            || ends_with(s, len, "ইবা")
            || ends_with(s, len, "িবি")
            || ends_with(s, len, "ইবি")
            || ends_with(s, len, "বেন")
            || ends_with(s, len, "িবে")
            || ends_with(s, len, "ইবে")
            || ends_with(s, len, "ছেন")
            || ends_with(s, len, "য়োন")
            || ends_with(s, len, "য়ের")
            || ends_with(s, len, "েরা")
            || ends_with(s, len, "দের"))
    {
        return len - 3;
    }
    // 2
    if len > 3
        && (ends_with(s, len, "িস")
            || ends_with(s, len, "েন")
            || ends_with(s, len, "লি")
            || ends_with(s, len, "লে")
            || ends_with(s, len, "লা")
            || ends_with(s, len, "তি")
            || ends_with(s, len, "তে")
            || ends_with(s, len, "তা")
            || ends_with(s, len, "বি")
            || ends_with(s, len, "বে")
            || ends_with(s, len, "বা")
            || ends_with(s, len, "ছি")
            || ends_with(s, len, "ছা")
            || ends_with(s, len, "ছে")
            || ends_with(s, len, "ুন")
            || ends_with(s, len, "ুক")
            || ends_with(s, len, "টা")
            || ends_with(s, len, "টি")
            || ends_with(s, len, "নি")
            || ends_with(s, len, "ের")
            || ends_with(s, len, "তে")
            || ends_with(s, len, "রা")
            || ends_with(s, len, "কে"))
    {
        return len - 2;
    }
    // 1
    if len > 2
        && (ends_with(s, len, "ি")
            || ends_with(s, len, "ী")
            || ends_with(s, len, "া")
            || ends_with(s, len, "ো")
            || ends_with(s, len, "ে")
            || ends_with(s, len, "ব")
            || ends_with(s, len, "ত"))
    {
        return len - 1;
    }
    len
}
