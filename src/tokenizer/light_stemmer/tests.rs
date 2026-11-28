//! Test vectors for the ported stemmers.
//!
//! Every vector is taken from the corresponding Lucene test case, so these tests are the evidence
//! that the ports in this module reproduce Lucene's behaviour. They are extracted mechanically;
//! when adding a language, extract its vectors rather than writing expectations by hand. Two
//! details of Lucene's harness are accounted for during extraction:
//!
//! - Lucene feeds its vectors through a whitespace tokenizer, so surrounding whitespace never
//!   reaches the stemmer (its Latvian source contains `"govi "`). Inputs are trimmed.
//! - `TestSoraniStemFilter` builds a full `SoraniAnalyzer`, which normalizes before stemming.
//!   See [`SORANI_REQUIRES_NORMALIZATION`].

use super::LightAlgorithm;

/// Vectors for [`LightAlgorithm::Bulgarian`], from Lucene's `TestBulgarianStem*`.
#[rustfmt::skip]
const BULGARIAN: &[(&str, &str)] = &[
    ("град", "град"),
    ("града", "град"),
    ("градът", "град"),
    ("градове", "град"),
    ("градовете", "град"),
    ("народ", "народ"),
    ("народа", "народ"),
    ("народът", "народ"),
    ("народи", "народ"),
    ("народите", "народ"),
    ("народе", "народ"),
    ("път", "път"),
    ("пътя", "път"),
    ("пътят", "път"),
    ("пътища", "път"),
    ("пътищата", "път"),
    ("градец", "градец"),
    ("градеца", "градец"),
    ("градецът", "градец"),
    ("градовце", "градовц"),
    ("градовцете", "градовц"),
    ("дядо", "дяд"),
    ("дядото", "дяд"),
    ("дядовци", "дяд"),
    ("дядовците", "дяд"),
    ("мъж", "мъж"),
    ("мъжа", "мъж"),
    ("мъже", "мъж"),
    ("мъжете", "мъж"),
    ("мъжо", "мъж"),
    ("мъжът", "мъжът"),
    ("крак", "крак"),
    ("крака", "крак"),
    ("кракът", "крак"),
    ("краката", "крак"),
    ("брат", "брат"),
    ("брата", "брат"),
    ("братът", "брат"),
    ("братя", "брат"),
    ("братята", "брат"),
    ("брате", "брат"),
    ("вест", "вест"),
    ("вестта", "вест"),
    ("вести", "вест"),
    ("вестите", "вест"),
    ("дърво", "дърв"),
    ("дървото", "дърв"),
    ("дърва", "дърв"),
    ("дървета", "дърв"),
    ("дървата", "дърв"),
    ("дърветата", "дърв"),
    ("море", "мор"),
    ("морето", "мор"),
    ("морета", "мор"),
    ("моретата", "мор"),
    ("изключение", "изключени"),
    ("изключението", "изключени"),
    ("изключенията", "изключени"),
    ("изключения", "изключн"),
    ("красив", "красив"),
    ("красивия", "красив"),
    ("красивият", "красив"),
    ("красива", "красив"),
    ("красивата", "красив"),
    ("красиво", "красив"),
    ("красивото", "красив"),
    ("красиви", "красив"),
    ("красивите", "красив"),
    ("собственик", "собственик"),
    ("собственика", "собственик"),
    ("собственикът", "собственик"),
    ("собственици", "собственик"),
    ("собствениците", "собственик"),
    ("подлог", "подлог"),
    ("подлога", "подлог"),
    ("подлогът", "подлог"),
    ("подлози", "подлог"),
    ("подлозите", "подлог"),
    ("кожух", "кожух"),
    ("кожуха", "кожух"),
    ("кожухът", "кожух"),
    ("кожуси", "кожух"),
    ("кожусите", "кожух"),
    ("център", "центр"),
    ("центъра", "центр"),
    ("центърът", "центр"),
    ("центрове", "центр"),
    ("центровете", "центр"),
    ("промяна", "промян"),
    ("промяната", "промян"),
    ("промени", "промян"),
    ("промените", "промян"),
    ("песен", "песн"),
    ("песента", "песн"),
    ("песни", "песн"),
    ("песните", "песн"),
    ("строй", "строй"),
    ("строеве", "строй"),
    ("строевете", "строй"),
    ("строя", "стр"),
    ("строят", "стр"),
];

/// Vectors for [`LightAlgorithm::Bengali`], from Lucene's `TestBengaliStem*`.
#[rustfmt::skip]
const BENGALI: &[(&str, &str)] = &[
    ("করেছিলাম", "কর"),
    ("করিতেছিলে", "কর"),
    ("খাইতাম", "খাই"),
    ("যাইবে", "যা"),
    ("করছিলাম", "কর"),
    ("করছিলে", "কর"),
    ("করতাম", "কর"),
    ("যাব", "যা"),
    ("যাবে", "যা"),
    ("করি", "কর"),
    ("করো", "কর"),
    ("মেয়েরা", "মে"),
    ("মেয়েদেরকে", "মে"),
    ("মেয়েদের", "মে"),
    ("একটি", "এক"),
    ("মানুষগুলি", "মানুষ"),
];

/// Vectors for [`LightAlgorithm::Latvian`], from Lucene's `TestLatvianStem*`.
#[rustfmt::skip]
const LATVIAN: &[(&str, &str)] = &[
    ("tēvs", "tēv"),
    ("tēvi", "tēv"),
    ("tēva", "tēv"),
    ("tēvu", "tēv"),
    ("tēvam", "tēv"),
    ("tēviem", "tēv"),
    ("tēvus", "tēv"),
    ("tēvā", "tēv"),
    ("tēvos", "tēv"),
    ("lācis", "lāc"),
    ("lāči", "lāc"),
    ("lāča", "lāc"),
    ("lāču", "lāc"),
    ("lācim", "lāc"),
    ("lāčiem", "lāc"),
    ("lāci", "lāc"),
    ("lāčus", "lāc"),
    ("lācī", "lāc"),
    ("lāčos", "lāc"),
    ("akmens", "akmen"),
    ("akmeņi", "akmen"),
    ("akmeņu", "akmen"),
    ("akmenim", "akmen"),
    ("akmeņiem", "akmen"),
    ("akmeni", "akmen"),
    ("akmeņus", "akmen"),
    ("akmenī", "akmen"),
    ("akmeņos", "akmen"),
    ("kurmis", "kurm"),
    ("kurmji", "kurm"),
    ("kurmja", "kurm"),
    ("kurmju", "kurm"),
    ("kurmim", "kurm"),
    ("kurmjiem", "kurm"),
    ("kurmi", "kurm"),
    ("kurmjus", "kurm"),
    ("kurmī", "kurm"),
    ("kurmjos", "kurm"),
    ("lietus", "liet"),
    ("lieti", "liet"),
    ("lietu", "liet"),
    ("lietum", "liet"),
    ("lietiem", "liet"),
    ("lietū", "liet"),
    ("lietos", "liet"),
    ("lapa", "lap"),
    ("lapas", "lap"),
    ("lapu", "lap"),
    ("lapai", "lap"),
    ("lapām", "lap"),
    ("lapā", "lap"),
    ("lapās", "lap"),
    ("puika", "puik"),
    ("puikas", "puik"),
    ("puiku", "puik"),
    ("puikam", "puik"),
    ("puikām", "puik"),
    ("puikā", "puik"),
    ("puikās", "puik"),
    ("egle", "egl"),
    ("egles", "egl"),
    ("egļu", "egl"),
    ("eglei", "egl"),
    ("eglēm", "egl"),
    ("egli", "egl"),
    ("eglē", "egl"),
    ("eglēs", "egl"),
    ("govs", "gov"),
    ("govis", "gov"),
    ("govju", "gov"),
    ("govij", "gov"),
    ("govīm", "gov"),
    ("govi", "gov"),
    ("govī", "gov"),
    ("govīs", "gov"),
    ("zils", "zil"),
    ("zilais", "zil"),
    ("zili", "zil"),
    ("zilie", "zil"),
    ("zila", "zil"),
    ("zilā", "zil"),
    ("zilas", "zil"),
    ("zilās", "zil"),
    ("zilu", "zil"),
    ("zilo", "zil"),
    ("zilam", "zil"),
    ("zilajam", "zil"),
    ("ziliem", "zil"),
    ("zilajiem", "zil"),
    ("zilai", "zil"),
    ("zilajai", "zil"),
    ("zilām", "zil"),
    ("zilajām", "zil"),
    ("zilus", "zil"),
    ("zilos", "zil"),
    ("zilajā", "zil"),
    ("zilajos", "zil"),
    ("zilajās", "zil"),
    ("krāsns", "krāsn"),
    ("krāšņu", "krāsn"),
    ("zvaigzne", "zvaigzn"),
    ("zvaigžņu", "zvaigzn"),
    ("kāpslis", "kāpsl"),
    ("kāpšļu", "kāpsl"),
    ("zizlis", "zizl"),
    ("zižļu", "zizl"),
    ("vilnis", "viln"),
    ("viļņu", "viln"),
    ("lelle", "lell"),
    ("leļļu", "lell"),
    ("pinne", "pinn"),
    ("piņņu", "pinn"),
    ("rīkste", "rīkst"),
    ("rīkšu", "rīkst"),
    ("usa", "usa"),
    ("60ms", "60ms"),
];

/// Vectors for [`LightAlgorithm::Sorani`], from Lucene's `TestSoraniStem*`.
#[rustfmt::skip]
const SORANI: &[(&str, &str)] = &[
    ("پیاوێک", "پیاو"),
    ("دەرگایەک", "دەرگا"),
    ("کتاویە", "کتاوی"),
    ("دەرگایە", "دەرگا"),
    ("پیاوان", "پیاو"),
    ("دەرگایان", "دەرگا"),
    ("پیاوەکان", "پیاو"),
    ("دەرگاکان", "دەرگا"),
    ("پیاوانە", "پیاو"),
    ("دەرگایانە", "دەرگا"),
    ("هۆتیلی", "هۆتیل"),
    ("هۆتیلێکی", "هۆتیل"),
    ("هۆتیلانی", "هۆتیل"),
    ("دوورەوە", "دوور"),
    ("نیوەشەودا", "نیوەشەو"),
    ("سۆرانا", "سۆران"),
    ("پارەمان", "پارە"),
    ("پارەتان", "پارە"),
    ("پارەیان", "پارە"),
];

/// Vectors for [`LightAlgorithm::Telugu`], from Lucene's `TestTeluguStem*`.
#[rustfmt::skip]
const TELUGU: &[(&str, &str)] = &[
    ("వస్తువులు", "వస్తువు"),
    ("పన్నులు", "పన్ను"),
    ("చేపిస్తున్నది", "చేపిస్తున్న"),
    ("చేపిస్తున్నడు", "చేపిస్తున్న"),
];

/// Vectors for [`LightAlgorithm::Slovak`], from Wikimedia's `SlovakStemmerTest`.
#[rustfmt::skip]
const SLOVAK: &[(&str, &str)] = &[
    ("automatoch", "autom"),
    ("dieťaťom", "dieť"),
    ("stříbrného", "stříbrn"),
    ("horthyovskému", "horthyovsk"),
    ("dojčaťa", "dojč"),
    ("pruskými", "prusk"),
    ("ranených", "ranen"),
    ("orkovi", "ork"),
    ("draľov", "draľ"),
    ("sinigrin", "sinigr"),
    ("venujúcich", "venujúk"),
    ("turečtiny", "tureck"),
    ("političtí", "politick"),
    ("dokážete", "dokáh"),
    ("zapíšte", "zapísk"),
    ("očami", "oča"),
    ("inému", "inému"),
    ("cete", "cet"),
    ("noch", "noch"),
    ("hrách", "hrách"),
    ("maata", "maat"),
    ("vami", "vam"),
    ("nové", "nov"),
    ("ozov", "ozov"),
    ("špin", "špin"),
    ("najmä", "najmä"),
    ("najml", "najml"),
    ("najznámejšími", "známejš"),
    ("najat", "naj"),
    ("bunkových", "bunk"),
    ("vysočinami", "vysok"),
    ("príčinách", "prík"),
    ("najnovšími", "novš"),
    ("najtestciných", "testk"),
    ("əliağa", "əliağ"),
    ("año", "año"),
    ("аблютомания", "аблютомания"),
    ("вищій", "вищій"),
    ("βικιπαίδεια", "βικιπαίδεια"),
    ("ვიკიპედია", "ვიკიპედია"),
    ("위키백과", "위키백과"),
    ("ውክፔዲያ", "ውክፔዲያ"),
    ("ᐅᐃᑭᐱᑎᐊ", "ᐅᐃᑭᐱᑎᐊ"),
];

/// Sorani vectors that Lucene only passes because `SoraniAnalyzer` normalizes first.
///
/// Both spell kaf as `\u{643}` (ARABIC LETTER KAF), whereas [`super::sorani`]'s suffixes use
/// `\u{6a9}` (ARABIC LETTER KEHEH), so the suffix cannot match until
/// `SoraniNormalizationFilter` folds one onto the other. That filter is not ported — see the
/// module documentation — so these are exercised with the single substitution applied, which
/// demonstrates that the gap is normalization and not the stemmer.
#[rustfmt::skip]
const SORANI_REQUIRES_NORMALIZATION: &[(&str, &str)] = &[
    ("پیاوەكە", "پیاو"),
    ("دەرگاكە", "دەرگا"),
];

fn check(algorithm: LightAlgorithm, vectors: &[(&str, &str)]) {
    let mut scratch = Vec::new();
    let mut failures = Vec::new();
    for &(input, expected) in vectors {
        let got = algorithm.stem(input, &mut scratch);
        if got != expected {
            failures.push(format!("{input:?} -> {got:?}, expected {expected:?}"));
        }
    }
    assert!(
        failures.is_empty(),
        "{:?}: {} of {} vectors failed:\n  {}",
        algorithm,
        failures.len(),
        vectors.len(),
        failures.join("\n  ")
    );
}

#[test]
fn test_bulgarian_vectors() {
    check(LightAlgorithm::Bulgarian, BULGARIAN);
}

#[test]
fn test_bengali_vectors() {
    check(LightAlgorithm::Bengali, BENGALI);
}

#[test]
fn test_latvian_vectors() {
    check(LightAlgorithm::Latvian, LATVIAN);
}

#[test]
fn test_slovak_vectors() {
    check(LightAlgorithm::Slovak, SLOVAK);
}

#[test]
fn test_sorani_vectors() {
    check(LightAlgorithm::Sorani, SORANI);
}

#[test]
fn test_telugu_vectors() {
    check(LightAlgorithm::Telugu, TELUGU);
}

/// The stemmer handles [`SORANI_REQUIRES_NORMALIZATION`] correctly once kaf is folded to keheh,
/// and provably does not handle it otherwise. Both halves are asserted so that porting
/// `SoraniNormalizationFilter` later is a visible improvement rather than a silent one.
#[test]
fn test_sorani_normalization_gap() {
    let mut scratch = Vec::new();
    for &(input, expected) in SORANI_REQUIRES_NORMALIZATION {
        let normalized = input.replace('\u{643}', "\u{6a9}");
        assert_eq!(
            LightAlgorithm::Sorani.stem(&normalized, &mut scratch),
            expected.replace('\u{643}', "\u{6a9}"),
            "{input:?} should stem correctly once normalized"
        );
        assert_ne!(
            LightAlgorithm::Sorani.stem(input, &mut scratch),
            expected,
            "{input:?} is expected to need normalization; if this now passes, the module docs \
             and this test are stale"
        );
    }
}

/// Short words must not panic.
///
/// Upstream's Slovak `palatalize` asserts `len > 3` while `remove_case` can reach it with 3, so
/// this walks every short input through every stemmer to pin the lower bounds.
#[test]
fn test_short_words_do_not_panic() {
    let mut scratch = Vec::new();
    for algorithm in ALL {
        for word in [
            "", "a", "ab", "abc", "abcd", "foxes", "es", "om", "naj", "najdd",
        ] {
            let _ = algorithm.stem(word, &mut scratch);
        }
    }
}

/// Lucene's `testEmptyTerm` cases: stemming the empty string must not panic.
#[test]
fn test_empty_term() {
    let mut scratch = Vec::new();
    for algorithm in ALL {
        assert_eq!(algorithm.stem("", &mut scratch), "");
    }
}

/// Every one of these languages is written in a script that takes multiple bytes per character in
/// UTF-8, so a port that indexed by byte rather than by `char` would corrupt output or panic.
#[test]
fn test_multi_byte_safety() {
    let mut scratch = Vec::new();
    for (algorithm, vectors) in [
        (LightAlgorithm::Bulgarian, BULGARIAN),
        (LightAlgorithm::Bengali, BENGALI),
        (LightAlgorithm::Latvian, LATVIAN),
        (LightAlgorithm::Slovak, SLOVAK),
        (LightAlgorithm::Sorani, SORANI),
        (LightAlgorithm::Telugu, TELUGU),
    ] {
        for &(input, _) in vectors {
            let got = algorithm.stem(input, &mut scratch);
            assert!(
                got.chars().count() <= input.chars().count(),
                "{algorithm:?}: {input:?} grew to {got:?}"
            );
            assert!(!got.chars().any(|c| c == char::REPLACEMENT_CHARACTER));
        }
    }
}

const ALL: [LightAlgorithm; 6] = [
    LightAlgorithm::Bengali,
    LightAlgorithm::Bulgarian,
    LightAlgorithm::Latvian,
    LightAlgorithm::Slovak,
    LightAlgorithm::Sorani,
    LightAlgorithm::Telugu,
];
