use super::{Token, TokenFilter, TokenStream};
use std::mem;

/// This class converts alphabetic, numeric, and symbolic Unicode characters
/// which are not in the first 127 ASCII characters (the "Basic Latin" Unicode
/// block) into their ASCII equivalents, if one exists.
#[derive(Clone)]
pub struct AsciiFoldingFilter;

impl<TailTokenStream> TokenFilter<TailTokenStream> for AsciiFoldingFilter
where
    TailTokenStream: TokenStream,
{
    type ResultTokenStream = AsciiFoldingFilterTokenStream<TailTokenStream>;

    fn transform(&self, token_stream: TailTokenStream) -> Self::ResultTokenStream {
        AsciiFoldingFilterTokenStream::wrap(token_stream)
    }
}

pub struct AsciiFoldingFilterTokenStream<TailTokenStream> {
    buffer: String,
    tail: TailTokenStream,
}

impl<TailTokenStream> TokenStream for AsciiFoldingFilterTokenStream<TailTokenStream>
where
    TailTokenStream: TokenStream,
{
    fn advance(&mut self) -> bool {
        if !self.tail.advance() {
            return false;
        }
        if !self.token_mut().text.is_ascii() {
            // ignore its already ascii
            to_ascii(&mut self.tail.token_mut().text, &mut self.buffer);
            mem::swap(&mut self.tail.token_mut().text, &mut self.buffer);
        }
        true
    }

    fn token(&self) -> &Token {
        self.tail.token()
    }

    fn token_mut(&mut self) -> &mut Token {
        self.tail.token_mut()
    }
}

impl<TailTokenStream> AsciiFoldingFilterTokenStream<TailTokenStream>
where
    TailTokenStream: TokenStream,
{
    fn wrap(tail: TailTokenStream) -> AsciiFoldingFilterTokenStream<TailTokenStream> {
        AsciiFoldingFilterTokenStream {
            tail,
            buffer: String::with_capacity(100),
        }
    }
}

// Returns a string that represents the ascii folded version of
// the character. If the `char` does not require ascii folding
// (e.g. simple ASCII chars like `A`) or if the `char`
// does not have a sensible ascii equivalent (e.g.: Kanjis like 馬,
// this function returns `None`.
fn fold_non_ascii_char(c: char) -> Option<&'static str> {
    match c {
        '\u{00C0}' | // À  [LATIN CAPITAL LETTER A WITH GRAVE]
        '\u{00C1}' | // Á  [LATIN CAPITAL LETTER A WITH ACUTE]
        '\u{00C2}' | // Â  [LATIN CAPITAL LETTER A WITH CIRCUMFLEX]
        '\u{00C3}' | // Ã  [LATIN CAPITAL LETTER A WITH TILDE]
        '\u{00C4}' | // Ä  [LATIN CAPITAL LETTER A WITH DIAERESIS]
        '\u{00C5}' | // Å  [LATIN CAPITAL LETTER A WITH RING ABOVE]
        '\u{0100}' | // Ā  [LATIN CAPITAL LETTER A WITH MACRON]
        '\u{0102}' | // Ă  [LATIN CAPITAL LETTER A WITH BREVE]
        '\u{0104}' | // Ą  [LATIN CAPITAL LETTER A WITH OGONEK]
        '\u{018F}' | // Ə  http://en.wikipedia.org/wiki/Schwa  [LATIN CAPITAL LETTER SCHWA]
        '\u{01CD}' | // Ǎ  [LATIN CAPITAL LETTER A WITH CARON]
        '\u{01DE}' | // Ǟ  [LATIN CAPITAL LETTER A WITH DIAERESIS AND MACRON]
        '\u{01E0}' | // Ǡ  [LATIN CAPITAL LETTER A WITH DOT ABOVE AND MACRON]
        '\u{01FA}' | // Ǻ  [LATIN CAPITAL LETTER A WITH RING ABOVE AND ACUTE]
        '\u{0200}' | // Ȁ  [LATIN CAPITAL LETTER A WITH DOUBLE GRAVE]
        '\u{0202}' | // Ȃ  [LATIN CAPITAL LETTER A WITH INVERTED BREVE]
        '\u{0226}' | // Ȧ  [LATIN CAPITAL LETTER A WITH DOT ABOVE]
        '\u{023A}' | // Ⱥ  [LATIN CAPITAL LETTER A WITH STROKE]
        '\u{1D00}' | // ᴀ  [LATIN LETTER SMALL CAPITAL A]
        '\u{1E00}' | // Ḁ  [LATIN CAPITAL LETTER A WITH RING BELOW]
        '\u{1EA0}' | // Ạ  [LATIN CAPITAL LETTER A WITH DOT BELOW]
        '\u{1EA2}' | // Ả  [LATIN CAPITAL LETTER A WITH HOOK ABOVE]
        '\u{1EA4}' | // Ấ  [LATIN CAPITAL LETTER A WITH CIRCUMFLEX AND ACUTE]
        '\u{1EA6}' | // Ầ  [LATIN CAPITAL LETTER A WITH CIRCUMFLEX AND GRAVE]
        '\u{1EA8}' | // Ẩ  [LATIN CAPITAL LETTER A WITH CIRCUMFLEX AND HOOK ABOVE]
        '\u{1EAA}' | // Ẫ  [LATIN CAPITAL LETTER A WITH CIRCUMFLEX AND TILDE]
        '\u{1EAC}' | // Ậ  [LATIN CAPITAL LETTER A WITH CIRCUMFLEX AND DOT BELOW]
        '\u{1EAE}' | // Ắ  [LATIN CAPITAL LETTER A WITH BREVE AND ACUTE]
        '\u{1EB0}' | // Ằ  [LATIN CAPITAL LETTER A WITH BREVE AND GRAVE]
        '\u{1EB2}' | // Ẳ  [LATIN CAPITAL LETTER A WITH BREVE AND HOOK ABOVE]
        '\u{1EB4}' | // Ẵ  [LATIN CAPITAL LETTER A WITH BREVE AND TILDE]
        '\u{1EB6}' | // Ặ  [LATIN CAPITAL LETTER A WITH BREVE AND DOT BELOW]
        '\u{24B6}' | // Ⓐ  [CIRCLED LATIN CAPITAL LETTER A]
        '\u{FF21}'  // Ａ  [FULLWIDTH LATIN CAPITAL LETTER A]
        => Some("A"),
        '\u{00E0}' | // à  [LATIN SMALL LETTER A WITH GRAVE]
        '\u{00E1}' | // á  [LATIN SMALL LETTER A WITH ACUTE]
        '\u{00E2}' | // â  [LATIN SMALL LETTER A WITH CIRCUMFLEX]
        '\u{00E3}' | // ã  [LATIN SMALL LETTER A WITH TILDE]
        '\u{00E4}' | // ä  [LATIN SMALL LETTER A WITH DIAERESIS]
        '\u{00E5}' | // å  [LATIN SMALL LETTER A WITH RING ABOVE]
        '\u{0101}' | // ā  [LATIN SMALL LETTER A WITH MACRON]
        '\u{0103}' | // ă  [LATIN SMALL LETTER A WITH BREVE]
        '\u{0105}' | // ą  [LATIN SMALL LETTER A WITH OGONEK]
        '\u{01CE}' | // ǎ  [LATIN SMALL LETTER A WITH CARON]
        '\u{01DF}' | // ǟ  [LATIN SMALL LETTER A WITH DIAERESIS AND MACRON]
        '\u{01E1}' | // ǡ  [LATIN SMALL LETTER A WITH DOT ABOVE AND MACRON]
        '\u{01FB}' | // ǻ  [LATIN SMALL LETTER A WITH RING ABOVE AND ACUTE]
        '\u{0201}' | // ȁ  [LATIN SMALL LETTER A WITH DOUBLE GRAVE]
        '\u{0203}' | // ȃ  [LATIN SMALL LETTER A WITH INVERTED BREVE]
        '\u{0227}' | // ȧ  [LATIN SMALL LETTER A WITH DOT ABOVE]
        '\u{0250}' | // ɐ  [LATIN SMALL LETTER TURNED A]
        '\u{0259}' | // ə  [LATIN SMALL LETTER SCHWA]
        '\u{025A}' | // ɚ  [LATIN SMALL LETTER SCHWA WITH HOOK]
        '\u{1D8F}' | // ᶏ  [LATIN SMALL LETTER A WITH RETROFLEX HOOK]
        '\u{1D95}' | // ᶕ  [LATIN SMALL LETTER SCHWA WITH RETROFLEX HOOK]
        '\u{1E01}' | // ạ  [LATIN SMALL LETTER A WITH RING BELOW]
        '\u{1E9A}' | // ả  [LATIN SMALL LETTER A WITH RIGHT HALF RING]
        '\u{1EA1}' | // ạ  [LATIN SMALL LETTER A WITH DOT BELOW]
        '\u{1EA3}' | // ả  [LATIN SMALL LETTER A WITH HOOK ABOVE]
        '\u{1EA5}' | // ấ  [LATIN SMALL LETTER A WITH CIRCUMFLEX AND ACUTE]
        '\u{1EA7}' | // ầ  [LATIN SMALL LETTER A WITH CIRCUMFLEX AND GRAVE]
        '\u{1EA9}' | // ẩ  [LATIN SMALL LETTER A WITH CIRCUMFLEX AND HOOK ABOVE]
        '\u{1EAB}' | // ẫ  [LATIN SMALL LETTER A WITH CIRCUMFLEX AND TILDE]
        '\u{1EAD}' | // ậ  [LATIN SMALL LETTER A WITH CIRCUMFLEX AND DOT BELOW]
        '\u{1EAF}' | // ắ  [LATIN SMALL LETTER A WITH BREVE AND ACUTE]
        '\u{1EB1}' | // ằ  [LATIN SMALL LETTER A WITH BREVE AND GRAVE]
        '\u{1EB3}' | // ẳ  [LATIN SMALL LETTER A WITH BREVE AND HOOK ABOVE]
        '\u{1EB5}' | // ẵ  [LATIN SMALL LETTER A WITH BREVE AND TILDE]
        '\u{1EB7}' | // ặ  [LATIN SMALL LETTER A WITH BREVE AND DOT BELOW]
        '\u{2090}' | // ₐ  [LATIN SUBSCRIPT SMALL LETTER A]
        '\u{2094}' | // ₔ  [LATIN SUBSCRIPT SMALL LETTER SCHWA]
        '\u{24D0}' | // ⓐ  [CIRCLED LATIN SMALL LETTER A]
        '\u{2C65}' | // ⱥ  [LATIN SMALL LETTER A WITH STROKE]
        '\u{2C6F}' | // Ɐ  [LATIN CAPITAL LETTER TURNED A]
        '\u{FF41}'  // ａ  [FULLWIDTH LATIN SMALL LETTER A]
        => Some("a"),
        '\u{A732}'  // Ꜳ  [LATIN CAPITAL LETTER AA]
        => Some("AA"),
        '\u{00C6}' | // Æ  [LATIN CAPITAL LETTER AE]
        '\u{01E2}' | // Ǣ  [LATIN CAPITAL LETTER AE WITH MACRON]
        '\u{01FC}' | // Ǽ  [LATIN CAPITAL LETTER AE WITH ACUTE]
        '\u{1D01}' // ᴁ  [LATIN LETTER SMALL CAPITAL AE]
        => Some("AE"),
        '\u{A734}' // Ꜵ  [LATIN CAPITAL LETTER AO]
        => Some("AO"),
        '\u{A736}'  // Ꜷ  [LATIN CAPITAL LETTER AU]
        => Some("AU"),
        '\u{A738}' | // Ꜹ  [LATIN CAPITAL LETTER AV]
        '\u{A73A}'  // Ꜻ  [LATIN CAPITAL LETTER AV WITH HORIZONTAL BAR]
        => Some("AV"),
        '\u{A73C}'  // Ꜽ  [LATIN CAPITAL LETTER AY]
        => Some("AY"),
        '\u{249C}'  // ⒜  [PARENTHESIZED LATIN SMALL LETTER A]
        => Some("(a)"),
        '\u{A733}' // ꜳ  [LATIN SMALL LETTER AA]
        => Some("aa"),
        '\u{00E6}' | // æ  [LATIN SMALL LETTER AE]
        '\u{01E3}' | // ǣ  [LATIN SMALL LETTER AE WITH MACRON]
        '\u{01FD}' | // ǽ  [LATIN SMALL LETTER AE WITH ACUTE]
        '\u{1D02}' // ᴂ  [LATIN SMALL LETTER TURNED AE]
        => Some("ae"),
        '\u{A735}' // ꜵ  [LATIN SMALL LETTER AO]
        => Some("ao"),
        '\u{A737}' // ꜷ  [LATIN SMALL LETTER AU]
        => Some("au"),
        '\u{A739}' | // ꜹ  [LATIN SMALL LETTER AV]
        '\u{A73B}' // ꜻ  [LATIN SMALL LETTER AV WITH HORIZONTAL BAR]
        => Some("av"),
        '\u{A73D}' // ꜽ  [LATIN SMALL LETTER AY]
        => Some("ay"),
        '\u{0181}' | // Ɓ  [LATIN CAPITAL LETTER B WITH HOOK]
        '\u{0182}' | // Ƃ  [LATIN CAPITAL LETTER B WITH TOPBAR]
        '\u{0243}' | // Ƀ  [LATIN CAPITAL LETTER B WITH STROKE]
        '\u{0299}' | // ʙ  [LATIN LETTER SMALL CAPITAL B]
        '\u{1D03}' | // ᴃ  [LATIN LETTER SMALL CAPITAL BARRED B]
        '\u{1E02}' | // Ḃ  [LATIN CAPITAL LETTER B WITH DOT ABOVE]
        '\u{1E04}' | // Ḅ  [LATIN CAPITAL LETTER B WITH DOT BELOW]
        '\u{1E06}' | // Ḇ  [LATIN CAPITAL LETTER B WITH LINE BELOW]
        '\u{24B7}' | // Ⓑ  [CIRCLED LATIN CAPITAL LETTER B]
        '\u{FF22}' // Ｂ  [FULLWIDTH LATIN CAPITAL LETTER B]
        => Some("B"),
        '\u{0180}' | // ƀ  [LATIN SMALL LETTER B WITH STROKE]
        '\u{0183}' | // ƃ  [LATIN SMALL LETTER B WITH TOPBAR]
        '\u{0253}' | // ɓ  [LATIN SMALL LETTER B WITH HOOK]
        '\u{1D6C}' | // ᵬ  [LATIN SMALL LETTER B WITH MIDDLE TILDE]
        '\u{1D80}' | // ᶀ  [LATIN SMALL LETTER B WITH PALATAL HOOK]
        '\u{1E03}' | // ḃ  [LATIN SMALL LETTER B WITH DOT ABOVE]
        '\u{1E05}' | // ḅ  [LATIN SMALL LETTER B WITH DOT BELOW]
        '\u{1E07}' | // ḇ  [LATIN SMALL LETTER B WITH LINE BELOW]
        '\u{24D1}' | // ⓑ  [CIRCLED LATIN SMALL LETTER B]
        '\u{FF42}' // ｂ  [FULLWIDTH LATIN SMALL LETTER B]
        => Some("b"),
        '\u{249D}' // ⒝  [PARENTHESIZED LATIN SMALL LETTER B]
        => Some("(b)"),
        '\u{00C7}' | // Ç  [LATIN CAPITAL LETTER C WITH CEDILLA]
        '\u{0106}' | // Ć  [LATIN CAPITAL LETTER C WITH ACUTE]
        '\u{0108}' | // Ĉ  [LATIN CAPITAL LETTER C WITH CIRCUMFLEX]
        '\u{010A}' | // Ċ  [LATIN CAPITAL LETTER C WITH DOT ABOVE]
        '\u{010C}' | // Č  [LATIN CAPITAL LETTER C WITH CARON]
        '\u{0187}' | // Ƈ  [LATIN CAPITAL LETTER C WITH HOOK]
        '\u{023B}' | // Ȼ  [LATIN CAPITAL LETTER C WITH STROKE]
        '\u{0297}' | // ʗ  [LATIN LETTER STRETCHED C]
        '\u{1D04}' | // ᴄ  [LATIN LETTER SMALL CAPITAL C]
        '\u{1E08}' | // Ḉ  [LATIN CAPITAL LETTER C WITH CEDILLA AND ACUTE]
        '\u{24B8}' | // Ⓒ  [CIRCLED LATIN CAPITAL LETTER C]
        '\u{FF23}' // Ｃ  [FULLWIDTH LATIN CAPITAL LETTER C]
        => Some("C"),
        '\u{00E7}' | // ç  [LATIN SMALL LETTER C WITH CEDILLA]
        '\u{0107}' | // ć  [LATIN SMALL LETTER C WITH ACUTE]
        '\u{0109}' | // ĉ  [LATIN SMALL LETTER C WITH CIRCUMFLEX]
        '\u{010B}' | // ċ  [LATIN SMALL LETTER C WITH DOT ABOVE]
        '\u{010D}' | // č  [LATIN SMALL LETTER C WITH CARON]
        '\u{0188}' | // ƈ  [LATIN SMALL LETTER C WITH HOOK]
        '\u{023C}' | // ȼ  [LATIN SMALL LETTER C WITH STROKE]
        '\u{0255}' | // ɕ  [LATIN SMALL LETTER C WITH CURL]
        '\u{1E09}' | // ḉ  [LATIN SMALL LETTER C WITH CEDILLA AND ACUTE]
        '\u{2184}' | // ↄ  [LATIN SMALL LETTER REVERSED C]
        '\u{24D2}' | // ⓒ  [CIRCLED LATIN SMALL LETTER C]
        '\u{A73E}' | // Ꜿ  [LATIN CAPITAL LETTER REVERSED C WITH DOT]
        '\u{A73F}' | // ꜿ  [LATIN SMALL LETTER REVERSED C WITH DOT]
        '\u{FF43}' // ｃ  [FULLWIDTH LATIN SMALL LETTER C]
        => Some("c"),
        '\u{249E}' // ⒞  [PARENTHESIZED LATIN SMALL LETTER C]
        => Some("(c)"),
        '\u{00D0}' | // Ð  [LATIN CAPITAL LETTER ETH]
        '\u{010E}' | // Ď  [LATIN CAPITAL LETTER D WITH CARON]
        '\u{0110}' | // Đ  [LATIN CAPITAL LETTER D WITH STROKE]
        '\u{0189}' | // Ɖ  [LATIN CAPITAL LETTER AFRICAN D]
        '\u{018A}' | // Ɗ  [LATIN CAPITAL LETTER D WITH HOOK]
        '\u{018B}' | // Ƌ  [LATIN CAPITAL LETTER D WITH TOPBAR]
        '\u{1D05}' | // ᴅ  [LATIN LETTER SMALL CAPITAL D]
        '\u{1D06}' | // ᴆ  [LATIN LETTER SMALL CAPITAL ETH]
        '\u{1E0A}' | // Ḋ  [LATIN CAPITAL LETTER D WITH DOT ABOVE]
        '\u{1E0C}' | // Ḍ  [LATIN CAPITAL LETTER D WITH DOT BELOW]
        '\u{1E0E}' | // Ḏ  [LATIN CAPITAL LETTER D WITH LINE BELOW]
        '\u{1E10}' | // Ḑ  [LATIN CAPITAL LETTER D WITH CEDILLA]
        '\u{1E12}' | // Ḓ  [LATIN CAPITAL LETTER D WITH CIRCUMFLEX BELOW]
        '\u{24B9}' | // Ⓓ  [CIRCLED LATIN CAPITAL LETTER D]
        '\u{A779}' | // Ꝺ  [LATIN CAPITAL LETTER INSULAR D]
        '\u{FF24}' // Ｄ  [FULLWIDTH LATIN CAPITAL LETTER D]
        => Some("D"),
        '\u{00F0}' | // ð  [LATIN SMALL LETTER ETH]
        '\u{010F}' | // ď  [LATIN SMALL LETTER D WITH CARON]
        '\u{0111}' | // đ  [LATIN SMALL LETTER D WITH STROKE]
        '\u{018C}' | // ƌ  [LATIN SMALL LETTER D WITH TOPBAR]
        '\u{0221}' | // ȡ  [LATIN SMALL LETTER D WITH CURL]
        '\u{0256}' | // ɖ  [LATIN SMALL LETTER D WITH TAIL]
        '\u{0257}' | // ɗ  [LATIN SMALL LETTER D WITH HOOK]
        '\u{1D6D}' | // ᵭ  [LATIN SMALL LETTER D WITH MIDDLE TILDE]
        '\u{1D81}' | // ᶁ  [LATIN SMALL LETTER D WITH PALATAL HOOK]
        '\u{1D91}' | // ᶑ  [LATIN SMALL LETTER D WITH HOOK AND TAIL]
        '\u{1E0B}' | // ḋ  [LATIN SMALL LETTER D WITH DOT ABOVE]
        '\u{1E0D}' | // ḍ  [LATIN SMALL LETTER D WITH DOT BELOW]
        '\u{1E0F}' | // ḏ  [LATIN SMALL LETTER D WITH LINE BELOW]
        '\u{1E11}' | // ḑ  [LATIN SMALL LETTER D WITH CEDILLA]
        '\u{1E13}' | // ḓ  [LATIN SMALL LETTER D WITH CIRCUMFLEX BELOW]
        '\u{24D3}' | // ⓓ  [CIRCLED LATIN SMALL LETTER D]
        '\u{A77A}' | // ꝺ  [LATIN SMALL LETTER INSULAR D]
        '\u{FF44}' // ｄ  [FULLWIDTH LATIN SMALL LETTER D]
        => Some("d"),
        '\u{01C4}' | // Ǆ  [LATIN CAPITAL LETTER DZ WITH CARON]
        '\u{01F1}' // Ǳ  [LATIN CAPITAL LETTER DZ]
        => Some("DZ"),
        '\u{01C5}' | // ǅ  [LATIN CAPITAL LETTER D WITH SMALL LETTER Z WITH CARON]
        '\u{01F2}' // ǲ  [LATIN CAPITAL LETTER D WITH SMALL LETTER Z]
        => Some("Dz"),
        '\u{249F}' // ⒟  [PARENTHESIZED LATIN SMALL LETTER D]
        => Some("(d)"),
        '\u{0238}' // ȸ  [LATIN SMALL LETTER DB DIGRAPH]
        => Some("db"),
        '\u{01C6}' | // ǆ  [LATIN SMALL LETTER DZ WITH CARON]
        '\u{01F3}' | // ǳ  [LATIN SMALL LETTER DZ]
        '\u{02A3}' | // ʣ  [LATIN SMALL LETTER DZ DIGRAPH]
        '\u{02A5}' // ʥ  [LATIN SMALL LETTER DZ DIGRAPH WITH CURL]
        => Some("dz"),
        '\u{00C8}' | // È  [LATIN CAPITAL LETTER E WITH GRAVE]
        '\u{00C9}' | // É  [LATIN CAPITAL LETTER E WITH ACUTE]
        '\u{00CA}' | // Ê  [LATIN CAPITAL LETTER E WITH CIRCUMFLEX]
        '\u{00CB}' | // Ë  [LATIN CAPITAL LETTER E WITH DIAERESIS]
        '\u{0112}' | // Ē  [LATIN CAPITAL LETTER E WITH MACRON]
        '\u{0114}' | // Ĕ  [LATIN CAPITAL LETTER E WITH BREVE]
        '\u{0116}' | // Ė  [LATIN CAPITAL LETTER E WITH DOT ABOVE]
        '\u{0118}' | // Ę  [LATIN CAPITAL LETTER E WITH OGONEK]
        '\u{011A}' | // Ě  [LATIN CAPITAL LETTER E WITH CARON]
        '\u{018E}' | // Ǝ  [LATIN CAPITAL LETTER REVERSED E]
        '\u{0190}' | // Ɛ  [LATIN CAPITAL LETTER OPEN E]
        '\u{0204}' | // Ȅ  [LATIN CAPITAL LETTER E WITH DOUBLE GRAVE]
        '\u{0206}' | // Ȇ  [LATIN CAPITAL LETTER E WITH INVERTED BREVE]
        '\u{0228}' | // Ȩ  [LATIN CAPITAL LETTER E WITH CEDILLA]
        '\u{0246}' | // Ɇ  [LATIN CAPITAL LETTER E WITH STROKE]
        '\u{1D07}' | // ᴇ  [LATIN LETTER SMALL CAPITAL E]
        '\u{1E14}' | // Ḕ  [LATIN CAPITAL LETTER E WITH MACRON AND GRAVE]
        '\u{1E16}' | // Ḗ  [LATIN CAPITAL LETTER E WITH MACRON AND ACUTE]
        '\u{1E18}' | // Ḙ  [LATIN CAPITAL LETTER E WITH CIRCUMFLEX BELOW]
        '\u{1E1A}' | // Ḛ  [LATIN CAPITAL LETTER E WITH TILDE BELOW]
        '\u{1E1C}' | // Ḝ  [LATIN CAPITAL LETTER E WITH CEDILLA AND BREVE]
        '\u{1EB8}' | // Ẹ  [LATIN CAPITAL LETTER E WITH DOT BELOW]
        '\u{1EBA}' | // Ẻ  [LATIN CAPITAL LETTER E WITH HOOK ABOVE]
        '\u{1EBC}' | // Ẽ  [LATIN CAPITAL LETTER E WITH TILDE]
        '\u{1EBE}' | // Ế  [LATIN CAPITAL LETTER E WITH CIRCUMFLEX AND ACUTE]
        '\u{1EC0}' | // Ề  [LATIN CAPITAL LETTER E WITH CIRCUMFLEX AND GRAVE]
        '\u{1EC2}' | // Ể  [LATIN CAPITAL LETTER E WITH CIRCUMFLEX AND HOOK ABOVE]
        '\u{1EC4}' | // Ễ  [LATIN CAPITAL LETTER E WITH CIRCUMFLEX AND TILDE]
        '\u{1EC6}' | // Ệ  [LATIN CAPITAL LETTER E WITH CIRCUMFLEX AND DOT BELOW]
        '\u{24BA}' | // Ⓔ  [CIRCLED LATIN CAPITAL LETTER E]
        '\u{2C7B}' | // ⱻ  [LATIN LETTER SMALL CAPITAL TURNED E]
        '\u{FF25}' // Ｅ  [FULLWIDTH LATIN CAPITAL LETTER E]
        => Some("E"),
        '\u{00E8}' | // è  [LATIN SMALL LETTER E WITH GRAVE]
        '\u{00E9}' | // é  [LATIN SMALL LETTER E WITH ACUTE]
        '\u{00EA}' | // ê  [LATIN SMALL LETTER E WITH CIRCUMFLEX]
        '\u{00EB}' | // ë  [LATIN SMALL LETTER E WITH DIAERESIS]
        '\u{0113}' | // ē  [LATIN SMALL LETTER E WITH MACRON]
        '\u{0115}' | // ĕ  [LATIN SMALL LETTER E WITH BREVE]
        '\u{0117}' | // ė  [LATIN SMALL LETTER E WITH DOT ABOVE]
        '\u{0119}' | // ę  [LATIN SMALL LETTER E WITH OGONEK]
        '\u{011B}' | // ě  [LATIN SMALL LETTER E WITH CARON]
        '\u{01DD}' | // ǝ  [LATIN SMALL LETTER TURNED E]
        '\u{0205}' | // ȅ  [LATIN SMALL LETTER E WITH DOUBLE GRAVE]
        '\u{0207}' | // ȇ  [LATIN SMALL LETTER E WITH INVERTED BREVE]
        '\u{0229}' | // ȩ  [LATIN SMALL LETTER E WITH CEDILLA]
        '\u{0247}' | // ɇ  [LATIN SMALL LETTER E WITH STROKE]
        '\u{0258}' | // ɘ  [LATIN SMALL LETTER REVERSED E]
        '\u{025B}' | // ɛ  [LATIN SMALL LETTER OPEN E]
        '\u{025C}' | // ɜ  [LATIN SMALL LETTER REVERSED OPEN E]
        '\u{025D}' | // ɝ  [LATIN SMALL LETTER REVERSED OPEN E WITH HOOK]
        '\u{025E}' | // ɞ  [LATIN SMALL LETTER CLOSED REVERSED OPEN E]
        '\u{029A}' | // ʚ  [LATIN SMALL LETTER CLOSED OPEN E]
        '\u{1D08}' | // ᴈ  [LATIN SMALL LETTER TURNED OPEN E]
        '\u{1D92}' | // ᶒ  [LATIN SMALL LETTER E WITH RETROFLEX HOOK]
        '\u{1D93}' | // ᶓ  [LATIN SMALL LETTER OPEN E WITH RETROFLEX HOOK]
        '\u{1D94}' | // ᶔ  [LATIN SMALL LETTER REVERSED OPEN E WITH RETROFLEX HOOK]
        '\u{1E15}' | // ḕ  [LATIN SMALL LETTER E WITH MACRON AND GRAVE]
        '\u{1E17}' | // ḗ  [LATIN SMALL LETTER E WITH MACRON AND ACUTE]
        '\u{1E19}' | // ḙ  [LATIN SMALL LETTER E WITH CIRCUMFLEX BELOW]
        '\u{1E1B}' | // ḛ  [LATIN SMALL LETTER E WITH TILDE BELOW]
        '\u{1E1D}' | // ḝ  [LATIN SMALL LETTER E WITH CEDILLA AND BREVE]
        '\u{1EB9}' | // ẹ  [LATIN SMALL LETTER E WITH DOT BELOW]
        '\u{1EBB}' | // ẻ  [LATIN SMALL LETTER E WITH HOOK ABOVE]
        '\u{1EBD}' | // ẽ  [LATIN SMALL LETTER E WITH TILDE]
        '\u{1EBF}' | // ế  [LATIN SMALL LETTER E WITH CIRCUMFLEX AND ACUTE]
        '\u{1EC1}' | // ề  [LATIN SMALL LETTER E WITH CIRCUMFLEX AND GRAVE]
        '\u{1EC3}' | // ể  [LATIN SMALL LETTER E WITH CIRCUMFLEX AND HOOK ABOVE]
        '\u{1EC5}' | // ễ  [LATIN SMALL LETTER E WITH CIRCUMFLEX AND TILDE]
        '\u{1EC7}' | // ệ  [LATIN SMALL LETTER E WITH CIRCUMFLEX AND DOT BELOW]
        '\u{2091}' | // ₑ  [LATIN SUBSCRIPT SMALL LETTER E]
        '\u{24D4}' | // ⓔ  [CIRCLED LATIN SMALL LETTER E]
        '\u{2C78}' | // ⱸ  [LATIN SMALL LETTER E WITH NOTCH]
        '\u{FF45}' // ｅ  [FULLWIDTH LATIN SMALL LETTER E]
        => Some("e"),
        '\u{24A0}' // ⒠  [PARENTHESIZED LATIN SMALL LETTER E]
        => Some("(e)"),
        '\u{0191}' | // Ƒ  [LATIN CAPITAL LETTER F WITH HOOK]
        '\u{1E1E}' | // Ḟ  [LATIN CAPITAL LETTER F WITH DOT ABOVE]
        '\u{24BB}' | // Ⓕ  [CIRCLED LATIN CAPITAL LETTER F]
        '\u{A730}' | // ꜰ  [LATIN LETTER SMALL CAPITAL F]
        '\u{A77B}' | // Ꝼ  [LATIN CAPITAL LETTER INSULAR F]
        '\u{A7FB}' | // ꟻ  [LATIN EPIGRAPHIC LETTER REVERSED F]
        '\u{FF26}' // Ｆ  [FULLWIDTH LATIN CAPITAL LETTER F]
        => Some("F"),
        '\u{0192}' | // ƒ  [LATIN SMALL LETTER F WITH HOOK]
        '\u{1D6E}' | // ᵮ  [LATIN SMALL LETTER F WITH MIDDLE TILDE]
        '\u{1D82}' | // ᶂ  [LATIN SMALL LETTER F WITH PALATAL HOOK]
        '\u{1E1F}' | // ḟ  [LATIN SMALL LETTER F WITH DOT ABOVE]
        '\u{1E9B}' | // ẛ  [LATIN SMALL LETTER LONG S WITH DOT ABOVE]
        '\u{24D5}' | // ⓕ  [CIRCLED LATIN SMALL LETTER F]
        '\u{A77C}' | // ꝼ  [LATIN SMALL LETTER INSULAR F]
        '\u{FF46}' // ｆ  [FULLWIDTH LATIN SMALL LETTER F]
        => Some("f"),
        '\u{24A1}' // ⒡  [PARENTHESIZED LATIN SMALL LETTER F]
        => Some("(f)"),
        '\u{FB00}' // ﬀ  [LATIN SMALL LIGATURE FF]
        => Some("ff"),
        '\u{FB03}' // ﬃ  [LATIN SMALL LIGATURE FFI]
        => Some("ffi"),
        '\u{FB04}' // ﬄ  [LATIN SMALL LIGATURE FFL]
        => Some("ffl"),
        '\u{FB01}' // ﬁ  [LATIN SMALL LIGATURE FI]
        => Some("fi"),
        '\u{FB02}' // ﬂ  [LATIN SMALL LIGATURE FL]
        => Some("fl"),
        '\u{011C}' | // Ĝ  [LATIN CAPITAL LETTER G WITH CIRCUMFLEX]
        '\u{011E}' | // Ğ  [LATIN CAPITAL LETTER G WITH BREVE]
        '\u{0120}' | // Ġ  [LATIN CAPITAL LETTER G WITH DOT ABOVE]
        '\u{0122}' | // Ģ  [LATIN CAPITAL LETTER G WITH CEDILLA]
        '\u{0193}' | // Ɠ  [LATIN CAPITAL LETTER G WITH HOOK]
        '\u{01E4}' | // Ǥ  [LATIN CAPITAL LETTER G WITH STROKE]
        '\u{01E5}' | // ǥ  [LATIN SMALL LETTER G WITH STROKE]
        '\u{01E6}' | // Ǧ  [LATIN CAPITAL LETTER G WITH CARON]
        '\u{01E7}' | // ǧ  [LATIN SMALL LETTER G WITH CARON]
        '\u{01F4}' | // Ǵ  [LATIN CAPITAL LETTER G WITH ACUTE]
        '\u{0262}' | // ɢ  [LATIN LETTER SMALL CAPITAL G]
        '\u{029B}' | // ʛ  [LATIN LETTER SMALL CAPITAL G WITH HOOK]
        '\u{1E20}' | // Ḡ  [LATIN CAPITAL LETTER G WITH MACRON]
        '\u{24BC}' | // Ⓖ  [CIRCLED LATIN CAPITAL LETTER G]
        '\u{A77D}' | // Ᵹ  [LATIN CAPITAL LETTER INSULAR G]
        '\u{A77E}' | // Ꝿ  [LATIN CAPITAL LETTER TURNED INSULAR G]
        '\u{FF27}' // Ｇ  [FULLWIDTH LATIN CAPITAL LETTER G]
        => Some("G"),
        '\u{011D}' | // ĝ  [LATIN SMALL LETTER G WITH CIRCUMFLEX]
        '\u{011F}' | // ğ  [LATIN SMALL LETTER G WITH BREVE]
        '\u{0121}' | // ġ  [LATIN SMALL LETTER G WITH DOT ABOVE]
        '\u{0123}' | // ģ  [LATIN SMALL LETTER G WITH CEDILLA]
        '\u{01F5}' | // ǵ  [LATIN SMALL LETTER G WITH ACUTE]
        '\u{0260}' | // ɠ  [LATIN SMALL LETTER G WITH HOOK]
        '\u{0261}' | // ɡ  [LATIN SMALL LETTER SCRIPT G]
        '\u{1D77}' | // ᵷ  [LATIN SMALL LETTER TURNED G]
        '\u{1D79}' | // ᵹ  [LATIN SMALL LETTER INSULAR G]
        '\u{1D83}' | // ᶃ  [LATIN SMALL LETTER G WITH PALATAL HOOK]
        '\u{1E21}' | // ḡ  [LATIN SMALL LETTER G WITH MACRON]
        '\u{24D6}' | // ⓖ  [CIRCLED LATIN SMALL LETTER G]
        '\u{A77F}' | // ꝿ  [LATIN SMALL LETTER TURNED INSULAR G]
        '\u{FF47}' // ｇ  [FULLWIDTH LATIN SMALL LETTER G]
        => Some("g"),
        '\u{24A2}' // ⒢  [PARENTHESIZED LATIN SMALL LETTER G]
        => Some("(g)"),
        '\u{0124}' | // Ĥ  [LATIN CAPITAL LETTER H WITH CIRCUMFLEX]
        '\u{0126}' | // Ħ  [LATIN CAPITAL LETTER H WITH STROKE]
        '\u{021E}' | // Ȟ  [LATIN CAPITAL LETTER H WITH CARON]
        '\u{029C}' | // ʜ  [LATIN LETTER SMALL CAPITAL H]
        '\u{1E22}' | // Ḣ  [LATIN CAPITAL LETTER H WITH DOT ABOVE]
        '\u{1E24}' | // Ḥ  [LATIN CAPITAL LETTER H WITH DOT BELOW]
        '\u{1E26}' | // Ḧ  [LATIN CAPITAL LETTER H WITH DIAERESIS]
        '\u{1E28}' | // Ḩ  [LATIN CAPITAL LETTER H WITH CEDILLA]
        '\u{1E2A}' | // Ḫ  [LATIN CAPITAL LETTER H WITH BREVE BELOW]
        '\u{24BD}' | // Ⓗ  [CIRCLED LATIN CAPITAL LETTER H]
        '\u{2C67}' | // Ⱨ  [LATIN CAPITAL LETTER H WITH DESCENDER]
        '\u{2C75}' | // Ⱶ  [LATIN CAPITAL LETTER HALF H]
        '\u{FF28}' // Ｈ  [FULLWIDTH LATIN CAPITAL LETTER H]
        => Some("H"),
        '\u{0125}' | // ĥ  [LATIN SMALL LETTER H WITH CIRCUMFLEX]
        '\u{0127}' | // ħ  [LATIN SMALL LETTER H WITH STROKE]
        '\u{021F}' | // ȟ  [LATIN SMALL LETTER H WITH CARON]
        '\u{0265}' | // ɥ  [LATIN SMALL LETTER TURNED H]
        '\u{0266}' | // ɦ  [LATIN SMALL LETTER H WITH HOOK]
        '\u{02AE}' | // ʮ  [LATIN SMALL LETTER TURNED H WITH FISHHOOK]
        '\u{02AF}' | // ʯ  [LATIN SMALL LETTER TURNED H WITH FISHHOOK AND TAIL]
        '\u{1E23}' | // ḣ  [LATIN SMALL LETTER H WITH DOT ABOVE]
        '\u{1E25}' | // ḥ  [LATIN SMALL LETTER H WITH DOT BELOW]
        '\u{1E27}' | // ḧ  [LATIN SMALL LETTER H WITH DIAERESIS]
        '\u{1E29}' | // ḩ  [LATIN SMALL LETTER H WITH CEDILLA]
        '\u{1E2B}' | // ḫ  [LATIN SMALL LETTER H WITH BREVE BELOW]
        '\u{1E96}' | // ẖ  [LATIN SMALL LETTER H WITH LINE BELOW]
        '\u{24D7}' | // ⓗ  [CIRCLED LATIN SMALL LETTER H]
        '\u{2C68}' | // ⱨ  [LATIN SMALL LETTER H WITH DESCENDER]
        '\u{2C76}' | // ⱶ  [LATIN SMALL LETTER HALF H]
        '\u{FF48}' // ｈ  [FULLWIDTH LATIN SMALL LETTER H]
        => Some("h"),
        '\u{01F6}' // Ƕ  http://en.wikipedia.org/wiki/Hwair  [LATIN CAPITAL LETTER HWAIR]
        => Some("HV"),
        '\u{24A3}' // ⒣  [PARENTHESIZED LATIN SMALL LETTER H]
        => Some("(h)"),
        '\u{0195}' // ƕ  [LATIN SMALL LETTER HV]
        => Some("hv"),
        '\u{00CC}' | // Ì  [LATIN CAPITAL LETTER I WITH GRAVE]
        '\u{00CD}' | // Í  [LATIN CAPITAL LETTER I WITH ACUTE]
        '\u{00CE}' | // Î  [LATIN CAPITAL LETTER I WITH CIRCUMFLEX]
        '\u{00CF}' | // Ï  [LATIN CAPITAL LETTER I WITH DIAERESIS]
        '\u{0128}' | // Ĩ  [LATIN CAPITAL LETTER I WITH TILDE]
        '\u{012A}' | // Ī  [LATIN CAPITAL LETTER I WITH MACRON]
        '\u{012C}' | // Ĭ  [LATIN CAPITAL LETTER I WITH BREVE]
        '\u{012E}' | // Į  [LATIN CAPITAL LETTER I WITH OGONEK]
        '\u{0130}' | // İ  [LATIN CAPITAL LETTER I WITH DOT ABOVE]
        '\u{0196}' | // Ɩ  [LATIN CAPITAL LETTER IOTA]
        '\u{0197}' | // Ɨ  [LATIN CAPITAL LETTER I WITH STROKE]
        '\u{01CF}' | // Ǐ  [LATIN CAPITAL LETTER I WITH CARON]
        '\u{0208}' | // Ȉ  [LATIN CAPITAL LETTER I WITH DOUBLE GRAVE]
        '\u{020A}' | // Ȋ  [LATIN CAPITAL LETTER I WITH INVERTED BREVE]
        '\u{026A}' | // ɪ  [LATIN LETTER SMALL CAPITAL I]
        '\u{1D7B}' | // ᵻ  [LATIN SMALL CAPITAL LETTER I WITH STROKE]
        '\u{1E2C}' | // Ḭ  [LATIN CAPITAL LETTER I WITH TILDE BELOW]
        '\u{1E2E}' | // Ḯ  [LATIN CAPITAL LETTER I WITH DIAERESIS AND ACUTE]
        '\u{1EC8}' | // Ỉ  [LATIN CAPITAL LETTER I WITH HOOK ABOVE]
        '\u{1ECA}' | // Ị  [LATIN CAPITAL LETTER I WITH DOT BELOW]
        '\u{24BE}' | // Ⓘ  [CIRCLED LATIN CAPITAL LETTER I]
        '\u{A7FE}' | // ꟾ  [LATIN EPIGRAPHIC LETTER I LONGA]
        '\u{FF29}' // Ｉ  [FULLWIDTH LATIN CAPITAL LETTER I]
        => Some("I"),
        '\u{00EC}' | // ì  [LATIN SMALL LETTER I WITH GRAVE]
        '\u{00ED}' | // í  [LATIN SMALL LETTER I WITH ACUTE]
        '\u{00EE}' | // î  [LATIN SMALL LETTER I WITH CIRCUMFLEX]
        '\u{00EF}' | // ï  [LATIN SMALL LETTER I WITH DIAERESIS]
        '\u{0129}' | // ĩ  [LATIN SMALL LETTER I WITH TILDE]
        '\u{012B}' | // ī  [LATIN SMALL LETTER I WITH MACRON]
        '\u{012D}' | // ĭ  [LATIN SMALL LETTER I WITH BREVE]
        '\u{012F}' | // į  [LATIN SMALL LETTER I WITH OGONEK]
        '\u{0131}' | // ı  [LATIN SMALL LETTER DOTLESS I]
        '\u{01D0}' | // ǐ  [LATIN SMALL LETTER I WITH CARON]
        '\u{0209}' | // ȉ  [LATIN SMALL LETTER I WITH DOUBLE GRAVE]
        '\u{020B}' | // ȋ  [LATIN SMALL LETTER I WITH INVERTED BREVE]
        '\u{0268}' | // ɨ  [LATIN SMALL LETTER I WITH STROKE]
        '\u{1D09}' | // ᴉ  [LATIN SMALL LETTER TURNED I]
        '\u{1D62}' | // ᵢ  [LATIN SUBSCRIPT SMALL LETTER I]
        '\u{1D7C}' | // ᵼ  [LATIN SMALL LETTER IOTA WITH STROKE]
        '\u{1D96}' | // ᶖ  [LATIN SMALL LETTER I WITH RETROFLEX HOOK]
        '\u{1E2D}' | // ḭ  [LATIN SMALL LETTER I WITH TILDE BELOW]
        '\u{1E2F}' | // ḯ  [LATIN SMALL LETTER I WITH DIAERESIS AND ACUTE]
        '\u{1EC9}' | // ỉ  [LATIN SMALL LETTER I WITH HOOK ABOVE]
        '\u{1ECB}' | // ị  [LATIN SMALL LETTER I WITH DOT BELOW]
        '\u{2071}' | // ⁱ  [SUPERSCRIPT LATIN SMALL LETTER I]
        '\u{24D8}' | // ⓘ  [CIRCLED LATIN SMALL LETTER I]
        '\u{FF49}' // ｉ  [FULLWIDTH LATIN SMALL LETTER I]
        => Some("i"),
        '\u{0132}' // Ĳ  [LATIN CAPITAL LIGATURE IJ]
        => Some("IJ"),
        '\u{24A4}' // ⒤  [PARENTHESIZED LATIN SMALL LETTER I]
        => Some("(i)"),
        '\u{0133}' // ĳ  [LATIN SMALL LIGATURE IJ]
        => Some("ij"),
        '\u{0134}' | // Ĵ  [LATIN CAPITAL LETTER J WITH CIRCUMFLEX]
        '\u{0248}' | // Ɉ  [LATIN CAPITAL LETTER J WITH STROKE]
        '\u{1D0A}' | // ᴊ  [LATIN LETTER SMALL CAPITAL J]
        '\u{24BF}' | // Ⓙ  [CIRCLED LATIN CAPITAL LETTER J]
        '\u{FF2A}' // Ｊ  [FULLWIDTH LATIN CAPITAL LETTER J]
        => Some("J"),
        '\u{0135}' | // ĵ  [LATIN SMALL LETTER J WITH CIRCUMFLEX]
        '\u{01F0}' | // ǰ  [LATIN SMALL LETTER J WITH CARON]
        '\u{0237}' | // ȷ  [LATIN SMALL LETTER DOTLESS J]
        '\u{0249}' | // ɉ  [LATIN SMALL LETTER J WITH STROKE]
        '\u{025F}' | // ɟ  [LATIN SMALL LETTER DOTLESS J WITH STROKE]
        '\u{0284}' | // ʄ  [LATIN SMALL LETTER DOTLESS J WITH STROKE AND HOOK]
        '\u{029D}' | // ʝ  [LATIN SMALL LETTER J WITH CROSSED-TAIL]
        '\u{24D9}' | // ⓙ  [CIRCLED LATIN SMALL LETTER J]
        '\u{2C7C}' | // ⱼ  [LATIN SUBSCRIPT SMALL LETTER J]
        '\u{FF4A}' // ｊ  [FULLWIDTH LATIN SMALL LETTER J]
        => Some("j"),
        '\u{24A5}' // ⒥  [PARENTHESIZED LATIN SMALL LETTER J]
        => Some("(j)"),
        '\u{0136}' | // Ķ  [LATIN CAPITAL LETTER K WITH CEDILLA]
        '\u{0198}' | // Ƙ  [LATIN CAPITAL LETTER K WITH HOOK]
        '\u{01E8}' | // Ǩ  [LATIN CAPITAL LETTER K WITH CARON]
        '\u{1D0B}' | // ᴋ  [LATIN LETTER SMALL CAPITAL K]
        '\u{1E30}' | // Ḱ  [LATIN CAPITAL LETTER K WITH ACUTE]
        '\u{1E32}' | // Ḳ  [LATIN CAPITAL LETTER K WITH DOT BELOW]
        '\u{1E34}' | // Ḵ  [LATIN CAPITAL LETTER K WITH LINE BELOW]
        '\u{24C0}' | // Ⓚ  [CIRCLED LATIN CAPITAL LETTER K]
        '\u{2C69}' | // Ⱪ  [LATIN CAPITAL LETTER K WITH DESCENDER]
        '\u{A740}' | // Ꝁ  [LATIN CAPITAL LETTER K WITH STROKE]
        '\u{A742}' | // Ꝃ  [LATIN CAPITAL LETTER K WITH DIAGONAL STROKE]
        '\u{A744}' | // Ꝅ  [LATIN CAPITAL LETTER K WITH STROKE AND DIAGONAL STROKE]
        '\u{FF2B}' // Ｋ  [FULLWIDTH LATIN CAPITAL LETTER K]
        => Some("K"),
        '\u{0137}' | // ķ  [LATIN SMALL LETTER K WITH CEDILLA]
        '\u{0199}' | // ƙ  [LATIN SMALL LETTER K WITH HOOK]
        '\u{01E9}' | // ǩ  [LATIN SMALL LETTER K WITH CARON]
        '\u{029E}' | // ʞ  [LATIN SMALL LETTER TURNED K]
        '\u{1D84}' | // ᶄ  [LATIN SMALL LETTER K WITH PALATAL HOOK]
        '\u{1E31}' | // ḱ  [LATIN SMALL LETTER K WITH ACUTE]
        '\u{1E33}' | // ḳ  [LATIN SMALL LETTER K WITH DOT BELOW]
        '\u{1E35}' | // ḵ  [LATIN SMALL LETTER K WITH LINE BELOW]
        '\u{24DA}' | // ⓚ  [CIRCLED LATIN SMALL LETTER K]
        '\u{2C6A}' | // ⱪ  [LATIN SMALL LETTER K WITH DESCENDER]
        '\u{A741}' | // ꝁ  [LATIN SMALL LETTER K WITH STROKE]
        '\u{A743}' | // ꝃ  [LATIN SMALL LETTER K WITH DIAGONAL STROKE]
        '\u{A745}' | // ꝅ  [LATIN SMALL LETTER K WITH STROKE AND DIAGONAL STROKE]
        '\u{FF4B}' // ｋ  [FULLWIDTH LATIN SMALL LETTER K]
        => Some("k"),
        '\u{24A6}' // ⒦  [PARENTHESIZED LATIN SMALL LETTER K]
        => Some("(k)"),
        '\u{0139}' | // Ĺ  [LATIN CAPITAL LETTER L WITH ACUTE]
        '\u{013B}' | // Ļ  [LATIN CAPITAL LETTER L WITH CEDILLA]
        '\u{013D}' | // Ľ  [LATIN CAPITAL LETTER L WITH CARON]
        '\u{013F}' | // Ŀ  [LATIN CAPITAL LETTER L WITH MIDDLE DOT]
        '\u{0141}' | // Ł  [LATIN CAPITAL LETTER L WITH STROKE]
        '\u{023D}' | // Ƚ  [LATIN CAPITAL LETTER L WITH BAR]
        '\u{029F}' | // ʟ  [LATIN LETTER SMALL CAPITAL L]
        '\u{1D0C}' | // ᴌ  [LATIN LETTER SMALL CAPITAL L WITH STROKE]
        '\u{1E36}' | // Ḷ  [LATIN CAPITAL LETTER L WITH DOT BELOW]
        '\u{1E38}' | // Ḹ  [LATIN CAPITAL LETTER L WITH DOT BELOW AND MACRON]
        '\u{1E3A}' | // Ḻ  [LATIN CAPITAL LETTER L WITH LINE BELOW]
        '\u{1E3C}' | // Ḽ  [LATIN CAPITAL LETTER L WITH CIRCUMFLEX BELOW]
        '\u{24C1}' | // Ⓛ  [CIRCLED LATIN CAPITAL LETTER L]
        '\u{2C60}' | // Ⱡ  [LATIN CAPITAL LETTER L WITH DOUBLE BAR]
        '\u{2C62}' | // Ɫ  [LATIN CAPITAL LETTER L WITH MIDDLE TILDE]
        '\u{A746}' | // Ꝇ  [LATIN CAPITAL LETTER BROKEN L]
        '\u{A748}' | // Ꝉ  [LATIN CAPITAL LETTER L WITH HIGH STROKE]
        '\u{A780}' | // Ꞁ  [LATIN CAPITAL LETTER TURNED L]
        '\u{FF2C}' // Ｌ  [FULLWIDTH LATIN CAPITAL LETTER L]
        => Some("L"),
        '\u{013A}' | // ĺ  [LATIN SMALL LETTER L WITH ACUTE]
        '\u{013C}' | // ļ  [LATIN SMALL LETTER L WITH CEDILLA]
        '\u{013E}' | // ľ  [LATIN SMALL LETTER L WITH CARON]
        '\u{0140}' | // ŀ  [LATIN SMALL LETTER L WITH MIDDLE DOT]
        '\u{0142}' | // ł  [LATIN SMALL LETTER L WITH STROKE]
        '\u{019A}' | // ƚ  [LATIN SMALL LETTER L WITH BAR]
        '\u{0234}' | // ȴ  [LATIN SMALL LETTER L WITH CURL]
        '\u{026B}' | // ɫ  [LATIN SMALL LETTER L WITH MIDDLE TILDE]
        '\u{026C}' | // ɬ  [LATIN SMALL LETTER L WITH BELT]
        '\u{026D}' | // ɭ  [LATIN SMALL LETTER L WITH RETROFLEX HOOK]
        '\u{1D85}' | // ᶅ  [LATIN SMALL LETTER L WITH PALATAL HOOK]
        '\u{1E37}' | // ḷ  [LATIN SMALL LETTER L WITH DOT BELOW]
        '\u{1E39}' | // ḹ  [LATIN SMALL LETTER L WITH DOT BELOW AND MACRON]
        '\u{1E3B}' | // ḻ  [LATIN SMALL LETTER L WITH LINE BELOW]
        '\u{1E3D}' | // ḽ  [LATIN SMALL LETTER L WITH CIRCUMFLEX BELOW]
        '\u{24DB}' | // ⓛ  [CIRCLED LATIN SMALL LETTER L]
        '\u{2C61}' | // ⱡ  [LATIN SMALL LETTER L WITH DOUBLE BAR]
        '\u{A747}' | // ꝇ  [LATIN SMALL LETTER BROKEN L]
        '\u{A749}' | // ꝉ  [LATIN SMALL LETTER L WITH HIGH STROKE]
        '\u{A781}' | // ꞁ  [LATIN SMALL LETTER TURNED L]
        '\u{FF4C}' // ｌ  [FULLWIDTH LATIN SMALL LETTER L]
        => Some("l"),
        '\u{01C7}' // Ǉ  [LATIN CAPITAL LETTER LJ]
        => Some("LJ"),
        '\u{1EFA}' // Ỻ  [LATIN CAPITAL LETTER MIDDLE-WELSH LL]
        => Some("LL"),
        '\u{01C8}' // ǈ  [LATIN CAPITAL LETTER L WITH SMALL LETTER J]
        => Some("Lj"),
        '\u{24A7}' // ⒧  [PARENTHESIZED LATIN SMALL LETTER L]
        => Some("(l)"),
        '\u{01C9}' // ǉ  [LATIN SMALL LETTER LJ]
        => Some("lj"),
        '\u{1EFB}' // ỻ  [LATIN SMALL LETTER MIDDLE-WELSH LL]
        => Some("ll"),
        '\u{02AA}' // ʪ  [LATIN SMALL LETTER LS DIGRAPH]
        => Some("ls"),
        '\u{02AB}' // ʫ  [LATIN SMALL LETTER LZ DIGRAPH]
        => Some("lz"),
        '\u{019C}' | // Ɯ  [LATIN CAPITAL LETTER TURNED M]
        '\u{1D0D}' | // ᴍ  [LATIN LETTER SMALL CAPITAL M]
        '\u{1E3E}' | // Ḿ  [LATIN CAPITAL LETTER M WITH ACUTE]
        '\u{1E40}' | // Ṁ  [LATIN CAPITAL LETTER M WITH DOT ABOVE]
        '\u{1E42}' | // Ṃ  [LATIN CAPITAL LETTER M WITH DOT BELOW]
        '\u{24C2}' | // Ⓜ  [CIRCLED LATIN CAPITAL LETTER M]
        '\u{2C6E}' | // Ɱ  [LATIN CAPITAL LETTER M WITH HOOK]
        '\u{A7FD}' | // ꟽ  [LATIN EPIGRAPHIC LETTER INVERTED M]
        '\u{A7FF}' | // ꟿ  [LATIN EPIGRAPHIC LETTER ARCHAIC M]
        '\u{FF2D}' // Ｍ  [FULLWIDTH LATIN CAPITAL LETTER M]
        => Some("M"),
        '\u{026F}' | // ɯ  [LATIN SMALL LETTER TURNED M]
        '\u{0270}' | // ɰ  [LATIN SMALL LETTER TURNED M WITH LONG LEG]
        '\u{0271}' | // ɱ  [LATIN SMALL LETTER M WITH HOOK]
        '\u{1D6F}' | // ᵯ  [LATIN SMALL LETTER M WITH MIDDLE TILDE]
        '\u{1D86}' | // ᶆ  [LATIN SMALL LETTER M WITH PALATAL HOOK]
        '\u{1E3F}' | // ḿ  [LATIN SMALL LETTER M WITH ACUTE]
        '\u{1E41}' | // ṁ  [LATIN SMALL LETTER M WITH DOT ABOVE]
        '\u{1E43}' | // ṃ  [LATIN SMALL LETTER M WITH DOT BELOW]
        '\u{24DC}' | // ⓜ  [CIRCLED LATIN SMALL LETTER M]
        '\u{FF4D}' // ｍ  [FULLWIDTH LATIN SMALL LETTER M]
        => Some("m"),
        '\u{24A8}' // ⒨  [PARENTHESIZED LATIN SMALL LETTER M]
        => Some("(m)"),
        '\u{00D1}' | // Ñ  [LATIN CAPITAL LETTER N WITH TILDE]
        '\u{0143}' | // Ń  [LATIN CAPITAL LETTER N WITH ACUTE]
        '\u{0145}' | // Ņ  [LATIN CAPITAL LETTER N WITH CEDILLA]
        '\u{0147}' | // Ň  [LATIN CAPITAL LETTER N WITH CARON]
        '\u{014A}' | // Ŋ  http://en.wikipedia.org/wiki/Eng_(letter)  [LATIN CAPITAL LETTER ENG]
        '\u{019D}' | // Ɲ  [LATIN CAPITAL LETTER N WITH LEFT HOOK]
        '\u{01F8}' | // Ǹ  [LATIN CAPITAL LETTER N WITH GRAVE]
        '\u{0220}' | // Ƞ  [LATIN CAPITAL LETTER N WITH LONG RIGHT LEG]
        '\u{0274}' | // ɴ  [LATIN LETTER SMALL CAPITAL N]
        '\u{1D0E}' | // ᴎ  [LATIN LETTER SMALL CAPITAL REVERSED N]
        '\u{1E44}' | // Ṅ  [LATIN CAPITAL LETTER N WITH DOT ABOVE]
        '\u{1E46}' | // Ṇ  [LATIN CAPITAL LETTER N WITH DOT BELOW]
        '\u{1E48}' | // Ṉ  [LATIN CAPITAL LETTER N WITH LINE BELOW]
        '\u{1E4A}' | // Ṋ  [LATIN CAPITAL LETTER N WITH CIRCUMFLEX BELOW]
        '\u{24C3}' | // Ⓝ  [CIRCLED LATIN CAPITAL LETTER N]
        '\u{FF2E}' // Ｎ  [FULLWIDTH LATIN CAPITAL LETTER N]
        => Some("N"),
        '\u{00F1}' | // ñ  [LATIN SMALL LETTER N WITH TILDE]
        '\u{0144}' | // ń  [LATIN SMALL LETTER N WITH ACUTE]
        '\u{0146}' | // ņ  [LATIN SMALL LETTER N WITH CEDILLA]
        '\u{0148}' | // ň  [LATIN SMALL LETTER N WITH CARON]
        '\u{0149}' | // ŉ  [LATIN SMALL LETTER N PRECEDED BY APOSTROPHE]
        '\u{014B}' | // ŋ  http://en.wikipedia.org/wiki/Eng_(letter)  [LATIN SMALL LETTER ENG]
        '\u{019E}' | // ƞ  [LATIN SMALL LETTER N WITH LONG RIGHT LEG]
        '\u{01F9}' | // ǹ  [LATIN SMALL LETTER N WITH GRAVE]
        '\u{0235}' | // ȵ  [LATIN SMALL LETTER N WITH CURL]
        '\u{0272}' | // ɲ  [LATIN SMALL LETTER N WITH LEFT HOOK]
        '\u{0273}' | // ɳ  [LATIN SMALL LETTER N WITH RETROFLEX HOOK]
        '\u{1D70}' | // ᵰ  [LATIN SMALL LETTER N WITH MIDDLE TILDE]
        '\u{1D87}' | // ᶇ  [LATIN SMALL LETTER N WITH PALATAL HOOK]
        '\u{1E45}' | // ṅ  [LATIN SMALL LETTER N WITH DOT ABOVE]
        '\u{1E47}' | // ṇ  [LATIN SMALL LETTER N WITH DOT BELOW]
        '\u{1E49}' | // ṉ  [LATIN SMALL LETTER N WITH LINE BELOW]
        '\u{1E4B}' | // ṋ  [LATIN SMALL LETTER N WITH CIRCUMFLEX BELOW]
        '\u{207F}' | // ⁿ  [SUPERSCRIPT LATIN SMALL LETTER N]
        '\u{24DD}' | // ⓝ  [CIRCLED LATIN SMALL LETTER N]
        '\u{FF4E}' // ｎ  [FULLWIDTH LATIN SMALL LETTER N]
        => Some("n"),
        '\u{01CA}' // Ǌ  [LATIN CAPITAL LETTER NJ]
        => Some("NJ"),
        '\u{01CB}' // ǋ  [LATIN CAPITAL LETTER N WITH SMALL LETTER J]
        => Some("Nj"),
        '\u{24A9}' // ⒩  [PARENTHESIZED LATIN SMALL LETTER N]
        => Some("(n)"),
        '\u{01CC}' // ǌ  [LATIN SMALL LETTER NJ]
        => Some("nj"),
        '\u{00D2}' | // Ò  [LATIN CAPITAL LETTER O WITH GRAVE]
        '\u{00D3}' | // Ó  [LATIN CAPITAL LETTER O WITH ACUTE]
        '\u{00D4}' | // Ô  [LATIN CAPITAL LETTER O WITH CIRCUMFLEX]
        '\u{00D5}' | // Õ  [LATIN CAPITAL LETTER O WITH TILDE]
        '\u{00D6}' | // Ö  [LATIN CAPITAL LETTER O WITH DIAERESIS]
        '\u{00D8}' | // Ø  [LATIN CAPITAL LETTER O WITH STROKE]
        '\u{014C}' | // Ō  [LATIN CAPITAL LETTER O WITH MACRON]
        '\u{014E}' | // Ŏ  [LATIN CAPITAL LETTER O WITH BREVE]
        '\u{0150}' | // Ő  [LATIN CAPITAL LETTER O WITH DOUBLE ACUTE]
        '\u{0186}' | // Ɔ  [LATIN CAPITAL LETTER OPEN O]
        '\u{019F}' | // Ɵ  [LATIN CAPITAL LETTER O WITH MIDDLE TILDE]
        '\u{01A0}' | // Ơ  [LATIN CAPITAL LETTER O WITH HORN]
        '\u{01D1}' | // Ǒ  [LATIN CAPITAL LETTER O WITH CARON]
        '\u{01EA}' | // Ǫ  [LATIN CAPITAL LETTER O WITH OGONEK]
        '\u{01EC}' | // Ǭ  [LATIN CAPITAL LETTER O WITH OGONEK AND MACRON]
        '\u{01FE}' | // Ǿ  [LATIN CAPITAL LETTER O WITH STROKE AND ACUTE]
        '\u{020C}' | // Ȍ  [LATIN CAPITAL LETTER O WITH DOUBLE GRAVE]
        '\u{020E}' | // Ȏ  [LATIN CAPITAL LETTER O WITH INVERTED BREVE]
        '\u{022A}' | // Ȫ  [LATIN CAPITAL LETTER O WITH DIAERESIS AND MACRON]
        '\u{022C}' | // Ȭ  [LATIN CAPITAL LETTER O WITH TILDE AND MACRON]
        '\u{022E}' | // Ȯ  [LATIN CAPITAL LETTER O WITH DOT ABOVE]
        '\u{0230}' | // Ȱ  [LATIN CAPITAL LETTER O WITH DOT ABOVE AND MACRON]
        '\u{1D0F}' | // ᴏ  [LATIN LETTER SMALL CAPITAL O]
        '\u{1D10}' | // ᴐ  [LATIN LETTER SMALL CAPITAL OPEN O]
        '\u{1E4C}' | // Ṍ  [LATIN CAPITAL LETTER O WITH TILDE AND ACUTE]
        '\u{1E4E}' | // Ṏ  [LATIN CAPITAL LETTER O WITH TILDE AND DIAERESIS]
        '\u{1E50}' | // Ṑ  [LATIN CAPITAL LETTER O WITH MACRON AND GRAVE]
        '\u{1E52}' | // Ṓ  [LATIN CAPITAL LETTER O WITH MACRON AND ACUTE]
        '\u{1ECC}' | // Ọ  [LATIN CAPITAL LETTER O WITH DOT BELOW]
        '\u{1ECE}' | // Ỏ  [LATIN CAPITAL LETTER O WITH HOOK ABOVE]
        '\u{1ED0}' | // Ố  [LATIN CAPITAL LETTER O WITH CIRCUMFLEX AND ACUTE]
        '\u{1ED2}' | // Ồ  [LATIN CAPITAL LETTER O WITH CIRCUMFLEX AND GRAVE]
        '\u{1ED4}' | // Ổ  [LATIN CAPITAL LETTER O WITH CIRCUMFLEX AND HOOK ABOVE]
        '\u{1ED6}' | // Ỗ  [LATIN CAPITAL LETTER O WITH CIRCUMFLEX AND TILDE]
        '\u{1ED8}' | // Ộ  [LATIN CAPITAL LETTER O WITH CIRCUMFLEX AND DOT BELOW]
        '\u{1EDA}' | // Ớ  [LATIN CAPITAL LETTER O WITH HORN AND ACUTE]
        '\u{1EDC}' | // Ờ  [LATIN CAPITAL LETTER O WITH HORN AND GRAVE]
        '\u{1EDE}' | // Ở  [LATIN CAPITAL LETTER O WITH HORN AND HOOK ABOVE]
        '\u{1EE0}' | // Ỡ  [LATIN CAPITAL LETTER O WITH HORN AND TILDE]
        '\u{1EE2}' | // Ợ  [LATIN CAPITAL LETTER O WITH HORN AND DOT BELOW]
        '\u{24C4}' | // Ⓞ  [CIRCLED LATIN CAPITAL LETTER O]
        '\u{A74A}' | // Ꝋ  [LATIN CAPITAL LETTER O WITH LONG STROKE OVERLAY]
        '\u{A74C}' | // Ꝍ  [LATIN CAPITAL LETTER O WITH LOOP]
        '\u{FF2F}' // Ｏ  [FULLWIDTH LATIN CAPITAL LETTER O]
        => Some("O"),
        '\u{00F2}' | // ò  [LATIN SMALL LETTER O WITH GRAVE]
        '\u{00F3}' | // ó  [LATIN SMALL LETTER O WITH ACUTE]
        '\u{00F4}' | // ô  [LATIN SMALL LETTER O WITH CIRCUMFLEX]
        '\u{00F5}' | // õ  [LATIN SMALL LETTER O WITH TILDE]
        '\u{00F6}' | // ö  [LATIN SMALL LETTER O WITH DIAERESIS]
        '\u{00F8}' | // ø  [LATIN SMALL LETTER O WITH STROKE]
        '\u{014D}' | // ō  [LATIN SMALL LETTER O WITH MACRON]
        '\u{014F}' | // ŏ  [LATIN SMALL LETTER O WITH BREVE]
        '\u{0151}' | // ő  [LATIN SMALL LETTER O WITH DOUBLE ACUTE]
        '\u{01A1}' | // ơ  [LATIN SMALL LETTER O WITH HORN]
        '\u{01D2}' | // ǒ  [LATIN SMALL LETTER O WITH CARON]
        '\u{01EB}' | // ǫ  [LATIN SMALL LETTER O WITH OGONEK]
        '\u{01ED}' | // ǭ  [LATIN SMALL LETTER O WITH OGONEK AND MACRON]
        '\u{01FF}' | // ǿ  [LATIN SMALL LETTER O WITH STROKE AND ACUTE]
        '\u{020D}' | // ȍ  [LATIN SMALL LETTER O WITH DOUBLE GRAVE]
        '\u{020F}' | // ȏ  [LATIN SMALL LETTER O WITH INVERTED BREVE]
        '\u{022B}' | // ȫ  [LATIN SMALL LETTER O WITH DIAERESIS AND MACRON]
        '\u{022D}' | // ȭ  [LATIN SMALL LETTER O WITH TILDE AND MACRON]
        '\u{022F}' | // ȯ  [LATIN SMALL LETTER O WITH DOT ABOVE]
        '\u{0231}' | // ȱ  [LATIN SMALL LETTER O WITH DOT ABOVE AND MACRON]
        '\u{0254}' | // ɔ  [LATIN SMALL LETTER OPEN O]
        '\u{0275}' | // ɵ  [LATIN SMALL LETTER BARRED O]
        '\u{1D16}' | // ᴖ  [LATIN SMALL LETTER TOP HALF O]
        '\u{1D17}' | // ᴗ  [LATIN SMALL LETTER BOTTOM HALF O]
        '\u{1D97}' | // ᶗ  [LATIN SMALL LETTER OPEN O WITH RETROFLEX HOOK]
        '\u{1E4D}' | // ṍ  [LATIN SMALL LETTER O WITH TILDE AND ACUTE]
        '\u{1E4F}' | // ṏ  [LATIN SMALL LETTER O WITH TILDE AND DIAERESIS]
        '\u{1E51}' | // ṑ  [LATIN SMALL LETTER O WITH MACRON AND GRAVE]
        '\u{1E53}' | // ṓ  [LATIN SMALL LETTER O WITH MACRON AND ACUTE]
        '\u{1ECD}' | // ọ  [LATIN SMALL LETTER O WITH DOT BELOW]
        '\u{1ECF}' | // ỏ  [LATIN SMALL LETTER O WITH HOOK ABOVE]
        '\u{1ED1}' | // ố  [LATIN SMALL LETTER O WITH CIRCUMFLEX AND ACUTE]
        '\u{1ED3}' | // ồ  [LATIN SMALL LETTER O WITH CIRCUMFLEX AND GRAVE]
        '\u{1ED5}' | // ổ  [LATIN SMALL LETTER O WITH CIRCUMFLEX AND HOOK ABOVE]
        '\u{1ED7}' | // ỗ  [LATIN SMALL LETTER O WITH CIRCUMFLEX AND TILDE]
        '\u{1ED9}' | // ộ  [LATIN SMALL LETTER O WITH CIRCUMFLEX AND DOT BELOW]
        '\u{1EDB}' | // ớ  [LATIN SMALL LETTER O WITH HORN AND ACUTE]
        '\u{1EDD}' | // ờ  [LATIN SMALL LETTER O WITH HORN AND GRAVE]
        '\u{1EDF}' | // ở  [LATIN SMALL LETTER O WITH HORN AND HOOK ABOVE]
        '\u{1EE1}' | // ỡ  [LATIN SMALL LETTER O WITH HORN AND TILDE]
        '\u{1EE3}' | // ợ  [LATIN SMALL LETTER O WITH HORN AND DOT BELOW]
        '\u{2092}' | // ₒ  [LATIN SUBSCRIPT SMALL LETTER O]
        '\u{24DE}' | // ⓞ  [CIRCLED LATIN SMALL LETTER O]
        '\u{2C7A}' | // ⱺ  [LATIN SMALL LETTER O WITH LOW RING INSIDE]
        '\u{A74B}' | // ꝋ  [LATIN SMALL LETTER O WITH LONG STROKE OVERLAY]
        '\u{A74D}' | // ꝍ  [LATIN SMALL LETTER O WITH LOOP]
        '\u{FF4F}' // ｏ  [FULLWIDTH LATIN SMALL LETTER O]
        => Some("o"),
        '\u{0152}' | // Œ  [LATIN CAPITAL LIGATURE OE]
        '\u{0276}' // ɶ  [LATIN LETTER SMALL CAPITAL OE]
        => Some("OE"),
        '\u{A74E}' // Ꝏ  [LATIN CAPITAL LETTER OO]
        => Some("OO"),
        '\u{0222}' | // Ȣ  http://en.wikipedia.org/wiki/OU  [LATIN CAPITAL LETTER OU]
        '\u{1D15}' // ᴕ  [LATIN LETTER SMALL CAPITAL OU]
        => Some("OU"),
        '\u{24AA}' // ⒪  [PARENTHESIZED LATIN SMALL LETTER O]
        => Some("(o)"),
        '\u{0153}' | // œ  [LATIN SMALL LIGATURE OE]
        '\u{1D14}' // ᴔ  [LATIN SMALL LETTER TURNED OE]
        => Some("oe"),
        '\u{A74F}' // ꝏ  [LATIN SMALL LETTER OO]
        => Some("oo"),
        '\u{0223}' // ȣ  http://en.wikipedia.org/wiki/OU  [LATIN SMALL LETTER OU]
        => Some("ou"),
        '\u{01A4}' | // Ƥ  [LATIN CAPITAL LETTER P WITH HOOK]
        '\u{1D18}' | // ᴘ  [LATIN LETTER SMALL CAPITAL P]
        '\u{1E54}' | // Ṕ  [LATIN CAPITAL LETTER P WITH ACUTE]
        '\u{1E56}' | // Ṗ  [LATIN CAPITAL LETTER P WITH DOT ABOVE]
        '\u{24C5}' | // Ⓟ  [CIRCLED LATIN CAPITAL LETTER P]
        '\u{2C63}' | // Ᵽ  [LATIN CAPITAL LETTER P WITH STROKE]
        '\u{A750}' | // Ꝑ  [LATIN CAPITAL LETTER P WITH STROKE THROUGH DESCENDER]
        '\u{A752}' | // Ꝓ  [LATIN CAPITAL LETTER P WITH FLOURISH]
        '\u{A754}' | // Ꝕ  [LATIN CAPITAL LETTER P WITH SQUIRREL TAIL]
        '\u{FF30}' // Ｐ  [FULLWIDTH LATIN CAPITAL LETTER P]
        => Some("P"),
        '\u{01A5}' | // ƥ  [LATIN SMALL LETTER P WITH HOOK]
        '\u{1D71}' | // ᵱ  [LATIN SMALL LETTER P WITH MIDDLE TILDE]
        '\u{1D7D}' | // ᵽ  [LATIN SMALL LETTER P WITH STROKE]
        '\u{1D88}' | // ᶈ  [LATIN SMALL LETTER P WITH PALATAL HOOK]
        '\u{1E55}' | // ṕ  [LATIN SMALL LETTER P WITH ACUTE]
        '\u{1E57}' | // ṗ  [LATIN SMALL LETTER P WITH DOT ABOVE]
        '\u{24DF}' | // ⓟ  [CIRCLED LATIN SMALL LETTER P]
        '\u{A751}' | // ꝑ  [LATIN SMALL LETTER P WITH STROKE THROUGH DESCENDER]
        '\u{A753}' | // ꝓ  [LATIN SMALL LETTER P WITH FLOURISH]
        '\u{A755}' | // ꝕ  [LATIN SMALL LETTER P WITH SQUIRREL TAIL]
        '\u{A7FC}' | // ꟼ  [LATIN EPIGRAPHIC LETTER REVERSED P]
        '\u{FF50}' // ｐ  [FULLWIDTH LATIN SMALL LETTER P]
        => Some("p"),
        '\u{24AB}' // ⒫  [PARENTHESIZED LATIN SMALL LETTER P]
        => Some("(p)"),
        '\u{024A}' | // Ɋ  [LATIN CAPITAL LETTER SMALL Q WITH HOOK TAIL]
        '\u{24C6}' | // Ⓠ  [CIRCLED LATIN CAPITAL LETTER Q]
        '\u{A756}' | // Ꝗ  [LATIN CAPITAL LETTER Q WITH STROKE THROUGH DESCENDER]
        '\u{A758}' | // Ꝙ  [LATIN CAPITAL LETTER Q WITH DIAGONAL STROKE]
        '\u{FF31}' // Ｑ  [FULLWIDTH LATIN CAPITAL LETTER Q]
        => Some("Q"),
        '\u{0138}' | // ĸ  http://en.wikipedia.org/wiki/Kra_(letter)  [LATIN SMALL LETTER KRA]
        '\u{024B}' | // ɋ  [LATIN SMALL LETTER Q WITH HOOK TAIL]
        '\u{02A0}' | // ʠ  [LATIN SMALL LETTER Q WITH HOOK]
        '\u{24E0}' | // ⓠ  [CIRCLED LATIN SMALL LETTER Q]
        '\u{A757}' | // ꝗ  [LATIN SMALL LETTER Q WITH STROKE THROUGH DESCENDER]
        '\u{A759}' | // ꝙ  [LATIN SMALL LETTER Q WITH DIAGONAL STROKE]
        '\u{FF51}' // ｑ  [FULLWIDTH LATIN SMALL LETTER Q]
        => Some("q"),
        '\u{24AC}' // ⒬  [PARENTHESIZED LATIN SMALL LETTER Q]
        => Some("(q)"),
        '\u{0239}' // ȹ  [LATIN SMALL LETTER QP DIGRAPH]
        => Some("qp"),
        '\u{0154}' | // Ŕ  [LATIN CAPITAL LETTER R WITH ACUTE]
        '\u{0156}' | // Ŗ  [LATIN CAPITAL LETTER R WITH CEDILLA]
        '\u{0158}' | // Ř  [LATIN CAPITAL LETTER R WITH CARON]
        '\u{0210}' | // Ȓ  [LATIN CAPITAL LETTER R WITH DOUBLE GRAVE]
        '\u{0212}' | // Ȓ  [LATIN CAPITAL LETTER R WITH INVERTED BREVE]
        '\u{024C}' | // Ɍ  [LATIN CAPITAL LETTER R WITH STROKE]
        '\u{0280}' | // ʀ  [LATIN LETTER SMALL CAPITAL R]
        '\u{0281}' | // ʁ  [LATIN LETTER SMALL CAPITAL INVERTED R]
        '\u{1D19}' | // ᴙ  [LATIN LETTER SMALL CAPITAL REVERSED R]
        '\u{1D1A}' | // ᴚ  [LATIN LETTER SMALL CAPITAL TURNED R]
        '\u{1E58}' | // Ṙ  [LATIN CAPITAL LETTER R WITH DOT ABOVE]
        '\u{1E5A}' | // Ṛ  [LATIN CAPITAL LETTER R WITH DOT BELOW]
        '\u{1E5C}' | // Ṝ  [LATIN CAPITAL LETTER R WITH DOT BELOW AND MACRON]
        '\u{1E5E}' | // Ṟ  [LATIN CAPITAL LETTER R WITH LINE BELOW]
        '\u{24C7}' | // Ⓡ  [CIRCLED LATIN CAPITAL LETTER R]
        '\u{2C64}' | // Ɽ  [LATIN CAPITAL LETTER R WITH TAIL]
        '\u{A75A}' | // Ꝛ  [LATIN CAPITAL LETTER R ROTUNDA]
        '\u{A782}' | // Ꞃ  [LATIN CAPITAL LETTER INSULAR R]
        '\u{FF32}' // Ｒ  [FULLWIDTH LATIN CAPITAL LETTER R]
        => Some("R"),
        '\u{0155}' | // ŕ  [LATIN SMALL LETTER R WITH ACUTE]
        '\u{0157}' | // ŗ  [LATIN SMALL LETTER R WITH CEDILLA]
        '\u{0159}' | // ř  [LATIN SMALL LETTER R WITH CARON]
        '\u{0211}' | // ȑ  [LATIN SMALL LETTER R WITH DOUBLE GRAVE]
        '\u{0213}' | // ȓ  [LATIN SMALL LETTER R WITH INVERTED BREVE]
        '\u{024D}' | // ɍ  [LATIN SMALL LETTER R WITH STROKE]
        '\u{027C}' | // ɼ  [LATIN SMALL LETTER R WITH LONG LEG]
        '\u{027D}' | // ɽ  [LATIN SMALL LETTER R WITH TAIL]
        '\u{027E}' | // ɾ  [LATIN SMALL LETTER R WITH FISHHOOK]
        '\u{027F}' | // ɿ  [LATIN SMALL LETTER REVERSED R WITH FISHHOOK]
        '\u{1D63}' | // ᵣ  [LATIN SUBSCRIPT SMALL LETTER R]
        '\u{1D72}' | // ᵲ  [LATIN SMALL LETTER R WITH MIDDLE TILDE]
        '\u{1D73}' | // ᵳ  [LATIN SMALL LETTER R WITH FISHHOOK AND MIDDLE TILDE]
        '\u{1D89}' | // ᶉ  [LATIN SMALL LETTER R WITH PALATAL HOOK]
        '\u{1E59}' | // ṙ  [LATIN SMALL LETTER R WITH DOT ABOVE]
        '\u{1E5B}' | // ṛ  [LATIN SMALL LETTER R WITH DOT BELOW]
        '\u{1E5D}' | // ṝ  [LATIN SMALL LETTER R WITH DOT BELOW AND MACRON]
        '\u{1E5F}' | // ṟ  [LATIN SMALL LETTER R WITH LINE BELOW]
        '\u{24E1}' | // ⓡ  [CIRCLED LATIN SMALL LETTER R]
        '\u{A75B}' | // ꝛ  [LATIN SMALL LETTER R ROTUNDA]
        '\u{A783}' | // ꞃ  [LATIN SMALL LETTER INSULAR R]
        '\u{FF52}' // ｒ  [FULLWIDTH LATIN SMALL LETTER R]
        => Some("r"),
        '\u{24AD}' // ⒭  [PARENTHESIZED LATIN SMALL LETTER R]
        => Some("(r)"),
        '\u{015A}' | // Ś  [LATIN CAPITAL LETTER S WITH ACUTE]
        '\u{015C}' | // Ŝ  [LATIN CAPITAL LETTER S WITH CIRCUMFLEX]
        '\u{015E}' | // Ş  [LATIN CAPITAL LETTER S WITH CEDILLA]
        '\u{0160}' | // Š  [LATIN CAPITAL LETTER S WITH CARON]
        '\u{0218}' | // Ș  [LATIN CAPITAL LETTER S WITH COMMA BELOW]
        '\u{1E60}' | // Ṡ  [LATIN CAPITAL LETTER S WITH DOT ABOVE]
        '\u{1E62}' | // Ṣ  [LATIN CAPITAL LETTER S WITH DOT BELOW]
        '\u{1E64}' | // Ṥ  [LATIN CAPITAL LETTER S WITH ACUTE AND DOT ABOVE]
        '\u{1E66}' | // Ṧ  [LATIN CAPITAL LETTER S WITH CARON AND DOT ABOVE]
        '\u{1E68}' | // Ṩ  [LATIN CAPITAL LETTER S WITH DOT BELOW AND DOT ABOVE]
        '\u{24C8}' | // Ⓢ  [CIRCLED LATIN CAPITAL LETTER S]
        '\u{A731}' | // ꜱ  [LATIN LETTER SMALL CAPITAL S]
        '\u{A785}' | // ꞅ  [LATIN SMALL LETTER INSULAR S]
        '\u{FF33}' // Ｓ  [FULLWIDTH LATIN CAPITAL LETTER S]
        => Some("S"),
        '\u{015B}' | // ś  [LATIN SMALL LETTER S WITH ACUTE]
        '\u{015D}' | // ŝ  [LATIN SMALL LETTER S WITH CIRCUMFLEX]
        '\u{015F}' | // ş  [LATIN SMALL LETTER S WITH CEDILLA]
        '\u{0161}' | // š  [LATIN SMALL LETTER S WITH CARON]
        '\u{017F}' | // ſ  http://en.wikipedia.org/wiki/Long_S  [LATIN SMALL LETTER LONG S]
        '\u{0219}' | // ș  [LATIN SMALL LETTER S WITH COMMA BELOW]
        '\u{023F}' | // ȿ  [LATIN SMALL LETTER S WITH SWASH TAIL]
        '\u{0282}' | // ʂ  [LATIN SMALL LETTER S WITH HOOK]
        '\u{1D74}' | // ᵴ  [LATIN SMALL LETTER S WITH MIDDLE TILDE]
        '\u{1D8A}' | // ᶊ  [LATIN SMALL LETTER S WITH PALATAL HOOK]
        '\u{1E61}' | // ṡ  [LATIN SMALL LETTER S WITH DOT ABOVE]
        '\u{1E63}' | // ṣ  [LATIN SMALL LETTER S WITH DOT BELOW]
        '\u{1E65}' | // ṥ  [LATIN SMALL LETTER S WITH ACUTE AND DOT ABOVE]
        '\u{1E67}' | // ṧ  [LATIN SMALL LETTER S WITH CARON AND DOT ABOVE]
        '\u{1E69}' | // ṩ  [LATIN SMALL LETTER S WITH DOT BELOW AND DOT ABOVE]
        '\u{1E9C}' | // ẜ  [LATIN SMALL LETTER LONG S WITH DIAGONAL STROKE]
        '\u{1E9D}' | // ẝ  [LATIN SMALL LETTER LONG S WITH HIGH STROKE]
        '\u{24E2}' | // ⓢ  [CIRCLED LATIN SMALL LETTER S]
        '\u{A784}' | // Ꞅ  [LATIN CAPITAL LETTER INSULAR S]
        '\u{FF53}' // ｓ  [FULLWIDTH LATIN SMALL LETTER S]
        => Some("s"),
        '\u{1E9E}' // ẞ  [LATIN CAPITAL LETTER SHARP S]
        => Some("SS"),
        '\u{24AE}' // ⒮  [PARENTHESIZED LATIN SMALL LETTER S]
        => Some("(s)"),
        '\u{00DF}' // ß  [LATIN SMALL LETTER SHARP S]
        => Some("ss"),
        '\u{FB06}' // ﬆ  [LATIN SMALL LIGATURE ST]
        => Some("st"),
        '\u{0162}' | // Ţ  [LATIN CAPITAL LETTER T WITH CEDILLA]
        '\u{0164}' | // Ť  [LATIN CAPITAL LETTER T WITH CARON]
        '\u{0166}' | // Ŧ  [LATIN CAPITAL LETTER T WITH STROKE]
        '\u{01AC}' | // Ƭ  [LATIN CAPITAL LETTER T WITH HOOK]
        '\u{01AE}' | // Ʈ  [LATIN CAPITAL LETTER T WITH RETROFLEX HOOK]
        '\u{021A}' | // Ț  [LATIN CAPITAL LETTER T WITH COMMA BELOW]
        '\u{023E}' | // Ⱦ  [LATIN CAPITAL LETTER T WITH DIAGONAL STROKE]
        '\u{1D1B}' | // ᴛ  [LATIN LETTER SMALL CAPITAL T]
        '\u{1E6A}' | // Ṫ  [LATIN CAPITAL LETTER T WITH DOT ABOVE]
        '\u{1E6C}' | // Ṭ  [LATIN CAPITAL LETTER T WITH DOT BELOW]
        '\u{1E6E}' | // Ṯ  [LATIN CAPITAL LETTER T WITH LINE BELOW]
        '\u{1E70}' | // Ṱ  [LATIN CAPITAL LETTER T WITH CIRCUMFLEX BELOW]
        '\u{24C9}' | // Ⓣ  [CIRCLED LATIN CAPITAL LETTER T]
        '\u{A786}' | // Ꞇ  [LATIN CAPITAL LETTER INSULAR T]
        '\u{FF34}' // Ｔ  [FULLWIDTH LATIN CAPITAL LETTER T]
        => Some("T"),
        '\u{0163}' | // ţ  [LATIN SMALL LETTER T WITH CEDILLA]
        '\u{0165}' | // ť  [LATIN SMALL LETTER T WITH CARON]
        '\u{0167}' | // ŧ  [LATIN SMALL LETTER T WITH STROKE]
        '\u{01AB}' | // ƫ  [LATIN SMALL LETTER T WITH PALATAL HOOK]
        '\u{01AD}' | // ƭ  [LATIN SMALL LETTER T WITH HOOK]
        '\u{021B}' | // ț  [LATIN SMALL LETTER T WITH COMMA BELOW]
        '\u{0236}' | // ȶ  [LATIN SMALL LETTER T WITH CURL]
        '\u{0287}' | // ʇ  [LATIN SMALL LETTER TURNED T]
        '\u{0288}' | // ʈ  [LATIN SMALL LETTER T WITH RETROFLEX HOOK]
        '\u{1D75}' | // ᵵ  [LATIN SMALL LETTER T WITH MIDDLE TILDE]
        '\u{1E6B}' | // ṫ  [LATIN SMALL LETTER T WITH DOT ABOVE]
        '\u{1E6D}' | // ṭ  [LATIN SMALL LETTER T WITH DOT BELOW]
        '\u{1E6F}' | // ṯ  [LATIN SMALL LETTER T WITH LINE BELOW]
        '\u{1E71}' | // ṱ  [LATIN SMALL LETTER T WITH CIRCUMFLEX BELOW]
        '\u{1E97}' | // ẗ  [LATIN SMALL LETTER T WITH DIAERESIS]
        '\u{24E3}' | // ⓣ  [CIRCLED LATIN SMALL LETTER T]
        '\u{2C66}' | // ⱦ  [LATIN SMALL LETTER T WITH DIAGONAL STROKE]
        '\u{FF54}' // ｔ  [FULLWIDTH LATIN SMALL LETTER T]
        => Some("t"),
        '\u{00DE}' | // Þ  [LATIN CAPITAL LETTER THORN]
        '\u{A766}' // Ꝧ  [LATIN CAPITAL LETTER THORN WITH STROKE THROUGH DESCENDER]
        => Some("TH"),
        '\u{A728}' // Ꜩ  [LATIN CAPITAL LETTER TZ]
        => Some("TZ"),
        '\u{24AF}' // ⒯  [PARENTHESIZED LATIN SMALL LETTER T]
        => Some("(t)"),
        '\u{02A8}' // ʨ  [LATIN SMALL LETTER TC DIGRAPH WITH CURL]
        => Some("tc"),
        '\u{00FE}' | // þ  [LATIN SMALL LETTER THORN]
        '\u{1D7A}' | // ᵺ  [LATIN SMALL LETTER TH WITH STRIKETHROUGH]
        '\u{A767}' // ꝧ  [LATIN SMALL LETTER THORN WITH STROKE THROUGH DESCENDER]
        => Some("th"),
        '\u{02A6}' // ʦ  [LATIN SMALL LETTER TS DIGRAPH]
        => Some("ts"),
        '\u{A729}' // ꜩ  [LATIN SMALL LETTER TZ]
        => Some("tz"),
        '\u{00D9}' | // Ù  [LATIN CAPITAL LETTER U WITH GRAVE]
        '\u{00DA}' | // Ú  [LATIN CAPITAL LETTER U WITH ACUTE]
        '\u{00DB}' | // Û  [LATIN CAPITAL LETTER U WITH CIRCUMFLEX]
        '\u{00DC}' | // Ü  [LATIN CAPITAL LETTER U WITH DIAERESIS]
        '\u{0168}' | // Ũ  [LATIN CAPITAL LETTER U WITH TILDE]
        '\u{016A}' | // Ū  [LATIN CAPITAL LETTER U WITH MACRON]
        '\u{016C}' | // Ŭ  [LATIN CAPITAL LETTER U WITH BREVE]
        '\u{016E}' | // Ů  [LATIN CAPITAL LETTER U WITH RING ABOVE]
        '\u{0170}' | // Ű  [LATIN CAPITAL LETTER U WITH DOUBLE ACUTE]
        '\u{0172}' | // Ų  [LATIN CAPITAL LETTER U WITH OGONEK]
        '\u{01AF}' | // Ư  [LATIN CAPITAL LETTER U WITH HORN]
        '\u{01D3}' | // Ǔ  [LATIN CAPITAL LETTER U WITH CARON]
        '\u{01D5}' | // Ǖ  [LATIN CAPITAL LETTER U WITH DIAERESIS AND MACRON]
        '\u{01D7}' | // Ǘ  [LATIN CAPITAL LETTER U WITH DIAERESIS AND ACUTE]
        '\u{01D9}' | // Ǚ  [LATIN CAPITAL LETTER U WITH DIAERESIS AND CARON]
        '\u{01DB}' | // Ǜ  [LATIN CAPITAL LETTER U WITH DIAERESIS AND GRAVE]
        '\u{0214}' | // Ȕ  [LATIN CAPITAL LETTER U WITH DOUBLE GRAVE]
        '\u{0216}' | // Ȗ  [LATIN CAPITAL LETTER U WITH INVERTED BREVE]
        '\u{0244}' | // Ʉ  [LATIN CAPITAL LETTER U BAR]
        '\u{1D1C}' | // ᴜ  [LATIN LETTER SMALL CAPITAL U]
        '\u{1D7E}' | // ᵾ  [LATIN SMALL CAPITAL LETTER U WITH STROKE]
        '\u{1E72}' | // Ṳ  [LATIN CAPITAL LETTER U WITH DIAERESIS BELOW]
        '\u{1E74}' | // Ṵ  [LATIN CAPITAL LETTER U WITH TILDE BELOW]
        '\u{1E76}' | // Ṷ  [LATIN CAPITAL LETTER U WITH CIRCUMFLEX BELOW]
        '\u{1E78}' | // Ṹ  [LATIN CAPITAL LETTER U WITH TILDE AND ACUTE]
        '\u{1E7A}' | // Ṻ  [LATIN CAPITAL LETTER U WITH MACRON AND DIAERESIS]
        '\u{1EE4}' | // Ụ  [LATIN CAPITAL LETTER U WITH DOT BELOW]
        '\u{1EE6}' | // Ủ  [LATIN CAPITAL LETTER U WITH HOOK ABOVE]
        '\u{1EE8}' | // Ứ  [LATIN CAPITAL LETTER U WITH HORN AND ACUTE]
        '\u{1EEA}' | // Ừ  [LATIN CAPITAL LETTER U WITH HORN AND GRAVE]
        '\u{1EEC}' | // Ử  [LATIN CAPITAL LETTER U WITH HORN AND HOOK ABOVE]
        '\u{1EEE}' | // Ữ  [LATIN CAPITAL LETTER U WITH HORN AND TILDE]
        '\u{1EF0}' | // Ự  [LATIN CAPITAL LETTER U WITH HORN AND DOT BELOW]
        '\u{24CA}' | // Ⓤ  [CIRCLED LATIN CAPITAL LETTER U]
        '\u{FF35}' // Ｕ  [FULLWIDTH LATIN CAPITAL LETTER U]
        => Some("U"),
        '\u{00F9}' | // ù  [LATIN SMALL LETTER U WITH GRAVE]
        '\u{00FA}' | // ú  [LATIN SMALL LETTER U WITH ACUTE]
        '\u{00FB}' | // û  [LATIN SMALL LETTER U WITH CIRCUMFLEX]
        '\u{00FC}' | // ü  [LATIN SMALL LETTER U WITH DIAERESIS]
        '\u{0169}' | // ũ  [LATIN SMALL LETTER U WITH TILDE]
        '\u{016B}' | // ū  [LATIN SMALL LETTER U WITH MACRON]
        '\u{016D}' | // ŭ  [LATIN SMALL LETTER U WITH BREVE]
        '\u{016F}' | // ů  [LATIN SMALL LETTER U WITH RING ABOVE]
        '\u{0171}' | // ű  [LATIN SMALL LETTER U WITH DOUBLE ACUTE]
        '\u{0173}' | // ų  [LATIN SMALL LETTER U WITH OGONEK]
        '\u{01B0}' | // ư  [LATIN SMALL LETTER U WITH HORN]
        '\u{01D4}' | // ǔ  [LATIN SMALL LETTER U WITH CARON]
        '\u{01D6}' | // ǖ  [LATIN SMALL LETTER U WITH DIAERESIS AND MACRON]
        '\u{01D8}' | // ǘ  [LATIN SMALL LETTER U WITH DIAERESIS AND ACUTE]
        '\u{01DA}' | // ǚ  [LATIN SMALL LETTER U WITH DIAERESIS AND CARON]
        '\u{01DC}' | // ǜ  [LATIN SMALL LETTER U WITH DIAERESIS AND GRAVE]
        '\u{0215}' | // ȕ  [LATIN SMALL LETTER U WITH DOUBLE GRAVE]
        '\u{0217}' | // ȗ  [LATIN SMALL LETTER U WITH INVERTED BREVE]
        '\u{0289}' | // ʉ  [LATIN SMALL LETTER U BAR]
        '\u{1D64}' | // ᵤ  [LATIN SUBSCRIPT SMALL LETTER U]
        '\u{1D99}' | // ᶙ  [LATIN SMALL LETTER U WITH RETROFLEX HOOK]
        '\u{1E73}' | // ṳ  [LATIN SMALL LETTER U WITH DIAERESIS BELOW]
        '\u{1E75}' | // ṵ  [LATIN SMALL LETTER U WITH TILDE BELOW]
        '\u{1E77}' | // ṷ  [LATIN SMALL LETTER U WITH CIRCUMFLEX BELOW]
        '\u{1E79}' | // ṹ  [LATIN SMALL LETTER U WITH TILDE AND ACUTE]
        '\u{1E7B}' | // ṻ  [LATIN SMALL LETTER U WITH MACRON AND DIAERESIS]
        '\u{1EE5}' | // ụ  [LATIN SMALL LETTER U WITH DOT BELOW]
        '\u{1EE7}' | // ủ  [LATIN SMALL LETTER U WITH HOOK ABOVE]
        '\u{1EE9}' | // ứ  [LATIN SMALL LETTER U WITH HORN AND ACUTE]
        '\u{1EEB}' | // ừ  [LATIN SMALL LETTER U WITH HORN AND GRAVE]
        '\u{1EED}' | // ử  [LATIN SMALL LETTER U WITH HORN AND HOOK ABOVE]
        '\u{1EEF}' | // ữ  [LATIN SMALL LETTER U WITH HORN AND TILDE]
        '\u{1EF1}' | // ự  [LATIN SMALL LETTER U WITH HORN AND DOT BELOW]
        '\u{24E4}' | // ⓤ  [CIRCLED LATIN SMALL LETTER U]
        '\u{FF55}' // ｕ  [FULLWIDTH LATIN SMALL LETTER U]
        => Some("u"),
        '\u{24B0}' // ⒰  [PARENTHESIZED LATIN SMALL LETTER U]
        => Some("(u)"),
        '\u{1D6B}' // ᵫ  [LATIN SMALL LETTER UE]
        => Some("ue"),
        '\u{01B2}' | // Ʋ  [LATIN CAPITAL LETTER V WITH HOOK]
        '\u{0245}' | // Ʌ  [LATIN CAPITAL LETTER TURNED V]
        '\u{1D20}' | // ᴠ  [LATIN LETTER SMALL CAPITAL V]
        '\u{1E7C}' | // Ṽ  [LATIN CAPITAL LETTER V WITH TILDE]
        '\u{1E7E}' | // Ṿ  [LATIN CAPITAL LETTER V WITH DOT BELOW]
        '\u{1EFC}' | // Ỽ  [LATIN CAPITAL LETTER MIDDLE-WELSH V]
        '\u{24CB}' | // Ⓥ  [CIRCLED LATIN CAPITAL LETTER V]
        '\u{A75E}' | // Ꝟ  [LATIN CAPITAL LETTER V WITH DIAGONAL STROKE]
        '\u{A768}' | // Ꝩ  [LATIN CAPITAL LETTER VEND]
        '\u{FF36}' // Ｖ  [FULLWIDTH LATIN CAPITAL LETTER V]
        => Some("V"),
        '\u{028B}' | // ʋ  [LATIN SMALL LETTER V WITH HOOK]
        '\u{028C}' | // ʌ  [LATIN SMALL LETTER TURNED V]
        '\u{1D65}' | // ᵥ  [LATIN SUBSCRIPT SMALL LETTER V]
        '\u{1D8C}' | // ᶌ  [LATIN SMALL LETTER V WITH PALATAL HOOK]
        '\u{1E7D}' | // ṽ  [LATIN SMALL LETTER V WITH TILDE]
        '\u{1E7F}' | // ṿ  [LATIN SMALL LETTER V WITH DOT BELOW]
        '\u{24E5}' | // ⓥ  [CIRCLED LATIN SMALL LETTER V]
        '\u{2C71}' | // ⱱ  [LATIN SMALL LETTER V WITH RIGHT HOOK]
        '\u{2C74}' | // ⱴ  [LATIN SMALL LETTER V WITH CURL]
        '\u{A75F}' | // ꝟ  [LATIN SMALL LETTER V WITH DIAGONAL STROKE]
        '\u{FF56}' // ｖ  [FULLWIDTH LATIN SMALL LETTER V]
        => Some("v"),
        '\u{A760}' // Ꝡ  [LATIN CAPITAL LETTER VY]
        => Some("VY"),
        '\u{24B1}' // ⒱  [PARENTHESIZED LATIN SMALL LETTER V]
        => Some("(v)"),
        '\u{A761}' // ꝡ  [LATIN SMALL LETTER VY]
        => Some("vy"),
        '\u{0174}' | // Ŵ  [LATIN CAPITAL LETTER W WITH CIRCUMFLEX]
        '\u{01F7}' | // Ƿ  http://en.wikipedia.org/wiki/Wynn  [LATIN CAPITAL LETTER WYNN]
        '\u{1D21}' | // ᴡ  [LATIN LETTER SMALL CAPITAL W]
        '\u{1E80}' | // Ẁ  [LATIN CAPITAL LETTER W WITH GRAVE]
        '\u{1E82}' | // Ẃ  [LATIN CAPITAL LETTER W WITH ACUTE]
        '\u{1E84}' | // Ẅ  [LATIN CAPITAL LETTER W WITH DIAERESIS]
        '\u{1E86}' | // Ẇ  [LATIN CAPITAL LETTER W WITH DOT ABOVE]
        '\u{1E88}' | // Ẉ  [LATIN CAPITAL LETTER W WITH DOT BELOW]
        '\u{24CC}' | // Ⓦ  [CIRCLED LATIN CAPITAL LETTER W]
        '\u{2C72}' | // Ⱳ  [LATIN CAPITAL LETTER W WITH HOOK]
        '\u{FF37}' // Ｗ  [FULLWIDTH LATIN CAPITAL LETTER W]
        => Some("W"),
        '\u{0175}' | // ŵ  [LATIN SMALL LETTER W WITH CIRCUMFLEX]
        '\u{01BF}' | // ƿ  http://en.wikipedia.org/wiki/Wynn  [LATIN LETTER WYNN]
        '\u{028D}' | // ʍ  [LATIN SMALL LETTER TURNED W]
        '\u{1E81}' | // ẁ  [LATIN SMALL LETTER W WITH GRAVE]
        '\u{1E83}' | // ẃ  [LATIN SMALL LETTER W WITH ACUTE]
        '\u{1E85}' | // ẅ  [LATIN SMALL LETTER W WITH DIAERESIS]
        '\u{1E87}' | // ẇ  [LATIN SMALL LETTER W WITH DOT ABOVE]
        '\u{1E89}' | // ẉ  [LATIN SMALL LETTER W WITH DOT BELOW]
        '\u{1E98}' | // ẘ  [LATIN SMALL LETTER W WITH RING ABOVE]
        '\u{24E6}' | // ⓦ  [CIRCLED LATIN SMALL LETTER W]
        '\u{2C73}' | // ⱳ  [LATIN SMALL LETTER W WITH HOOK]
        '\u{FF57}' // ｗ  [FULLWIDTH LATIN SMALL LETTER W]
        => Some("w"),
        '\u{24B2}' // ⒲  [PARENTHESIZED LATIN SMALL LETTER W]
        => Some("(w)"),
        '\u{1E8A}' | // Ẋ  [LATIN CAPITAL LETTER X WITH DOT ABOVE]
        '\u{1E8C}' | // Ẍ  [LATIN CAPITAL LETTER X WITH DIAERESIS]
        '\u{24CD}' | // Ⓧ  [CIRCLED LATIN CAPITAL LETTER X]
        '\u{FF38}' // Ｘ  [FULLWIDTH LATIN CAPITAL LETTER X]
        => Some("X"),
        '\u{1D8D}' | // ᶍ  [LATIN SMALL LETTER X WITH PALATAL HOOK]
        '\u{1E8B}' | // ẋ  [LATIN SMALL LETTER X WITH DOT ABOVE]
        '\u{1E8D}' | // ẍ  [LATIN SMALL LETTER X WITH DIAERESIS]
        '\u{2093}' | // ₓ  [LATIN SUBSCRIPT SMALL LETTER X]
        '\u{24E7}' | // ⓧ  [CIRCLED LATIN SMALL LETTER X]
        '\u{FF58}' // ｘ  [FULLWIDTH LATIN SMALL LETTER X]
        => Some("x"),
        '\u{24B3}' // ⒳  [PARENTHESIZED LATIN SMALL LETTER X]
        => Some("(x)"),
        '\u{00DD}' | // Ý  [LATIN CAPITAL LETTER Y WITH ACUTE]
        '\u{0176}' | // Ŷ  [LATIN CAPITAL LETTER Y WITH CIRCUMFLEX]
        '\u{0178}' | // Ÿ  [LATIN CAPITAL LETTER Y WITH DIAERESIS]
        '\u{01B3}' | // Ƴ  [LATIN CAPITAL LETTER Y WITH HOOK]
        '\u{0232}' | // Ȳ  [LATIN CAPITAL LETTER Y WITH MACRON]
        '\u{024E}' | // Ɏ  [LATIN CAPITAL LETTER Y WITH STROKE]
        '\u{028F}' | // ʏ  [LATIN LETTER SMALL CAPITAL Y]
        '\u{1E8E}' | // Ẏ  [LATIN CAPITAL LETTER Y WITH DOT ABOVE]
        '\u{1EF2}' | // Ỳ  [LATIN CAPITAL LETTER Y WITH GRAVE]
        '\u{1EF4}' | // Ỵ  [LATIN CAPITAL LETTER Y WITH DOT BELOW]
        '\u{1EF6}' | // Ỷ  [LATIN CAPITAL LETTER Y WITH HOOK ABOVE]
        '\u{1EF8}' | // Ỹ  [LATIN CAPITAL LETTER Y WITH TILDE]
        '\u{1EFE}' | // Ỿ  [LATIN CAPITAL LETTER Y WITH LOOP]
        '\u{24CE}' | // Ⓨ  [CIRCLED LATIN CAPITAL LETTER Y]
        '\u{FF39}' // Ｙ  [FULLWIDTH LATIN CAPITAL LETTER Y]
        => Some("Y"),
        '\u{00FD}' | // ý  [LATIN SMALL LETTER Y WITH ACUTE]
        '\u{00FF}' | // ÿ  [LATIN SMALL LETTER Y WITH DIAERESIS]
        '\u{0177}' | // ŷ  [LATIN SMALL LETTER Y WITH CIRCUMFLEX]
        '\u{01B4}' | // ƴ  [LATIN SMALL LETTER Y WITH HOOK]
        '\u{0233}' | // ȳ  [LATIN SMALL LETTER Y WITH MACRON]
        '\u{024F}' | // ɏ  [LATIN SMALL LETTER Y WITH STROKE]
        '\u{028E}' | // ʎ  [LATIN SMALL LETTER TURNED Y]
        '\u{1E8F}' | // ẏ  [LATIN SMALL LETTER Y WITH DOT ABOVE]
        '\u{1E99}' | // ẙ  [LATIN SMALL LETTER Y WITH RING ABOVE]
        '\u{1EF3}' | // ỳ  [LATIN SMALL LETTER Y WITH GRAVE]
        '\u{1EF5}' | // ỵ  [LATIN SMALL LETTER Y WITH DOT BELOW]
        '\u{1EF7}' | // ỷ  [LATIN SMALL LETTER Y WITH HOOK ABOVE]
        '\u{1EF9}' | // ỹ  [LATIN SMALL LETTER Y WITH TILDE]
        '\u{1EFF}' | // ỿ  [LATIN SMALL LETTER Y WITH LOOP]
        '\u{24E8}' | // ⓨ  [CIRCLED LATIN SMALL LETTER Y]
        '\u{FF59}' // ｙ  [FULLWIDTH LATIN SMALL LETTER Y]
        => Some("y"),
        '\u{24B4}' // ⒴  [PARENTHESIZED LATIN SMALL LETTER Y]
        => Some("(y)"),
        '\u{0179}' | // Ź  [LATIN CAPITAL LETTER Z WITH ACUTE]
        '\u{017B}' | // Ż  [LATIN CAPITAL LETTER Z WITH DOT ABOVE]
        '\u{017D}' | // Ž  [LATIN CAPITAL LETTER Z WITH CARON]
        '\u{01B5}' | // Ƶ  [LATIN CAPITAL LETTER Z WITH STROKE]
        '\u{021C}' | // Ȝ  http://en.wikipedia.org/wiki/Yogh  [LATIN CAPITAL LETTER YOGH]
        '\u{0224}' | // Ȥ  [LATIN CAPITAL LETTER Z WITH HOOK]
        '\u{1D22}' | // ᴢ  [LATIN LETTER SMALL CAPITAL Z]
        '\u{1E90}' | // Ẑ  [LATIN CAPITAL LETTER Z WITH CIRCUMFLEX]
        '\u{1E92}' | // Ẓ  [LATIN CAPITAL LETTER Z WITH DOT BELOW]
        '\u{1E94}' | // Ẕ  [LATIN CAPITAL LETTER Z WITH LINE BELOW]
        '\u{24CF}' | // Ⓩ  [CIRCLED LATIN CAPITAL LETTER Z]
        '\u{2C6B}' | // Ⱬ  [LATIN CAPITAL LETTER Z WITH DESCENDER]
        '\u{A762}' | // Ꝣ  [LATIN CAPITAL LETTER VISIGOTHIC Z]
        '\u{FF3A}' // Ｚ  [FULLWIDTH LATIN CAPITAL LETTER Z]
        => Some("Z"),
        '\u{017A}' | // ź  [LATIN SMALL LETTER Z WITH ACUTE]
        '\u{017C}' | // ż  [LATIN SMALL LETTER Z WITH DOT ABOVE]
        '\u{017E}' | // ž  [LATIN SMALL LETTER Z WITH CARON]
        '\u{01B6}' | // ƶ  [LATIN SMALL LETTER Z WITH STROKE]
        '\u{021D}' | // ȝ  http://en.wikipedia.org/wiki/Yogh  [LATIN SMALL LETTER YOGH]
        '\u{0225}' | // ȥ  [LATIN SMALL LETTER Z WITH HOOK]
        '\u{0240}' | // ɀ  [LATIN SMALL LETTER Z WITH SWASH TAIL]
        '\u{0290}' | // ʐ  [LATIN SMALL LETTER Z WITH RETROFLEX HOOK]
        '\u{0291}' | // ʑ  [LATIN SMALL LETTER Z WITH CURL]
        '\u{1D76}' | // ᵶ  [LATIN SMALL LETTER Z WITH MIDDLE TILDE]
        '\u{1D8E}' | // ᶎ  [LATIN SMALL LETTER Z WITH PALATAL HOOK]
        '\u{1E91}' | // ẑ  [LATIN SMALL LETTER Z WITH CIRCUMFLEX]
        '\u{1E93}' | // ẓ  [LATIN SMALL LETTER Z WITH DOT BELOW]
        '\u{1E95}' | // ẕ  [LATIN SMALL LETTER Z WITH LINE BELOW]
        '\u{24E9}' | // ⓩ  [CIRCLED LATIN SMALL LETTER Z]
        '\u{2C6C}' | // ⱬ  [LATIN SMALL LETTER Z WITH DESCENDER]
        '\u{A763}' | // ꝣ  [LATIN SMALL LETTER VISIGOTHIC Z]
        '\u{FF5A}' // ｚ  [FULLWIDTH LATIN SMALL LETTER Z]
        => Some("z"),
        '\u{24B5}' // ⒵  [PARENTHESIZED LATIN SMALL LETTER Z]
        => Some("(z)"),
        '\u{2070}' | // ⁰  [SUPERSCRIPT ZERO]
        '\u{2080}' | // ₀  [SUBSCRIPT ZERO]
        '\u{24EA}' | // ⓪  [CIRCLED DIGIT ZERO]
        '\u{24FF}' | // ⓿  [NEGATIVE CIRCLED DIGIT ZERO]
        '\u{FF10}' // ０  [FULLWIDTH DIGIT ZERO]
        => Some("0"),
        '\u{00B9}' | // ¹  [SUPERSCRIPT ONE]
        '\u{2081}' | // ₁  [SUBSCRIPT ONE]
        '\u{2460}' | // ①  [CIRCLED DIGIT ONE]
        '\u{24F5}' | // ⓵  [DOUBLE CIRCLED DIGIT ONE]
        '\u{2776}' | // ❶  [DINGBAT NEGATIVE CIRCLED DIGIT ONE]
        '\u{2780}' | // ➀  [DINGBAT CIRCLED SANS-SERIF DIGIT ONE]
        '\u{278A}' | // ➊  [DINGBAT NEGATIVE CIRCLED SANS-SERIF DIGIT ONE]
        '\u{FF11}' // １  [FULLWIDTH DIGIT ONE]
        => Some("1"),
        '\u{2488}' // ⒈  [DIGIT ONE FULL STOP]
        => Some("1."),
        '\u{2474}' // ⑴  [PARENTHESIZED DIGIT ONE]
        => Some("(1)"),
        '\u{00B2}' | // ²  [SUPERSCRIPT TWO]
        '\u{2082}' | // ₂  [SUBSCRIPT TWO]
        '\u{2461}' | // ②  [CIRCLED DIGIT TWO]
        '\u{24F6}' | // ⓶  [DOUBLE CIRCLED DIGIT TWO]
        '\u{2777}' | // ❷  [DINGBAT NEGATIVE CIRCLED DIGIT TWO]
        '\u{2781}' | // ➁  [DINGBAT CIRCLED SANS-SERIF DIGIT TWO]
        '\u{278B}' | // ➋  [DINGBAT NEGATIVE CIRCLED SANS-SERIF DIGIT TWO]
        '\u{FF12}' // ２  [FULLWIDTH DIGIT TWO]
        => Some("2"),
        '\u{2489}' // ⒉  [DIGIT TWO FULL STOP]
        => Some("2."),
        '\u{2475}' // ⑵  [PARENTHESIZED DIGIT TWO]
        => Some("(2)"),
        '\u{00B3}' | // ³  [SUPERSCRIPT THREE]
        '\u{2083}' | // ₃  [SUBSCRIPT THREE]
        '\u{2462}' | // ③  [CIRCLED DIGIT THREE]
        '\u{24F7}' | // ⓷  [DOUBLE CIRCLED DIGIT THREE]
        '\u{2778}' | // ❸  [DINGBAT NEGATIVE CIRCLED DIGIT THREE]
        '\u{2782}' | // ➂  [DINGBAT CIRCLED SANS-SERIF DIGIT THREE]
        '\u{278C}' | // ➌  [DINGBAT NEGATIVE CIRCLED SANS-SERIF DIGIT THREE]
        '\u{FF13}' // ３  [FULLWIDTH DIGIT THREE]
        => Some("3"),
        '\u{248A}' // ⒊  [DIGIT THREE FULL STOP]
        => Some("3."),
        '\u{2476}' // ⑶  [PARENTHESIZED DIGIT THREE]
        => Some("(3)"),
        '\u{2074}' | // ⁴  [SUPERSCRIPT FOUR]
        '\u{2084}' | // ₄  [SUBSCRIPT FOUR]
        '\u{2463}' | // ④  [CIRCLED DIGIT FOUR]
        '\u{24F8}' | // ⓸  [DOUBLE CIRCLED DIGIT FOUR]
        '\u{2779}' | // ❹  [DINGBAT NEGATIVE CIRCLED DIGIT FOUR]
        '\u{2783}' | // ➃  [DINGBAT CIRCLED SANS-SERIF DIGIT FOUR]
        '\u{278D}' | // ➍  [DINGBAT NEGATIVE CIRCLED SANS-SERIF DIGIT FOUR]
        '\u{FF14}' // ４  [FULLWIDTH DIGIT FOUR]
        => Some("4"),
        '\u{248B}' // ⒋  [DIGIT FOUR FULL STOP]
        => Some("4."),
        '\u{2477}' // ⑷  [PARENTHESIZED DIGIT FOUR]
        => Some("(4)"),
        '\u{2075}' | // ⁵  [SUPERSCRIPT FIVE]
        '\u{2085}' | // ₅  [SUBSCRIPT FIVE]
        '\u{2464}' | // ⑤  [CIRCLED DIGIT FIVE]
        '\u{24F9}' | // ⓹  [DOUBLE CIRCLED DIGIT FIVE]
        '\u{277A}' | // ❺  [DINGBAT NEGATIVE CIRCLED DIGIT FIVE]
        '\u{2784}' | // ➄  [DINGBAT CIRCLED SANS-SERIF DIGIT FIVE]
        '\u{278E}' | // ➎  [DINGBAT NEGATIVE CIRCLED SANS-SERIF DIGIT FIVE]
        '\u{FF15}' // ５  [FULLWIDTH DIGIT FIVE]
        => Some("5"),
        '\u{248C}' // ⒌  [DIGIT FIVE FULL STOP]
        => Some("5."),
        '\u{2478}' // ⑸  [PARENTHESIZED DIGIT FIVE]
        => Some("(5)"),
        '\u{2076}' | // ⁶  [SUPERSCRIPT SIX]
        '\u{2086}' | // ₆  [SUBSCRIPT SIX]
        '\u{2465}' | // ⑥  [CIRCLED DIGIT SIX]
        '\u{24FA}' | // ⓺  [DOUBLE CIRCLED DIGIT SIX]
        '\u{277B}' | // ❻  [DINGBAT NEGATIVE CIRCLED DIGIT SIX]
        '\u{2785}' | // ➅  [DINGBAT CIRCLED SANS-SERIF DIGIT SIX]
        '\u{278F}' | // ➏  [DINGBAT NEGATIVE CIRCLED SANS-SERIF DIGIT SIX]
        '\u{FF16}' // ６  [FULLWIDTH DIGIT SIX]
        => Some("6"),
        '\u{248D}' // ⒍  [DIGIT SIX FULL STOP]
        => Some("6."),
        '\u{2479}' // ⑹  [PARENTHESIZED DIGIT SIX]
        => Some("(6)"),
        '\u{2077}' | // ⁷  [SUPERSCRIPT SEVEN]
        '\u{2087}' | // ₇  [SUBSCRIPT SEVEN]
        '\u{2466}' | // ⑦  [CIRCLED DIGIT SEVEN]
        '\u{24FB}' | // ⓻  [DOUBLE CIRCLED DIGIT SEVEN]
        '\u{277C}' | // ❼  [DINGBAT NEGATIVE CIRCLED DIGIT SEVEN]
        '\u{2786}' | // ➆  [DINGBAT CIRCLED SANS-SERIF DIGIT SEVEN]
        '\u{2790}' | // ➐  [DINGBAT NEGATIVE CIRCLED SANS-SERIF DIGIT SEVEN]
        '\u{FF17}' // ７  [FULLWIDTH DIGIT SEVEN]
        => Some("7"),
        '\u{248E}' // ⒎  [DIGIT SEVEN FULL STOP]
        => Some("7."),
        '\u{247A}' // ⑺  [PARENTHESIZED DIGIT SEVEN]
        => Some("(7)"),
        '\u{2078}' | // ⁸  [SUPERSCRIPT EIGHT]
        '\u{2088}' | // ₈  [SUBSCRIPT EIGHT]
        '\u{2467}' | // ⑧  [CIRCLED DIGIT EIGHT]
        '\u{24FC}' | // ⓼  [DOUBLE CIRCLED DIGIT EIGHT]
        '\u{277D}' | // ❽  [DINGBAT NEGATIVE CIRCLED DIGIT EIGHT]
        '\u{2787}' | // ➇  [DINGBAT CIRCLED SANS-SERIF DIGIT EIGHT]
        '\u{2791}' | // ➑  [DINGBAT NEGATIVE CIRCLED SANS-SERIF DIGIT EIGHT]
        '\u{FF18}' // ８  [FULLWIDTH DIGIT EIGHT]
        => Some("8"),
        '\u{248F}' // ⒏  [DIGIT EIGHT FULL STOP]
        => Some("8."),
        '\u{247B}' // ⑻  [PARENTHESIZED DIGIT EIGHT]
        => Some("(8)"),
        '\u{2079}' | // ⁹  [SUPERSCRIPT NINE]
        '\u{2089}' | // ₉  [SUBSCRIPT NINE]
        '\u{2468}' | // ⑨  [CIRCLED DIGIT NINE]
        '\u{24FD}' | // ⓽  [DOUBLE CIRCLED DIGIT NINE]
        '\u{277E}' | // ❾  [DINGBAT NEGATIVE CIRCLED DIGIT NINE]
        '\u{2788}' | // ➈  [DINGBAT CIRCLED SANS-SERIF DIGIT NINE]
        '\u{2792}' | // ➒  [DINGBAT NEGATIVE CIRCLED SANS-SERIF DIGIT NINE]
        '\u{FF19}' // ９  [FULLWIDTH DIGIT NINE]
        => Some("9"),
        '\u{2490}' // ⒐  [DIGIT NINE FULL STOP]
        => Some("9."),
        '\u{247C}' // ⑼  [PARENTHESIZED DIGIT NINE]
        => Some("(9)"),
        '\u{2469}' | // ⑩  [CIRCLED NUMBER TEN]
        '\u{24FE}' | // ⓾  [DOUBLE CIRCLED NUMBER TEN]
        '\u{277F}' | // ❿  [DINGBAT NEGATIVE CIRCLED NUMBER TEN]
        '\u{2789}' | // ➉  [DINGBAT CIRCLED SANS-SERIF NUMBER TEN]
        '\u{2793}' // ➓  [DINGBAT NEGATIVE CIRCLED SANS-SERIF NUMBER TEN]
        => Some("10"),
        '\u{2491}' // ⒑  [NUMBER TEN FULL STOP]
        => Some("10."),
        '\u{247D}' // ⑽  [PARENTHESIZED NUMBER TEN]
        => Some("(10)"),
        '\u{246A}' | // ⑪  [CIRCLED NUMBER ELEVEN]
        '\u{24EB}' // ⓫  [NEGATIVE CIRCLED NUMBER ELEVEN]
        => Some("11"),
        '\u{2492}' // ⒒  [NUMBER ELEVEN FULL STOP]
        => Some("11."),
        '\u{247E}' // ⑾  [PARENTHESIZED NUMBER ELEVEN]
        => Some("(11)"),
        '\u{246B}' | // ⑫  [CIRCLED NUMBER TWELVE]
        '\u{24EC}' // ⓬  [NEGATIVE CIRCLED NUMBER TWELVE]
        => Some("12"),
        '\u{2493}' // ⒓  [NUMBER TWELVE FULL STOP]
        => Some("12."),
        '\u{247F}' // ⑿  [PARENTHESIZED NUMBER TWELVE]
        => Some("(12)"),
        '\u{246C}' | // ⑬  [CIRCLED NUMBER THIRTEEN]
        '\u{24ED}' // ⓭  [NEGATIVE CIRCLED NUMBER THIRTEEN]
        => Some("13"),
        '\u{2494}' // ⒔  [NUMBER THIRTEEN FULL STOP]
        => Some("13."),
        '\u{2480}' // ⒀  [PARENTHESIZED NUMBER THIRTEEN]
        => Some("(13)"),
        '\u{246D}' | // ⑭  [CIRCLED NUMBER FOURTEEN]
        '\u{24EE}' // ⓮  [NEGATIVE CIRCLED NUMBER FOURTEEN]
        => Some("14"),
        '\u{2495}' // ⒕  [NUMBER FOURTEEN FULL STOP]
        => Some("14."),
        '\u{2481}' // ⒁  [PARENTHESIZED NUMBER FOURTEEN]
        => Some("(14)"),
        '\u{246E}' | // ⑮  [CIRCLED NUMBER FIFTEEN]
        '\u{24EF}' // ⓯  [NEGATIVE CIRCLED NUMBER FIFTEEN]
        => Some("15"),
        '\u{2496}' // ⒖  [NUMBER FIFTEEN FULL STOP]
        => Some("15."),
        '\u{2482}' // ⒂  [PARENTHESIZED NUMBER FIFTEEN]
        => Some("(15)"),
        '\u{246F}' | // ⑯  [CIRCLED NUMBER SIXTEEN]
        '\u{24F0}' // ⓰  [NEGATIVE CIRCLED NUMBER SIXTEEN]
        => Some("16"),
        '\u{2497}' // ⒗  [NUMBER SIXTEEN FULL STOP]
        => Some("16."),
        '\u{2483}' // ⒃  [PARENTHESIZED NUMBER SIXTEEN]
        => Some("(16)"),
        '\u{2470}' | // ⑰  [CIRCLED NUMBER SEVENTEEN]
        '\u{24F1}' // ⓱  [NEGATIVE CIRCLED NUMBER SEVENTEEN]
        => Some("17"),
        '\u{2498}' // ⒘  [NUMBER SEVENTEEN FULL STOP]
        => Some("17."),
        '\u{2484}' // ⒄  [PARENTHESIZED NUMBER SEVENTEEN]
        => Some("(17)"),
        '\u{2471}' | // ⑱  [CIRCLED NUMBER EIGHTEEN]
        '\u{24F2}' // ⓲  [NEGATIVE CIRCLED NUMBER EIGHTEEN]
        => Some("18"),
        '\u{2499}' // ⒙  [NUMBER EIGHTEEN FULL STOP]
        => Some("18."),
        '\u{2485}' // ⒅  [PARENTHESIZED NUMBER EIGHTEEN]
        => Some("(18)"),
        '\u{2472}' | // ⑲  [CIRCLED NUMBER NINETEEN]
        '\u{24F3}' // ⓳  [NEGATIVE CIRCLED NUMBER NINETEEN]
        => Some("19"),
        '\u{249A}' // ⒚  [NUMBER NINETEEN FULL STOP]
        => Some("19."),
        '\u{2486}' // ⒆  [PARENTHESIZED NUMBER NINETEEN]
        => Some("(19)"),
        '\u{2473}' | // ⑳  [CIRCLED NUMBER TWENTY]
        '\u{24F4}' // ⓴  [NEGATIVE CIRCLED NUMBER TWENTY]
        => Some("20"),
        '\u{249B}' // ⒛  [NUMBER TWENTY FULL STOP]
        => Some("20."),
        '\u{2487}' // ⒇  [PARENTHESIZED NUMBER TWENTY]
        => Some("(20)"),
        '\u{00AB}' | // «  [LEFT-POINTING DOUBLE ANGLE QUOTATION MARK]
        '\u{00BB}' | // »  [RIGHT-POINTING DOUBLE ANGLE QUOTATION MARK]
        '\u{201C}' | // “  [LEFT DOUBLE QUOTATION MARK]
        '\u{201D}' | // ”  [RIGHT DOUBLE QUOTATION MARK]
        '\u{201E}' | // „  [DOUBLE LOW-9 QUOTATION MARK]
        '\u{2033}' | // ″  [DOUBLE PRIME]
        '\u{2036}' | // ‶  [REVERSED DOUBLE PRIME]
        '\u{275D}' | // ❝  [HEAVY DOUBLE TURNED COMMA QUOTATION MARK ORNAMENT]
        '\u{275E}' | // ❞  [HEAVY DOUBLE COMMA QUOTATION MARK ORNAMENT]
        '\u{276E}' | // ❮  [HEAVY LEFT-POINTING ANGLE QUOTATION MARK ORNAMENT]
        '\u{276F}' | // ❯  [HEAVY RIGHT-POINTING ANGLE QUOTATION MARK ORNAMENT]
        '\u{FF02}' // ＂  [FULLWIDTH QUOTATION MARK]
        => Some("\""),
        '\u{2018}' | // ‘  [LEFT SINGLE QUOTATION MARK]
        '\u{2019}' | // ’  [RIGHT SINGLE QUOTATION MARK]
        '\u{201A}' | // ‚  [SINGLE LOW-9 QUOTATION MARK]
        '\u{201B}' | // ‛  [SINGLE HIGH-REVERSED-9 QUOTATION MARK]
        '\u{2032}' | // ′  [PRIME]
        '\u{2035}' | // ‵  [REVERSED PRIME]
        '\u{2039}' | // ‹  [SINGLE LEFT-POINTING ANGLE QUOTATION MARK]
        '\u{203A}' | // ›  [SINGLE RIGHT-POINTING ANGLE QUOTATION MARK]
        '\u{275B}' | // ❛  [HEAVY SINGLE TURNED COMMA QUOTATION MARK ORNAMENT]
        '\u{275C}' | // ❜  [HEAVY SINGLE COMMA QUOTATION MARK ORNAMENT]
        '\u{FF07}' // ＇  [FULLWIDTH APOSTROPHE]
        => Some("\'"),
        '\u{2010}' | // ‐  [HYPHEN]
        '\u{2011}' | // ‑  [NON-BREAKING HYPHEN]
        '\u{2012}' | // ‒  [FIGURE DASH]
        '\u{2013}' | // –  [EN DASH]
        '\u{2014}' | // —  [EM DASH]
        '\u{207B}' | // ⁻  [SUPERSCRIPT MINUS]
        '\u{208B}' | // ₋  [SUBSCRIPT MINUS]
        '\u{FF0D}' // －  [FULLWIDTH HYPHEN-MINUS]
        => Some("-"),
        '\u{2045}' | // ⁅  [LEFT SQUARE BRACKET WITH QUILL]
        '\u{2772}' | // ❲  [LIGHT LEFT TORTOISE SHELL BRACKET ORNAMENT]
        '\u{FF3B}' // ［  [FULLWIDTH LEFT SQUARE BRACKET]
        => Some("["),
        '\u{2046}' | // ⁆  [RIGHT SQUARE BRACKET WITH QUILL]
        '\u{2773}' | // ❳  [LIGHT RIGHT TORTOISE SHELL BRACKET ORNAMENT]
        '\u{FF3D}' // ］  [FULLWIDTH RIGHT SQUARE BRACKET]
        => Some("]"),
        '\u{207D}' | // ⁽  [SUPERSCRIPT LEFT PARENTHESIS]
        '\u{208D}' | // ₍  [SUBSCRIPT LEFT PARENTHESIS]
        '\u{2768}' | // ❨  [MEDIUM LEFT PARENTHESIS ORNAMENT]
        '\u{276A}' | // ❪  [MEDIUM FLATTENED LEFT PARENTHESIS ORNAMENT]
        '\u{FF08}' // （  [FULLWIDTH LEFT PARENTHESIS]
        => Some("("),
        '\u{2E28}' // ⸨  [LEFT DOUBLE PARENTHESIS]
        => Some("(("),
        '\u{207E}' | // ⁾  [SUPERSCRIPT RIGHT PARENTHESIS]
        '\u{208E}' | // ₎  [SUBSCRIPT RIGHT PARENTHESIS]
        '\u{2769}' | // ❩  [MEDIUM RIGHT PARENTHESIS ORNAMENT]
        '\u{276B}' | // ❫  [MEDIUM FLATTENED RIGHT PARENTHESIS ORNAMENT]
        '\u{FF09}' // ）  [FULLWIDTH RIGHT PARENTHESIS]
        => Some(")"),
        '\u{2E29}' // ⸩  [RIGHT DOUBLE PARENTHESIS]
        => Some("))"),
        '\u{276C}' | // ❬  [MEDIUM LEFT-POINTING ANGLE BRACKET ORNAMENT]
        '\u{2770}' | // ❰  [HEAVY LEFT-POINTING ANGLE BRACKET ORNAMENT]
        '\u{FF1C}' // ＜  [FULLWIDTH LESS-THAN SIGN]
        => Some("<"),
        '\u{276D}' | // ❭  [MEDIUM RIGHT-POINTING ANGLE BRACKET ORNAMENT]
        '\u{2771}' | // ❱  [HEAVY RIGHT-POINTING ANGLE BRACKET ORNAMENT]
        '\u{FF1E}' // ＞  [FULLWIDTH GREATER-THAN SIGN]
        => Some(">"),
        '\u{2774}' | // ❴  [MEDIUM LEFT CURLY BRACKET ORNAMENT]
        '\u{FF5B}' // ｛  [FULLWIDTH LEFT CURLY BRACKET]
        => Some("{"),
        '\u{2775}' | // ❵  [MEDIUM RIGHT CURLY BRACKET ORNAMENT]
        '\u{FF5D}' // ｝  [FULLWIDTH RIGHT CURLY BRACKET]
        => Some("}"),
        '\u{207A}' | // ⁺  [SUPERSCRIPT PLUS SIGN]
        '\u{208A}' | // ₊  [SUBSCRIPT PLUS SIGN]
        '\u{FF0B}' // ＋  [FULLWIDTH PLUS SIGN]
        => Some("+"),
        '\u{207C}' | // ⁼  [SUPERSCRIPT EQUALS SIGN]
        '\u{208C}' | // ₌  [SUBSCRIPT EQUALS SIGN]
        '\u{FF1D}' // ＝  [FULLWIDTH EQUALS SIGN]
        => Some("="),
        '\u{FF01}' // ！  [FULLWIDTH EXCLAMATION MARK]
        => Some("!"),
        '\u{203C}' // ‼  [DOUBLE EXCLAMATION MARK]
        => Some("!!"),
        '\u{2049}' // ⁉  [EXCLAMATION QUESTION MARK]
        => Some("!?"),
        '\u{FF03}' // ＃  [FULLWIDTH NUMBER SIGN]
        => Some("#"),
        '\u{FF04}' // ＄  [FULLWIDTH DOLLAR SIGN]
        => Some("$"),
        '\u{2052}' | // ⁒  [COMMERCIAL MINUS SIGN]
        '\u{FF05}' // ％  [FULLWIDTH PERCENT SIGN]
        => Some("%"),
        '\u{FF06}' // ＆  [FULLWIDTH AMPERSAND]
        => Some("&"),
        '\u{204E}' | // ⁎  [LOW ASTERISK]
        '\u{FF0A}' // ＊  [FULLWIDTH ASTERISK]
        => Some("*"),
        '\u{FF0C}' // ，  [FULLWIDTH COMMA]
        => Some("),"),
        '\u{FF0E}' // ．  [FULLWIDTH FULL STOP]
        => Some("."),
        '\u{2044}' | // ⁄  [FRACTION SLASH]
        '\u{FF0F}' // ／  [FULLWIDTH SOLIDUS]
        => Some("/"),
        '\u{FF1A}' // ：  [FULLWIDTH COLON]
        => Some(":"),
        '\u{204F}' | // ⁏  [REVERSED SEMICOLON]
        '\u{FF1B}' // ；  [FULLWIDTH SEMICOLON]
        => Some(";"),
        '\u{FF1F}' // ？  [FULLWIDTH QUESTION MARK]
        => Some("?"),
        '\u{2047}' // ⁇  [DOUBLE QUESTION MARK]
        => Some("??"),
        '\u{2048}' // ⁈  [QUESTION EXCLAMATION MARK]
        => Some("?!"),
        '\u{FF20}' // ＠  [FULLWIDTH COMMERCIAL AT]
        => Some("@"),
        '\u{FF3C}' // ＼  [FULLWIDTH REVERSE SOLIDUS]
        => Some("\\"),
        '\u{2038}' | // ‸  [CARET]
        '\u{FF3E}' // ＾  [FULLWIDTH CIRCUMFLEX ACCENT]
        => Some("^"),
        '\u{FF3F}' // ＿  [FULLWIDTH LOW LINE]
        => Some("_"),
        '\u{2053}' | // ⁓  [SWUNG DASH]
        '\u{FF5E}' // ～  [FULLWIDTH TILDE]
        => Some("~"),
        _ => None
    }
}

// https://github.com/apache/lucene-solr/blob/master/lucene/analysis/common/src/java/org/apache/lucene/analysis/miscellaneous/ASCIIFoldingFilter.java#L187
fn to_ascii(text: &mut String, output: &mut String) {
    output.clear();

    for c in text.chars() {
        if let Some(folded) = fold_non_ascii_char(c) {
            output.push_str(folded);
        } else {
            output.push(c);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::to_ascii;
    use tokenizer::AsciiFoldingFilter;
    use tokenizer::SimpleTokenizer;
    use tokenizer::RawTokenizer;
    use tokenizer::TokenStream;
    use tokenizer::Tokenizer;

    #[test]
    fn test_ascii_folding() {
        assert_eq!(folding_helper("Ràmon"), vec!["Ramon".to_string()]);
        assert_eq!(folding_helper("accentué"), vec!["accentue".to_string()]);
        assert_eq!(folding_helper("âäàéè"), vec!["aaaee".to_string()]);
    }

    #[test]
    fn test_no_change() {
        assert_eq!(folding_helper("Usagi"), vec!["Usagi".to_string()]);
    }

    fn folding_helper(text: &str) -> Vec<String> {
        let mut tokens = vec![];
        let mut token_stream = SimpleTokenizer
            .filter(AsciiFoldingFilter)
            .token_stream(text);
        while token_stream.advance() {
            let token_text = token_stream.token().text.clone();
            tokens.push(token_text);
        }
        tokens
    }

    fn folding_using_raw_tokenizer_helper(text: &str) -> String {
        let mut token_stream = RawTokenizer
            .filter(AsciiFoldingFilter)
            .token_stream(text);
        token_stream.advance();

        token_stream.token().text.clone()
    }

    fn repeat_token_helper(text: &str, number: usize) -> Vec<String> {
        let mut vec: Vec<String> = Vec::with_capacity(number);
        let mut i: usize = 0;
        while i < number {
            vec.push(text.to_string());
            i = i + 1;
        }

        vec
    }

    #[test]
    fn test_latin1_characters() {
        let latin1_string = "Des mot clés À LA CHAÎNE À Á Â Ã Ä Å Æ Ç È É Ê Ë Ì Í Î Ï Ĳ Ð Ñ
                   Ò Ó Ô Õ Ö Ø Œ Þ Ù Ú Û Ü Ý Ÿ à á â ã ä å æ ç è é ê ë ì í î ï ĳ
                   ð ñ ò ó ô õ ö ø œ ß þ ù ú û ü ý ÿ ﬁ ﬂ";
        let mut vec = vec![
            "Des".to_string(),
            "mot".to_string(),
            "cles".to_string(),
            "A".to_string(),
            "LA".to_string(),
            "CHAINE".to_string(),
        ];
        vec.append(&mut repeat_token_helper("A", 6));
        vec.append(&mut repeat_token_helper("AE", 1));
        vec.append(&mut repeat_token_helper("C", 1));
        vec.append(&mut repeat_token_helper("E", 4));
        vec.append(&mut repeat_token_helper("I", 4));
        vec.append(&mut repeat_token_helper("IJ", 1));
        vec.append(&mut repeat_token_helper("D", 1));
        vec.append(&mut repeat_token_helper("N", 1));
        vec.append(&mut repeat_token_helper("O", 6));
        vec.append(&mut repeat_token_helper("OE", 1));
        vec.append(&mut repeat_token_helper("TH", 1));
        vec.append(&mut repeat_token_helper("U", 4));
        vec.append(&mut repeat_token_helper("Y", 2));
        vec.append(&mut repeat_token_helper("a", 6));
        vec.append(&mut repeat_token_helper("ae", 1));
        vec.append(&mut repeat_token_helper("c", 1));
        vec.append(&mut repeat_token_helper("e", 4));
        vec.append(&mut repeat_token_helper("i", 4));
        vec.append(&mut repeat_token_helper("ij", 1));
        vec.append(&mut repeat_token_helper("d", 1));
        vec.append(&mut repeat_token_helper("n", 1));
        vec.append(&mut repeat_token_helper("o", 6));
        vec.append(&mut repeat_token_helper("oe", 1));
        vec.append(&mut repeat_token_helper("ss", 1));
        vec.append(&mut repeat_token_helper("th", 1));
        vec.append(&mut repeat_token_helper("u", 4));
        vec.append(&mut repeat_token_helper("y", 2));
        vec.append(&mut repeat_token_helper("fi", 1));
        vec.append(&mut repeat_token_helper("fl", 1));

        assert_eq!(folding_helper(latin1_string), vec);
    }

    #[test]
    fn test_unmodified_letters() {
        assert_eq!(
            folding_using_raw_tokenizer_helper("§ ¦ ¤ END"),
            "§ ¦ ¤ END".to_string()
        );
    }

    #[test]
    fn test_to_ascii() {
        let mut input = "Rámon".to_string();
        let mut buffer = String::new();
        to_ascii(&mut input, &mut buffer);
        assert_eq!("Ramon", buffer);
    }
}
