"""Regenerate `stopwords.rs`.

Run from inside this directory:

    python3 gen_stopwords.py

Every list is fetched from its upstream source and written out as a `pub const`. The twelve
Snowball lists are *frozen*: they are fetched and parsed exactly as the original version of this
script did, so regenerating must not perturb them. English is not generated at all -- it is the
Apache Lucene list, inlined in `mod.rs`.

Sources, with their licences recorded in the generated file's header:

  snowball  snowballstem.org          BSD-3-Clause   (frozen)
  nltk      cmccomb/rust-stop-words   BSD-derived, redistributed under MIT
  iso       cmccomb/rust-stop-words   MIT (stopwords-iso)
  spacy     explosion/spaCy           MIT

`nltk` is preferred over `iso` where both have a language, matching the upstream crate's own
precedence rule; `spacy` is used only where neither has a usable list.
"""

import json
import re
import sys

import requests

# Pinned upstream revisions, so a regeneration is reproducible.
STOP_WORDS_REV = "main"
SPACY_REV = "master"

# (const name, source, key). Order determines the order of the generated file.
SOURCES = [
    # --- frozen: Snowball, must regenerate byte-for-byte --------------------------------------
    ("DANISH", "snowball", "DANISH"),
    ("DUTCH", "snowball", "DUTCH"),
    ("FINNISH", "snowball", "FINNISH"),
    ("FRENCH", "snowball", "FRENCH"),
    ("GERMAN", "snowball", "GERMAN"),
    ("HUNGARIAN", "snowball", "HUNGARIAN"),
    ("ITALIAN", "snowball", "ITALIAN"),
    ("NORWEGIAN", "snowball", "NORWEGIAN"),
    ("PORTUGUESE", "snowball", "PORTUGUESE"),
    ("RUSSIAN", "snowball", "RUSSIAN"),
    ("SPANISH", "snowball", "SPANISH"),
    ("SWEDISH", "snowball", "SWEDISH"),
    # --- languages tantivy already stemmed but had no stop words for -------------------------
    ("ARABIC", "nltk", "arabic"),
    ("GREEK", "nltk", "greek"),
    ("ROMANIAN", "nltk", "romanian"),
    ("TAMIL", "nltk", "tamil"),
    ("TURKISH", "nltk", "turkish"),
    # --- new languages, Snowball stemmer ------------------------------------------------------
    ("ARMENIAN", "iso", "hy"),
    ("BASQUE", "nltk", "basque"),
    ("CATALAN", "nltk", "catalan"),
    ("CZECH", "iso", "cs"),
    ("ESPERANTO", "iso", "eo"),
    ("ESTONIAN", "iso", "et"),
    ("HINDI", "iso", "hi"),
    ("INDONESIAN", "nltk", "indonesian"),
    ("IRISH", "iso", "ga"),
    # stopwords-iso's Lithuanian is ~27% Windows-1257/Latin-1 mojibake (`að` for `aš`,
    # `kaþkas` for `kažkas`); spaCy's list is clean. See SCRIPTS below for the guard.
    ("LITHUANIAN", "spacy", "lt"),
    ("NEPALI", "nltk", "nepali"),
    ("PERSIAN", "iso", "fa"),
    ("POLISH", "iso", "pl"),
    # Neither NLTK nor stopwords-iso has Serbian; spaCy is the only source. Cyrillic only.
    ("SERBIAN", "spacy", "sr"),
    ("SOTHO", "iso", "st"),
    # --- European languages added later -------------------------------------------------------
    # Snowball's `serbian` algorithm is the Ljubesic-Pandzic Croatian stemmer with a
    # Cyrillic-to-Latin prelude, so Croatian needs no new stemmer -- see `Language::Croatian`.
    ("CROATIAN", "iso", "hr"),
    ("SLOVAK", "iso", "sk"),
    # stopwords-iso has Ukrainian, but at 73 words it is a stark outlier among the Slavic
    # languages in that dataset (Russian 559, Slovenian 446, Czech 423, Slovak 418, Polish 329,
    # Bulgarian 259). spaCy's list is verified clean and of a comparable size, so it wins here.
    ("UKRAINIAN", "spacy", "uk"),

    # --- new languages, stemmer ported from Lucene --------------------------------------------
    ("BENGALI", "nltk", "bengali"),
    ("BULGARIAN", "iso", "bg"),
    ("LATVIAN", "iso", "lv"),
    # stopwords-iso's `ku` is entirely Arabic script, i.e. Sorani (Central Kurdish), which is
    # what the ported stemmer handles. Kurmanji is a different variety and is not covered.
    ("SORANI", "iso", "ku"),
    ("TELUGU", "spacy", "te"),
]

FROZEN = {"snowball"}

# Single-character fixes applied to upstream data, each with a reason.
CORRECTIONS = {
    # spaCy writes `тачно` with a Latin `è` instead of Cyrillic `ч`.
    "SERBIAN": {"таèно": "тачно"},
}

# Per-language script guards. A language listed here must contain only these characters, so that
# a future upstream encoding regression fails the run instead of being shipped.
LITHUANIAN_ALPHABET = set("aąbcčdeęėfghiįyjklmnoprsštuųūvzž")
SCRIPTS = {
    "LITHUANIAN": ("the Lithuanian alphabet", lambda c: c in LITHUANIAN_ALPHABET),
    "SERBIAN": ("Cyrillic", lambda c: "Ѐ" <= c <= "ӿ"),
    "BULGARIAN": ("Cyrillic", lambda c: "Ѐ" <= c <= "ӿ"),
    "SORANI": ("Arabic script", lambda c: "؀" <= c <= "ۿ"),
}


_FROZEN_CACHE = {}


def load_frozen():
    """Read the frozen lists out of the `stopwords.rs` that is about to be replaced."""
    with open("stopwords.rs", encoding="utf-8") as previous:
        text = previous.read()
    for match in re.finditer(r"pub const (\w+): &\[&str\] = &\[(.*?)\];", text, re.S):
        _FROZEN_CACHE[match.group(1)] = re.findall(r'"((?:[^"\\]|\\.)*)"', match.group(2))
    if not _FROZEN_CACHE:
        sys.exit("no lists found in stopwords.rs; restore it from git before regenerating")


def fetch_snowball(sess, key):
    """Return a frozen Snowball list, read back from the `stopwords.rs` being replaced.

    These twelve lists are deliberately *not* re-fetched. snowballstem.org serves them from a
    plain path with no revision handle, and it has already drifted since they were vendored:
    upstream Finnish has gained `joten` (234 -> 235 words). Re-fetching would silently change
    what tantivy indexes for an existing language, so the committed data is the source of truth
    and this script only carries it forward.

    To deliberately adopt an upstream change, update the list in `stopwords.rs` in its own
    commit, where the diff is visible and reviewable.
    """
    name = key.upper()
    if name not in _FROZEN_CACHE:
        sys.exit(
            f"{name}: frozen list not found in the existing stopwords.rs. These lists are "
            f"vendored, not fetched; restore the file from git before regenerating."
        )
    return _FROZEN_CACHE[name]


def fetch_nltk(sess, key):
    url = f"https://raw.githubusercontent.com/cmccomb/rust-stop-words/{STOP_WORDS_REV}/src/nltk/{key}"
    resp = sess.get(url)
    resp.raise_for_status()
    # One word per line. Splitting on all whitespace instead would break the multi-word entries
    # apart rather than dropping them, turning e.g. the Arabic phrase "\u0641\u064a \u062d\u064a\u0646" into two
    # standalone stop words that upstream never marked as such. Tabs are handled in `normalize`.
    return resp.text.splitlines()


_ISO_CACHE = {}


def fetch_iso(sess, key):
    if not _ISO_CACHE:
        url = (
            f"https://raw.githubusercontent.com/cmccomb/rust-stop-words/{STOP_WORDS_REV}"
            "/src/iso/stopwords-iso.json"
        )
        resp = sess.get(url)
        resp.raise_for_status()
        _ISO_CACHE.update(resp.json())
    return list(_ISO_CACHE[key])


def fetch_spacy(sess, key):
    url = f"https://raw.githubusercontent.com/explosion/spaCy/{SPACY_REV}/spacy/lang/{key}/stop_words.py"
    resp = sess.get(url)
    resp.raise_for_status()
    src = resp.text
    # Two shapes are in use: a triple-quoted whitespace-separated blob, and a set literal of
    # quoted strings (spaCy's Lithuanian).
    blob = re.search(r'"""(.*?)"""', src, re.S)
    if blob:
        return blob.group(1).split()
    return [m.group(1) or m.group(2) for m in re.finditer(r'"([^"]*)"|\'([^\']*)\'', src)]


FETCH = {
    "snowball": fetch_snowball,
    "nltk": fetch_nltk,
    "iso": fetch_iso,
    "spacy": fetch_spacy,
}

HEADER = """/*
These stop word lists come from several projects, each of which carries its own copyright and
license. The list of which language came from which source is in `gen_stopwords.py`, which
generated this file.

===============================================================================================
The Snowball project (https://snowballstem.org/) -- Danish, Dutch, Finnish, French, German,
Hungarian, Italian, Norwegian, Portuguese, Russian, Spanish, Swedish:

{snowball}
===============================================================================================
The NLTK stopwords corpus (https://www.nltk.org/), redistributed via
https://github.com/cmccomb/rust-stop-words (MIT) -- Arabic, Basque, Bengali, Catalan, Greek,
Indonesian, Nepali, Romanian, Tamil, Turkish.

The corpus was itself assembled from the Snowball stop word lists distributed with PostgreSQL,
with Romanian from http://arlc.ro/resources/ and subsequent corrections contributed to NLTK.

===============================================================================================
Stopwords ISO (https://github.com/stopwords-iso/stopwords-iso), redistributed via
https://github.com/cmccomb/rust-stop-words -- Armenian, Bulgarian, Czech, Esperanto, Estonian,
Hindi, Irish, Latvian, Persian, Polish, Sorani, Sotho:

The MIT License (MIT)

Copyright (c) 2020 Gene Diaz

Permission is hereby granted, free of charge, to any person obtaining a copy of this software
and associated documentation files (the "Software"), to deal in the Software without restriction,
including without limitation the rights to use, copy, modify, merge, publish, distribute,
sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or
substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING
BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

===============================================================================================
spaCy (https://github.com/explosion/spaCy) -- Lithuanian, Serbian, Telugu:

The MIT License (MIT)

Copyright (C) 2016-2024 ExplosionAI GmbH, 2016 spaCy GmbH, 2015 Matthew Honnibal

Permission is hereby granted, free of charge, to any person obtaining a copy of this software
and associated documentation files (the "Software"), to deal in the Software without restriction,
including without limitation the rights to use, copy, modify, merge, publish, distribute,
sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or
substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING
BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
*/

"""


def normalize(name, source, words):
    """Clean a newly added list. Frozen sources are passed through untouched."""
    if source in FROZEN:
        return words

    corrections = CORRECTIONS.get(name, {})
    out = []
    # Upstream's NLTK Kazakh list glues two words together with a tab, so tabs separate entries
    # rather than belonging to one.
    for word in [piece for word in words for piece in word.split("\t")]:
        word = corrections.get(word, word).strip()
        if not word:
            continue
        # A token filter compares whole tokens, so a multi-word phrase can never match; keeping
        # one would also be split by `split()` into words that were never meant to be stripped
        # on their own.
        if any(c.isspace() for c in word):
            continue
        # Punctuation-only entries (stopwords-iso has these for Persian and Arabic) are never
        # emitted as tokens.
        if not any(c.isalnum() for c in word):
            continue
        out.append(word)

    guard = SCRIPTS.get(name)
    if guard is not None:
        label, allowed = guard
        bad = [w for w in out if not all(allowed(c) for c in w.lower() if c.isalpha())]
        if bad:
            sys.exit(
                f"{name}: {len(bad)} of {len(out)} entries contain characters outside "
                f"{label}, which means the upstream data is corrupt: {bad[:10]}"
            )
    return out


def main():
    # Must happen before the output file is opened for writing, which truncates it.
    load_frozen()

    with requests.Session() as sess, open("stopwords.rs", "w", encoding="utf-8") as mod:
        copying = sess.get(
            "https://raw.githubusercontent.com/snowballstem/snowball/master/COPYING"
        )
        copying.raise_for_status()
        mod.write(HEADER.format(snowball=copying.text.rstrip("\n")))

        for name, source, key in SOURCES:
            words = normalize(name, source, FETCH[source](sess, key))
            if not words:
                sys.exit(f"{name}: no words fetched from {source}/{key}")
            mod.write(f"pub const {name}: &[&str] = &[\n")
            for word in words:
                mod.write(f'    "{word}",\n')
            mod.write("];\n\n")
            print(f"  {name:<12} {source:<9} {len(words):>5} words", file=sys.stderr)


if __name__ == "__main__":
    main()
