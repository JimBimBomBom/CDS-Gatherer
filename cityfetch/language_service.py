"""
language_service.py
-------------------
Predefined list of Wikidata language codes for fetching city data.

These are BCP 47 language tags that Wikidata supports for city labels.
The list includes major world languages with substantial city coverage.
"""

from __future__ import annotations

# Comprehensive list of Wikidata language codes for city data
# Ordered by approximate number of cities with labels (descending)
LANGUAGE_CODES = [
    # Major global languages with extensive coverage
    "en",    # English
    "de",    # German
    "fr",    # French
    "es",    # Spanish
    "it",    # Italian
    "pt",    # Portuguese
    "pl",    # Polish
    "nl",    # Dutch
    "ru",    # Russian
    "ja",    # Japanese
    "zh",    # Chinese
    "ar",    # Arabic
    "ko",    # Korean
    "sv",    # Swedish
    "tr",    # Turkish
    "fi",    # Finnish
    "hu",    # Hungarian
    "no",    # Norwegian
    "cs",    # Czech
    "sk",    # Slovak
    "da",    # Danish
    "uk",    # Ukrainian
    "ro",    # Romanian
    "el",    # Greek
    "he",    # Hebrew
    "id",    # Indonesian
    "th",    # Thai
    "vi",    # Vietnamese
    "hi",    # Hindi
    "bn",    # Bengali
    "fa",    # Persian
    "ms",    # Malay
    "tl",    # Tagalog
    "ca",    # Catalan
    "sr",    # Serbian
    "hr",    # Croatian
    "bg",    # Bulgarian
    "sl",    # Slovenian
    "lt",    # Lithuanian
    "lv",    # Latvian
    "et",    # Estonian
    "is",    # Icelandic
    "ga",    # Irish
    "sq",    # Albanian
    "mk",    # Macedonian
    "be",    # Belarusian
    "ka",    # Georgian
    "hy",    # Armenian
    "az",    # Azerbaijani
    "kk",    # Kazakh
    "uz",    # Uzbek
    "ta",    # Tamil
    "te",    # Telugu
    "mr",    # Marathi
    "ur",    # Urdu
    "sw",    # Swahili
    "af",    # Afrikaans
    "eu",    # Basque
    "gl",    # Galician
    "cy",    # Welsh
    "br",    # Breton
    "lb",    # Luxembourgish
    "mt",    # Maltese
    "eo",    # Esperanto
    "la",    # Latin
]
