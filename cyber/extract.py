"""
Extract cybersecurity signals from job posting text.

Provides:
- is_cyber_role(): classify whether a job is cyber/infosec related
- extract_tech_mentions(): scan description for vendor/product mentions
"""

import re
from cyber.tech_taxonomy import TAXONOMY, CATEGORIES


# --- Cyber role classification ---

# Strong title signals: if any of these appear in the job title, it's a cyber role
_TITLE_KEYWORDS = [
    r"\bcyber\b",
    r"\bsoc\b",
    r"\bsecurity\s+(?:analyst|engineer|architect|consultant|manager|lead|officer|director|advisor|specialist|operations|monitoring)",
    r"\binfosec\b",
    r"\binformation\s+security\b",
    r"\bpenetration\s+test",
    r"\bpen\s*test",
    r"\bthreat\s+(?:intelligence|analyst|hunter|hunting)",
    r"\bincident\s+response\b",
    r"\bvulnerability\s+(?:analyst|manager|management|engineer)",
    r"\bsecurity\s+(?:clearance|vetting)\b",  # not a cyber role, exclude below
    r"\bciso\b",
    r"\bsiem\b",
    r"\bedr\b",
    r"\bndr\b",
    r"\bsoar\b",
    r"\bsecurity\s+operat",  # security operations / operational
    r"\bred\s+team",
    r"\bblue\s+team",
    r"\bpurple\s+team",
    r"\bforensic",
    r"\bmalware\s+analyst",
    r"\bcryptograph",
    r"\biam\s+(?:engineer|analyst|specialist|manager|architect)",
    r"\bidentity\s+(?:and\s+)?access\s+management",
]

# Weaker description signals: need multiple hits in the description body
_DESCRIPTION_KEYWORDS = [
    r"\bcyber\s*security\b",
    r"\bcybersecurity\b",
    r"\binformation\s+security\b",
    r"\binfosec\b",
    r"\bsecurity\s+operations\s+cent(?:er|re)\b",
    r"\bsoc\b",
    r"\bthreat\s+(?:intelligence|landscape|detection|hunting|actor)",
    r"\bincident\s+response\b",
    r"\bpenetration\s+test",
    r"\bvulnerability\s+(?:scanning|assessment|management)",
    r"\bsiem\b",
    r"\bedr\b",
    r"\bndr\b",
    r"\bsoar\b",
    r"\bfirewall\s+(?:rule|policy|management|config)",
    r"\bmalware\b",
    r"\bransomware\b",
    r"\bphishing\b",
    r"\biso\s*27001\b",
    r"\bcyber\s+essentials\b",
    r"\bnist\s+(?:csf|framework|800)",
    r"\bncsc\b",
    r"\bgovassure\b",
    r"\bsoc[\s-]*cmm\b",
    r"\bsecurity\s+monitoring\b",
    r"\blog\s+(?:management|analysis|monitoring)\b",
    r"\bendpoint\s+(?:detection|protection|security)\b",
    r"\bnetwork\s+(?:detection|security|monitoring)\b",
    r"\bzero\s+trust\b",
    r"\biam\b",
    r"\bprivileged\s+access\b",
    r"\bidentity\s+(?:and\s+)?access\b",
]

# Exclude: titles that contain "security" but aren't cyber roles
_TITLE_EXCLUDES = [
    r"\bsecurity\s+guard\b",
    r"\bsecurity\s+officer\b(?!\s+\(cyber)",  # physical security officer
    r"\bphysical\s+security\b",
    r"\bsecurity\s+clearance\s+(?:officer|admin|coordinator)",
    r"\bvetting\s+officer\b",
    r"\bsocial\s+security\b",
    r"\bsecurity\s+reception",
    r"\bdoor\s+security\b",
    r"\bnational\s+security\b(?!\s+cyber)",  # national security (policy) vs national security cyber
]

_compiled_title = [re.compile(p, re.IGNORECASE) for p in _TITLE_KEYWORDS]
_compiled_desc = [re.compile(p, re.IGNORECASE) for p in _DESCRIPTION_KEYWORDS]
_compiled_excludes = [re.compile(p, re.IGNORECASE) for p in _TITLE_EXCLUDES]


def is_cyber_role(title: str, description: str = "") -> bool:
    """
    Classify whether a job posting is cybersecurity-related.

    Uses title keywords (strong signal) and description keywords (weaker,
    needs multiple hits). Excludes physical security and vetting roles.
    """
    title = title or ""
    description = description or ""

    # Check exclusions first
    for pat in _compiled_excludes:
        if pat.search(title):
            return False

    # Strong signal: title keyword match
    for pat in _compiled_title:
        if pat.search(title):
            return True

    # Weaker signal: need 3+ distinct description keyword hits
    desc_hits = sum(1 for pat in _compiled_desc if pat.search(description))
    if desc_hits >= 3:
        return True

    return False


# --- Technology extraction ---

# Pre-compile taxonomy patterns for performance
_compiled_taxonomy: dict[str, list[tuple[str, re.Pattern]]] = {}

for _cat, _vendors in TAXONOMY.items():
    _entries = []
    for _vendor, _patterns in _vendors.items():
        for _pattern in _patterns:
            # Escape special regex chars in the pattern, then wrap in word boundaries
            escaped = re.escape(_pattern.strip())
            # Use word boundaries to avoid substring matches (e.g. "central" matching "entra")
            _entries.append((_vendor, re.compile(r"\b" + escaped + r"\b", re.IGNORECASE)))
    _compiled_taxonomy[_cat] = _entries


# --- Example-context detection ---
# Phrases that indicate tech is mentioned as an example/requirement,
# not necessarily confirming the org uses it.

_EXAMPLE_INDICATORS = re.compile(
    r"\b(?:"
    r"e\.?\s?g\.?\s*[,:]?\s*"           # e.g., / eg,
    r"|such\s+as\s+"                     # such as
    r"|tools?\s+like\s+"                 # tools like
    r"|platforms?\s+like\s+"             # platforms like
    r"|tooling\s+(?:like|such\s+as)\s+"  # tooling like / tooling such as
    r"|for\s+example\s+"                 # for example
    r"|familiarity\s+with\s+"            # familiarity with
    r"|experience\s+(?:of|with|in)\s+"   # experience with / of / in
    r"|knowledge\s+of\s+"               # knowledge of
    r")",
    re.IGNORECASE,
)

# Sentence boundaries that close an example window.
# Matches: period followed by space/end (real sentence end, not abbreviations),
# newline, semicolon, or closing parenthesis.
_SENTENCE_END = re.compile(r"\.(?:\s|$)|\n|;|\)")


def _find_example_spans(text: str) -> list[tuple[int, int]]:
    """Find character ranges where tech mentions are likely examples, not confirmed usage."""
    spans = []
    for m in _EXAMPLE_INDICATORS.finditer(text):
        indicator_end = m.end()
        # Search for sentence boundary AFTER the indicator text itself,
        # so periods inside "e.g." don't close the window immediately.
        rest = text[indicator_end:]
        end_match = _SENTENCE_END.search(rest)
        end = indicator_end + end_match.start() if end_match else min(indicator_end + 200, len(text))
        spans.append((m.start(), end))
    return spans


def _in_example_context(pos: int, spans: list[tuple[int, int]]) -> bool:
    """Check whether a character position falls within any example span."""
    for start, end in spans:
        if start <= pos <= end:
            return True
    return False


_SOC_TITLE_PATTERNS = [
    re.compile(p, re.IGNORECASE)
    for p in [
        r"\bsoc\b",
        r"\bsecurity\s+operat",           # security operations / operational
        r"\bsecurity\s+monitoring\b",
    ]
]

_SOC_DESC_PATTERNS = [
    re.compile(p, re.IGNORECASE)
    for p in [
        r"\bsecurity\s+operations?\s+cent(?:er|re)\b",
        r"\bsoc\s+(?:team|analyst|manager|engineer|lead|operations)\b",
    ]
]


def is_soc_role(title: str, description: str = "") -> bool:
    """
    Classify whether a cyber job is specifically SOC-related.

    Should only be called on jobs already identified as cyber roles.
    Uses tight title patterns (SOC, security operations) and
    description patterns (security operations centre, SOC team).
    """
    title = title or ""
    description = description or ""

    for pat in _SOC_TITLE_PATTERNS:
        if pat.search(title):
            return True

    for pat in _SOC_DESC_PATTERNS:
        if pat.search(description):
            return True

    return False


def _get_containing_span(pos: int, spans: list[tuple[int, int]]) -> tuple[int, int] | None:
    """Return the example span containing a position, or None."""
    for start, end in spans:
        if start <= pos <= end:
            return (start, end)
    return None


def _extract_sentence(text: str, pos: int, max_len: int = 200) -> str:
    """Extract the sentence containing position `pos`, trimmed to max_len."""
    # Search backwards for sentence start
    start = 0
    for i in range(pos - 1, -1, -1):
        if text[i] in "\n" or (text[i] == "." and i + 1 < len(text) and text[i + 1] == " "):
            start = i + 1
            break

    # Search forwards for sentence end
    end = len(text)
    for i in range(pos, len(text)):
        if text[i] == "\n" or (text[i] == "." and i + 1 < len(text) and text[i + 1] == " "):
            end = i + 1
            break

    sentence = text[start:end].strip()
    if len(sentence) > max_len:
        # Trim around the match position, keeping it centered
        offset = pos - start
        trim_start = max(0, offset - max_len // 2)
        trim_end = min(len(sentence), trim_start + max_len)
        sentence = ("..." if trim_start > 0 else "") + sentence[trim_start:trim_end].strip() + ("..." if trim_end < len(sentence) else "")

    return sentence


def extract_tech_mentions(text: str) -> dict[str, list[dict]]:
    """
    Scan text for cybersecurity vendor/product mentions with confidence tagging.

    Confidence levels:
    - "confirmed": mentioned outside any example/desirable context
    - "probable": sole vendor named in an example context (e.g. "experience
      with Splunk") — likely what the org actually uses
    - "example": one of multiple vendors listed in an example context
      (e.g. "tools such as Splunk, QRadar") — illustrative, not specific

    Returns a dict mapping each category to a list of
    {"vendor": str, "confidence": str, "snippet": str} dicts.
    Empty categories are included.
    """
    if not text:
        return {cat: [] for cat in CATEGORIES}

    example_spans = _find_example_spans(text)

    result = {}
    for cat in CATEGORIES:
        # First pass: find all vendor matches with positions
        found: dict[str, dict] = {}  # vendor -> {confidence, pos, snippet}
        for vendor, pattern in _compiled_taxonomy[cat]:
            if vendor in found:
                continue
            m = pattern.search(text)
            if m:
                in_example = _in_example_context(m.start(), example_spans)
                found[vendor] = {
                    "confidence": "example" if in_example else "confirmed",
                    "pos": m.start(),
                    "snippet": _extract_sentence(text, m.start()),
                }

        # Second pass: upgrade lone example vendors to "probable".
        # If only one vendor from this category appears in an example span,
        # it's likely the specific tool the org uses, not a generic example.
        # But NOT if the span text contains list connectors (& / and / or / ,)
        # which suggest multiple items even if we only recognise one.
        example_vendors = [v for v, d in found.items() if d["confidence"] == "example"]
        if example_vendors:
            for vendor in example_vendors:
                span = _get_containing_span(found[vendor]["pos"], example_spans)
                if span:
                    others_in_span = sum(
                        1 for v in example_vendors
                        if v != vendor and _get_containing_span(found[v]["pos"], example_spans) == span
                    )
                    if others_in_span == 0:
                        # Check for list connectors in the span text
                        span_text = text[span[0]:span[1]]
                        has_list_connector = bool(re.search(
                            r'\s+(?:&|and|or)\s+|,\s*\w', span_text, re.IGNORECASE
                        ))
                        if not has_list_connector:
                            found[vendor]["confidence"] = "probable"

        result[cat] = sorted(
            [{"vendor": v, "confidence": d["confidence"], "snippet": d["snippet"]} for v, d in found.items()],
            key=lambda d: d["vendor"],
        )

    return result
