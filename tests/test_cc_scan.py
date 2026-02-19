import re

from src.data_sources.commoncrawl.cc_scan import (
    asd_disambiguated,
    compile_boilerplate_patterns,
    compile_patterns,
    extract_registered_domain,
    find_term_matches,
    is_boilerplate_density,
    is_boilerplate_listiness,
    is_boilerplate_signature,
    normalize_listiness_phrases,
)


def test_regex_matching_basic():
    terms = {
        "adhd_patterns": [r"\badhd\b"],
        "autism_patterns": [r"autism"],
    }
    patterns = compile_patterns(terms)
    text = "This discusses ADHD and autism in brief."
    matches = find_term_matches(text, patterns, None, 200)
    labels = {label for label, _ in matches}
    assert "adhd_patterns[0]" in labels
    assert "autism_patterns[0]" in labels


def test_asd_disambiguation_window():
    text = "ASD is mentioned here, and autism appears later nearby."
    asd_pattern = re.compile(r"\bASD\b", re.IGNORECASE)
    match = asd_pattern.search(text)
    assert match is not None
    assert asd_disambiguated(text, match.span(), window=100)

    text_far = "ASD is mentioned here. " + ("x" * 300) + " autism"
    match_far = asd_pattern.search(text_far)
    assert match_far is not None
    assert not asd_disambiguated(text_far, match_far.span(), window=50)


def test_registered_domain_extraction():
    url = "https://sub.example.co.uk/path"
    assert extract_registered_domain(url) == "example.co.uk"

    url2 = "http://www.example.com/page"
    assert extract_registered_domain(url2) == "example.com"

    assert extract_registered_domain(None) == ""


def test_boilerplate_signature_removes_snippet():
    patterns = compile_boilerplate_patterns([r"skip\s+to\s+content"])
    snippet = "Skip to content Toggle navigation Home"
    assert is_boilerplate_signature(snippet, patterns)


def test_boilerplate_checks_keep_normal_prose():
    patterns = compile_boilerplate_patterns([r"skip\s+to\s+content"])
    snippet = (
        "Autism research continues to improve screening and support options "
        "for children, families, and clinicians in routine care."
    )
    assert not is_boilerplate_signature(snippet, patterns)
    assert not is_boilerplate_density(snippet, min_snippet_words=8, min_alpha_ratio=0.45)


def test_boilerplate_listiness_detects_taxonomy_snippet():
    snippet = (
        "Resource center | Categories | Tags | Topics | A-Z | All conditions | More topics"
    )
    phrases = normalize_listiness_phrases(
        ["resource center", "categories", "tags", "topics", "a-z", "all conditions"]
    )
    thresholds = {
        "min_separator_density": 0.12,
        "max_sentence_punct": 1,
        "min_short_fragments": 4,
        "short_fragment_max_words": 3,
    }
    assert is_boilerplate_listiness(snippet, phrases, thresholds)


def test_boilerplate_listiness_keeps_substantive_snippet():
    snippet = (
        "The care team discusses autism screening methods and treatment planning "
        "in complete sentences with clear context and recommendations."
    )
    phrases = normalize_listiness_phrases(
        ["resource center", "categories", "tags", "topics", "a-z", "all conditions"]
    )
    thresholds = {
        "min_separator_density": 0.12,
        "max_sentence_punct": 1,
        "min_short_fragments": 4,
        "short_fragment_max_words": 3,
    }
    assert not is_boilerplate_listiness(snippet, phrases, thresholds)
