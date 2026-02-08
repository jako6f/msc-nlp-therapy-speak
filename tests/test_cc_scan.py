import re

from src.data_sources.commoncrawl.cc_scan import (
    asd_disambiguated,
    compile_patterns,
    extract_registered_domain,
    find_term_matches,
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
