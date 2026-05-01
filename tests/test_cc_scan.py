from src.data_sources.commoncrawl.cc_scan import (
    asd_disambiguated,
    compile_asd_pattern,
    compile_boilerplate_patterns,
    compile_patterns,
    extract_registered_domain,
    find_term_matches,
    is_boilerplate_directory_index,
    is_boilerplate_signature,
    normalize_listiness_phrases,
)


def _directory_index_thresholds():
    return {
        "score_threshold": 4,
        "min_separator_per_1k": 10.0,
        "min_short_fragment_ratio": 0.45,
        "min_short_fragments": 10,
        "short_fragment_min_chars": 3,
        "short_fragment_max_chars": 35,
        "max_sentence_terminators_per_1k": 2.5,
        "low_narrative_min_separator_per_1k": 8.0,
        "min_lexicon_hits": 3,
        "min_capitalized_token_ratio": 0.3,
        "min_single_letter_tokens": 6,
    }


def test_regex_matching_basic():
    terms = {
        "adhd_patterns": [{"name": "adhd", "pattern": r"\badhd\b"}],
        "autism_patterns": [{"name": "autism", "pattern": r"autism"}],
        "baseline_patterns": [{"name": "worry", "pattern": r"\bworry\b"}],
    }
    patterns = compile_patterns(terms)
    text = "This discusses ADHD and autism in brief."
    matches = find_term_matches(text, patterns, None, 200)
    labels = {term.matched_term for term, _ in matches}
    assert "adhd" in labels
    assert "autism" in labels

    pattern_meta = {term.matched_term: (term.term_role, term.term_group) for term in patterns}
    assert pattern_meta["adhd"] == ("target", "adhd")
    assert pattern_meta["worry"] == ("baseline", "baseline_negative")


def test_asd_disambiguation_window():
    text = "ASD is mentioned here, and autism appears later nearby."
    asd_pattern = compile_asd_pattern(
        {"asd_pattern": {"name": "asd", "pattern": r"\bASD\b"}}
    )
    assert asd_pattern is not None
    match = asd_pattern.pattern.search(text)
    assert match is not None
    assert asd_disambiguated(text, match.span(), window=100)

    text_far = "ASD is mentioned here. " + ("x" * 300) + " autism"
    match_far = asd_pattern.pattern.search(text_far)
    assert match_far is not None
    assert not asd_disambiguated(text_far, match_far.span(), window=50)


def test_registered_domain_extraction():
    assert extract_registered_domain("https://sub.example.co.uk/path") == "example.co.uk"
    assert extract_registered_domain("http://www.example.com/page") == "example.com"
    assert extract_registered_domain(None) == ""


def test_boilerplate_signature_removes_snippet():
    patterns = compile_boilerplate_patterns([r"skip\s+to\s+content"])
    snippet = "Skip to content Toggle navigation Home"
    assert is_boilerplate_signature(snippet, patterns)


def test_boilerplate_directory_index_detects_conditions_list():
    snippet = (
        "All conditions A-Z index: Symptoms, Diagnosis, Treatment, Diseases, Disorders, "
        "A to Z A, B, C, D, E, F, G, H, I, J, Acne, ADHD, Allergy, ALS, Anxiety, "
        "Arthritis, Asthma, Autism, Back Pain, Bipolar, Bronchitis, Depression, Diabetes"
    )
    lexicon = normalize_listiness_phrases(
        ["all conditions", "conditions a-z", "symptoms", "diagnosis", "treatment"]
    )
    assert is_boilerplate_directory_index(
        snippet, lexicon=lexicon, thresholds=_directory_index_thresholds()
    )


def test_boilerplate_directory_index_keeps_prose():
    snippet = (
        "This clinic article explains ADHD and autism assessments and discusses "
        "diagnosis criteria in complete narrative sentences for families."
    )
    lexicon = normalize_listiness_phrases(
        ["all conditions", "conditions a-z", "symptoms", "diagnosis", "treatment"]
    )
    assert not is_boilerplate_directory_index(
        snippet, lexicon=lexicon, thresholds=_directory_index_thresholds()
    )
