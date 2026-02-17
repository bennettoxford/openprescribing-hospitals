from viewer.utils import normalise_string, tokenize, coverage_score


class TestNormaliseString:
    def test_empty_string(self):
        assert normalise_string("") == ""
        assert normalise_string("   ") == ""

    def test_none_and_non_string(self):
        assert normalise_string(None) == ""
        assert normalise_string(123) == ""

    def test_lowercase(self):
        assert normalise_string("Paracetamol") == "paracetamol"

    def test_normalises_whitespace(self):
        assert normalise_string(" paracetamol  500mg") == "paracetamol 500mg"

    def test_removes_punctuation(self):
        assert normalise_string("paracetamol, 500mg.") == "paracetamol 500mg"
        assert normalise_string("drug-name") == "drugname"


class TestTokenize:
    def test_empty_string(self):
        assert tokenize("") == []
        assert tokenize("   ") == []

    def test_single_word(self):
        assert tokenize("paracetamol") == ["paracetamol"]

    def test_multiple_words(self):
        assert tokenize("paracetamol 500mg") == ["paracetamol", "500mg"]

    def test_normalises_before_splitting(self):
        assert tokenize("Paracetamol, 500mg.") == ["paracetamol", "500mg"]


class TestCoverageScore:
    def test_empty_search_tokens(self):
        assert coverage_score([], ["paracetamol", "500mg"]) == 1.0

    def test_all_tokens_match(self):
        assert coverage_score(["paracetamol", "500"], ["paracetamol", "500mg"]) == 1.0

    def test_partial_match(self):
        assert coverage_score(["paracetamol", "500", "tablet"], ["paracetamol", "500mg"]) == 2 / 3

    def test_no_match(self):
        assert coverage_score(["xyz"], ["paracetamol", "500mg"]) == 0.0

    def test_substring_match(self):
        assert coverage_score(["para"], ["paracetamol"]) == 1.0
        assert coverage_score(["500"], ["500mg"]) == 1.0
