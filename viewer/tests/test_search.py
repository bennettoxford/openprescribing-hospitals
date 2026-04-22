import pytest

from viewer.models import ATC, Ingredient, VMP, VTM
from viewer.search import normalise_string, tokenize


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
        assert normalise_string("Co-proxamol") == "coproxamol"


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


def _vmp_names(data):
    names = []
    for r in data["results"]:
        if r.get("vmps"):
            names.extend(v["name"] for v in r["vmps"])
        elif r.get("type") == "vmp":
            names.append(r["name"])
    return names


def _result_names(data):
    return [result["name"] for result in data["results"]]


def _find_result(data, code, result_type):
    return next(
        result
        for result in data["results"]
        if result["code"] == code and result["type"] == result_type
    )


@pytest.fixture
def vtm_paracetamol():
    return VTM.objects.create(vtm="VTM001", name="Paracetamol")


@pytest.fixture
def ingredient_paracetamol():
    return Ingredient.objects.create(code="ING001", name="Paracetamol")


@pytest.fixture
def atc_paracetamol():
    return ATC.objects.create(
        code="N02BE01",
        name="Paracetamol",
        level_1="N",
        level_2="N02",
        level_3="N02B",
        level_4="N02BE",
        level_5="N02BE01",
    )


@pytest.mark.django_db
class TestSearchProducts:
    @pytest.mark.parametrize("search_type", ["product", "ingredient", "atc"])
    def test_empty_term_returns_empty_results(self, client, search_type):
        response = client.get(
            "/api/search-products/", {"type": search_type, "term": ""}
        )
        assert response.status_code == 200
        assert response.json()["results"] == []

    def test_product_search_finds_by_name(self, client, vtm_paracetamol, ingredient_paracetamol):
        vmp = VMP.objects.create(
            code="VMP001",
            name="Paracetamol 500mg tablets",
            vtm=vtm_paracetamol,
        )
        vmp.ingredients.add(ingredient_paracetamol)

        response = client.get(
            "/api/search-products/", {"type": "product", "term": "paracetamol"}
        )
        assert response.status_code == 200
        data = response.json()
        assert len(data["results"]) >= 1
        assert "Paracetamol 500mg tablets" in _vmp_names(data)

    def test_product_search_finds_by_code(self, client, vtm_paracetamol):
        VMP.objects.create(
            code="VMP999",
            name="Test Product XYZ",
            vtm=vtm_paracetamol,
        )

        response = client.get(
            "/api/search-products/", {"type": "product", "term": "VMP999"}
        )
        assert response.status_code == 200
        data = response.json()
        assert len(data["results"]) >= 1
        assert "VMP999" in {result["code"] for result in data["results"]} | {
            vmp["code"]
            for result in data["results"]
            for vmp in result.get("vmps", [])
        }

    def test_product_search_requires_all_query_terms(self, client, vtm_paracetamol, ingredient_paracetamol):
        VMP.objects.create(
            code="VMP500",
            name="Paracetamol 500mg capsules",
            vtm=vtm_paracetamol,
        ).ingredients.add(ingredient_paracetamol)

        response = client.get(
            "/api/search-products/",
            {"type": "product", "term": "paracetamol granules"},
        )

        assert response.status_code == 200
        assert response.json()["results"] == []

    def test_product_search_finds_via_ingredient_only(
        self, client, vtm_paracetamol, ingredient_paracetamol
    ):
        VMP.objects.create(
            code="VMPX",
            name="Generic analgesic tablets",
            vtm=vtm_paracetamol,
        ).ingredients.add(ingredient_paracetamol)

        response = client.get(
            "/api/search-products/", {"type": "product", "term": "paracetamol"}
        )
        assert response.status_code == 200
        data = response.json()
        assert len(data["results"]) >= 1
        assert "Generic analgesic tablets" in _vmp_names(data)

    def test_product_search_finds_via_vtm_only(self, client, vtm_paracetamol):
        VMP.objects.create(
            code="VMPY",
            name="500mg tablets",
            vtm=vtm_paracetamol,
        )

        response = client.get(
            "/api/search-products/", {"type": "product", "term": "paracetamol"}
        )
        assert response.status_code == 200
        data = response.json()
        assert len(data["results"]) >= 1
        assert "500mg tablets" in _vmp_names(data)

    def test_product_search_returns_standalone_vmp_without_vtm(
        self, client, ingredient_paracetamol
    ):
        VMP.objects.create(
            code="VMPZ",
            name="Orphan product 100mg",
            vtm=None,
        ).ingredients.add(ingredient_paracetamol)

        response = client.get(
            "/api/search-products/", {"type": "product", "term": "orphan"}
        )
        assert response.status_code == 200
        data = response.json()
        assert len(data["results"]) >= 1
        standalone = [r for r in data["results"] if r.get("type") == "vmp"]
        assert any(r["name"] == "Orphan product 100mg" for r in standalone)

    @pytest.mark.parametrize("term", ["co-amox", "co amox", "coamox"])
    def test_product_search_matches_hyphenated_spaced_and_compact_forms(self, client, term):
        vtm = VTM.objects.create(vtm="VTMCO1", name="Co-amoxiclav")
        VMP.objects.create(code="VMPCO1", name="Co-amoxiclav 125mg tablets", vtm=vtm)
        VMP.objects.create(code="VMPCO2", name="Co-amoxiclav 625mg tablets", vtm=vtm)

        response = client.get(
            "/api/search-products/", {"type": "product", "term": term}
        )

        assert response.status_code == 200
        data = response.json()
        grouped_result = _find_result(data, "VTMCO1", "vtm")
        assert grouped_result["name"] == "Co-amoxiclav"
        assert {vmp["code"] for vmp in grouped_result["vmps"]} == {"VMPCO1", "VMPCO2"}

    def test_product_search_ranks_visible_prefix_match_ahead_of_infix(self, client):
        VMP.objects.create(
            code="MOR001",
            name="Morphine sulfate 10mg tablets",
            vtm=None,
        )
        VMP.objects.create(
            code="APO001",
            name="Apomorphine 10mg tablets",
            vtm=None,
        )

        response = client.get(
            "/api/search-products/",
            {"type": "product", "term": "morphine"},
        )

        assert response.status_code == 200
        data = response.json()
        assert _result_names(data)[0] == "Morphine sulfate 10mg tablets"

    def test_product_search_ranks_label_prefix_ahead_of_infix_for_multi_word_query(self, client):
        omeprazole_vtm = VTM.objects.create(vtm="VTMOMP", name="Omeprazole")
        esomeprazole_vtm = VTM.objects.create(vtm="VTMESO", name="Esomeprazole")
        VMP.objects.create(
            code="OMP10D", name="Omeprazole 10mg dispersible tablets", vtm=omeprazole_vtm
        )
        VMP.objects.create(
            code="OMP10", name="Omeprazole 10mg tablets", vtm=omeprazole_vtm
        )
        VMP.objects.create(
            code="ESO20", name="Esomeprazole 20mg tablets", vtm=esomeprazole_vtm
        )

        response = client.get(
            "/api/search-products/",
            {"type": "product", "term": "omeprazole tablets"},
        )

        assert response.status_code == 200
        names = _result_names(response.json())
        omeprazole_idx = next(i for i, n in enumerate(names) if n.startswith("Omeprazole"))
        esomeprazole_idx = next(i for i, n in enumerate(names) if n.startswith("Esomeprazole"))
        assert omeprazole_idx < esomeprazole_idx

    def test_product_search_ranks_top_level_label_ahead_of_child_only_match(self, client):
        morphine_vtm = VTM.objects.create(vtm="VTMMOR", name="Morphine")
        opium_vtm = VTM.objects.create(vtm="VTMOPI", name="Opium")
        VMP.objects.create(code="MOR10", name="Morphine sulfate 10mg tablets", vtm=morphine_vtm)
        VMP.objects.create(code="OPI10", name="Morphine tincture", vtm=opium_vtm)

        response = client.get(
            "/api/search-products/",
            {"type": "product", "term": "morphine"},
        )

        assert response.status_code == 200
        data = response.json()
        assert _result_names(data)[0] == "Morphine"

    def test_product_search_groups_vtm_only_when_all_children_match(self, client):
        vtm = VTM.objects.create(vtm="VTMOM1", name="Omeprazole")
        VMP.objects.create(code="OMP20", name="Omeprazole 20mg capsules", vtm=vtm)
        VMP.objects.create(code="OMP40", name="Omeprazole 40mg capsules", vtm=vtm)

        response = client.get(
            "/api/search-products/",
            {"type": "product", "term": "omeprazole 20mg"},
        )

        assert response.status_code == 200
        data = response.json()
        assert [result["type"] for result in data["results"]] == ["vmp"]
        assert data["results"][0]["name"] == "Omeprazole 20mg capsules"

    def test_product_search_groups_vtm_when_all_children_match(self, client):
        vtm = VTM.objects.create(vtm="VTMCOA", name="Co-amoxiclav")
        VMP.objects.create(code="COA125", name="Co-amoxiclav 125mg tablets", vtm=vtm)
        VMP.objects.create(code="COA625", name="Co-amoxiclav 625mg tablets", vtm=vtm)

        response = client.get(
            "/api/search-products/",
            {"type": "product", "term": "co amox"},
        )

        assert response.status_code == 200
        data = response.json()
        assert [result["type"] for result in data["results"]] == ["vtm"]
        assert data["results"][0]["name"] == "Co-amoxiclav"
        assert len(data["results"][0]["vmps"]) == 2

    def test_ingredient_search_finds_by_name(
        self, client, ingredient_paracetamol, vtm_paracetamol
    ):
        vmp = VMP.objects.create(
            code="VMP003",
            name="Paracetamol 500mg",
            vtm=vtm_paracetamol,
        )
        vmp.ingredients.add(ingredient_paracetamol)

        response = client.get(
            "/api/search-products/", {"type": "ingredient", "term": "paracetamol"}
        )
        assert response.status_code == 200
        data = response.json()
        assert len(data["results"]) >= 1
        ing_result = next(r for r in data["results"] if r["name"] == "Paracetamol")
        assert ing_result is not None
        assert "vmp_count" in ing_result
        assert ing_result["vmp_count"] >= 1

    def test_atc_search_finds_by_code(self, client, atc_paracetamol):
        response = client.get(
            "/api/search-products/", {"type": "atc", "term": "N02BE01"}
        )
        assert response.status_code == 200
        data = response.json()
        assert len(data["results"]) >= 1
        assert any(r["code"] == "N02BE01" for r in data["results"])

    def test_atc_search_finds_by_name(self, client, atc_paracetamol):
        response = client.get(
            "/api/search-products/", {"type": "atc", "term": "paracetamol"}
        )
        assert response.status_code == 200
        data = response.json()
        assert len(data["results"]) >= 1


    def test_atc_result_has_expected_structure(self, client, atc_paracetamol):
        response = client.get(
            "/api/search-products/", {"type": "atc", "term": "N02BE"}
        )
        assert response.status_code == 200
        data = response.json()
        if data["results"]:
            item = data["results"][0]
            assert "code" in item
            assert "name" in item
            assert "type" in item
            assert item["type"] == "atc"
            assert "vmp_count" in item
            assert "hierarchy_path" in item

    def test_default_type_is_product(self, client):
        vtm = VTM.objects.create(vtm="VTMDEF", name="Paracetamol")
        VMP.objects.create(code="VMPDEF", name="Paracetamol 500mg tablets", vtm=vtm)

        response = client.get("/api/search-products/", {"term": "paracetamol"})
        assert response.status_code == 200
        assert "Paracetamol" in _result_names(response.json())

    def test_unknown_type_returns_empty_results(self, client):
        response = client.get(
            "/api/search-products/", {"type": "invalid", "term": "paracetamol"}
        )
        assert response.status_code == 200
        assert response.json()["results"] == []
