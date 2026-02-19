import pytest

from viewer.models import VMP, VTM, Ingredient, ATC


def _vmp_names(data):
    """Extract all VMP names from search results (VTMs and standalone VMPs)."""
    names = []
    for r in data["results"]:
        if r.get("vmps"):
            names.extend(v["name"] for v in r["vmps"])
        elif r.get("type") == "vmp":
            names.append(r["name"])
    return names


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

    def test_product_search_finds_by_name(
        self, client, vtm_paracetamol, ingredient_paracetamol
    ):
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
        vmp_codes = []
        for r in data["results"]:
            if r.get("vmps"):
                vmp_codes.extend(v["code"] for v in r["vmps"])
            elif r.get("type") == "vmp":
                vmp_codes.append(r["code"])
        assert "VMP999" in vmp_codes or any(
            r["code"] == "VMP999" for r in data["results"]
        )

    def test_product_search_multi_word(
        self, client, vtm_paracetamol, ingredient_paracetamol
    ):
        VMP.objects.create(
            code="VMP500",
            name="Paracetamol 500mg tablets",
            vtm=vtm_paracetamol,
        )

        response = client.get(
            "/api/search-products/", {"type": "product", "term": "paracetamol 500mg"}
        )
        assert response.status_code == 200
        data = response.json()
        assert len(data["results"]) >= 1

    def test_product_search_finds_via_ingredient_only(
        self, client, vtm_paracetamol, ingredient_paracetamol
    ):
        """Product matches only via ingredient, not VMP name."""
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
        """Product matches only via VTM name, not VMP name."""
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
        """VMP with no VTM appears as standalone result."""
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

    def test_product_search_orders_by_relevance(
        self, client, ingredient_paracetamol
    ):
        """Better matches (more tokens) rank higher than partial matches."""
        vtm_partial = VTM.objects.create(vtm="VTM500", name="Paracetamol")
        vtm_full = VTM.objects.create(vtm="VTM501", name="Paracetamol 500mg")
        VMP.objects.create(
            code="VMPA",
            name="Paracetamol",
            vtm=vtm_partial,
        ).ingredients.add(ingredient_paracetamol)
        VMP.objects.create(
            code="VMPB",
            name="Paracetamol 500mg tablets",
            vtm=vtm_full,
        ).ingredients.add(ingredient_paracetamol)

        response = client.get(
            "/api/search-products/", {"type": "product", "term": "paracetamol 500mg"}
        )
        assert response.status_code == 200
        data = response.json()
        assert len(data["results"]) >= 2
        first = data["results"][0]
        first_name = first.get("name") or (
            first["vmps"][0]["name"] if first.get("vmps") else ""
        )
        assert "500mg" in first_name

    def test_product_search_partial_match_returns_relevant_product(
        self, client, vtm_paracetamol, ingredient_paracetamol
    ):
        """Partial matches are included: results appear when at least one search
        token matches, even if others do not."""
        VMP.objects.create(
            code="VMPHC001",
            name="Paracetamol 500mg capsules",
            vtm=vtm_paracetamol,
        ).ingredients.add(ingredient_paracetamol)

        response = client.get(
            "/api/search-products/",
            {"type": "product", "term": "paracetamol granules"},
        )
        assert response.status_code == 200
        data = response.json()
        assert len(data["results"]) >= 1
        assert "Paracetamol 500mg capsules" in _vmp_names(data)

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
        response = client.get("/api/search-products/", {"term": "xyz"})
        assert response.status_code == 200
        assert "results" in response.json()

    def test_unknown_type_returns_empty_results(self, client):
        response = client.get(
            "/api/search-products/", {"type": "invalid", "term": "paracetamol"}
        )
        assert response.status_code == 200
        assert response.json()["results"] == []
