import pytest
import pandas as pd

from unittest.mock import patch
from pipeline.flows.load_vmp_vtm import (
    extract_vmp_data,
    extract_who_routes,
    extract_route_mapping,
    load_vtms,
    load_who_routes,
    load_ingredients,
    validate_atcs,
    load_ont_form_routes,
    load_vmps,
    load_vmp_ingredient_strengths,
)
from viewer.models import VMP, VTM, Ingredient, WHORoute, ATC, OntFormRoute, VMPIngredientStrength


@pytest.fixture
def sample_vmp_data():
    return pd.DataFrame(
        {
            "vmp_code": ["12345", "67890"],
            "vmp_name": ["Test Drug 1", "Test Drug 2"],
            "vtm_code": ["VTM123", "VTM456"],
            "vtm_name": ["Test VTM 1", "Test VTM 2"],
            "bnf_code": ["0101010", "0101020"],
            "df_ind": ["1", "2"],
            "udfs": [5.0, 10.0],
            "udfs_uom": ["mg", "ml"],
            "unit_dose_uom": ["tablet", "ml"],
            "ingredients": [
                [
                    {
                        "ingredient_code": "ING123",
                        "ingredient_name": "Ingredient 1",
                        "strnt_nmrtr_val": 500.0,
                        "strnt_nmrtr_uom_name": "mg",
                        "strnt_dnmtr_val": 1.0,
                        "strnt_dnmtr_uom_name": "tablet",
                        "basis_of_strength_type": 1,
                        "basis_of_strength_name": "Ingredient 1"
                    },
                    {
                        "ingredient_code": "ING456",
                        "ingredient_name": "Ingredient 2",
                        "strnt_nmrtr_val": 250.0,
                        "strnt_nmrtr_uom_name": "mg",
                        "strnt_dnmtr_val": 1.0,
                        "strnt_dnmtr_uom_name": "tablet",
                        "basis_of_strength_type": 1,
                        "basis_of_strength_name": "Ingredient 2"
                    },
                ],
                [
                    {
                        "ingredient_code": "ING789",
                        "ingredient_name": "Ingredient 3",
                        "strnt_nmrtr_val": 100.0,
                        "strnt_nmrtr_uom_name": "mg",
                        "strnt_dnmtr_val": 5.0,
                        "strnt_dnmtr_uom_name": "ml",
                        "basis_of_strength_type": 2,
                        "basis_of_strength_name": "Ingredient 3 Base"
                    }
                ],
            ],
            "ont_form_routes": [
                [{"route_code": "RT1", "route_name": "tablet.oral"}],
                [{"route_code": "RT2", "route_name": "solution.intravenous"}],
            ],
            "atcs": [[{"atc_code": "A01AA01"}], [{"atc_code": "B01AB01"}]],
        }
    )


@pytest.fixture
def sample_who_routes_data():
    return pd.DataFrame(
        {"who_route_code": ["O", "P"], "who_route_description": ["Oral", "Parenteral"]}
    )


@pytest.fixture
def sample_route_mapping_data():
    return pd.DataFrame(
        {
            "dmd_ontformroute": ["solution.intravenous", "tablet.oral"],
            "who_route": ["P", "O"],
        }
    )


class TestLoadVMPVTM:
    @patch("pipeline.flows.load_vmp_vtm.fetch_table_data_from_bq")
    def test_extract_vmp_data(self, mock_fetch, sample_vmp_data):

        mock_fetch.return_value = sample_vmp_data

        result = extract_vmp_data()

        mock_fetch.assert_called_once()
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2
        assert all(
            col in result.columns
            for col in [
                "vmp_code",
                "vmp_name",
                "vtm_code",
                "vtm_name",
                "ingredients",
                "ont_form_routes",
                "atcs",
                "df_ind",
                "udfs",
                "udfs_uom",
                "unit_dose_uom",
            ]
        )

    @patch("pipeline.flows.load_vmp_vtm.fetch_table_data_from_bq")
    def test_extract_who_routes(self, mock_fetch, sample_who_routes_data):

        mock_fetch.return_value = sample_who_routes_data

        result = extract_who_routes()

        mock_fetch.assert_called_once()
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2

    @patch("pipeline.flows.load_vmp_vtm.fetch_table_data_from_bq")
    def test_extract_route_mapping(self, mock_fetch, sample_route_mapping_data):

        mock_fetch.return_value = sample_route_mapping_data

        result = extract_route_mapping()

        mock_fetch.assert_called_once()
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2

    @pytest.mark.django_db
    def test_load_vtms(self, sample_vmp_data):

        result = load_vtms(sample_vmp_data)

        assert isinstance(result, dict)
        assert len(result) == 2

        vtms = VTM.objects.all()
        assert vtms.count() == 2

        vtm = vtms.get(vtm="VTM123")
        assert vtm.name == "Test VTM 1"

    @pytest.mark.django_db
    def test_load_who_routes(self, sample_who_routes_data):

        result = load_who_routes(sample_who_routes_data)

        assert isinstance(result, dict)
        assert len(result) == 2

        routes = WHORoute.objects.all()
        assert routes.count() == 2

        route = routes.get(code="O")
        assert route.name == "Oral"

    @pytest.mark.django_db
    def test_load_ingredients(self, sample_vmp_data):

        result = load_ingredients(sample_vmp_data)

        assert isinstance(result, dict)
        assert len(result) == 3

        ingredients = Ingredient.objects.all()
        assert ingredients.count() == 3

        ing = ingredients.get(code="ING123")
        assert ing.name == "Ingredient 1"

    @pytest.mark.django_db
    def test_validate_atcs(self, sample_vmp_data):

        ATC.objects.create(code="A01AA01", name="Test ATC 1")
        ATC.objects.create(code="B01AB01", name="Test ATC 2")

        result = validate_atcs(sample_vmp_data)

        assert isinstance(result, dict)
        assert len(result) == 2
        assert "A01AA01" in result
        assert "B01AB01" in result

    @pytest.mark.django_db
    def test_load_ont_form_routes(self, sample_vmp_data, sample_route_mapping_data):

        who_routes = WHORoute.objects.bulk_create(
            [WHORoute(code="O", name="Oral"), WHORoute(code="P", name="Parenteral")]
        )
        who_route_mapping = {route.code: route.id for route in who_routes}

        result = load_ont_form_routes(
            sample_vmp_data, sample_route_mapping_data, who_route_mapping
        )

        assert isinstance(result, dict)
        assert len(result) == 2

        routes = OntFormRoute.objects.all()
        assert routes.count() == 2

        tablet_route = routes.get(name="tablet.oral")
        assert tablet_route.who_route.code == "O"

        solution_route = routes.get(name="solution.intravenous")
        assert solution_route.who_route.code == "P"

    @pytest.mark.django_db
    def test_load_vmps_with_relationships(self, sample_vmp_data):

        vtms = VTM.objects.bulk_create(
            [VTM(vtm="VTM123", name="Test VTM 1"), VTM(vtm="VTM456", name="Test VTM 2")]
        )
        vtm_mapping = {vtm.vtm: vtm.id for vtm in vtms}

        ingredients = Ingredient.objects.bulk_create(
            [
                Ingredient(code="ING123", name="Ingredient 1"),
                Ingredient(code="ING456", name="Ingredient 2"),
                Ingredient(code="ING789", name="Ingredient 3"),
            ]
        )
        ingredient_mapping = {ing.code: ing.id for ing in ingredients}

        atcs = ATC.objects.bulk_create(
            [
                ATC(code="A01AA01", name="Test ATC 1"),
                ATC(code="B01AB01", name="Test ATC 2"),
            ]
        )
        atc_mapping = {atc.code: atc.id for atc in atcs}

        who_routes = WHORoute.objects.bulk_create(
            [WHORoute(code="O", name="Oral"), WHORoute(code="P", name="Parenteral")]
        )

        ont_form_routes = OntFormRoute.objects.bulk_create(
            [
                OntFormRoute(name="tablet.oral", who_route=who_routes[0]),
                OntFormRoute(name="solution.intravenous", who_route=who_routes[1]),
            ]
        )
        ont_form_route_mapping = {route.name: route.id for route in ont_form_routes}

        load_vmps(
            sample_vmp_data,
            vtm_mapping,
            ingredient_mapping,
            atc_mapping,
            ont_form_route_mapping,
        )

        vmps = VMP.objects.all()
        assert vmps.count() == 2

        vmp1 = vmps.get(code="12345")
        assert vmp1.name == "Test Drug 1"
        assert vmp1.vtm.vtm == "VTM123"
        assert vmp1.bnf_code == "0101010"
        assert vmp1.df_ind == "1"
        assert vmp1.udfs == 5.0
        assert vmp1.udfs_uom == "mg"
        assert vmp1.unit_dose_uom == "tablet"
        assert vmp1.ingredients.count() == 2
        assert vmp1.ont_form_routes.count() == 1
        assert vmp1.atcs.count() == 1
        assert vmp1.who_routes.count() == 1

        assert "Ingredient 1" in [i.name for i in vmp1.ingredients.all()]
        assert "tablet.oral" in [r.name for r in vmp1.ont_form_routes.all()]
        assert "A01AA01" in [a.code for a in vmp1.atcs.all()]

        vmp2 = vmps.get(code="67890")
        assert vmp2.df_ind == "2"
        assert vmp2.udfs == 10.0

    @pytest.mark.django_db
    def test_load_vmp_ingredient_strengths(self, sample_vmp_data):
        
        vtms = VTM.objects.bulk_create(
            [VTM(vtm="VTM123", name="Test VTM 1"), VTM(vtm="VTM456", name="Test VTM 2")]
        )
        
        ingredients = Ingredient.objects.bulk_create(
            [
                Ingredient(code="ING123", name="Ingredient 1"),
                Ingredient(code="ING456", name="Ingredient 2"),
                Ingredient(code="ING789", name="Ingredient 3"),
            ]
        )
        ingredient_mapping = {ing.code: ing.id for ing in ingredients}
        
        VMP.objects.bulk_create(
            [
                VMP(code="12345", name="Test Drug 1", vtm=vtms[0]),
                VMP(code="67890", name="Test Drug 2", vtm=vtms[1]),
            ]
        )
        
        load_vmp_ingredient_strengths(sample_vmp_data, ingredient_mapping)
        
        strengths = VMPIngredientStrength.objects.all()
        assert strengths.count() == 3  # 2 ingredients for first VMP, 1 for second
        
        strength1 = VMPIngredientStrength.objects.get(
            vmp__code="12345", 
            ingredient__code="ING123"
        )
        assert strength1.strnt_nmrtr_val == 500.0
        assert strength1.strnt_nmrtr_uom_name == "mg"
        assert strength1.strnt_dnmtr_val == 1.0
        assert strength1.strnt_dnmtr_uom_name == "tablet"
        assert strength1.basis_of_strength_type == 1
        assert strength1.basis_of_strength_name == "Ingredient 1"
        
        strength3 = VMPIngredientStrength.objects.get(
            vmp__code="67890", 
            ingredient__code="ING789"
        )
        assert strength3.strnt_nmrtr_val == 100.0
        assert strength3.strnt_nmrtr_uom_name == "mg"
        assert strength3.strnt_dnmtr_val == 5.0
        assert strength3.strnt_dnmtr_uom_name == "ml"
        assert strength3.basis_of_strength_type == 2
        assert strength3.basis_of_strength_name == "Ingredient 3 Base"
