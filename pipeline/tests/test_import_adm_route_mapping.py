from pipeline.flows.import_adm_route_mapping import (
    create_adm_route_mapping,
)

class TestCreateAdmRouteMapping:
    def test_create_adm_route_mapping(self):
        """Test the route mapping creation with sample routes"""
        sample_routes = [
            'tablet.oral',
            'solution.intravenous',
            'implant.subcutaneous',
            'powder.cutaneous',  # Should be excluded
        ]
        
        result = create_adm_route_mapping(sample_routes)
        
        assert isinstance(result, dict)
        assert result['tablet.oral'] == 'O'
        assert result['solution.intravenous'] == 'P'
        assert result['implant.subcutaneous'] == 'implant'
        assert result['powder.cutaneous'] is None  # Excluded route

    def test_mapping_rules(self):
        """Test specific mapping rules for different route patterns"""
        test_routes = [
            'solution.oral',
            'solution.intravenous',
            'solution.intramuscular',
            'gel.vaginal',
            'suppository.rectal',
            'spray.nasal',
            'tablet.sublingual',
            'patch.transdermal',
        ]
        
        result = create_adm_route_mapping(test_routes)
        
        assert result['solution.oral'] == 'O'
        assert result['solution.intravenous'] == 'P'
        assert result['solution.intramuscular'] == 'P'
        assert result['gel.vaginal'] == 'V'
        assert result['suppository.rectal'] == 'R'
        assert result['spray.nasal'] == 'N'
        assert result['tablet.sublingual'] == 'SL'
        assert result['patch.transdermal'] == 'TD'

    def test_excluded_routes_handling(self):
        """Test that excluded routes are properly handled"""
        test_routes = [
            "solution.auricular",  # should be excluded
            "solution.cutaneous",  # should be excluded
            "gasinhalation.inhalation",  # exact exclude match
        ]
        result = create_adm_route_mapping(test_routes)
        assert all(v is None for v in result.values())
