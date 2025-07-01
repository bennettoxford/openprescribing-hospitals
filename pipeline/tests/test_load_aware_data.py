import pytest
import pandas as pd
from unittest.mock import patch

from pipeline.flows.load_aware_data import (
    extract_aware_data,
    load_aware_antibiotics,
    validate_existing_vmps_vtms,
    load_aware_vmp_mappings,
)
from viewer.models import AWAREAntibiotic, AWAREVMPMapping, VMP, VTM


@pytest.fixture
def sample_aware_data():
    return pd.DataFrame({
        "Antibiotic": [
            "Amoxicillin",
            "Amoxicillin", 
            "Azithromycin",
            "Ceftriaxone"
        ],
        "vtm_nm": [
            "Amoxicillin",
            "Amoxicillin",
            "Azithromycin", 
            "Ceftriaxone"
        ],
        "vtm_id": [12345, 12345, 67890, 11111],
        "vmp_nm": [
            "Amoxicillin 250mg capsules",
            "Amoxicillin 500mg capsules",
            "Azithromycin 250mg tablets",
            "Ceftriaxone 1g injection"
        ],
        "vmp_id": [98765, 98766, 54321, 22222],
        "aware_2019": ["Access", "Access", "Watch", "Watch"],
        "aware_2024": ["Access", "Access", "Watch", "Reserve"],
        "vtm_id_updated": [False, False, False, True],
        "vmp_id_updated": [False, False, True, False]
    })


@pytest.fixture
def sample_vtms():
    return [
        {"vtm": "12345", "name": "Amoxicillin"},
        {"vtm": "67890", "name": "Azithromycin"},
        {"vtm": "11111", "name": "Ceftriaxone"}
    ]


@pytest.fixture
def sample_vmps():
    return [
        {"code": "98765", "name": "Amoxicillin 250mg capsules"},
        {"code": "98766", "name": "Amoxicillin 500mg capsules"},
        {"code": "54321", "name": "Azithromycin 250mg tablets"},
        {"code": "22222", "name": "Ceftriaxone 1g injection"}
    ]


class TestLoadAWaReData:
    
    @patch("pipeline.flows.load_aware_data.fetch_table_data_from_bq")
    def test_extract_aware_data(self, mock_fetch, sample_aware_data):
        """Test extraction of AWaRe data from BigQuery"""
        mock_fetch.return_value = sample_aware_data
        
        result = extract_aware_data()
        
        mock_fetch.assert_called_once()
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 4
        assert all(
            col in result.columns
            for col in [
                "Antibiotic", "vtm_nm", "vtm_id", "vmp_nm", "vmp_id", 
                "aware_2019", "aware_2024"
            ]
        )

    @pytest.mark.django_db
    def test_load_aware_antibiotics_empty_database(self, sample_aware_data):
        """Test loading antibiotics into empty database"""
        result = load_aware_antibiotics(sample_aware_data)
        
        assert isinstance(result, dict)
        assert len(result) == 3  # 3 unique antibiotics
        
        antibiotics = AWAREAntibiotic.objects.all()
        assert antibiotics.count() == 3
        
        amoxicillin = antibiotics.get(name="Amoxicillin")
        assert amoxicillin.aware_2019 == "Access"
        assert amoxicillin.aware_2024 == "Access"
        
        azithromycin = antibiotics.get(name="Azithromycin")
        assert azithromycin.aware_2019 == "Watch"
        assert azithromycin.aware_2024 == "Watch"
        
        ceftriaxone = antibiotics.get(name="Ceftriaxone")
        assert ceftriaxone.aware_2019 == "Watch"
        assert ceftriaxone.aware_2024 == "Reserve"

    @pytest.mark.django_db
    def test_load_aware_antibiotics_replaces_existing(self, sample_aware_data):
        """Test that loading antibiotics replaces existing records"""

        AWAREAntibiotic.objects.create(
            name="Existing Antibiotic",
            aware_2019="Access",
            aware_2024="Watch"
        )
        
        assert AWAREAntibiotic.objects.count() == 1
        
        result = load_aware_antibiotics(sample_aware_data)
        
        assert AWAREAntibiotic.objects.count() == 3
        assert not AWAREAntibiotic.objects.filter(name="Existing Antibiotic").exists()
        assert AWAREAntibiotic.objects.filter(name="Amoxicillin").exists()

    @pytest.mark.django_db
    def test_validate_existing_vmps_vtms_all_exist(self, sample_aware_data, sample_vtms, sample_vmps):
        """Test validation when all VMPs and VTMs exist"""
 
        vtm_objects = []
        for vtm_data in sample_vtms:
            vtm_objects.append(VTM(vtm=vtm_data["vtm"], name=vtm_data["name"]))
        VTM.objects.bulk_create(vtm_objects)
        
        vmp_objects = []
        for i, vmp_data in enumerate(sample_vmps):
            vmp_objects.append(VMP(
                code=vmp_data["code"], 
                name=vmp_data["name"],
                vtm=vtm_objects[i % len(vtm_objects)]
            ))
        VMP.objects.bulk_create(vmp_objects)
        
        vmp_mapping, vtm_mapping = validate_existing_vmps_vtms(sample_aware_data)
        
        assert isinstance(vmp_mapping, dict)
        assert isinstance(vtm_mapping, dict)
        assert len(vmp_mapping) == 4  # All 4 VMPs should be found
        assert len(vtm_mapping) == 3  # All 3 VTMs should be found
        
        assert "98765" in vmp_mapping
        assert "12345" in vtm_mapping

    @pytest.mark.django_db
    def test_validate_existing_vmps_vtms_some_missing(self, sample_aware_data):
        """Test validation when some VMPs and VTMs are missing"""
        vtm = VTM.objects.create(vtm="12345", name="Amoxicillin")
        VMP.objects.create(code="98765", name="Amoxicillin 250mg capsules", vtm=vtm)
        
        vmp_mapping, vtm_mapping = validate_existing_vmps_vtms(sample_aware_data)
        
        assert len(vmp_mapping) == 1  # Only 1 VMP exists
        assert len(vtm_mapping) == 1  # Only 1 VTM exists
        assert "98765" in vmp_mapping
        assert "12345" in vtm_mapping

    @pytest.mark.django_db
    def test_load_aware_vmp_mappings_success(self, sample_aware_data):
        """Test successful loading of VMP mappings"""

        vtm1 = VTM.objects.create(vtm="12345", name="Amoxicillin")
        vtm2 = VTM.objects.create(vtm="67890", name="Azithromycin")
        vtm3 = VTM.objects.create(vtm="11111", name="Ceftriaxone")
        
        vmp1 = VMP.objects.create(code="98765", name="Amoxicillin 250mg capsules", vtm=vtm1)
        vmp2 = VMP.objects.create(code="98766", name="Amoxicillin 500mg capsules", vtm=vtm1)
        vmp3 = VMP.objects.create(code="54321", name="Azithromycin 250mg tablets", vtm=vtm2)
        vmp4 = VMP.objects.create(code="22222", name="Ceftriaxone 1g injection", vtm=vtm3)
        
        antibiotic1 = AWAREAntibiotic.objects.create(name="Amoxicillin", aware_2019="Access", aware_2024="Access")
        antibiotic2 = AWAREAntibiotic.objects.create(name="Azithromycin", aware_2019="Watch", aware_2024="Watch")
        antibiotic3 = AWAREAntibiotic.objects.create(name="Ceftriaxone", aware_2019="Watch", aware_2024="Reserve")
        
        # Create mappings
        antibiotic_mapping = {
            "Amoxicillin": antibiotic1.id,
            "Azithromycin": antibiotic2.id,
            "Ceftriaxone": antibiotic3.id
        }
        vmp_mapping = {
            "98765": vmp1.id,
            "98766": vmp2.id,
            "54321": vmp3.id,
            "22222": vmp4.id
        }
        vtm_mapping = {
            "12345": vtm1.id,
            "67890": vtm2.id,
            "11111": vtm3.id
        }
        
        load_aware_vmp_mappings(sample_aware_data, antibiotic_mapping, vmp_mapping, vtm_mapping)
        
        mappings = AWAREVMPMapping.objects.all()
        assert mappings.count() == 4

        amox_mapping = AWAREVMPMapping.objects.get(
            aware_antibiotic=antibiotic1,
            vmp=vmp1
        )
        assert amox_mapping.vtm == vtm1

    @pytest.mark.django_db
    def test_load_aware_vmp_mappings_with_missing_references(self, sample_aware_data):
        """Test VMP mapping loading when some references are missing"""
 
        vtm1 = VTM.objects.create(vtm="12345", name="Amoxicillin")
        vmp1 = VMP.objects.create(code="98765", name="Amoxicillin 250mg capsules", vtm=vtm1)
        antibiotic1 = AWAREAntibiotic.objects.create(name="Amoxicillin", aware_2019="Access", aware_2024="Access")
        
        antibiotic_mapping = {"Amoxicillin": antibiotic1.id}
        vmp_mapping = {"98765": vmp1.id}  # Missing other VMPs
        vtm_mapping = {"12345": vtm1.id}  # Missing other VTMs
        
        load_aware_vmp_mappings(sample_aware_data, antibiotic_mapping, vmp_mapping, vtm_mapping)
        
        # Should only create 1 mapping (for the complete set of references)
        mappings = AWAREVMPMapping.objects.all()
        assert mappings.count() == 1
        
        mapping = mappings.first()
        assert mapping.aware_antibiotic == antibiotic1
        assert mapping.vmp == vmp1
        assert mapping.vtm == vtm1

    @pytest.mark.django_db
    def test_load_aware_vmp_mappings_removes_duplicates(self):
        """Test that duplicate mappings are removed"""

        data = pd.DataFrame({
            "Antibiotic": ["Amoxicillin", "Amoxicillin"],
            "vmp_id": [98765, 98765],  # Same VMP
            "vtm_id": [12345, 12345]   # Same VTM
        })
        
        vtm = VTM.objects.create(vtm="12345", name="Amoxicillin")
        vmp = VMP.objects.create(code="98765", name="Amoxicillin 250mg capsules", vtm=vtm)
        antibiotic = AWAREAntibiotic.objects.create(name="Amoxicillin")
        
        antibiotic_mapping = {"Amoxicillin": antibiotic.id}
        vmp_mapping = {"98765": vmp.id}
        vtm_mapping = {"12345": vtm.id}
        
        load_aware_vmp_mappings(data, antibiotic_mapping, vmp_mapping, vtm_mapping)
        
        # Should only create 1 mapping despite 2 input rows
        assert AWAREVMPMapping.objects.count() == 1

    @pytest.mark.django_db
    def test_load_aware_vmp_mappings_replaces_existing(self, sample_aware_data):
        """Test that existing mappings are replaced"""

        vtm = VTM.objects.create(vtm="99999", name="Existing VTM")
        vmp = VMP.objects.create(code="99999", name="Existing VMP", vtm=vtm)
        antibiotic = AWAREAntibiotic.objects.create(name="Existing Antibiotic")
        AWAREVMPMapping.objects.create(aware_antibiotic=antibiotic, vmp=vmp, vtm=vtm)
        
        assert AWAREVMPMapping.objects.count() == 1
        
        vtm1 = VTM.objects.create(vtm="12345", name="Amoxicillin")
        vmp1 = VMP.objects.create(code="98765", name="Amoxicillin 250mg capsules", vtm=vtm1)
        antibiotic1 = AWAREAntibiotic.objects.create(name="Amoxicillin")
        
        antibiotic_mapping = {"Amoxicillin": antibiotic1.id}
        vmp_mapping = {"98765": vmp1.id}
        vtm_mapping = {"12345": vtm1.id}
        
        single_row_data = sample_aware_data.iloc[:1]
        
        load_aware_vmp_mappings(single_row_data, antibiotic_mapping, vmp_mapping, vtm_mapping)
        
        assert AWAREVMPMapping.objects.count() == 1
        mapping = AWAREVMPMapping.objects.first()
        assert mapping.aware_antibiotic == antibiotic1

   
    @pytest.mark.django_db
    def test_antibiotic_deduplication(self):
        """Test that duplicate antibiotics in source data are properly deduplicated"""

        data = pd.DataFrame({
            "Antibiotic": ["Amoxicillin", "Amoxicillin", "Amoxicillin"],
            "vmp_id": [98765, 98766, 98767],
            "vtm_id": [12345, 12345, 12345],
            "aware_2019": ["Access", "Access", "Access"],
            "aware_2024": ["Access", "Access", "Access"]
        })
        
        result = load_aware_antibiotics(data)
        
        # Should create only 1 antibiotic despite 3 rows
        assert AWAREAntibiotic.objects.count() == 1
        assert len(result) == 1
        assert "Amoxicillin" in result
