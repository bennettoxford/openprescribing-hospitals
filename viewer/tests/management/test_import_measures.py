import pytest
from datetime import date, datetime, timedelta
from django.core.management import call_command
from viewer.models import Measure, MeasureTag
from viewer.management.commands.import_measures import (
    validate_measure_yaml,
    validate_measure_tags,
    validate_date_format,
    validate_review_dates
)
from schema import SchemaError
from unittest.mock import patch

@pytest.fixture
def measure_tags():
    tags = ['test-tag-1', 'test-tag-2', 'test-tag-3']
    for tag in tags:
        MeasureTag.objects.create(name=tag)
    return tags

@pytest.fixture
def valid_measure_data():
    today = datetime.now().date()
    return {
        'name': 'Test Measure',
        'short_name': 'TEST',
        'description': 'Test description',
        'why_it_matters': 'Test why it matters',
        'how_is_it_calculated': 'Test calculation method',
        'tags': ['test-tag-1', 'test-tag-2'],
        'quantity_type': 'dose',
        'authored_by': 'Test Author',
        'checked_by': 'Test Checker',
        'date_reviewed': today.strftime('%Y-%m-%d'),
        'next_review': (today + timedelta(days=180)).strftime('%Y-%m-%d'),
        'status': 'in_development'
    }

@pytest.mark.django_db()
class TestImportMeasures:
    def test_validate_measure_yaml_valid(self, valid_measure_data):
        validate_measure_yaml(valid_measure_data)

    def test_validate_measure_yaml_invalid_quantity_type(self, valid_measure_data):
        valid_measure_data['quantity_type'] = 'invalid'
        with pytest.raises(SchemaError):
            validate_measure_yaml(valid_measure_data)

    def test_validate_measure_yaml_valid_default_view_mode(self, valid_measure_data):
        valid_measure_data['default_view_mode'] = 'icb'
        validate_measure_yaml(valid_measure_data)

    def test_validate_measure_yaml_invalid_default_view_mode(self, valid_measure_data):
        valid_measure_data['default_view_mode'] = 'invalid_mode'
        with pytest.raises(SchemaError):
            validate_measure_yaml(valid_measure_data)

    def test_validate_measure_yaml_missing_required_field(self, valid_measure_data):
        del valid_measure_data['name']
        with pytest.raises(SchemaError):
            validate_measure_yaml(valid_measure_data)

    def test_validate_measure_tags_valid(self, measure_tags):
        validate_measure_tags(['test-tag-1', 'test-tag-2'])

    def test_validate_measure_tags_invalid(self, measure_tags):
        with pytest.raises(ValueError):
            validate_measure_tags(['non-existent-tag'])

    def test_validate_date_format_valid(self):
        assert validate_date_format('2024-01-01')
        assert validate_date_format(date(2024, 1, 1))
        assert validate_date_format('')

    def test_validate_date_format_invalid(self):
        assert not validate_date_format('2024/01/01')
        assert not validate_date_format('invalid-date')

    def test_validate_review_dates_valid(self):
        today = datetime.now().date()
        data = {
            'date_reviewed': today.strftime('%Y-%m-%d'),
            'next_review': (today + timedelta(days=180)).strftime('%Y-%m-%d')
        }
        assert validate_review_dates(data) is True

    def test_validate_review_dates_invalid_order(self):
        data = {
            'date_reviewed': '2024-12-31',
            'next_review': '2024-01-01'
        }
        assert isinstance(validate_review_dates(data), str)

    @pytest.mark.django_db
    def test_command_creates_measure(self, tmp_path, measure_tags):
        measures_dir = tmp_path / 'measures'
        measures_dir.mkdir()

        test_measure_dir = measures_dir / 'test-measure'
        test_measure_dir.mkdir()

        today = datetime.now().date()
        next_review = (today + timedelta(days=180)).strftime('%Y-%m-%d')
        
        yaml_content = f"""
name: Test Measure
short_name: test-measure
description: Test description
why_it_matters: Test why it matters
how_is_it_calculated: Test calculation method
tags: ['test-tag-1', 'test-tag-2']
quantity_type: dose
authored_by: Test Author
checked_by: Test Checker
date_reviewed: '{today}'
next_review: '{next_review}'
status: in_development
default_view_mode: icb
"""
        (test_measure_dir / 'definition.yaml').write_text(yaml_content)
        (test_measure_dir / 'vmps.sql').write_text('')

        with patch('viewer.management.commands.import_measures.Path') as mock_path:
            mock_path.return_value.parent.parent.parent = tmp_path
            call_command('import_measures', 'test-measure')

        measure = Measure.objects.get(short_name='test-measure')
        assert measure.name == 'Test Measure'
        assert measure.default_view_mode == 'icb'
        assert set(measure.tags.values_list('name', flat=True)) == {'test-tag-1', 'test-tag-2'}

    @pytest.mark.django_db
    def test_command_creates_measure_with_default_view_mode(self, tmp_path, measure_tags):
        measures_dir = tmp_path / 'measures'
        measures_dir.mkdir()

        test_measure_dir = measures_dir / 'test-measure-default'
        test_measure_dir.mkdir()

        today = datetime.now().date()
        next_review = (today + timedelta(days=180)).strftime('%Y-%m-%d')
        
        yaml_content = f"""
name: Test Measure Default
short_name: test-measure-default
description: Test description
why_it_matters: Test why it matters
how_is_it_calculated: Test calculation method
tags: ['test-tag-1']
quantity_type: dose
authored_by: Test Author
checked_by: Test Checker
date_reviewed: '{today}'
next_review: '{next_review}'
status: in_development
"""
        (test_measure_dir / 'definition.yaml').write_text(yaml_content)
        (test_measure_dir / 'vmps.sql').write_text('')

        with patch('viewer.management.commands.import_measures.Path') as mock_path:
            mock_path.return_value.parent.parent.parent = tmp_path
            call_command('import_measures', 'test-measure-default')

        measure = Measure.objects.get(short_name='test-measure-default')
        assert measure.name == 'Test Measure Default'
        assert measure.default_view_mode == 'percentiles'  # Should use default value

    @pytest.mark.django_db
    def test_command_invalid_folder_name(self, capsys):
        call_command('import_measures', 'Invalid Folder Name')
        captured = capsys.readouterr()
        assert "Invalid folder name" in captured.out 