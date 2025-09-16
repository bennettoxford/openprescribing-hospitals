# Measures creation

This directory contains measure definitions and the SQL queries that define the products included in each measure.

Each measure should have its own directory containing:
- `definition.yaml` - The measure's metadata and configuration
- `vmps.sql` - The SQL query that defines the measure's logic

## YAML Definition Format

Each measure requires a `definition.yaml` file with the following structure:

```yaml
name: full name # Full name of the measure/indicator
short_name: short name # Abbreviated name or acronym
description: Description # Brief 1-2 sentence description of what this measure identifies
why_it_matters: |
    Why it matters
how_is_it_calculated: >
    How it is calculated
tags:
    # List relevant categories (e.g. Safety, Prescribing, Monitoring)
    - Safety
quantity_type: dose # Type of measurement (e.g. scmd, dose, ingredient, ddd, indicative_cost)
authored_by: John Doe # Name of original author
checked_by: Jane Smith # Name of clinical/technical reviewer
date_reviewed: 2024-03-20 # Date of last review (YYYY-MM-DD)
next_review: 2025-03-20 # Date of next scheduled review (YYYY-MM-DD)
first_published: 2024-03-20 # Date of first publication (YYYY-MM-DD)
status: in_development # in_development, preview, published
default_view_mode: percentiles # Default view mode (percentiles, icb, region, national)
annotations:  # Optional data annotations - vertical lines with labels on charts
    - date: 2025-08-01
      label: "Annotation label"
      description: "Annotation description"
      colour: "#DC3220"
```

## Importing Measures

To import measures, run the `import_measures` management command with the following syntax:

```bash
python manage.py import_measures [folder_name]
```

If no folder name is provided, all measures in the `measures` directory will be imported. The name of the folder determines the slug of the measure.

## Fetching measure products

To fetch the products included in a measure, run the `get_measure_vmps` management command with the following syntax:

```bash
python manage.py get_measure_vmps [measure_slug]
```

## Calculating measure values

To calculate the value of a measure, run the `compute_measures` management command with the following syntax:

```bash
python manage.py compute_measures [measure_slug]
```



