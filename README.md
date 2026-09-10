# OpenPrescribing Hospitals

[OpenPrescribing Hospitals](https://hospitals.openprescribing.net/) is an open
platform for the exploration of medicines use in NHS hospitals in England. Its
main data source is the
[Secondary Care Medicines Data (SCMD)](https://opendata.nhsbsa.net/dataset/secondary-care-medicines-data-indicative-price)
from the NHS Business Services Authority.

The platform is in beta. It lets users:

- analyse medicines use by product, organisation, and quantity type;
- compare hospital trusts with prescribing measures;
- inspect product details and data coverage; and
- export data and charts for further analysis.

## Project structure

The repository contains the web application and the data pipeline:

- `viewer/` — Django application, views, models, APIs, and tests.
- `templates/` — Django HTML templates.
- `src/` — Svelte 5 components and JavaScript tests.
- `pipeline/` — Prefect flows that import, process, and load data with BigQuery.
- `viewer/measures/` — measure definitions and product-selection SQL.

## Technology

- Python 3.11, Django, and PostgreSQL
- Svelte 5, Vite, Tailwind CSS, and Highcharts
- Prefect and Google BigQuery for the data pipeline
- `uv` for Python dependencies and npm for front-end dependencies

## Licence

This project is available under the [MIT Licence](LICENSE).

