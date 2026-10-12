# Local development

Local work uses Docker Compose and `Dockerfile.dev`. The `Dockerfile` is the deployment image.

## Start

Install Docker. Copy `.env.example` to `.env`. Then run:

```bash
docker compose up --build
```

In a second terminal, load the sample data:

```bash
docker compose exec web python manage.py load_sample_data
```

Open <http://localhost:8000>.

The first start can take several minutes. The sample load can also take several minutes. Run the load again when the files in `sample_data/` change.

Stop the application with `docker compose down`. That command keeps the Postgres volume. To start again with an empty database, run `docker compose down -v`.

Run `docker compose up --build` again when `pyproject.toml` or `uv.lock` changes. The `vite` service runs `npm install` at each start, so Node dependency changes need only a restart.

## Services

| Service | Role | Address |
| --- | --- | --- |
| `web` | Django development server | <http://localhost:8000> |
| `vite` | Vite development server for the Svelte components | <http://localhost:5173> |
| `db` | Postgres 18 | `localhost:5433` on the host, port 5432 inside Compose |

## Sample data

`load_sample_data` reads the CSV files in `sample_data/`. These rows are a subset of the public SCMD.

Refresh the CSV files from BigQuery on the host:

```bash
uv run --group pipeline python -m pipeline.sample.dump_sample_data
```

The command uses the pipeline dependency group and the Prefect GCP credentials from the Prefect Cloud workspace.

## Tests

```bash
docker compose run --rm web pytest viewer
docker compose run --rm vite sh -c "npm install && npm run test:run"
```

The local image does not include the pipeline dependency group. Run the pipeline tests on the host:

```bash
uv sync --locked --group pipeline --group dev
uv run pytest
```

## Run on the host

You need Python 3.11, uv, Node 22, and Postgres 18.

Copy `.env.example` to `.env`. Set `DATABASE_HOST` and `DATABASE_PORT` for your Postgres server.

```bash
uv sync --locked --group dev
npm install
npm run dev
uv run python manage.py migrate
uv run python manage.py createcachetable
uv run python manage.py load_sample_data
uv run python manage.py runserver
```

Keep the Vite process running while you use the site. Run the Vite command in its own terminal.
