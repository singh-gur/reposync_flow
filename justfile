# Prefect project commands

install:
    uv sync

run-local:
    PREFECT_API_URL='' PREFECT_SERVER_ALLOW_EPHEMERAL_MODE=true uv run python -c "from reposync_flow.flows.example_flow import run_flow; run_flow()"

deploy:
    uv run prefect deploy --all

deploy-scheduled:
    uv run prefect deploy reposync_flow/flows/example_flow.py:run_flow --name reposync_flow --cron '0 2 * * *' -p kubernetes

list-deployments:
    uv run prefect deployment ls

status:
    uv run prefect version && uv run prefect profile ls

# Generate requirements.txt (top-level deps only, no transitive deps, no annotations)
reqs:
    uv pip compile pyproject.toml --no-deps --no-annotate --no-header > requirements.txt
