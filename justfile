# Prefect project commands

install:
    uv sync

run-local:
    PREFECT_API_URL='' PREFECT_SERVER_ALLOW_EPHEMERAL_MODE=true uv run python -c "from reposync_flow.flows.repo_mirror_flow import run_flow; run_flow()"

deploy:
    uv run prefect deploy --all

deploy-scheduled:
    uv run prefect deploy reposync_flow/flows/repo_mirror_flow.py:run_flow --name repo_mirror_flow --cron '0 2 * * *' -p kubernetes

list-deployments:
    uv run prefect deployment ls

status:
    uv run prefect version && uv run prefect profile ls

# Generate requirements.txt (top-level deps only, no transitive deps, no annotations)
reqs:
    uv pip compile pyproject.toml --no-deps --no-annotate --no-header > requirements.txt

# Usage: just set-var pg_backup_bucket my-bucket
set-var name value:
    .venv/bin/prefect variable set "{{ name }}" "{{ value }}"

# Usage: just set-var pg_backup_bucket my-bucket
set-var-overwrite name value:
    .venv/bin/prefect variable set --overwrite "{{ name }}" "{{ value }}"

# Set a Secret block (sensitive)

# Usage: just set-secret PG_PASSWORD my-secret-value
set-secret name value:
    #!/usr/bin/env bash
    set -euo pipefail
    .venv/bin/python -c 'import os; from prefect.blocks.system import Secret; Secret(value="{{ value }}").save("{{ name }}", overwrite=True)'

# List Prefect variables
list-vars:
    .venv/bin/prefect variable ls

# List Prefect secrets
list-secrets:
    .venv/bin/prefect block ls
