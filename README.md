# Repo Sync Flow

Prefect flow that syncs git repos from github to local git server

## Development Setup

1. Install `uv` if you do not already have it:

```bash
pipx install uv
```

2. Install dependencies:

```bash
uv sync
```

3. Run the example flow locally:

```bash
just run-local
```

The local command clears `PREFECT_API_URL` and enables Prefect's ephemeral API mode so the example flow does not depend on an already running Prefect server.

## Deployments

This template is configured for a Kubernetes work pool by default.

Before deploying, update these values if needed:

- `prefect.yaml`: `work_pool.name`
- `prefect.yaml`: `pull[0].prefect.deployments.steps.git_clone.repository`
- `prefect.yaml`: schedule, timezone, deployment name, and any runtime parameters

Deploy the configured flow:

```bash
just deploy
```

Deploy with the default schedule from the cookiecutter prompts:

```bash
just deploy-scheduled
```

## Development Commands

```bash
just install           # Install project dependencies
just run-local         # Run the example flow locally
just deploy            # Deploy all deployments from prefect.yaml
just deploy-scheduled  # Deploy the example flow with a cron schedule
just list-deployments  # List deployments
just status            # Show Prefect version and active profile
```

## Project Layout

```text
reposync_flow/
  flows/
    example_flow.py
main.py
prefect.yaml
```
