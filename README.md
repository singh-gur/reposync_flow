# Repo Sync Flow

Prefect flow that mirrors repositories from source remotes to one or more targets by reading a YAML config file and creating a Kubernetes Job for each mirror operation.

## Development Setup

1. Install `uv` if you do not already have it:

```bash
pipx install uv
```

2. Install dependencies:

```bash
uv sync
```

3. Create a Kubernetes Secret in the target namespace with the shared target credentials used by the mirror utility:

```bash
kubectl create secret generic repo-mirror-target-auth \
  --from-literal=TARGET_USER=username \
  --from-literal=TARGET_TOKEN=pat-token-here \
  --namespace prefect
```

4. Run the flow locally:

```bash
just run-local
```

The local command clears `PREFECT_API_URL` and enables Prefect's ephemeral API mode so the flow does not depend on an already running Prefect server.

The flow itself now expects to run inside Kubernetes with in-cluster API access. Running it locally is only useful if your environment already provides compatible in-cluster configuration.

Each mirror operation creates a Kubernetes Job that runs:

```bash
regv2.gsingh.io/personal/util_scripts git_mirror_repo ...
```

## Config File

The default config path is `configs/repos.yaml`.

```yaml
repos:
  - source: https://github.com/some-user/some-repo.git
    targets:
      - https://git.gsingh.io/gurbakhshish/some-repo.git
      - https://git.gsingh.io/team/some-repo.git
```

Each repo entry has:

- `source`: the source git remote to mirror from
- `targets`: one or more destination remotes to mirror to

## Deployments

This project is configured for a Kubernetes work pool by default.

Before deploying, update these values if needed:

- `prefect.yaml`: `work_pool.name`
- `prefect.yaml`: `work_pool.job_variables.namespace`
- `prefect.yaml`: `pull[0].prefect.deployments.steps.git_clone.repository`
- `prefect.yaml`: schedule, timezone, deployment name, and runtime parameters such as `config_path`

Your worker runtime must provide:

- in-cluster Kubernetes API access
- RBAC that allows creating, reading, and deleting Jobs and reading Pod logs in the target namespace
- a Kubernetes Secret named `repo-mirror-target-auth` with `TARGET_USER` and `TARGET_TOKEN` keys
- pull access to `regv2.gsingh.io/personal/util_scripts`

The default flow parameters in `prefect.yaml` are:

- `config_path`: `configs/repos.yaml`
- `job_namespace`: `prefect`
- `mirror_image`: `regv2.gsingh.io/personal/util_scripts`
- `target_secret_name`: `repo-mirror-target-auth`
- `service_account_name`: `default`

Deploy the configured flow:

```bash
just deploy
```

Deploy with the configured cron schedule:

```bash
just deploy-scheduled
```

## Development Commands

```bash
just install           # Install project dependencies
just run-local         # Run the repo mirror flow locally
just deploy            # Deploy all deployments from prefect.yaml
just deploy-scheduled  # Deploy the repo mirror flow with a cron schedule
just list-deployments  # List deployments
just status            # Show Prefect version and active profile
```

## Project Layout

```text
reposync_flow/
  flows/
    repo_mirror_flow.py
configs/
  repos.yaml
main.py
prefect.yaml
```
