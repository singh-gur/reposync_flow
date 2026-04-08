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

3. Create the shared target credentials in Prefect:

```bash
prefect variable set repo_mirror_target_user username
```

```python
from prefect.blocks.system import Secret

Secret(value="pat-token-here").save("repo-mirror-target-token", overwrite=True)
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

## Concurrency

The flow launches one Prefect task per `source -> target` pair, and each task
creates one Kubernetes Job.

Concurrency is controlled by the `max_concurrency` flow parameter.

- Default: `5`
- Meaning: at most 5 mirror tasks and therefore at most 5 Kubernetes Jobs are in flight at once
- Behavior: rolling concurrency, not batch concurrency

Rolling concurrency means the flow does not wait for an entire batch of jobs to
finish before starting more. If `max_concurrency` is `5` and there are `7`
mirror operations:

1. The first 5 start immediately.
2. As soon as any 1 completes, the 6th starts.
3. As soon as another completes, the 7th starts.

This keeps the pipeline full while still putting an upper bound on cluster load.

Reduce `max_concurrency` if your worker, cluster, or git endpoints should be
protected from too many simultaneous mirror operations.

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
- access to the Prefect API so the flow can load the `repo_mirror_target_user` Variable and the `repo-mirror-target-token` Secret block
- pull access to `regv2.gsingh.io/personal/util_scripts`
- an image pull secret named `regv2-secret` in the job namespace if the registry requires authentication

The Kubernetes work pool should run the Prefect worker with a service account that has those permissions. This repo now matches `dbbackup_flow` by setting `work_pool.job_variables.service_account_name` to `prefect-worker` in `prefect.yaml`.

The default flow parameters in `prefect.yaml` are:

- `config_path`: `configs/repos.yaml`
- `job_namespace`: `prefect`
- `mirror_image`: `regv2.gsingh.io/personal/util_scripts`
- `target_user_variable_name`: `repo_mirror_target_user`
- `target_token_block_name`: `repo-mirror-target-token`
- `service_account_name`: `default`
- `image_pull_secret`: `regv2-secret`
- `max_concurrency`: `5`

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
