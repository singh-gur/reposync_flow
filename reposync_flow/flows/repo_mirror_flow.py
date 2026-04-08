"""Prefect flow for mirroring repositories via Prefect Kubernetes Jobs."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any, TypedDict, cast

import yaml
from prefect import flow, get_run_logger, task
from prefect.blocks.system import Secret
from prefect.runtime import flow_run
from prefect.utilities.asyncutils import run_coro_as_sync
from prefect.variables import Variable
from prefect_kubernetes.credentials import KubernetesCredentials
from prefect_kubernetes.jobs import KubernetesJob, KubernetesJobRun

DEFAULT_CONFIG_PATH = "configs/repos.yaml"
DEFAULT_JOB_NAMESPACE = "prefect"
DEFAULT_MIRROR_IMAGE = "regv2.gsingh.io/personal/util_scripts"
DEFAULT_SERVICE_ACCOUNT_NAME = "default"
DEFAULT_IMAGE_PULL_SECRET = "regv2-secret"
DEFAULT_TARGET_USER_VARIABLE_NAME = "repo_mirror_target_user"
DEFAULT_TARGET_TOKEN_BLOCK_NAME = "repo-mirror-target-token"
JOB_TIMEOUT_SECONDS = 1800
JOB_TTL_SECONDS = 300
CONTAINER_NAME = "repo-mirror"


class RepoDefinition(TypedDict):
    """Repository mirror configuration loaded from YAML."""

    source: str
    targets: list[str]


def _load_repo_definitions(config_path: str) -> list[RepoDefinition]:
    """Load and validate repository mirror definitions from a YAML file."""

    config_file = Path(config_path)
    if not config_file.exists():
        raise FileNotFoundError(f"Config file does not exist: {config_file}")

    with config_file.open("r", encoding="utf-8") as handle:
        raw_config = yaml.safe_load(handle) or {}

    if not isinstance(raw_config, dict):
        raise ValueError("Config file must contain a top-level mapping")

    raw_repos = raw_config.get("repos")
    if not isinstance(raw_repos, list) or not raw_repos:
        raise ValueError("Config file must contain a non-empty 'repos' list")

    repo_definitions: list[RepoDefinition] = []
    for index, raw_repo in enumerate(raw_repos, start=1):
        if not isinstance(raw_repo, dict):
            raise ValueError(f"Repo entry {index} must be a mapping")

        source = raw_repo.get("source")
        targets = raw_repo.get("targets")

        if not isinstance(source, str) or not source:
            raise ValueError(f"Repo entry {index} must include a non-empty 'source' string")
        if not isinstance(targets, list) or not targets:
            raise ValueError(f"Repo entry {index} must include a non-empty 'targets' list")
        if any(not isinstance(target, str) or not target for target in targets):
            raise ValueError(f"Repo entry {index} has an invalid target URL")

        repo_definitions.append({"source": source, "targets": targets})

    return repo_definitions


def _slugify(value: str) -> str:
    """Convert a string into a Kubernetes-safe slug fragment."""

    slug = re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-")
    return slug or "repo"


def _build_job_name(source: str, target: str) -> str:
    """Build a deterministic Kubernetes Job name for a mirror operation."""

    repo_name = target.rstrip("/").rsplit("/", maxsplit=1)[-1].removesuffix(".git")
    source_name = source.rstrip("/").rsplit("/", maxsplit=1)[-1].removesuffix(".git")
    flow_run_id = flow_run.id or "manual"
    run_suffix = _slugify(flow_run_id)[:8]
    base_name = f"repo-mirror-{_slugify(source_name)}-{_slugify(repo_name)}-{run_suffix}"
    return base_name[:63].rstrip("-")


def _build_command_string(source: str, target: str) -> str:
    """Build the shell command executed by the Kubernetes Job container."""

    return " ".join(
        [
            "git_mirror_repo",
            "--source",
            f'"{source}"',
            "--target",
            f'"{target}"',
            "--target-user",
            '"$TARGET_USER"',
            "--target-token",
            '"$TARGET_TOKEN"',
        ]
    )


def _load_target_user(variable_name: str) -> str:
    """Load the shared target username from a Prefect Variable."""

    target_user = Variable.get(variable_name)
    if not isinstance(target_user, str) or not target_user:
        raise ValueError(f"Prefect Variable '{variable_name}' must contain a non-empty string")
    return target_user


def _load_target_token(block_name: str) -> str:
    """Load the shared target token from a Prefect Secret block."""

    secret_block = cast(Secret[Any], run_coro_as_sync(Secret.aload(block_name)))
    target_token = secret_block.get()
    if not isinstance(target_token, str) or not target_token:
        raise ValueError(f"Prefect Secret block '{block_name}' must contain a non-empty string")
    return target_token


def _build_env_vars(target_user: str, target_token: str) -> list[dict[str, Any]]:
    """Build container env vars from Prefect-managed target credentials."""

    return [
        {
            "name": "TARGET_USER",
            "value": target_user,
        },
        {
            "name": "TARGET_TOKEN",
            "value": target_token,
        },
    ]


def _build_job_manifest(
    job_name: str,
    namespace: str,
    mirror_image: str,
    source: str,
    target: str,
    target_user: str,
    target_token: str,
    service_account_name: str,
    image_pull_secret: str | None,
    ttl_seconds_after_finished: int,
) -> dict[str, Any]:
    """Build the Prefect Kubernetes Job manifest for a single mirror operation."""

    pod_spec: dict[str, Any] = {
        "restartPolicy": "Never",
        "serviceAccountName": service_account_name,
        "containers": [
            {
                "name": CONTAINER_NAME,
                "image": mirror_image,
                "command": ["/bin/sh", "-c"],
                "args": [_build_command_string(source, target)],
                "env": _build_env_vars(target_user, target_token),
            }
        ],
    }

    if image_pull_secret:
        pod_spec["imagePullSecrets"] = [{"name": image_pull_secret}]

    return {
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {
            "name": job_name,
            "namespace": namespace,
            "labels": {
                "app": "repo-mirror",
                "prefect-flow": "repo-mirror-flow",
            },
        },
        "spec": {
            "backoffLimit": 0,
            "ttlSecondsAfterFinished": ttl_seconds_after_finished,
            "template": {
                "spec": pod_spec,
            },
        },
    }


@task
def mirror_repository(
    source: str,
    target: str,
    job_namespace: str,
    mirror_image: str,
    target_user: str,
    target_token: str,
    service_account_name: str,
    image_pull_secret: str | None = DEFAULT_IMAGE_PULL_SECRET,
    kubernetes_credentials: KubernetesCredentials | None = None,
    include_logs: bool = True,
    timeout_seconds: int = JOB_TIMEOUT_SECONDS,
    ttl_seconds_after_finished: int = JOB_TTL_SECONDS,
) -> None:
    """Run one repository mirror operation as a Prefect-managed Kubernetes Job."""

    logger = get_run_logger()
    job_name = _build_job_name(source, target)
    job_manifest = _build_job_manifest(
        job_name=job_name,
        namespace=job_namespace,
        mirror_image=mirror_image,
        source=source,
        target=target,
        target_user=target_user,
        target_token=target_token,
        service_account_name=service_account_name,
        image_pull_secret=image_pull_secret,
        ttl_seconds_after_finished=ttl_seconds_after_finished,
    )

    if kubernetes_credentials is None:
        kubernetes_credentials = KubernetesCredentials()

    logger.info("Creating Kubernetes Job %s for %s -> %s", job_name, source, target)

    job = KubernetesJob(
        v1_job=job_manifest,
        namespace=job_namespace,
        credentials=kubernetes_credentials,
        delete_after_completion=True,
        timeout_seconds=timeout_seconds,
    )

    job_run = cast(KubernetesJobRun, run_coro_as_sync(job.atrigger()))
    job_run.wait_for_completion()

    logs = job_run.fetch_result() if include_logs else None
    if logs:
        logger.info("Kubernetes Job logs for %s:\n%s", job_name, logs)

    logger.info("Kubernetes Job %s completed successfully", job_name)


@flow(name="repo_mirror_flow", log_prints=True)
def run_flow(
    config_path: str = DEFAULT_CONFIG_PATH,
    job_namespace: str = DEFAULT_JOB_NAMESPACE,
    mirror_image: str = DEFAULT_MIRROR_IMAGE,
    target_user_variable_name: str = DEFAULT_TARGET_USER_VARIABLE_NAME,
    target_token_block_name: str = DEFAULT_TARGET_TOKEN_BLOCK_NAME,
    service_account_name: str = DEFAULT_SERVICE_ACCOUNT_NAME,
    image_pull_secret: str | None = DEFAULT_IMAGE_PULL_SECRET,
    kubernetes_credentials: KubernetesCredentials | None = None,
    include_logs: bool = True,
    timeout_seconds: int = JOB_TIMEOUT_SECONDS,
) -> dict[str, int | str]:
    """Mirror all configured repositories by launching one Kubernetes Job per target."""

    logger = get_run_logger()
    repo_definitions = _load_repo_definitions(config_path)
    target_user = _load_target_user(target_user_variable_name)
    target_token = _load_target_token(target_token_block_name)

    mirrored_target_count = 0
    for repo_definition in repo_definitions:
        source = repo_definition["source"]
        for target in repo_definition["targets"]:
            mirror_repository(
                source,
                target,
                job_namespace,
                mirror_image,
                target_user,
                target_token,
                service_account_name,
                image_pull_secret,
                kubernetes_credentials,
                include_logs,
                timeout_seconds,
            )
            mirrored_target_count += 1

    logger.info(
        "Completed mirroring for %s repos across %s targets",
        len(repo_definitions),
        mirrored_target_count,
    )

    return {
        "config_path": config_path,
        "repo_count": len(repo_definitions),
        "target_count": mirrored_target_count,
    }
