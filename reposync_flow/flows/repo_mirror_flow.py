"""Prefect flow for mirroring repositories via Prefect Kubernetes Jobs."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any, TypedDict, cast

import yaml
from prefect import flow, get_run_logger, task
from prefect.runtime import flow_run
from prefect.utilities.asyncutils import run_coro_as_sync
from prefect_kubernetes.credentials import KubernetesCredentials
from prefect_kubernetes.jobs import KubernetesJob, KubernetesJobRun

DEFAULT_CONFIG_PATH = "configs/repos.yaml"
DEFAULT_JOB_NAMESPACE = "prefect"
DEFAULT_MIRROR_IMAGE = "regv2.gsingh.io/personal/util_scripts"
DEFAULT_TARGET_SECRET_NAME = "repo-mirror-target-auth"
DEFAULT_SERVICE_ACCOUNT_NAME = "default"
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


def _build_env_vars(target_secret_name: str) -> list[dict[str, Any]]:
    """Build container env vars backed by the target auth Kubernetes Secret."""

    return [
        {
            "name": "TARGET_USER",
            "valueFrom": {
                "secretKeyRef": {
                    "name": target_secret_name,
                    "key": "TARGET_USER",
                }
            },
        },
        {
            "name": "TARGET_TOKEN",
            "valueFrom": {
                "secretKeyRef": {
                    "name": target_secret_name,
                    "key": "TARGET_TOKEN",
                }
            },
        },
    ]


def _build_job_manifest(
    job_name: str,
    namespace: str,
    mirror_image: str,
    source: str,
    target: str,
    target_secret_name: str,
    service_account_name: str,
    ttl_seconds_after_finished: int,
) -> dict[str, Any]:
    """Build the Prefect Kubernetes Job manifest for a single mirror operation."""

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
                "spec": {
                    "restartPolicy": "Never",
                    "serviceAccountName": service_account_name,
                    "containers": [
                        {
                            "name": CONTAINER_NAME,
                            "image": mirror_image,
                            "command": ["/bin/sh", "-c"],
                            "args": [_build_command_string(source, target)],
                            "env": _build_env_vars(target_secret_name),
                        }
                    ],
                }
            },
        },
    }


@task
def mirror_repository(
    source: str,
    target: str,
    job_namespace: str,
    mirror_image: str,
    target_secret_name: str,
    service_account_name: str,
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
        target_secret_name=target_secret_name,
        service_account_name=service_account_name,
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
    completed = job_run.wait_for_completion()
    if not completed:
        raise RuntimeError(f"Kubernetes Job did not complete successfully: {job_name}")

    logs = job_run.fetch_result() if include_logs else None
    if logs:
        logger.info("Kubernetes Job logs for %s:\n%s", job_name, logs)


@flow(name="repo_mirror_flow", log_prints=True)
def run_flow(
    config_path: str = DEFAULT_CONFIG_PATH,
    job_namespace: str = DEFAULT_JOB_NAMESPACE,
    mirror_image: str = DEFAULT_MIRROR_IMAGE,
    target_secret_name: str = DEFAULT_TARGET_SECRET_NAME,
    service_account_name: str = DEFAULT_SERVICE_ACCOUNT_NAME,
    kubernetes_credentials: KubernetesCredentials | None = None,
    include_logs: bool = True,
    timeout_seconds: int = JOB_TIMEOUT_SECONDS,
) -> dict[str, int | str]:
    """Mirror all configured repositories by launching one Kubernetes Job per target."""

    logger = get_run_logger()
    repo_definitions = _load_repo_definitions(config_path)

    mirrored_target_count = 0
    for repo_definition in repo_definitions:
        source = repo_definition["source"]
        for target in repo_definition["targets"]:
            mirror_repository(
                source,
                target,
                job_namespace,
                mirror_image,
                target_secret_name,
                service_account_name,
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
