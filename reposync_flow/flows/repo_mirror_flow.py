"""Prefect flow for mirroring repositories via Prefect Kubernetes Jobs.

This module turns a YAML file of source and target repository mappings into one
Kubernetes Job per mirror operation. The flow resolves shared credentials from
Prefect-managed configuration, submits mirror tasks through Prefect, and keeps a
bounded number of Kubernetes Jobs in flight using rolling concurrency.
"""

from __future__ import annotations

import re
import threading
from pathlib import Path, PurePosixPath
from typing import Any, NotRequired, TypedDict, cast

import yaml
from prefect import flow, get_run_logger, task
from prefect.futures import PrefectFuture
from prefect.blocks.system import Secret
from prefect.runtime import flow_run
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
DEFAULT_SOURCE_SSH_SECRET_NAME = "repo-mirror-github-ssh"
DEFAULT_SOURCE_SSH_SECRET_KEY = "id_ed25519"
DEFAULT_SOURCE_SSH_KEY_PATH = "/var/run/repo-mirror-ssh/id_ed25519"
JOB_TIMEOUT_SECONDS = 1800
JOB_TTL_SECONDS = 300
CONTAINER_NAME = "repo-mirror"


class RepoDefinition(TypedDict):
    """Repository mirror configuration loaded from YAML.

    Attributes:
        source: Git remote URL used as the source of truth.
        targets: One or more destination git remotes that should receive a
            mirrored copy of the source repository.
        enabled: Optional flag that controls whether this source should be
            mirrored. Missing values default to enabled.
    """

    source: str
    targets: list[str]
    enabled: NotRequired[bool]


def _load_repo_definitions(config_path: str) -> list[RepoDefinition]:
    """Load and validate repository mirror definitions from a YAML file.

    Args:
        config_path: Path to the YAML file containing the top-level `repos`
            list.

    Returns:
        A validated list of repository definitions ready for flow execution.

    Raises:
        FileNotFoundError: If the config file does not exist.
        ValueError: If the YAML structure is missing required fields or has the
            wrong types.
    """

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
        enabled = raw_repo.get("enabled", True)

        if not isinstance(source, str) or not source:
            raise ValueError(f"Repo entry {index} must include a non-empty 'source' string")
        if not isinstance(targets, list) or not targets:
            raise ValueError(f"Repo entry {index} must include a non-empty 'targets' list")
        if any(not isinstance(target, str) or not target for target in targets):
            raise ValueError(f"Repo entry {index} has an invalid target URL")
        if not isinstance(enabled, bool):
            raise ValueError(f"Repo entry {index} 'enabled' value must be a boolean")

        repo_definition: RepoDefinition = {"source": source, "targets": targets}
        if not enabled:
            repo_definition["enabled"] = False
        repo_definitions.append(repo_definition)

    return repo_definitions


def _slugify(value: str) -> str:
    """Convert a string into a Kubernetes-safe slug fragment.

    The resulting value is used as part of Job names, so it is constrained to
    lowercase alphanumeric characters and hyphens.
    """

    slug = re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-")
    return slug or "repo"


def _repo_name_from_remote(remote_url: str) -> str:
    """Extract a repository name from a git remote URL."""

    return remote_url.rstrip("/").rsplit("/", maxsplit=1)[-1].removesuffix(".git")


def _build_task_run_name(target: str) -> str:
    """Build a deterministic Prefect task run name for a mirror operation."""

    return f"repo_{_slugify(_repo_name_from_remote(target)).replace('-', '_')}"


def _build_job_name(source: str, target: str) -> str:
    """Build a deterministic Kubernetes Job name for a mirror operation.

    The name combines the source repo name, target repo name, and a short flow
    run suffix. This keeps names stable within a run while avoiding collisions
    across different flow runs.
    """

    repo_name = _repo_name_from_remote(target)
    source_name = _repo_name_from_remote(source)
    flow_run_id = flow_run.id or "manual"
    run_suffix = _slugify(flow_run_id)[:8]
    base_name = f"repo-mirror-{_slugify(source_name)}-{_slugify(repo_name)}-{run_suffix}"
    return base_name[:63].rstrip("-")


def _build_command_string(source: str, target: str, source_ssh_key_path: str | None) -> str:
    """Build the shell command executed by the Kubernetes Job container.

    The command references `TARGET_USER` and `TARGET_TOKEN` through environment
    variables so the credential values do not have to be embedded directly in
    the CLI argument construction logic outside of the container spec.
    """

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
        + (["--ssh-key", f'"{source_ssh_key_path}"'] if source_ssh_key_path else [])
    )


def _load_target_user(variable_name: str) -> str:
    """Load the shared target username from a Prefect Variable.

    Args:
        variable_name: Name of the Prefect Variable expected to contain the
            shared target username.

    Returns:
        The configured target username.

    Raises:
        ValueError: If the variable is missing or does not contain a non-empty
            string.
    """

    target_user = Variable.get(variable_name)
    if not isinstance(target_user, str) or not target_user:
        raise ValueError(f"Prefect Variable '{variable_name}' must contain a non-empty string")
    return target_user


def _load_target_token(block_name: str) -> str:
    """Load the shared target token from a Prefect Secret block.

    Args:
        block_name: Name of the Prefect Secret block that stores the target
            token.

    Returns:
        The resolved token value.

    Raises:
        ValueError: If the block does not resolve to a non-empty string.
    """

    secret_block = cast(Secret[Any], Secret.load(block_name))
    target_token = secret_block.get()
    if not isinstance(target_token, str) or not target_token:
        raise ValueError(f"Prefect Secret block '{block_name}' must contain a non-empty string")
    return target_token


def _build_env_vars(target_user: str, target_token: str) -> list[dict[str, Any]]:
    """Build container env vars from Prefect-managed target credentials.

    These values are injected into the Job container and referenced by the
    mirror command. The values originate from Prefect configuration rather than
    Kubernetes Secrets in this repo's current design.
    """

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
    source_ssh_secret_name: str | None,
    source_ssh_secret_key: str,
    source_ssh_key_path: str | None,
    ttl_seconds_after_finished: int,
) -> dict[str, Any]:
    """Build the Prefect Kubernetes Job manifest for a single mirror operation.

    Args:
        job_name: Kubernetes Job name to create.
        namespace: Namespace where the Job should run.
        mirror_image: Container image containing the `git_mirror_repo` utility.
        source: Source git remote URL.
        target: Destination git remote URL.
        target_user: Shared target username resolved from Prefect.
        target_token: Shared target token resolved from Prefect.
        service_account_name: Service account used by the spawned Job pod.
        image_pull_secret: Optional image pull secret for private registries.
        source_ssh_secret_name: Optional Kubernetes Secret containing a source
            SSH private key.
        source_ssh_secret_key: Secret data key containing the SSH private key.
        source_ssh_key_path: Container path where the SSH key is mounted and
            passed to `git_mirror_repo`.
        ttl_seconds_after_finished: TTL applied to the finished Job.

    Returns:
        A Kubernetes Job manifest dictionary accepted by `KubernetesJob`.
    """

    effective_ssh_key_path = source_ssh_key_path if source_ssh_secret_name else None
    container_spec: dict[str, Any] = {
        "name": CONTAINER_NAME,
        "image": mirror_image,
        "command": ["/bin/sh", "-c"],
        "args": [_build_command_string(source, target, effective_ssh_key_path)],
        "env": _build_env_vars(target_user, target_token),
    }

    pod_spec: dict[str, Any] = {
        "restartPolicy": "Never",
        "serviceAccountName": service_account_name,
        "containers": [container_spec],
    }

    if source_ssh_secret_name and effective_ssh_key_path:
        ssh_key_path = PurePosixPath(effective_ssh_key_path)
        ssh_volume_name = "source-ssh-key"
        pod_spec["volumes"] = [
            {
                "name": ssh_volume_name,
                "secret": {
                    "secretName": source_ssh_secret_name,
                    "defaultMode": 0o400,
                    "items": [
                        {
                            "key": source_ssh_secret_key,
                            "path": ssh_key_path.name,
                        }
                    ],
                },
            }
        ]
        container_spec["volumeMounts"] = [
            {
                "name": ssh_volume_name,
                "mountPath": str(ssh_key_path.parent),
                "readOnly": True,
            }
        ]

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


@task(task_run_name="repo_{repo_task_name}")
def mirror_repository(
    source: str,
    target: str,
    repo_task_name: str,
    job_namespace: str,
    mirror_image: str,
    target_user: str,
    target_token: str,
    service_account_name: str,
    image_pull_secret: str | None = DEFAULT_IMAGE_PULL_SECRET,
    source_ssh_secret_name: str | None = DEFAULT_SOURCE_SSH_SECRET_NAME,
    source_ssh_secret_key: str = DEFAULT_SOURCE_SSH_SECRET_KEY,
    source_ssh_key_path: str | None = DEFAULT_SOURCE_SSH_KEY_PATH,
    kubernetes_credentials: KubernetesCredentials | None = None,
    include_logs: bool = True,
    timeout_seconds: int = JOB_TIMEOUT_SECONDS,
    ttl_seconds_after_finished: int = JOB_TTL_SECONDS,
) -> None:
    """Run one repository mirror operation as a Prefect-managed Kubernetes Job.

    This task is the unit of parallelism for the flow. Each task creates a
    single Kubernetes Job, waits for it to complete, and optionally emits pod
    logs back into the Prefect task logs.

    Raises:
        RuntimeError: If the Kubernetes Job fails according to
            `prefect_kubernetes`.
    """

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
        source_ssh_secret_name=source_ssh_secret_name,
        source_ssh_secret_key=source_ssh_secret_key,
        source_ssh_key_path=source_ssh_key_path,
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

    job_run = cast(KubernetesJobRun, job.trigger())
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
    source_ssh_secret_name: str | None = DEFAULT_SOURCE_SSH_SECRET_NAME,
    source_ssh_secret_key: str = DEFAULT_SOURCE_SSH_SECRET_KEY,
    source_ssh_key_path: str | None = DEFAULT_SOURCE_SSH_KEY_PATH,
    kubernetes_credentials: KubernetesCredentials | None = None,
    include_logs: bool = True,
    timeout_seconds: int = JOB_TIMEOUT_SECONDS,
    max_concurrency: int = 5,
) -> dict[str, int | str]:
    """Mirror all configured repositories using bounded rolling concurrency.

    The flow submits one Prefect task per source/target pair. Each task in turn
    creates a Kubernetes Job that runs the mirror image. Concurrency is bounded
    by `max_concurrency`, but it is rolling rather than batched: as soon as one
    in-flight mirror task finishes, the flow submits the next pending one. This
    keeps up to `max_concurrency` Jobs active without waiting for an entire
    batch to drain before scheduling more work.

    Args:
        config_path: Path to the YAML repo mapping file.
        job_namespace: Namespace where mirror Jobs are created.
        mirror_image: Container image that contains the mirror utility.
        target_user_variable_name: Prefect Variable name for the shared target
            username.
        target_token_block_name: Prefect Secret block name for the shared
            target token.
        service_account_name: Service account used by each spawned mirror Job.
        image_pull_secret: Optional image pull secret for the mirror image.
        source_ssh_secret_name: Optional Kubernetes Secret containing a source
            SSH private key. Set to `None` to disable SSH key mounting.
        source_ssh_secret_key: Secret data key containing the SSH private key.
        source_ssh_key_path: Container path where the SSH key is mounted and
            passed to `git_mirror_repo`.
        kubernetes_credentials: Prefect Kubernetes credentials block. If not
            provided, in-cluster/default client behavior is used by
            `prefect_kubernetes`.
        include_logs: Whether to fetch and emit pod logs after Job completion.
        timeout_seconds: Timeout passed to `KubernetesJob`.
        max_concurrency: Maximum number of mirror tasks to keep in flight at
            once. Must be at least 1.

    Returns:
        A summary containing the config path, number of repos, and number of
        source/target mirror operations launched.

    Raises:
        ValueError: If `max_concurrency` is less than 1.
    """

    logger = get_run_logger()
    if max_concurrency < 1:
        raise ValueError("max_concurrency must be at least 1")

    repo_definitions = _load_repo_definitions(config_path)
    enabled_repo_definitions = [
        repo_definition
        for repo_definition in repo_definitions
        if repo_definition.get("enabled", True)
    ]
    skipped_repo_count = len(repo_definitions) - len(enabled_repo_definitions)

    if skipped_repo_count:
        logger.info("Skipping %s disabled repo source(s)", skipped_repo_count)

    if not enabled_repo_definitions:
        logger.info("No enabled repo sources configured; nothing to mirror")
        return {
            "config_path": config_path,
            "repo_count": 0,
            "skipped_repo_count": skipped_repo_count,
            "target_count": 0,
        }

    target_user = _load_target_user(target_user_variable_name)
    target_token = _load_target_token(target_token_block_name)

    mirrored_target_count = 0
    in_flight_futures: list[PrefectFuture[None]] = []
    completed_futures: list[PrefectFuture[None]] = []
    completion_lock = threading.Lock()
    completion_event = threading.Event()

    def mark_completed(task_future: PrefectFuture[None]) -> None:
        """Queue a completed future so the scheduler can free a slot quickly."""

        with completion_lock:
            completed_futures.append(task_future)
            completion_event.set()

    def drain_completed(block: bool) -> None:
        """Drain completed futures and surface any task failures.

        Args:
            block: When `True`, wait until at least one in-flight future
                completes. When `False`, only process futures that have already
                finished.
        """

        while in_flight_futures:
            if block:
                completion_event.wait()
            with completion_lock:
                ready_futures = completed_futures[:]
                completed_futures.clear()
                completion_event.clear()

            if not ready_futures:
                if block:
                    continue
                break

            for completed_future in ready_futures:
                if completed_future in in_flight_futures:
                    in_flight_futures.remove(completed_future)
                completed_future.result()

    for repo_definition in enabled_repo_definitions:
        source = repo_definition["source"]
        for target in repo_definition["targets"]:
            while len(in_flight_futures) >= max_concurrency:
                drain_completed(block=True)

            task_future = mirror_repository.submit(
                source,
                target,
                _build_task_run_name(target).removeprefix("repo_"),
                job_namespace,
                mirror_image,
                target_user,
                target_token,
                service_account_name,
                image_pull_secret,
                source_ssh_secret_name,
                source_ssh_secret_key,
                source_ssh_key_path,
                kubernetes_credentials,
                include_logs,
                timeout_seconds,
            )
            task_future.add_done_callback(mark_completed)
            in_flight_futures.append(task_future)
            mirrored_target_count += 1
            drain_completed(block=False)

    drain_completed(block=True)

    logger.info(
        "Completed mirroring for %s repos across %s targets",
        len(enabled_repo_definitions),
        mirrored_target_count,
    )

    return {
        "config_path": config_path,
        "repo_count": len(enabled_repo_definitions),
        "skipped_repo_count": skipped_repo_count,
        "target_count": mirrored_target_count,
    }
