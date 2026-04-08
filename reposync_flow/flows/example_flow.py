"""Example Prefect flow."""

from datetime import UTC, datetime

from prefect import flow, get_run_logger, task


@task
def build_message(name: str) -> str:
    """Create a greeting used by the example flow."""
    return f"Hello, {name}."


@flow(name="run_flow", log_prints=True)
def run_flow(name: str = "Prefect") -> dict[str, str]:
    """Run a small example flow to validate local and deployed execution."""
    logger = get_run_logger()
    started_at = datetime.now(UTC).isoformat()
    message = build_message(name)

    logger.info("Flow started at %s", started_at)
    logger.info(message)

    return {
        "message": message,
        "started_at": started_at,
    }
