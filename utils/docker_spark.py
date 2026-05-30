from __future__ import annotations

from typing import Any

import docker


SPARK_MASTER_CONTAINER = "spark-master"
SPARK_SUBMIT = "/opt/spark/bin/spark-submit"


def _get_spark_container():
    """Find the spark-master container by name filter."""
    client = docker.from_env()
    containers = client.containers.list(filters={"name": SPARK_MASTER_CONTAINER})
    if not containers:
        raise RuntimeError(
            f"No running container found matching '{SPARK_MASTER_CONTAINER}'"
        )
    return containers[0]


def run_spark_job(
    application: str,
    args: dict[str, str] | None = None,
) -> tuple[int, str]:
    """Execute a spark-submit command inside the spark-master container.

    Args:
        application: Path to the Python/Java application inside the container
                     (e.g. '/opt/spark/transform/format_accessibility.py').
        args: CLI argument pairs (e.g. {'--raw-path': '/data/raw/...', ...}).

    Returns:
        Tuple of (exit_code, stdout_output).

    Raises:
        RuntimeError: If the spark-master container is not running.
    """
    container = _get_spark_container()
    cmd_parts = [SPARK_SUBMIT, application]
    if args:
        for key, value in args.items():
            cmd_parts.append(key)
            cmd_parts.append(value)
    cmd = " ".join(cmd_parts)
    exit_code, output = container.exec_run(cmd)
    stdout = output.decode("utf-8", errors="replace") if output else ""
    if exit_code != 0:
        print(f"[docker_spark] spark-submit exited with code {exit_code}")
        print(stdout)
    return exit_code, stdout


def spark_submit_or_raise(application: str, args: dict[str, Any] | None = None) -> str:
    """Run spark-submit and raise on failure. Returns stdout on success."""
    exit_code, output = run_spark_job(application, args)
    if exit_code != 0:
        raise RuntimeError(
            f"spark-submit failed (exit {exit_code}) for {application}"
        )
    return output
