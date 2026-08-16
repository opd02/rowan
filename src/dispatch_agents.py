#!/usr/bin/env python3

import re
import shlex
import sqlite3
import subprocess
from pathlib import Path


DB_PATH = Path.home() / "rowan" / "orchestrator.db"

SSH_KEY = "/home/opd02/.ssh/id_ed25519"
CLUSTER = "doyle161@negishi.rcac.purdue.edu"

WORKER_SCRIPT = "/home/doyle161/rowan-agent/repair_worker.slurm"


def ssh(command, timeout=30):
    result = subprocess.run(
        [
            "ssh",
            "-i",
            SSH_KEY,
            CLUSTER,
            command,
        ],
        capture_output=True,
        text=True,
        timeout=timeout,
    )

    if result.returncode != 0:
        raise RuntimeError(
            result.stderr.strip()
            or result.stdout.strip()
        )

    return result.stdout.strip()


def log_event(connection, workflow_id, event_type, message):
    connection.execute(
        """
        INSERT INTO events (
            workflow_id,
            event_type,
            message
        )
        VALUES (?, ?, ?)
        """,
        (
            workflow_id,
            event_type,
            message,
        ),
    )


def submit_agent(workflow):
    failure = workflow["next_action"] or "UNKNOWN_FAILURE"

    command = (
        "sbatch "
        f"--job-name=rowan-agent-{workflow['id']} "
        f"{shlex.quote(WORKER_SCRIPT)} "
        f"{workflow['id']} "
        f"{shlex.quote(workflow['cluster_directory'])} "
        f"{shlex.quote(str(workflow['current_job_id']))} "
        f"{shlex.quote(failure)}"
    )

    output = ssh(command)

    match = re.search(
        r"Submitted batch job (\d+)",
        output
    )

    if not match:
        raise RuntimeError(
            f"Could not parse agent job ID from: {output}"
        )

    return match.group(1)


def main():
    connection = sqlite3.connect(DB_PATH)
    connection.row_factory = sqlite3.Row

    try:
        workflows = connection.execute(
            """
            SELECT *
            FROM workflows
            WHERE state = 'AGENT_RESTART_QUEUED'
            ORDER BY id
            """
        ).fetchall()

        print(
            f"Rowan: {len(workflows)} workflow(s) "
            "waiting for an agent."
        )

        for workflow in workflows:
            failed_job_id = workflow["current_job_id"]

            print(
                f"Dispatching agent for "
                f"{workflow['name']}..."
            )

            agent_job_id = submit_agent(workflow)

            connection.execute(
                """
                UPDATE workflows
                SET
                    state = 'AGENT_RUNNING',
                    current_job_id = ?,
                    current_job_type = 'AGENT_REPAIR',
                    next_action = 'WAIT_FOR_AGENT',
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
                  AND state = 'AGENT_RESTART_QUEUED'
                """,
                (
                    agent_job_id,
                    workflow["id"],
                ),
            )

            connection.execute(
                """
                INSERT INTO job_history (
                    workflow_id,
                    slurm_job_id,
                    job_type,
                    status,
                    submitted_at,
                    completed_at
                )
                VALUES (?, ?, 'AGENT_REPAIR', 'PENDING',
                        CURRENT_TIMESTAMP, NULL)
                """,
                (
                    workflow["id"],
                    agent_job_id,
                ),
            )

            log_event(
                connection,
                workflow["id"],
                "AGENT_SUBMITTED",
                (
                    f"Submitted Rowan Codex repair agent "
                    f"{agent_job_id} for failed VASP job "
                    f"{failed_job_id}."
                ),
            )

            print(
                f"  Agent Slurm job: {agent_job_id}"
            )

        connection.commit()

    except Exception:
        connection.rollback()
        raise

    finally:
        connection.close()


if __name__ == "__main__":
    main()
