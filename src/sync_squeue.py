#!/usr/bin/env python3

import sqlite3
import subprocess
from pathlib import Path
from datetime import datetime, timezone


# ---------------------------------------------------------
# Configuration
# ---------------------------------------------------------

DB_PATH = Path.home() / "rowan" / "orchestrator.db"

# Change this if your ~/.ssh/config uses a different alias.
#CLUSTER_HOST = "-i /home/opd02/.ssh/id_ed25519 'doyle161@negishi.rcac.purdue.edu'"
SSH_KEY = "/home/opd02/.ssh/id_ed25519"
CLUSTER_USER = "doyle161"
CLUSTER_HOST = "negishi.rcac.purdue.edu"

# ---------------------------------------------------------
# Slurm
# ---------------------------------------------------------
def query_squeue():
    command = [
        "ssh",
        "-i",
        SSH_KEY,
        f"{CLUSTER_USER}@{CLUSTER_HOST}",
        "squeue -h -u $USER -o '%i|%j|%T|%Z'"
    ]

    result = subprocess.run(
        command,
        capture_output=True,
        text=True,
        timeout=30
    )

    if result.returncode != 0:
        raise RuntimeError(
            "Could not query Slurm:\n"
            + result.stderr.strip()
        )

    return result.stdout

def parse_squeue(output):
    jobs = []

    for line in output.splitlines():
        line = line.strip()
        print(line)
        if not line:
            continue

        parts = line.split("|", 3)

        if len(parts) != 4:
            print(f"WARNING: Could not parse: {line}")
            continue

        jobs.append({
            "job_id": parts[0].strip(),
            "job_name": parts[1].strip(),
            "state": parts[2].strip(),
            "cluster_directory": parts[3].strip(),
        })

    return jobs


# ---------------------------------------------------------
# Rowan helpers
# ---------------------------------------------------------

def rowan_state(slurm_state):
    """
    Convert Slurm's state into a Rowan workflow state.
    """

    mapping = {
        "RUNNING": "SLURM_RUNNING",
        "PENDING": "SLURM_PENDING",
        "CONFIGURING": "SLURM_CONFIGURING",
        "COMPLETING": "SLURM_COMPLETING",
    }

    return mapping.get(
        slurm_state,
        f"SLURM_{slurm_state}"
    )


def create_unique_workflow_name(
    connection,
    cluster_directory,
    job_name,
    job_id,
    current_workflow_id=None
):
    """
    Prefer the Slurm job name as the Rowan workflow name.

    Fall back to the directory name only if Slurm has no useful
    job name.

    Because workflows.name is UNIQUE, append the job ID if another
    workflow already uses the same name.
    """

    if job_name and job_name.strip():
        candidate = job_name.strip()
    else:
        directory_name = Path(cluster_directory).name

        if directory_name:
            candidate = directory_name
        else:
            candidate = f"job_{job_id}"

    if current_workflow_id is None:
        existing = connection.execute(
            """
            SELECT id
            FROM workflows
            WHERE name = ?
            """,
            (candidate,)
        ).fetchone()

    else:
        existing = connection.execute(
            """
            SELECT id
            FROM workflows
            WHERE name = ?
              AND id != ?
            """,
            (
                candidate,
                current_workflow_id
            )
        ).fetchone()

    if existing is None:
        return candidate

    return f"{candidate}__{job_id}"

# ---------------------------------------------------------
# Database synchronization
# ---------------------------------------------------------

def log_event(
    connection,
    workflow_id,
    event_type,
    message
):
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
            message
        )
    )


def sync_job(connection, job):
    """
    Synchronize one active Slurm job into Rowan.

    Workflow identity is primarily based on cluster_directory.
    """

    job_id = job["job_id"]
    job_name = job["job_name"]
    slurm_state = job["state"]
    cluster_directory = job["cluster_directory"]

    state = rowan_state(slurm_state)

    if job_name.startswith("rowan-agent-"):
    	return

    # -----------------------------------------------------
    # Does Rowan already know this calculation directory?
    # -----------------------------------------------------

    workflow = connection.execute(
        """
        SELECT *
        FROM workflows
        WHERE cluster_directory = ?
        ORDER BY id
        LIMIT 1
        """,
        (cluster_directory,)
    ).fetchone()

    # -----------------------------------------------------
    # New workflow
    # -----------------------------------------------------

    if workflow is None:

        workflow_name = create_unique_workflow_name(
            connection,
            cluster_directory,
            job_name,
            job_id
        )

        cursor = connection.execute(
            """
            INSERT INTO workflows (
                name,
                cluster_directory,
                state,
                current_job_id,
                current_job_type,
                next_action,
                retry_count,
                updated_at
            )
            VALUES (?, ?, ?, ?, ?, NULL, 0, CURRENT_TIMESTAMP)
            """,
            (
                workflow_name,
                cluster_directory,
                state,
                job_id,
                "UNCLASSIFIED"
            )
        )

        workflow_id = cursor.lastrowid

        log_event(
            connection,
            workflow_id,
            "WORKFLOW_DISCOVERED",
            (
                f"Rowan discovered Slurm job {job_id} "
                f"({job_name}) in {cluster_directory}."
            )
        )

        print(
            f"NEW workflow {workflow_name}: "
            f"{job_id} {slurm_state}"
        )

    # -----------------------------------------------------
    # Existing workflow
    # -----------------------------------------------------

    else:

        workflow_id = workflow["id"]
        previous_job_id = workflow["current_job_id"]
        previous_state = workflow["state"]
        workflow_name = create_unique_workflow_name(
            connection,
            cluster_directory,
            job_name,
            job_id,
            current_workflow_id=workflow_id
        )

        connection.execute(
            """
            UPDATE workflows
            SET
                name = ?,
                current_job_id = ?,
                state = ?,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
            """,
            (
                workflow_name,
                job_id,
                state,
                workflow_id
            )
        )




        # New Slurm attempt in same calculation directory
        if previous_job_id != job_id:

            log_event(
                connection,
                workflow_id,
                "NEW_SLURM_ATTEMPT",
                (
                    f"Workflow moved from Slurm job "
                    f"{previous_job_id} to {job_id}."
                )
            )

            print(
                f"NEW ATTEMPT {workflow['name']}: "
                f"{previous_job_id} -> {job_id}"
            )

        # Same job, different state
        elif previous_state != state:

            log_event(
                connection,
                workflow_id,
                "SLURM_STATE_CHANGED",
                (
                    f"Slurm job {job_id} changed from "
                    f"{previous_state} to {state}."
                )
            )

            print(
                f"STATE {workflow['name']}: "
                f"{previous_state} -> {state}"
            )

        else:

            print(
                f"KNOWN {workflow['name']}: "
                f"{job_id} {slurm_state}"
            )

    # -----------------------------------------------------
    # Add job to history if Rowan has never seen this
    # Slurm job ID before.
    # -----------------------------------------------------

    existing_history = connection.execute(
        """
        SELECT id
        FROM job_history
        WHERE slurm_job_id = ?
        LIMIT 1
        """,
        (job_id,)
    ).fetchone()

    if existing_history is None:

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
            VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP, NULL)
            """,
            (
                workflow_id,
                job_id,
                "UNCLASSIFIED",
                slurm_state
            )
        )

    else:

        connection.execute(
            """
            UPDATE job_history
            SET status = ?
            WHERE slurm_job_id = ?
            """,
            (
                slurm_state,
                job_id
            )
        )


def sync_all(jobs):

    connection = sqlite3.connect(DB_PATH)
    connection.row_factory = sqlite3.Row

    try:

        connection.execute(
            "PRAGMA foreign_keys = ON"
        )

        for job in jobs:
            sync_job(connection, job)

        connection.commit()

    except Exception:
        connection.rollback()
        raise

    finally:
        connection.close()


# ---------------------------------------------------------
# Main
# ---------------------------------------------------------

def main():

    print("Rowan: querying Purdue Slurm...")

    raw = query_squeue()
    jobs = parse_squeue(raw)

    print(f"Found {len(jobs)} active job(s).\n")

    sync_all(jobs)

    print("\nRowan synchronization complete.")


if __name__ == "__main__":
    main()
