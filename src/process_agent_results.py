#!/usr/bin/env python3

import json
import posixpath
import re
import shlex
import sqlite3
import subprocess
from pathlib import Path


DB_PATH = Path.home() / "rowan" / "orchestrator.db"

SSH_KEY = "/home/opd02/.ssh/id_ed25519"
CLUSTER = "doyle161@negishi.rcac.purdue.edu"


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


def valid_relative_script(path):
    if not path:
        return False

    if posixpath.isabs(path):
        return False

    normalized = posixpath.normpath(path)

    if normalized == "..":
        return False

    if normalized.startswith("../"):
        return False

    return True


def submit_vasp(workdir, submit_script):
    if not valid_relative_script(submit_script):
        raise RuntimeError(
            f"Unsafe submit script returned: {submit_script}"
        )

    command = (
        f"cd {shlex.quote(workdir)} && "
        f"test -f {shlex.quote(submit_script)} && "
        f"sbatch {shlex.quote(submit_script)}"
    )

    output = ssh(command)

    match = re.search(
        r"Submitted batch job (\d+)",
        output
    )

    if not match:
        raise RuntimeError(
            f"Could not parse VASP job ID from: {output}"
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
            WHERE state = 'AGENT_RESULT_READY'
            ORDER BY id
            """
        ).fetchall()

        print(
            f"Rowan: {len(workflows)} agent result(s) "
            "ready."
        )

        for workflow in workflows:

            agent_job_id = workflow["current_job_id"]
            workdir = workflow["cluster_directory"]

            result_path = (
                f"{workdir}/.rowan/"
                f"agent_result_{agent_job_id}.json"
            )

            raw = ssh(
                f"cat {shlex.quote(result_path)}"
            )

            result = json.loads(raw)

            action = result["action"]
            reason = result["reason"]
            submit_script = result["submit_script"]

            print(
                f"{workflow['name']}: {action}"
            )
            print(
                f"  {reason}"
            )

            if action == "RESTART_READY":

                new_job_id = submit_vasp(
                    workdir,
                    submit_script,
                )

                connection.execute(
                    """
                    UPDATE workflows
                    SET
                        state = 'SLURM_SUBMITTED',
                        current_job_id = ?,
                        current_job_type = 'VASP_RESTART',
                        next_action = NULL,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE id = ?
                    """,
                    (
                        new_job_id,
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
                    VALUES (
                        ?,
                        ?,
                        'VASP_RESTART',
                        'SUBMITTED',
                        CURRENT_TIMESTAMP,
                        NULL
                    )
                    """,
                    (
                        workflow["id"],
                        new_job_id,
                    ),
                )

                log_event(
                    connection,
                    workflow["id"],
                    "REPAIR_RESUBMITTED",
                    (
                        f"Codex prepared repair after agent "
                        f"{agent_job_id}. Rowan submitted "
                        f"replacement VASP job {new_job_id}. "
                        f"Reason: {reason}"
                    ),
                )

                print(
                    f"  Replacement VASP job: {new_job_id}"
                )

            elif action == "NO_RESTART_NEEDED":

                connection.execute(
                    """
                    UPDATE workflows
                    SET
                        state = 'CALCULATION_COMPLETE',
                        next_action = NULL,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE id = ?
                    """,
                    (workflow["id"],),
                )

                log_event(
                    connection,
                    workflow["id"],
                    "NO_RESTART_NEEDED",
                    reason,
                )

            else:

                connection.execute(
                    """
                    UPDATE workflows
                    SET
                        state = 'NEEDS_HUMAN_REVIEW',
                        next_action = 'REVIEW_AGENT_RESULT',
                        updated_at = CURRENT_TIMESTAMP
                    WHERE id = ?
                    """,
                    (workflow["id"],),
                )

                log_event(
                    connection,
                    workflow["id"],
                    "HUMAN_REVIEW_REQUESTED",
                    reason,
                )

        connection.commit()

    except Exception:
        connection.rollback()
        raise

    finally:
        connection.close()


if __name__ == "__main__":
    main()
