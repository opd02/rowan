#!/usr/bin/env python3

import re
import sqlite3
import subprocess
from pathlib import Path


# =========================================================
# Configuration
# =========================================================

DB_PATH = Path.home() / "rowan" / "orchestrator.db"

SSH_KEY = "/home/opd02/.ssh/id_ed25519"
CLUSTER = "doyle161@negishi.rcac.purdue.edu"


# Slurm terminal states where a VASP calculation should
# be inspected by a Rowan Codex repair agent.
RESTARTABLE_FAILURES = {
    "TIMEOUT",
    "FAILED",
    "OUT_OF_MEMORY",
    "NODE_FAIL",
    "BOOT_FAIL",
    "PREEMPTED",
}


# States Rowan considers potentially still active.
#
# SUBMITTED is a Rowan-created state used immediately after
# process_agent_results.py submits a replacement VASP job,
# before sync_squeue.py has observed it as PENDING/RUNNING.
NONTERMINAL_STATES = {
    "SUBMITTED",
    "PENDING",
    "RUNNING",
    "CONFIGURING",
    "COMPLETING",
    "SUSPENDED",
    "RESIZING",
    "REQUEUED",
}


# =========================================================
# SSH
# =========================================================

def ssh(remote_command, timeout=30):
    """
    Run a command on Negishi and return stdout.
    """

    result = subprocess.run(
        [
            "ssh",
            "-i",
            SSH_KEY,
            CLUSTER,
            remote_command,
        ],
        capture_output=True,
        text=True,
        timeout=timeout,
    )

    if result.returncode != 0:
        raise RuntimeError(
            "SSH command failed:\n"
            + result.stderr.strip()
        )

    return result.stdout.strip()


# =========================================================
# squeue
# =========================================================

def get_active_job_ids():
    """
    Return all Slurm job IDs currently visible in squeue.

    This intentionally includes Rowan agent jobs.

    sync_squeue.py ignores Rowan agent jobs when creating/
    updating VASP workflows, but reconcile_sacct.py still
    needs to know whether an agent job is currently alive.
    """

    output = ssh(
        "squeue -h -u $USER -o '%i'"
    )

    return {
        line.strip()
        for line in output.splitlines()
        if line.strip()
    }


# =========================================================
# sacct
# =========================================================

def normalize_state(state):
    """
    Normalize Slurm states.

    Examples:

        CANCELLED+
        CANCELLED by 12345

    become:

        CANCELLED
    """

    state = state.strip()

    if not state:
        return "UNKNOWN"

    state = state.split()[0]
    state = state.rstrip("+")

    return state


def valid_job_id(job_id):
    """
    Safety check before inserting a job ID into the remote
    sacct shell command.
    """

    return bool(
        re.fullmatch(
            r"[0-9_.+-]+",
            str(job_id)
        )
    )


def get_sacct_record(job_id):
    """
    Query Slurm accounting for one job.

    Returns the top-level Slurm allocation rather than
    .batch/.extern job steps.
    """

    if not valid_job_id(job_id):
        raise ValueError(
            f"Unexpected Slurm job ID: {job_id}"
        )

    command = (
        f"sacct -X -n -P -j {job_id} "
        "--format=JobIDRaw,State,ExitCode,End,WorkDir"
    )

    output = ssh(command)

    if not output:
        return None

    for line in output.splitlines():

        parts = line.split("|")

        if len(parts) < 5:
            continue

        returned_job_id = parts[0].strip()

        # Ignore job steps and only use the allocation itself.
        if returned_job_id != str(job_id):
            continue

        end_time = parts[3].strip()

        if end_time in {"", "Unknown", "N/A", "None"}:
            end_time = None

        return {
            "job_id": returned_job_id,
            "state": normalize_state(parts[1]),
            "exit_code": parts[2].strip(),
            "end_time": end_time,
            "work_dir": parts[4].strip(),
        }

    return None


# =========================================================
# Event logging
# =========================================================

def log_event(
    connection,
    workflow_id,
    event_type,
    message,
):
    """
    Add an entry to Rowan's events table.
    """

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


# =========================================================
# Terminal job handling
# =========================================================

def handle_terminal_job(
    connection,
    history,
    record,
):
    """
    Handle a job that sacct reports as terminal.

    This function understands two fundamentally different
    kinds of Slurm jobs:

        1. VASP calculations
        2. Rowan Codex repair agents

    They must not be treated the same way.
    """

    workflow_id = history["workflow_id"]
    job_id = history["slurm_job_id"]
    job_type = history["job_type"]
    state = record["state"]

    current_job_id = history["current_job_id"]

    is_current_attempt = (
        str(current_job_id) == str(job_id)
    )

    # -----------------------------------------------------
    # First: permanently record how this Slurm attempt ended
    # -----------------------------------------------------

    connection.execute(
        """
        UPDATE job_history
        SET
            status = ?,
            completed_at = ?
        WHERE id = ?
        """,
        (
            state,
            record["end_time"],
            history["history_id"],
        ),
    )

    # =====================================================
    # ROWAN AGENT JOB
    # =====================================================

    if job_type == "AGENT_REPAIR":

        # This should normally never happen, but don't let an
        # old agent result overwrite a newer workflow state.
        if not is_current_attempt:

            log_event(
                connection,
                workflow_id,
                "OLD_AGENT_FINISHED",
                (
                    f"Old Rowan repair agent {job_id} ended "
                    f"as {state}, but workflow is currently "
                    f"tracking job {current_job_id}."
                ),
            )

            print(
                f"  Old Rowan agent ended as {state}; "
                "workflow has already moved on."
            )

            return

        # -------------------------------------------------
        # Agent itself completed successfully.
        #
        # This does NOT yet mean the VASP calculation has
        # been fixed.
        #
        # It means process_agent_results.py should inspect
        # the agent's JSON result.
        # -------------------------------------------------

        if state == "COMPLETED":

            connection.execute(
                """
                UPDATE workflows
                SET
                    state = 'AGENT_RESULT_READY',
                    next_action = 'READ_AGENT_RESULT',
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
                """,
                (workflow_id,),
            )

            log_event(
                connection,
                workflow_id,
                "AGENT_COMPLETED",
                (
                    f"Rowan repair agent {job_id} completed "
                    "successfully. Agent result is ready "
                    "for processing."
                ),
            )

            print(
                "  Rowan repair agent completed successfully."
            )

            print(
                "  Workflow -> AGENT_RESULT_READY"
            )

            return

        # -------------------------------------------------
        # Agent itself failed.
        #
        # Don't recursively wake another Codex agent.
        # Stop and ask for human review instead.
        # -------------------------------------------------

        connection.execute(
            """
            UPDATE workflows
            SET
                state = 'NEEDS_HUMAN_REVIEW',
                next_action = ?,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
            """,
            (
                f"AGENT_FAILED_{state}",
                workflow_id,
            ),
        )

        log_event(
            connection,
            workflow_id,
            "AGENT_FAILED",
            (
                f"Rowan repair agent {job_id} ended as "
                f"{state} with exit code "
                f"{record['exit_code']}."
            ),
        )

        print(
            f"  Rowan repair agent itself failed: {state}"
        )

        print(
            "  Workflow -> NEEDS_HUMAN_REVIEW"
        )

        return

    # =====================================================
    # VASP / CALCULATION JOB
    # =====================================================

    # -----------------------------------------------------
    # An old VASP attempt may have disappeared because the
    # calculation's own script already submitted a new job.
    #
    # Record the old result, but don't disturb the new job.
    # -----------------------------------------------------

    if not is_current_attempt:

        log_event(
            connection,
            workflow_id,
            "SLURM_ATTEMPT_FINISHED",
            (
                f"Old Slurm attempt {job_id} ended as "
                f"{state}. Workflow is already using "
                f"job {current_job_id}."
            ),
        )

        print(
            f"  Historical VASP attempt ended as {state}."
        )

        print(
            f"  Replacement/current job: {current_job_id}"
        )

        return

    # -----------------------------------------------------
    # Current VASP job completed normally according to Slurm
    # -----------------------------------------------------

    if state == "COMPLETED":

        connection.execute(
            """
            UPDATE workflows
            SET
                state = 'SLURM_COMPLETED',
                next_action = NULL,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
            """,
            (workflow_id,),
        )

        log_event(
            connection,
            workflow_id,
            "SLURM_COMPLETED",
            (
                f"Current Slurm job {job_id} completed "
                f"normally with exit code "
                f"{record['exit_code']}."
            ),
        )

        print(
            "  Current VASP attempt COMPLETED."
        )

        return

    # -----------------------------------------------------
    # Cancelled jobs are deliberately NOT automatically
    # restarted.
    # -----------------------------------------------------

    if state == "CANCELLED":

        connection.execute(
            """
            UPDATE workflows
            SET
                state = 'CANCELLED',
                next_action = NULL,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
            """,
            (workflow_id,),
        )

        log_event(
            connection,
            workflow_id,
            "SLURM_CANCELLED",
            (
                f"Current Slurm job {job_id} was cancelled. "
                "Rowan will not automatically restart it."
            ),
        )

        print(
            "  Current VASP attempt was CANCELLED."
        )

        print(
            "  No automatic repair requested."
        )

        return

    # -----------------------------------------------------
    # Current VASP attempt failed in a restart-worthy way.
    #
    # Do NOT launch Codex here.
    #
    # We only change workflow state.
    # dispatch_agents.py will see this state later in the
    # Rowan cycle and submit the Codex worker.
    # -----------------------------------------------------

    if state in RESTARTABLE_FAILURES:

        next_action = (
            f"INSPECT_AND_RESTART_AFTER_{state}"
        )

        connection.execute(
            """
            UPDATE workflows
            SET
                state = 'AGENT_RESTART_QUEUED',
                next_action = ?,
                retry_count = retry_count + 1,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
            """,
            (
                next_action,
                workflow_id,
            ),
        )

        log_event(
            connection,
            workflow_id,
            "RESTART_REQUIRED",
            (
                f"Current Slurm job {job_id} ended as "
                f"{state} with exit code "
                f"{record['exit_code']}. "
                "Rowan repair agent requested."
            ),
        )

        print(
            f"  VASP FAILURE DETECTED: {state}"
        )

        print(
            "  Workflow -> AGENT_RESTART_QUEUED"
        )

        return

    # -----------------------------------------------------
    # Anything Rowan doesn't recognize gets stopped rather
    # than guessed about.
    # -----------------------------------------------------

    connection.execute(
        """
        UPDATE workflows
        SET
            state = 'NEEDS_HUMAN_REVIEW',
            next_action = ?,
            updated_at = CURRENT_TIMESTAMP
        WHERE id = ?
        """,
        (
            f"REVIEW_SLURM_STATE_{state}",
            workflow_id,
        ),
    )

    log_event(
        connection,
        workflow_id,
        "UNHANDLED_SLURM_STATE",
        (
            f"Current Slurm job {job_id} ended in "
            f"unhandled state {state}."
        ),
    )

    print(
        f"  Unhandled terminal state: {state}"
    )

    print(
        "  Workflow -> NEEDS_HUMAN_REVIEW"
    )


# =========================================================
# Main reconciliation
# =========================================================

def main():

    if not DB_PATH.exists():
        raise FileNotFoundError(
            f"Rowan database does not exist: {DB_PATH}"
        )

    print(
        "Rowan: checking Slurm accounting..."
    )

    active_job_ids = get_active_job_ids()

    print(
        f"{len(active_job_ids)} job(s) currently in squeue."
    )

    connection = sqlite3.connect(DB_PATH)
    connection.row_factory = sqlite3.Row

    try:

        connection.execute(
            "PRAGMA foreign_keys = ON"
        )

        # -------------------------------------------------
        # Look at every Slurm attempt Rowan currently thinks
        # may still be alive.
        # -------------------------------------------------

        placeholders = ",".join(
            "?" for _ in NONTERMINAL_STATES
        )

        histories = connection.execute(
            f"""
            SELECT
                j.id AS history_id,
                j.workflow_id,
                j.slurm_job_id,
                j.job_type,
                j.status,

                w.name AS workflow_name,
                w.cluster_directory,
                w.current_job_id,
                w.current_job_type,
                w.state AS workflow_state

            FROM job_history AS j

            JOIN workflows AS w
                ON w.id = j.workflow_id

            WHERE
                j.status IS NULL
                OR j.status IN ({placeholders})

            ORDER BY j.id
            """,
            tuple(NONTERMINAL_STATES),
        ).fetchall()

        # -------------------------------------------------
        # Anything still in squeue requires no accounting
        # reconciliation yet.
        # -------------------------------------------------

        missing_jobs = [
            history
            for history in histories
            if str(history["slurm_job_id"])
            not in active_job_ids
        ]

        print(
            f"{len(missing_jobs)} previously-active "
            "job(s) no longer in squeue."
        )

        # -------------------------------------------------
        # Ask sacct what happened to every vanished job.
        # -------------------------------------------------

        for history in missing_jobs:

            job_id = history["slurm_job_id"]

            print()
            print(
                f"Checking {job_id} "
                f"({history['workflow_name']}, "
                f"{history['job_type']})..."
            )

            record = get_sacct_record(job_id)

            # Slurm accounting can occasionally lag behind
            # squeue. If there is no record yet, simply try
            # again during Rowan's next cycle.
            if record is None:

                print(
                    "  No sacct record yet; "
                    "will check again later."
                )

                continue

            state = record["state"]

            print(
                f"  sacct: {state} "
                f"(exit {record['exit_code']})"
            )

            # sacct may briefly still report an active state.
            if state in NONTERMINAL_STATES:

                print(
                    "  sacct still considers this job "
                    "active; will retry later."
                )

                continue

            handle_terminal_job(
                connection,
                history,
                record,
            )

        connection.commit()

    except Exception:

        connection.rollback()
        raise

    finally:

        connection.close()

    print()
    print(
        "Rowan accounting reconciliation complete."
    )


if __name__ == "__main__":
    main()
