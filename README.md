# Rowan

Rowan is a lightweight orchestration system for monitoring long-running DFT calculations on an HPC cluster and automatically recovering calculations that fail because of wall-time limits or other restartable Slurm errors.

The system is designed around a simple principle:

**The Raspberry Pi manages workflow state, Slurm manages execution, and Codex handles repair/setup work on the cluster.**

Rowan does not move large VASP files off the cluster. Files such as `OUTCAR`, `WAVECAR`, `CHGCAR`, `vasprun.xml`, and `CONTCAR` remain on the HPC filesystem where Codex can inspect them directly.

## Architecture

```text
                         ┌──────────────────────────┐
                         │      Raspberry Pi        │
                         │                          │
                         │  Rowan orchestrator      │
                         │  SQLite database         │
                         │  systemd timer           │
                         │  SSH client              │
                         └────────────┬─────────────┘
                                      │
                                      │ SSH
                                      ▼
                         ┌──────────────────────────┐
                         │     Purdue / Negishi     │
                         │                          │
                         │  Slurm                   │
                         │  VASP calculations       │
                         │  Codex CLI               │
                         │  Calculation files       │
                         └──────────────────────────┘
```

The Pi is intentionally kept lightweight. It does not run VASP, analyze `OUTCAR` files, or run AI models locally. Its job is to periodically ask the cluster what is happening and decide whether another action is required.

## What Runs on the Raspberry Pi

The main Rowan installation currently lives under:

```text
~/rowan/
```

The Pi contains the workflow database and the scripts responsible for monitoring Slurm.

A typical structure is:

```text
rowan/
├── orchestrator.db
├── schema.sql
├── run_cycle.sh
├── src/
│   ├── sync_squeue.py
│   ├── reconcile_sacct.py
│   ├── dispatch_agents.py
│   └── process_agent_results.py
└── cluster/
    ├── repair_worker.slurm
    └── restart_schema.json
```

### `orchestrator.db`

SQLite is Rowan's persistent memory.

The database currently contains three tables:

```text
workflows
job_history
events
```

`workflows` stores the current state of each calculation, including its calculation directory, current Slurm job ID, and next required action.

`job_history` stores every Slurm attempt associated with a workflow. This is important because a single VASP calculation may be submitted several times before it converges.

`events` provides an audit trail of Rowan's decisions and state changes.

For example:

```text
VASP job 41846237
    ↓
TIMEOUT

Codex agent 41850001
    ↓
COMPLETED

Replacement VASP job 41850127
    ↓
RUNNING
```

All three Slurm jobs remain recorded rather than overwriting one another.

## `sync_squeue.py`

This script asks the Purdue cluster what jobs are currently active.

Conceptually, it runs:

```bash
ssh negishi "squeue ..."
```

and retrieves information such as:

```text
41846237|Pt3Cu-mp-12|PENDING|/depot/.../mp_alphaCH_12/00
```

Rowan records:

```text
Job ID:       41846237
Job name:     Pt3Cu-mp-12
State:        PENDING
Directory:    /depot/.../mp_alphaCH_12/00
```

The calculation directory is especially important because Slurm job IDs can change when a calculation resubmits itself.

For example:

```text
Job 1001
/depot/project/site_01
```

may later become:

```text
Job 1027
/depot/project/site_01
```

Rowan recognizes these as different Slurm attempts belonging to the same calculation.

Rowan agent jobs whose names begin with:

```text
rowan-agent-
```

are intentionally ignored by normal VASP workflow discovery.

## `reconcile_sacct.py`

`squeue` only shows jobs that currently exist in the queue.

When a job disappears, Rowan uses `sacct` to determine what actually happened.

Possible results include:

```text
COMPLETED
TIMEOUT
FAILED
OUT_OF_MEMORY
NODE_FAIL
CANCELLED
```

This distinction is critical.

A job disappearing from `squeue` does not necessarily mean the scientific calculation completed successfully.

For example:

```text
Job disappears
      ↓
sacct
      ↓
TIMEOUT
```

Rowan then marks the workflow:

```text
AGENT_RESTART_QUEUED
```

and records why the repair agent is needed.

If a calculation has already resubmitted itself under a new Slurm ID, Rowan only records the old attempt historically and leaves the new job alone.

## `dispatch_agents.py`

This script looks for workflows whose state is:

```text
AGENT_RESTART_QUEUED
```

It then remotely submits a Codex repair worker to Slurm.

The Pi does not run Codex itself.

Instead, the Pi performs something conceptually equivalent to:

```bash
ssh negishi "sbatch ~/rowan-agent/repair_worker.slurm ..."
```

The workflow then changes to:

```text
AGENT_RUNNING
```

and the Codex worker receives its own Slurm job ID.

## What Runs on the Cluster

The HPC cluster remains responsible for all computationally meaningful work.

The cluster contains:

```text
VASP
Slurm
Codex CLI
DFT calculation directories
Rowan repair worker scripts
```

Large VASP outputs never need to be transferred to the Pi.

## `repair_worker.slurm`

The repair worker is a centrally stored Slurm script on the cluster.

A single copy can be reused for any calculation.

Its job is to launch Codex non-interactively on a compute node:

```text
Slurm compute node
       ↓
codex exec
       ↓
inspect failed calculation
       ↓
prepare safe continuation
       ↓
write structured result
```

Codex runs with automatic approval handling inside a workspace-write sandbox.

The repair worker receives information such as:

```text
workflow ID
calculation directory
failed Slurm job ID
failure type
```

Codex can inspect files including:

```text
INCAR
POSCAR
CONTCAR
OUTCAR
OSZICAR
vasprun.xml
submission scripts
```

Codex is currently instructed not to submit the replacement VASP job itself.

Instead, it prepares the calculation for continuation and returns a structured decision.

## Agent Results

Each Codex worker writes a small JSON file under the affected calculation directory:

```text
<calculation>/.rowan/agent_result_<agent_job_id>.json
```

For example:

```json
{
  "action": "RESTART_READY",
  "reason": "Geometry optimization was interrupted by wall time and can safely continue.",
  "submit_script": "submit.slurm"
}
```

Possible actions are:

```text
RESTART_READY
NO_RESTART_NEEDED
HUMAN_REVIEW
```

This file is the machine-readable handoff between Codex and Rowan.

Codex logs remain on the cluster for debugging and auditing.

## `process_agent_results.py`

After the Codex Slurm job finishes, `reconcile_sacct.py` changes the workflow state to:

```text
AGENT_RESULT_READY
```

`process_agent_results.py` then reads the JSON result remotely.

If Codex returns:

```text
RESTART_READY
```

Rowan validates the returned submission-script path and executes:

```bash
sbatch <submit_script>
```

inside the calculation directory.

The workflow then moves back into the normal Slurm monitoring cycle.

If Codex returns:

```text
HUMAN_REVIEW
```

Rowan stops automatic processing and records:

```text
NEEDS_HUMAN_REVIEW
```

rather than guessing.

## Automated Monitoring

The Raspberry Pi runs Rowan periodically using a `systemd` timer.

The current interval is approximately every 15 minutes.

Each cycle runs:

```text
sync_squeue.py
       ↓
reconcile_sacct.py
       ↓
process_agent_results.py
       ↓
dispatch_agents.py
```

A lock prevents two Rowan cycles from running simultaneously.

The complete recovery loop therefore looks like:

```text
VASP calculation running
        ↓
Pi checks squeue
        ↓
job disappears
        ↓
Pi checks sacct
        ↓
TIMEOUT / FAILED
        ↓
workflow = AGENT_RESTART_QUEUED
        ↓
Pi submits Codex worker
        ↓
Codex runs on HPC compute node
        ↓
Codex inspects and prepares repair
        ↓
agent_result.json
        ↓
Pi reads result
        ↓
Rowan submits replacement VASP job
        ↓
new Slurm job appears
        ↓
normal monitoring resumes
```

## Why Use a Raspberry Pi?

The Pi does not provide computational power.

Its purpose is to provide an inexpensive, low-power machine that can remain online continuously and act as Rowan's persistent orchestrator.

This avoids requiring a personal laptop or desktop to remain running.

The Pi only needs enough resources to:

```text
run Python
maintain SQLite
make SSH connections
run systemd timers
parse lightweight Slurm output
```

A Raspberry Pi Zero W is sufficient for the current implementation.

## Security Model

The Pi uses a dedicated SSH key to connect to the HPC cluster.

The private key remains on the Pi and should never be committed to GitHub.

The repository should also exclude:

```text
SSH private keys
live SQLite databases
runtime logs
environment secrets
```

The Pi does not need to expose an SSH port to the public internet in order for Rowan to function. It only needs outbound SSH access to the cluster.

Codex runs on the cluster rather than receiving cluster credentials directly.

## Current Scope

Rowan is currently an experimental personal research tool rather than a production-ready workflow-management package.

The current focus is:

```text
Slurm
VASP
automatic failure detection
Codex-assisted restart preparation
wall-time recovery
workflow provenance
```

Future extensions could include:

```text
VASP convergence detection
calculation groups
binding-energy campaigns
NEB workflow dependencies
DOS calculations
automatic post-processing
notifications
support for additional HPC clusters
configuration files instead of hard-coded paths
support for Quantum ESPRESSO, CP2K, or ORCA
```

The long-term goal is for Rowan to handle the repetitive operational parts of computational chemistry workflows while leaving scientific decisions and unusual failures visible to the researcher.
