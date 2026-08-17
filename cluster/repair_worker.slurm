#!/bin/bash
#SBATCH --job-name=rowan-agent
#SBATCH --account=zeng46
#SBATCH --partition=cpu
#SBATCH --qos=standby
#SBATCH --time=00:45:00
#SBATCH --cpus-per-task=1
#SBATCH --mem=2G

set -euo pipefail

WORKFLOW_ID="$1"
WORKDIR="$2"
FAILED_JOB="$3"
FAILURE="$4"

ROWAN_HOME="$HOME/rowan-agent"
RESULT_DIR="$WORKDIR/.rowan"
RESULT_FILE="$RESULT_DIR/agent_result_${SLURM_JOB_ID}.json"

mkdir -p "$RESULT_DIR"

echo "Rowan workflow: $WORKFLOW_ID"
echo "Calculation: $WORKDIR"
echo "Failed VASP job: $FAILED_JOB"
echo "Failure: $FAILURE"
echo "Agent Slurm job: $SLURM_JOB_ID"

codex exec \
    --approve-for-me \
    --skip-git-repo-check \
    -C "$WORKDIR" \
    --output-schema "$ROWAN_HOME/restart_schema.json" \
    -o "$RESULT_FILE" \
    - <<EOF
You are the repair worker for Rowan, an unattended VASP workflow
orchestrator.

The calculation in this directory previously ran as Slurm job:

$FAILED_JOB

Slurm reported:

$FAILURE

Inspect the existing calculation files and determine whether this
calculation can safely be continued.

You may inspect files such as INCAR, POSCAR, CONTCAR, OUTCAR,
OSZICAR, vasprun.xml, and existing Slurm submission/restart scripts.

Rules:

- Never delete completed scientific output.
- Never overwrite an existing OUTCAR, vasprun.xml, WAVECAR, or CHGCAR.
- Do not alter scientific settings unless required to repair a clear
  technical failure.
- Do NOT call sbatch.
- Do NOT submit any jobs yourself.
- Prefer the project's existing restart/resubmission convention.
- If necessary, prepare files so the calculation is ready to continue.
- The submit_script field must contain a path RELATIVE to this
  calculation directory to a file that Rowan can submit using sbatch.
- Never return an absolute submit_script path or a path containing '..'.

Return:

RESTART_READY
    when the calculation has been safely prepared and may be resubmitted.

NO_RESTART_NEEDED
    when inspection shows that the scientific calculation is already
    complete and should not be resubmitted.

HUMAN_REVIEW
    when the failure is ambiguous, repeated, scientifically meaningful,
    or cannot safely be repaired automatically.
EOF
