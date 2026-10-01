"""Slurm job_state -> OGC API - Processes status (statusCode of OGC 18-062r2)."""

STATE_MAP = {
    "PENDING": "accepted",
    "REQUEUED": "accepted",
    "RESV_DEL_HOLD": "accepted",
    "CONFIGURING": "running",
    "RUNNING": "running",
    "COMPLETING": "running",
    "SUSPENDED": "running",  # no OGC equivalent, the message says "suspended"
    "COMPLETED": "successful",
    "CANCELLED": "dismissed",
    "FAILED": "failed",
    "TIMEOUT": "failed",
    "NODE_FAIL": "failed",
    "OUT_OF_MEMORY": "failed",
    "BOOT_FAIL": "failed",
    "DEADLINE": "failed",
    "PREEMPTED": "failed",
}
FINAL = frozenset({"successful", "failed", "dismissed"})


def to_ogc_status(job_state):
    """Map the job_state list of the REST API to (OGC status, Slurm state).

    slurmrestd reports job_state as a list: the base state first, then flags such as COMPLETING.
    An unknown state counts as running, so that polling continues.
    """
    if isinstance(job_state, str):
        job_state = [job_state]
    for state in job_state:
        if state in STATE_MAP:
            return STATE_MAP[state], state
    return "running", ",".join(job_state)
