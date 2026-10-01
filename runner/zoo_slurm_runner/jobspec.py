"""Job description and job script for POST job/submit.

No field of the execute request reaches a scheduler argument: account, partition and limits come from
the site profile and the identity, resources from the CWL, capped by the profile.
Secrets (storage keys) travel in the job environment, not in the script, and are merged into the CWL
parameters on the compute node.
"""
import base64
import json
import shlex

import yaml

# Processing parameters that carry credentials. They go into the job environment as ZOO_PARAM_<name>.
SECRET_PARAMS = (
    "STAGEOUT_AWS_ACCESS_KEY_ID",
    "STAGEOUT_AWS_SECRET_ACCESS_KEY",
    "STAGEIN_AWS_ACCESS_KEY_ID",
    "STAGEIN_AWS_SECRET_ACCESS_KEY",
)
OUTPUTS_OBJECT = "zoo-outputs.json"

# Merges the secret parameters from the environment into params.json, then uploads the CWL output object.
_MERGE = """\
import json, os
p = json.load(open("params.json"))
for k, v in os.environ.items():
    if k.startswith("ZOO_PARAM_"):
        p[k[len("ZOO_PARAM_"):]] = v
fd = os.open("params.json", os.O_WRONLY | os.O_TRUNC, 0o600)
os.write(fd, json.dumps(p).encode()); os.close(fd)
"""
_UPLOAD = """\
import boto3, os, sys
s3 = boto3.client("s3", endpoint_url=os.environ["ZOO_PARAM_STAGEOUT_AWS_SERVICEURL"],
                  aws_access_key_id=os.environ["ZOO_PARAM_STAGEOUT_AWS_ACCESS_KEY_ID"],
                  aws_secret_access_key=os.environ["ZOO_PARAM_STAGEOUT_AWS_SECRET_ACCESS_KEY"],
                  region_name=os.environ.get("ZOO_PARAM_STAGEOUT_AWS_REGION") or None)
s3.upload_file("outputs.json", sys.argv[1], sys.argv[2])
"""


def _b64(text):
    return base64.b64encode(text.encode()).decode()


def outputs_key(params):
    """S3 key of the uploaded CWL output object, next to the stage-out catalogue."""
    return f"{params['process'].strip('/')}/{OUTPUTS_OBJECT}"


def split_params(params):
    """Return (params without secrets, environment entries for the secrets and the stage-out target)."""
    public = {k: v for k, v in params.items() if k not in SECRET_PARAMS}
    env = [f"ZOO_PARAM_{k}={params[k]}" for k in SECRET_PARAMS if params.get(k)]
    # The upload step needs the stage-out endpoint and region as well; they are not secret.
    for k in ("STAGEOUT_AWS_SERVICEURL", "STAGEOUT_AWS_REGION"):
        if params.get(k):
            env.append(f"ZOO_PARAM_{k}={params[k]}")
    return public, env


def render_job_script(profile, user_name, cwl, params, bucket):
    work_root = profile.work_root.format(user=user_name)
    engine = " ".join([profile.cwl_engine, *profile.cwl_engine_args])
    modules = "\n".join(f"module load {shlex.quote(m)}" for m in profile.modules)
    return f"""#!/bin/bash
set -eo pipefail
umask 077
WORK={shlex.quote(work_root)}/"$SLURM_JOB_ID"
mkdir -p "$WORK" && cd "$WORK"
# Per-job HOME, per-user image cache: a shared /tmp/.apptainer breaks the second user.
export HOME="$WORK"
export APPTAINER_CACHEDIR={shlex.quote(work_root)}/.apptainer-cache
mkdir -p "$APPTAINER_CACHEDIR"
trap 'cd / && rm -rf "$WORK"' EXIT
{modules}
echo {_b64(yaml.safe_dump(cwl, sort_keys=False))} | base64 -d > app-package.cwl
echo {_b64(json.dumps(params))} | base64 -d > params.json
echo {_b64(_MERGE)} | base64 -d > merge.py
echo {_b64(_UPLOAD)} | base64 -d > upload.py
python3 merge.py
{engine} --outdir out 'app-package.cwl#main' params.json > outputs.json
apptainer exec --cleanenv --env-file <(env | grep '^ZOO_PARAM_') {shlex.quote(profile.upload_image)} \\
  python3 upload.py {shlex.quote(bucket)} {shlex.quote(outputs_key(params))}
echo "done"
"""


def build_job_description(profile, identity, cwl, params, ogc_job_id, process_id, cores, ram_mb,
                          time_limit_min=None):
    """Return the 'job' object for POST job/submit."""
    public, secret_env = split_params(params)
    work_root = profile.work_root.format(user=identity["user_name"])
    limit = min(time_limit_min or profile.default_time_limit_min, profile.max_time_limit_min)
    job = {
        "name": f"zoo-{process_id}-{ogc_job_id[:8]}",
        # slurm-user: slurmrestd reports an empty user_name when it cannot resolve the uid.
        "comment": f"ogc-job={ogc_job_id};platform-user={identity['platform_user']};slurm-user={identity['user_name']}",
        "current_working_directory": work_root,
        "standard_output": f"{work_root}/zoo-%j.out",
        "standard_error": f"{work_root}/zoo-%j.err",
        "environment": ["PATH=/usr/local/bin:/usr/bin:/bin", *secret_env],
        "time_limit": {"set": True, "number": int(limit)},
        "cpus_per_task": int(min(cores, profile.max_cpus_per_task)),
        "memory_per_node": {"set": True, "number": int(min(ram_mb, profile.max_memory_mb))},
        "requeue": False,
        # Submitted on hold; the runner releases it after checking the uid Slurm assigned (minimum_uid).
        "hold": True,
        "script": render_job_script(profile, identity["user_name"], cwl, public, params["STAGEOUT_OUTPUT"]),
    }
    partition = identity.get("partition") or profile.default_partition
    if partition:
        job["partition"] = partition
    if identity.get("account"):
        job["account"] = identity["account"]
    return job
