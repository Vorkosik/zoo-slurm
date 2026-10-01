"""Unit tests: state mapping, resource caps, request parameters, job environment, credentials, site profile."""
import base64
import re

import pytest

from zoo_slurm_runner.jobspec import SECRET_PARAMS, build_job_description, split_params
from zoo_slurm_runner.profile import SiteProfile
from zoo_slurm_runner.states import STATE_MAP, to_ogc_status

PROFILE = SiteProfile(name="t", rest_url="http://x:6820", default_partition="all", max_cpus_per_task=4,
                      max_memory_mb=8192, default_time_limit_min=60, max_time_limit_min=720,
                      work_root="/tmp/zoo-jobs/{user}")
IDENTITY = {"user_name": "slurm-test-1", "platform_user": "slurm-test-1", "account": None}
PARAMS = {"process": "processing-results/abc", "aoi": "1,2,3,4", "STAGEOUT_OUTPUT": "results",
          "STAGEOUT_AWS_SERVICEURL": "http://s3:9000", "STAGEOUT_AWS_REGION": "us-east-1",
          "STAGEOUT_AWS_ACCESS_KEY_ID": "KEYID", "STAGEOUT_AWS_SECRET_ACCESS_KEY": "TOPSECRET"}
CWL = {"cwlVersion": "v1.2", "$graph": []}


def job(**kw):
    args = dict(profile=PROFILE, identity=IDENTITY, cwl=CWL, params=PARAMS, ogc_job_id="abcdef1234",
                process_id="echo", cores=2, ram_mb=1024)
    args.update(kw)
    return build_job_description(**args)


@pytest.mark.parametrize("state,expected", sorted(STATE_MAP.items()))
def test_every_state(state, expected):
    assert to_ogc_status([state])[0] == expected


def test_flags_after_base_state():
    assert to_ogc_status(["RUNNING", "COMPLETING"]) == ("running", "RUNNING")
    assert to_ogc_status(["SOMETHING_NEW"])[0] == "running"


def test_limits_capped_and_time_limit_set():
    j = job(cores=64, ram_mb=999999, time_limit_min=99999)
    assert j["cpus_per_task"] == 4
    assert j["memory_per_node"] == {"set": True, "number": 8192}
    assert j["time_limit"] == {"set": True, "number": 720}
    assert job()["time_limit"]["number"] == 60


def test_request_cannot_set_account_or_partition():
    params = {**PARAMS, "account": "admin", "partition": "gpu", "qos": "high"}
    j = job(params=params)
    assert j["partition"] == "all"
    assert "account" not in j and "qos" not in j


def test_environment_only_listed_variables():
    names = {e.split("=", 1)[0] for e in job()["environment"]}
    allowed = {"PATH"} | {f"ZOO_PARAM_{k}" for k in (*SECRET_PARAMS, "STAGEOUT_AWS_SERVICEURL", "STAGEOUT_AWS_REGION")}
    assert names <= allowed
    assert "ZOO_PARAM_STAGEOUT_AWS_SECRET_ACCESS_KEY" in names


def test_credentials_not_in_script():
    script = job()["script"]
    decoded = " ".join(base64.b64decode(b).decode(errors="ignore")
                       for b in re.findall(r"echo ([A-Za-z0-9+/=]{8,}) \| base64", script))
    for text in (script, decoded):
        assert "TOPSECRET" not in text and "KEYID" not in text


def test_comment_carries_ogc_job_and_owner():
    from zoo_slurm_runner.runner import _comment_field

    c = job()["comment"]
    assert _comment_field(c, "ogc-job") == "abcdef1234"
    assert _comment_field(c, "slurm-user") == "slurm-test-1"
    assert _comment_field(c, "missing") is None


def test_split_params_keeps_public():
    public, env = split_params(PARAMS)
    assert "STAGEOUT_AWS_SECRET_ACCESS_KEY" not in public and public["aoi"] == "1,2,3,4"


def test_profile_from_conf_section():
    from zoo_slurm_runner.profile import profile_from_conf

    p = profile_from_conf({"name": "slinky", "rest_url": "http://slurm-restapi.slurm.svc:6820",
                           "default_partition": "all", "max_cpus_per_task": "4",
                           "cwl_engine_args": "--singularity --debug", "work_root": "/tmp/zoo-jobs/{user}",
                           "allowed_accounts": "u1:a,b u2:c"})
    assert p.max_cpus_per_task == 4 and p.cwl_engine_args == ("--singularity", "--debug")
    assert p.allowed_accounts == {"u1": ["a", "b"], "u2": ["c"]}
    assert p.api == "http://slurm-restapi.slurm.svc:6820/slurm/v0.0.45"


def test_account_mapping():
    p = SiteProfile(name="t", rest_url="http://x", allowed_accounts={"u1": ["proj-a"]})
    assert p.account_for("u1") == "proj-a"
    with pytest.raises(PermissionError):
        p.account_for("u2")
    assert PROFILE.account_for("anyone") is None
