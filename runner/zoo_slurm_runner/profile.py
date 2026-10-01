"""Site profile: per-centre settings and limits. The runner never takes them from the execute request."""
import os
from dataclasses import dataclass, field
from typing import Optional

import yaml


@dataclass(frozen=True)
class SiteProfile:
    name: str
    rest_url: str
    api_version: str = "v0.0.45"
    default_partition: Optional[str] = None
    max_cpus_per_task: int = 8
    max_memory_mb: int = 32768
    default_time_limit_min: int = 60
    max_time_limit_min: int = 1440
    work_root: str = "/home/{user}/zoo-jobs"
    cwl_engine: str = "cwltool"
    cwl_engine_args: tuple = ("--singularity",)
    modules: tuple = ()
    # Image with python3 and boto3 that uploads the CWL output object after the run.
    upload_image: str = "docker://ghcr.io/terradue/ogc-eo-application-package-hands-on/stage:1.3.2"
    # Platform user -> allowed Slurm accounts. Empty: no account is set (cluster without accounting).
    allowed_accounts: dict = field(default_factory=dict)
    minimum_uid: int = 1000

    @property
    def api(self):
        return f"{self.rest_url.rstrip('/')}/slurm/{self.api_version}"

    def account_for(self, user_name):
        """Return the Slurm account for a user, None if the site has no accounts. Raise if not allowed."""
        if not self.allowed_accounts:
            return None
        accounts = self.allowed_accounts.get(user_name)
        if not accounts:
            raise PermissionError(f"user {user_name} has no Slurm account at site {self.name}")
        return accounts[0]


def load_profile(path=None):
    path = path or os.environ.get("SLURM_SITE_PROFILE", "/assets/site-profile.yaml")
    with open(path) as f:
        data = yaml.safe_load(f)
    for key in ("cwl_engine_args", "modules"):
        if key in data:
            data[key] = tuple(data[key])
    return SiteProfile(**data)


def profile_from_conf(section):
    """Build the profile from a main.cfg section (chart value customConfig.main.<section>).

    The chart has no channel for extra files, but customConfig sections reach the service as
    conf[<section>] with string values. Lists are space-separated; allowed_accounts is
    "user1:acc1,acc2 user2:acc3".
    """
    data = {}
    for name, f in SiteProfile.__dataclass_fields__.items():
        if name not in section:
            continue
        raw = str(section[name]).strip()
        if name in ("cwl_engine_args", "modules"):
            data[name] = tuple(raw.split())
        elif name == "allowed_accounts":
            data[name] = {u: a.split(",") for u, a in (e.split(":", 1) for e in raw.split())}
        elif f.type in (int, "int"):
            data[name] = int(raw)
        else:
            data[name] = raw or None
    return SiteProfile(**data)
