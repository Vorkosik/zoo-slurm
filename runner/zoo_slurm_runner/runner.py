"""SlurmRestRunner: runs the wrapped CWL Application Package as one Slurm job through slurmrestd.

The job runs under the POSIX account of the requesting user. Reuses ZooCalrissianRunner.wrap() (stage-in and stage-out through
eoap-cwlwrap) and the template's execution handler (workspace storage, STAC output handling).
"""
import json
import os
import time

import boto3
from loguru import logger
from zoo_calrissian_runner import ZooCalrissianRunner

from .client import SlurmRestClient, SlurmRestError
from .http import no_proxy_env
from .jobspec import build_job_description, outputs_key
from .profile import load_profile, profile_from_conf
from .states import FINAL, to_ogc_status
from .tokens import ClientCredentialsTokenProvider, KeycloakExchangeTokenProvider, TokenError

try:
    import zoo
except ImportError:
    from zoo_runner_common.zoostub import ZooStub

    zoo = ZooStub()

RUNNER_CLASS = "zoo_slurm_runner.SlurmRestRunner"


def _comment_field(comment, key):
    """Value of key in a job comment of the form k1=v1;k2=v2 (set by the runner at submission)."""
    for part in (comment or "").split(";"):
        name, _, value = part.partition("=")
        if name == key:
            return value
    return None


class SlurmRestRunner(ZooCalrissianRunner):
    def __init__(self, cwl, conf, inputs, outputs, execution_handler=None, profile=None, client=None,
                 user_tokens=None, poll_tokens=None):
        super().__init__(cwl=cwl, conf=conf, inputs=inputs, outputs=outputs,
                         execution_handler=execution_handler)
        site = self.zoo_conf.conf.get("slurm_site")
        self.profile = profile or (profile_from_conf(site) if site else load_profile())
        self.client = client or SlurmRestClient(self.profile, os.environ.get("SLURM_REST_CA_BUNDLE"))
        self._user_tokens = user_tokens
        self._poll_tokens = poll_tokens
        self.monitor_interval = int(os.environ.get("SLURM_MONITOR_INTERVAL", "30"))
        self.max_poll_errors = int(os.environ.get("SLURM_MAX_POLL_ERRORS", "10"))
        self.job_id = None
        self.last_job = None

    # -- identities -----------------------------------------------------------------------------
    @property
    def lenv(self):
        return self.zoo_conf.conf["lenv"]

    def user_tokens(self):
        if self._user_tokens is None:
            subject = self.zoo_conf.conf.get("auth_env", {}).get("jwt")
            if not subject:
                raise TokenError("no user token in auth_env")
            self._user_tokens = KeycloakExchangeTokenProvider(subject_token=subject)
        return self._user_tokens

    def poll_tokens(self):
        if self._poll_tokens is None:
            self._poll_tokens = ClientCredentialsTokenProvider()
        return self._poll_tokens

    def identity(self):
        """POSIX identity for the job: user name from the exchanged token's sun claim, account from the profile."""
        tokens = self.user_tokens()
        user_name = tokens.slurm_user()
        if not user_name:
            raise PermissionError("the user has no Slurm account (no claim sun after token exchange)")
        platform_user = self.zoo_conf.conf.get("auth_env", {}).get("preferred_username", user_name)
        return {"user_name": user_name, "platform_user": platform_user,
                "account": self.profile.account_for(user_name)}

    # -- parameters -----------------------------------------------------------------------------
    def orchestrator_parameters(self, wrapped):
        """Processing parameters for the wrapped workflow 'main', as ZooCalrissianRunner.execute() builds them."""
        from types import SimpleNamespace

        main = next(e for e in wrapped.get("$graph", []) if e.get("id") == "main" and e.get("class") == "Workflow")
        inputs = main.get("inputs", [])
        workflow = SimpleNamespace(inputs=[SimpleNamespace(id=i.get("id"), type_=i.get("type")) for i in inputs])
        params = {
            "process": self.get_namespace_name(),
            **self.inputs.get_processing_parameters(workflow),
            **self.handler.get_additional_parameters(),
        }
        ids = {i["id"] for i in inputs}
        for i in inputs:
            t = i.get("type")
            if isinstance(t, str) and "string_format.yaml" in t:
                name = i["id"].split("#")[-1].split("/")[-1]
                if isinstance(params.get(name), str):
                    params[name] = {"value": params[name]}
        out = {}
        for key, value in params.items():
            if key.startswith("ADES_") and key.replace("ADES_", "", 1) in ids:
                key = key.replace("ADES_", "", 1)
            out[key] = value
        return out

    def _ram_mb(self):
        return int(str(self.get_max_ram()).removesuffix("Mi"))

    # -- execution ------------------------------------------------------------------------------
    def execute(self):
        self.update_status(progress=2, message="Pre-execution hook")
        self.handler.pre_execution_hook()
        if not self.assert_parameters():
            return self._fail("mandatory parameters missing")

        try:
            identity = self.identity()
        except (TokenError, PermissionError) as e:
            logger.error(f"identity: {e}")
            return self._fail("authorisation for HPC failed")

        self.update_status(progress=5, message="wrapping workflow")
        wrapped = self.wrap()
        params = self.orchestrator_parameters(wrapped)
        job = build_job_description(
            self.profile, identity, wrapped, params, ogc_job_id=self.lenv["usid"],
            process_id=self.get_workflow_id(), cores=self.get_max_cores(), ram_mb=self._ram_mb())

        try:
            self.job_id = self.client.submit(identity["user_name"], self.user_tokens().token(), job)
        except (SlurmRestError, TokenError) as e:
            logger.error(f"submit: {e}")
            return self._fail(f"Slurm submission failed: {e}")
        self.record_backend(self.job_id, identity["user_name"])
        refused = self.check_uid(identity["user_name"])
        if refused:
            return self._fail(refused)
        self.update_status(progress=10, message=f"submitted as Slurm job {self.job_id}")

        status, raw = self.monitor()
        logger.info(f"Slurm job {self.job_id} ended with {raw}, OGC status {status}")
        if status != "successful":
            return self._fail(f"Slurm job {self.job_id} ended with {raw}")

        self.update_status(progress=90, message="reading outputs")
        try:
            output = self.read_outputs(params)
        except Exception as e:  # noqa: BLE001 - any storage error means no results
            logger.error(f"outputs: {e}")
            return self._fail("no results")
        self.outputs.set_output(output)
        tool_logs = [self.write_job_record()]
        self.handler.handle_outputs(log=None, output=output, usage_report=None, tool_logs=tool_logs)
        self.update_status(progress=97, message="Post-execution hook")
        self.handler.post_execution_hook(log=None, output=output, usage_report=None, tool_logs=tool_logs)
        self.update_status(progress=100, message="execution successful")
        return zoo.SERVICE_SUCCEEDED

    def record_backend(self, job_id, user_name):
        """Keep runner class and Slurm job where a runner-aware runDismiss can find them.

        ZOO writes <tmpPath>/<usid>_lenv.cfg when the job starts and does not update it, so the runner
        adds its keys to the file itself, as ZooWESRunner does for run_id.
        """
        import configparser

        recorded = {"runner_class": RUNNER_CLASS, "slurm_job_id": str(job_id), "slurm_user": user_name}
        self.lenv.update(recorded)
        path = os.path.join(self.zoo_conf.conf["main"]["tmpPath"], f"{self.lenv['usid']}_lenv.cfg")
        saved = configparser.RawConfigParser()
        saved.optionxform = str  # keep the key case of ZOO's file
        saved.read(path)
        if not saved.has_section("lenv"):
            saved.add_section("lenv")
        for key, value in recorded.items():
            saved.set("lenv", key, value)
        with open(path, "w") as f:
            saved.write(f)

    def check_uid(self, user_name):
        """Release the held job, or cancel it if its uid is below the site's minimum_uid.

        The uid is only known once Slurm has the job (user_id in GET job), so the job is submitted on
        hold and never runs before this check. DisableRootJobs refuses only uid 0; this check covers
        service accounts such as the SlurmUser. Returns an error message, or None if
        the job was released.
        """
        poll = self.poll_tokens()
        try:
            job = self.client.get_job(self.job_id, poll.slurm_user(), poll.token())
        except (SlurmRestError, TokenError) as e:
            logger.error(f"uid check: {e}")
            job = {}
        uid = job.get("user_id")
        if isinstance(uid, int) and uid >= self.profile.minimum_uid:
            try:
                self.client.release(self.job_id, user_name, self.user_tokens().token())
                return None
            except (SlurmRestError, TokenError) as e:
                logger.error(f"release: {e}")
                return f"Slurm job {self.job_id} could not be released: {e}"
        logger.error(f"Slurm job {self.job_id} runs as uid {uid}, below minimum_uid {self.profile.minimum_uid}: cancel")
        try:
            self.client.cancel(self.job_id, user_name, self.user_tokens().token())
        except (SlurmRestError, TokenError) as e:
            logger.error(f"cancel after uid check: {e}")
        return "authorisation for HPC failed: account not allowed"

    def write_job_record(self):
        """Write the final Slurm job record into the job's working directory and return its path.

        The template links it as a tool log of the OGC job; it ties OGC job, Slurm job and POSIX user
        together, so that every job is attributable to a person. The template has changed into that directory.
        """
        job = self.last_job or {}
        keys = ("job_id", "name", "comment", "user_name", "user_id", "account", "partition", "job_state",
                "exit_code", "nodes", "submit_time", "start_time", "end_time")
        path = os.path.join(os.getcwd(), f"slurm-job-{self.job_id}.json")
        with open(path, "w") as f:
            json.dump({k: job.get(k) for k in keys}, f, indent=2)
        return path

    def monitor(self):
        errors = 0
        status, raw = "accepted", "PENDING"
        poll = self.poll_tokens()
        while status not in FINAL:
            time.sleep(self.monitor_interval)
            try:
                job = self.client.get_job(self.job_id, poll.slurm_user(), poll.token())
                self.last_job = job
                errors = 0
            except (SlurmRestError, TokenError, OSError) as e:
                errors += 1
                logger.warning(f"poll {errors}/{self.max_poll_errors}: {e}")
                if errors >= self.max_poll_errors:
                    return "failed", "HPC status unknown"
                continue
            status, raw = to_ogc_status(job.get("job_state", []))
            progress = {"accepted": 15, "running": 50}.get(status, 90)
            self.update_status(progress=progress, message=f"Slurm job {self.job_id}: {raw}")
        return status, raw

    def read_outputs(self, params):
        with no_proxy_env():
            s3 = boto3.client(
                "s3", endpoint_url=params["STAGEOUT_AWS_SERVICEURL"],
                aws_access_key_id=params["STAGEOUT_AWS_ACCESS_KEY_ID"],
                aws_secret_access_key=params["STAGEOUT_AWS_SECRET_ACCESS_KEY"],
                region_name=params.get("STAGEOUT_AWS_REGION") or None)
            body = s3.get_object(Bucket=params["STAGEOUT_OUTPUT"], Key=outputs_key(params))["Body"].read()
        return json.loads(body)

    def _fail(self, message):
        self.lenv["message"] = message
        self.update_status(progress=100, message=message)
        return zoo.SERVICE_FAILED

    # -- dismiss ------------------------------------------------------------------------------
    def find_job(self, ogc_job, poll):
        """Find the Slurm job of an OGC job by its comment ogc-job=<id>.

        The ZOO kernel deletes the job's files, including the lenv file, before the Python runDismiss runs
        (the kernel queues runDismiss, then deletes every file of the job), so the recorded slurm_job_id
        is normally gone. Job ids alone are not unique either: they restart when slurmctld loses its
        state. The comment is set by the runner at submission.
        """
        tag = f"ogc-job={ogc_job};"
        job_id = self.lenv.get("slurm_job_id")
        if job_id:
            job = self.client.get_job(job_id, poll.slurm_user(), poll.token())
            if tag in (job.get("comment") or "") + ";":
                return job
        matches = [j for j in self.client.list_jobs(poll.slurm_user(), poll.token())
                   if tag in (j.get("comment") or "") + ";"]
        return max(matches, key=lambda j: j.get("job_id", 0)) if matches else None

    def dismiss(self):
        """Cancel the Slurm job of this ZOO job on behalf of the requesting user."""
        ogc_job = self.lenv.get("gs_usid") or self.lenv.get("usid")  # gs_usid: the job named in DELETE /jobs/{id}
        try:
            requester = self.user_tokens().slurm_user()
        except TokenError as e:
            logger.error(f"dismiss identity: {e}")
            return self._fail("authorisation for HPC failed")
        poll = self.poll_tokens()
        job = self.find_job(ogc_job, poll)
        if job is None:
            logger.info(f"dismiss: no Slurm job for ZOO job {ogc_job}")
            return zoo.SERVICE_SUCCEEDED
        job_id = job["job_id"]
        owner = job.get("user_name") or _comment_field(job.get("comment"), "slurm-user")
        if not requester or requester != owner:
            # Refuse before any write call: only the owner may cancel.
            logger.error(f"dismiss refused: requester {requester}, owner {owner} of Slurm job {job_id}")
            return self._fail("dismiss refused: not the owner of the job")
        status, raw = to_ogc_status(job.get("job_state", []))
        if status not in FINAL:
            try:
                self.client.cancel(job_id, requester, self.user_tokens().token())
            except SlurmRestError as e:
                logger.error(f"cancel: {e}")
                return self._fail(f"dismiss failed: {e}")
            # Wait until Slurm confirms the final state: a 200 answer alone does not prove the cancel.
            deadline = time.time() + 120
            while status not in FINAL and time.time() < deadline:
                time.sleep(2)
                status, raw = to_ogc_status(self.client.get_job(job_id, poll.slurm_user(), poll.token())
                                            .get("job_state", []))
        logger.info(f"dismiss: Slurm job {job_id} is {raw}")
        return zoo.SERVICE_SUCCEEDED if status in FINAL else zoo.SERVICE_FAILED
