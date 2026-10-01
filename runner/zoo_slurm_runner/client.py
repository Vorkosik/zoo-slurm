"""Minimal client for the Slurm REST API (slurmrestd)."""
from .http import session


class SlurmRestError(Exception):
    def __init__(self, message, status=None, errors=None):
        super().__init__(message)
        self.status = status
        self.errors = errors or []


class SlurmRestClient:
    def __init__(self, profile, ca_bundle=None, timeout_s=30):
        self.api = profile.api
        self.verify = ca_bundle if ca_bundle else True
        self.timeout_s = timeout_s
        self.http = session()

    def _call(self, method, path, user_name, token, body=None):
        headers = {"X-SLURM-USER-NAME": user_name, "X-SLURM-USER-TOKEN": token}
        r = self.http.request(method, f"{self.api}{path}", headers=headers, json=body,
                              timeout=self.timeout_s, verify=self.verify)
        try:
            data = r.json()
        except ValueError:
            data = {"errors": [{"description": r.text[:200]}]}
        errors = data.get("errors") or []
        if r.status_code != 200 or errors:
            text = "; ".join(e.get("description") or e.get("error") or str(e) for e in errors) or r.reason
            raise SlurmRestError(f"{method} {path}: {r.status_code} {text}", r.status_code, errors)
        return data

    def submit(self, user_name, token, job):
        data = self._call("POST", "/job/submit", user_name, token, {"job": job})
        return int(data["job_id"])

    def get_job(self, job_id, user_name, token):
        jobs = self._call("GET", f"/job/{job_id}", user_name, token).get("jobs") or []
        if not jobs:
            raise SlurmRestError(f"job {job_id} not found", 404)
        return jobs[0]

    def release(self, job_id, user_name, token):
        """POST job/{id} with hold=false: release a job submitted on hold."""
        self._call("POST", f"/job/{job_id}", user_name, token, {"hold": False})

    def list_jobs(self, user_name, token):
        """GET jobs: all jobs the caller may see (all jobs unless the site sets PrivateData=jobs)."""
        return self._call("GET", "/jobs", user_name, token).get("jobs") or []

    def cancel(self, job_id, user_name, token):
        self._call("DELETE", f"/job/{job_id}", user_name, token)
