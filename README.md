# zoo-slurm

Run CWL Application Packages deployed in ZOO-Project DRU (OGC API - Processes) as Slurm jobs,
through the Slurm REST API, under the POSIX account of the requesting user.

The repository holds two parts:

| Part | Path | Used by |
|---|---|---|
| Service template (cookiecutter) | repository root (`cookiecutter.json`, `{{cookiecutter.service_name}}/`) | ZOO-DRU clones it from `cookiecutter.templateUrl` when a process is deployed |
| Runner `zoo_slurm_runner` | `runner/` | installed into the ZOO-FPM image (`pip install "zoo-slurm-runner @ git+<repo>@<tag>#subdirectory=runner"`) |

The template is the EOEPCA service template (`gfenoy/eoepca-proc-service-template`, branch
`feature/zoo-template-common`, Apache-2.0) with the runner class changed from
`ZooCalrissianRunner` to `SlurmRestRunner`. Workspace stage-out and STAC output handling are
unchanged.

## What the runner does

1. Exchanges the user's token (Keycloak Standard Token Exchange) for a token of the runner
   client. That token carries the claim `sun`, which Slurm takes as the POSIX user name.
2. Wraps the Application Package with stage-in and stage-out steps (reused from
   `zoo-calrissian-runner`) and submits it as one batch job (`cwltool --singularity`) on hold.
3. Releases the job if Slurm maps the user to a uid of at least `minimum_uid`, else cancels it.
4. Polls the job with a separate polling client, maps Slurm states to OGC states, reads the CWL
   output object from object storage and hands it to the template's STAC output handling.
5. On dismiss, finds the Slurm job by its comment `ogc-job=<id>` and cancels it with the
   owner's token, only if the requester owns the job.

Account, partition and limits never come from the execute request; they come from the site
profile (`[slurm_site]` section of ZOO's `main.cfg`, see `runner/zoo_slurm_runner/profile.py`).
Storage credentials go into the job environment, not into the job script.

## Configuration (environment of ZOO-FPM)

| Variable | Meaning |
|---|---|
| `SLURM_TOKEN_URL` | Keycloak token endpoint |
| `SLURM_TOKEN_CLIENT_ID`, `SLURM_TOKEN_CLIENT_SECRET` | Client that may exchange tokens and maps `slurm_user` to `sun` |
| `SLURM_POLL_CLIENT_ID`, `SLURM_POLL_CLIENT_SECRET` | Service-account client with a fixed `sun` of an unprivileged account |
| `SLURM_MONITOR_INTERVAL` | Seconds between polls (default 30) |
| `SLURM_REST_CA_BUNDLE` | CA bundle for a TLS connection to slurmrestd (optional) |

## Tests

```
cd runner && pip install pytest && python -m pytest tests
```

The unit tests need the packages of the ZOO-Project DRU image (`zoo-runner-common`,
`zoo-calrissian-runner`); run them inside that image.
