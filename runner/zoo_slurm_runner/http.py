"""HTTP access that ignores the proxy environment of the ZOO-FPM pod.

The zoo-project-dru chart sets HTTP_PROXY to its Kubernetes API proxy (useKubeProxy) for Calrissian.
Keycloak and slurmrestd must be reached directly; through that proxy the token endpoint answers 403.
"""
import contextlib
import os

import requests

_PROXY_VARS = ("HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy")


def session():
    s = requests.Session()
    s.trust_env = False
    return s


@contextlib.contextmanager
def no_proxy_env():
    """Remove the proxy variables for libraries that read them directly (boto3)."""
    saved = {k: os.environ.pop(k) for k in _PROXY_VARS if k in os.environ}
    try:
        yield
    finally:
        os.environ.update(saved)
