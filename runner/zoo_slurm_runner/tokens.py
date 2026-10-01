"""Tokens for slurmrestd.

Slurm (JWKS mode) accepts any token signed by the realm and takes the user name from the claim sun; it
does not check the audience. The token a user gets for ZOO must therefore not carry sun.

- KeycloakExchangeTokenProvider: Standard Token Exchange V2 of the user's platform token by the runner
  client. Only the runner client carries the mapper slurm_user -> sun, so only the exchanged token is a
  Slurm token. Used for submit and cancel.
- ClientCredentialsTokenProvider: service-account token of a polling client whose claim sun names an
  unprivileged polling account (SLURM_POLL_CLIENT_ID). Used only for GET requests, because
  the exchanged token expires with the user's token.
"""
import os
import time

import jwt

from .http import session

EXCHANGE = "urn:ietf:params:oauth:grant-type:token-exchange"
ACCESS_TOKEN = "urn:ietf:params:oauth:token-type:access_token"


class TokenError(Exception):
    pass


def unverified_claims(token):
    # The token comes straight from the token endpoint over the runner's own connection; slurmrestd
    # verifies it. Reading it unverified only tells the runner which user name Slurm will see.
    return jwt.decode(token, options={"verify_signature": False})


class _Client:
    def __init__(self, token_url=None, client_id=None, client_secret=None, timeout_s=30):
        self.token_url = token_url or os.environ["SLURM_TOKEN_URL"]
        self.client_id = client_id or os.environ["SLURM_TOKEN_CLIENT_ID"]
        self.client_secret = client_secret or os.environ["SLURM_TOKEN_CLIENT_SECRET"]
        self.timeout_s = timeout_s
        self.http = session()

    def _post(self, form):
        form = {"client_id": self.client_id, "client_secret": self.client_secret, **form}
        r = self.http.post(self.token_url, data=form, timeout=self.timeout_s)
        if r.status_code != 200:
            # Keycloak's error body names the reason, e.g. "Client is not within the token audience".
            raise TokenError(f"token endpoint answered {r.status_code}: {r.text[:200]}")
        return r.json()["access_token"]


class KeycloakExchangeTokenProvider(_Client):
    def __init__(self, subject_token, audience=None, **kwargs):
        super().__init__(**kwargs)
        self.subject_token = subject_token
        self.audience = audience or os.environ.get("SLURM_TOKEN_AUDIENCE")
        self._token = None

    def token(self):
        if self._token is None or _expires_soon(self._token):
            form = {"grant_type": EXCHANGE, "subject_token": self.subject_token,
                    "subject_token_type": ACCESS_TOKEN, "requested_token_type": ACCESS_TOKEN}
            if self.audience:
                form["audience"] = self.audience
            self._token = self._post(form)
        return self._token

    def slurm_user(self):
        """User name that Slurm derives from the exchanged token (claim sun), or None."""
        return unverified_claims(self.token()).get("sun")


class ClientCredentialsTokenProvider(_Client):
    def __init__(self, **kwargs):
        # A separate polling client (hardcoded claim sun=<polling account>) if configured, else the exchange client.
        # A separate client keeps the polling account out of the exchange client's service account.
        kwargs.setdefault("client_id", os.environ.get("SLURM_POLL_CLIENT_ID"))
        kwargs.setdefault("client_secret", os.environ.get("SLURM_POLL_CLIENT_SECRET"))
        super().__init__(**kwargs)
        self._token = None

    def token(self):
        if self._token is None or _expires_soon(self._token):
            self._token = self._post({"grant_type": "client_credentials"})
        return self._token

    def slurm_user(self):
        return unverified_claims(self.token()).get("sun")


def _expires_soon(token, margin_s=30):
    return unverified_claims(token).get("exp", 0) - margin_s < time.time()
