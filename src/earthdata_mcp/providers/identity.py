"""Per-call EDL identity (PRD 022).

Everything the per-call ``edl_token`` contract needs lives here, so audit scope
stays narrow (PRD 022 story 20):

* :func:`parse_edl_token` — the **one** place a raw token string is parsed. It
  registers the token with the redaction chokepoint, decodes the EDL JWT's
  ``uid``/``exp`` claims, and returns an opaque :class:`EDLIdentity` handle the
  rest of the pipeline threads. A malformed token raises
  :class:`TokenInvalidError` and an already-expired one
  :class:`TokenExpiredError` — both at the boundary, before any network call.
* :func:`get_token_session` — bounded, expiring per-identity session cache
  (story 15). A :class:`TokenSession` caches the per-token harmony-py client
  and the per-DAAC S3 temporary credentials, all constructed **without any
  process-env mutation** — the env-var export path in ``providers/auth.py``
  survives untouched as the no-token fallback.
* :func:`fetch_s3_credentials` — direct bearer-header HTTP against the DAAC
  ``s3credentials`` endpoints (the earthaccess environment-strategy login
  insists on process env, so the per-token path bypasses it; the endpoint
  registry itself is still earthaccess's own ``DAACS`` table, not hand-rolled).
* :func:`classify_credential_failure` — maps a provider failure onto the three
  first-class classifications (``TOKEN_EXPIRED`` / ``TOKEN_INVALID`` /
  ``EULA_NOT_ACCEPTED``), reachable from both the tool boundary and the worker.
* :func:`encrypt_credential` / :func:`decrypt_credential` — Fernet/MultiFernet
  under the repo-private ``EARTHDATA_CREDENTIAL_KEY``, for the durable job
  row's encrypted-credential column (never Redis, never plaintext at rest).
"""

from __future__ import annotations

import base64
import binascii
import hashlib
import json
import re
import threading
import time
from collections import OrderedDict
from dataclasses import dataclass, field
from datetime import datetime, timezone

import httpx

from earthdata_mcp.config import Settings, get_settings
from earthdata_mcp.redaction import redact, register_secret

__all__ = [
    "TOKEN_EXPIRED",
    "TOKEN_INVALID",
    "EULA_NOT_ACCEPTED",
    "CredentialError",
    "TokenExpiredError",
    "TokenInvalidError",
    "EulaNotAcceptedError",
    "CredentialKeyUnavailable",
    "EDLIdentity",
    "TokenSession",
    "classify_credential_failure",
    "decrypt_credential",
    "encrypt_credential",
    "ensure_credential_key",
    "fetch_s3_credentials",
    "get_token_session",
    "parse_edl_token",
    "reset_token_sessions",
    "service_identity_scope",
]

# -- classifications ---------------------------------------------------------

#: EDL/downstream rejected a formerly-valid token (including mid-job).
TOKEN_EXPIRED = "TOKEN_EXPIRED"
#: Malformed or revoked at first use.
TOKEN_INVALID = "TOKEN_INVALID"
#: Authorization failure indicating a license-acceptance requirement.
EULA_NOT_ACCEPTED = "EULA_NOT_ACCEPTED"


class CredentialError(Exception):
    """A classified per-call-credential failure (PRD 022).

    ``classification`` is one of the three module-level constants; it leads the
    message string so a consumer parsing the structured error surface (or a
    job's stored ``error``) can branch on it without a separate field. The
    message is redacted at construction so a raw token can never ride out
    through an error envelope.
    """

    classification: str = TOKEN_INVALID

    def __init__(self, message: str, *, resolution_url: str | None = None) -> None:
        super().__init__(f"{self.classification}: {redact(message)}")
        self.resolution_url = resolution_url


class TokenExpiredError(CredentialError):
    classification = TOKEN_EXPIRED


class TokenInvalidError(CredentialError):
    classification = TOKEN_INVALID


class EulaNotAcceptedError(CredentialError):
    classification = EULA_NOT_ACCEPTED


class CredentialKeyUnavailable(RuntimeError):
    """Per-call tokens need the repo-private Fernet key and none is configured."""

    def __init__(self) -> None:
        super().__init__(
            "per-call edl_token support requires EARTHDATA_CREDENTIAL_KEY (a "
            "Fernet key, e.g. from cryptography.fernet.Fernet.generate_key(); "
            "comma-separate multiple keys to rotate, first key encrypts)"
        )


# -- the opaque identity handle ----------------------------------------------


@dataclass(frozen=True)
class EDLIdentity:
    """The opaque handle a parsed ``edl_token`` becomes (PRD 022 story 20).

    Carries the token value plus the derived EDL username and expiry, so the
    rest of the pipeline threads this instead of loose raw strings. ``token``
    is excluded from ``repr`` so an identity can never leak the credential by
    being logged or formatted.
    """

    token: str = field(repr=False)
    username: str | None = None
    expires_at: datetime | None = None

    @property
    def scope_key(self) -> str:
        """The identity dimension for cache/dedupe scoping: the EDL username,
        or a token digest when the username could not be derived."""
        if self.username:
            return self.username
        return "token:" + hashlib.sha256(self.token.encode()).hexdigest()[:16]

    def is_expired(self, now: datetime | None = None) -> bool:
        """True when the token's ``exp`` claim is in the past."""
        if self.expires_at is None:
            return False
        return (now or datetime.now(timezone.utc)) >= self.expires_at


def parse_edl_token(edl_token: str | None) -> EDLIdentity | None:
    """Parse a per-call token into an :class:`EDLIdentity` — the one boundary.

    ``None``/empty/whitespace means "no per-call identity" and returns ``None``
    (the env-credential fallback, byte-for-byte today's behavior). Otherwise
    the token is registered for redaction first, then its EDL JWT payload is
    decoded for the ``uid`` (username) and ``exp`` (expiry) claims. A token
    that is not JWT-shaped raises :class:`TokenInvalidError`; one whose ``exp``
    is already past raises :class:`TokenExpiredError` — both before any
    network call, so a bad paste fails fast with a classification the consumer
    can act on (PRD 022 stories 6/8).
    """
    if edl_token is None:
        return None
    token = edl_token.strip()
    if not token:
        return None
    register_secret(token)
    try:
        claims = _decode_jwt_payload(token)
    except (ValueError, binascii.Error) as exc:
        raise TokenInvalidError(
            "edl_token is not a well-formed EDL token (expected a JWT); "
            "paste the token exactly as issued by Earthdata Login"
        ) from exc
    username = claims.get("uid") or None
    expires_at: datetime | None = None
    exp = claims.get("exp")
    if isinstance(exp, (int, float)):
        expires_at = datetime.fromtimestamp(exp, tz=timezone.utc)
    identity = EDLIdentity(token=token, username=username, expires_at=expires_at)
    if identity.is_expired():
        raise TokenExpiredError(
            "edl_token expired at "
            f"{expires_at.isoformat() if expires_at else '?'} — renew the token "
            "at Earthdata Login and retry"
        )
    return identity


def _decode_jwt_payload(token: str) -> dict:
    """Decode a JWT's payload segment (no signature verification — we hold no
    EDL signing key; the claims are advisory, NASA's rejection is the truth)."""
    parts = token.split(".")
    if len(parts) != 3:
        raise ValueError("not a three-segment JWT")
    payload = parts[1]
    padded = payload + "=" * (-len(payload) % 4)
    decoded = json.loads(base64.urlsafe_b64decode(padded.encode()))
    if not isinstance(decoded, dict):
        raise ValueError("JWT payload is not an object")
    return decoded


def service_identity_scope(settings: Settings | None = None) -> str:
    """The scope key for env-credential fetches (the service identity).

    Prefers the configured EDL username; falls back to the env token's own
    ``uid`` claim; last resort is the literal ``"service"``. Never does a
    network round-trip — this feeds planning-time dedupe scoping.
    """
    settings = settings or get_settings()
    if settings.edl_username:
        return settings.edl_username
    if settings.earthdata_token:
        try:
            identity = parse_edl_token(settings.earthdata_token)
        except CredentialError:
            identity = None
        if identity is not None:
            return identity.scope_key
    return "service"


# -- bounded, expiring per-identity session cache (story 15) ------------------


class TokenSession:
    """Per-identity session state: cached clients + cached S3 credentials.

    Construction mutates no process state; everything inside is keyed to one
    token. Thread-safe: the worker and the tool process both reach here.
    """

    def __init__(self, identity: EDLIdentity) -> None:
        self.identity = identity
        self._lock = threading.Lock()
        self._clients: dict[str, object] = {}
        self._s3_creds: dict[str, dict] = {}

    def client_for(self, key: str, build) -> object:
        """Return the cached client under ``key``, building it once."""
        with self._lock:
            client = self._clients.get(key)
            if client is None:
                client = build()
                self._clients[key] = client
            return client

    async def get_s3_credentials(
        self,
        daac: str | None = None,
        provider: str | None = None,
        *,
        http_client: httpx.AsyncClient | None = None,
    ) -> dict:
        """Per-token in-region S3 temporary credentials, cached per endpoint."""
        endpoint = s3_credentials_endpoint(daac=daac, provider=provider)
        with self._lock:
            cached = self._s3_creds.get(endpoint)
        if cached is not None and not _s3_creds_expired(cached):
            return cached
        creds = await fetch_s3_credentials(
            self.identity, endpoint=endpoint, http_client=http_client
        )
        with self._lock:
            self._s3_creds[endpoint] = creds
        return creds


#: Cache bounds: at most this many identities, each session good for this long.
_SESSION_CACHE_MAX = 32
_SESSION_TTL_SECONDS = 3600.0

_sessions: OrderedDict[str, tuple[TokenSession, float]] = OrderedDict()
_sessions_lock = threading.Lock()


def get_token_session(identity: EDLIdentity, *, now: float | None = None) -> TokenSession:
    """The cached :class:`TokenSession` for ``identity`` (bounded LRU + TTL).

    Two different tokens always yield two distinct sessions; the same token
    yields the same session until it expires out of the cache — so per-user
    calls don't re-authenticate on every tool invocation (story 15).
    """
    key = hashlib.sha256(identity.token.encode()).hexdigest()
    current = now if now is not None else time.monotonic()
    with _sessions_lock:
        entry = _sessions.get(key)
        if entry is not None:
            session, created = entry
            if current - created < _SESSION_TTL_SECONDS:
                _sessions.move_to_end(key)
                return session
            del _sessions[key]
        session = TokenSession(identity)
        _sessions[key] = (session, current)
        while len(_sessions) > _SESSION_CACHE_MAX:
            _sessions.popitem(last=False)
        return session


def reset_token_sessions() -> None:
    """Drop every cached session (test helper)."""
    with _sessions_lock:
        _sessions.clear()


# -- direct bearer-header S3 temporary credentials ----------------------------


def s3_credentials_endpoint(daac: str | None = None, provider: str | None = None) -> str:
    """Resolve a DAAC/provider to its ``s3credentials`` endpoint.

    The registry is earthaccess's own ``DAACS`` table (data only — importing it
    performs no login and mutates no env), so we never hand-roll NASA's DAAC
    topology; only the *fetch* bypasses earthaccess's env-bound login.
    """
    from earthaccess import daac as earthaccess_daac

    for entry in earthaccess_daac.DAACS:
        short_name = entry.get("short-name")
        clouds = tuple(entry.get("cloud-providers") or ())
        endpoint = entry.get("s3-credentials")
        if not endpoint:
            continue
        if daac and (daac == short_name or daac in clouds):
            return endpoint
        if provider and provider in clouds:
            return endpoint
    raise ValueError(
        f"no S3 credentials endpoint known for daac={daac!r} provider={provider!r}"
    )


async def fetch_s3_credentials(
    identity: EDLIdentity,
    *,
    daac: str | None = None,
    provider: str | None = None,
    endpoint: str | None = None,
    http_client: httpx.AsyncClient | None = None,
) -> dict:
    """GET the DAAC's ``s3credentials`` endpoint with the per-token bearer.

    A 401/403 response is classified (:func:`classify_http_response`) instead
    of surfacing as a bare HTTP error. ``http_client`` is the injectable seam
    unit tests use; the live test drives the real endpoint.
    """
    resolved = endpoint or s3_credentials_endpoint(daac=daac, provider=provider)
    headers = {"Authorization": f"Bearer {identity.token}"}

    async def _get(client: httpx.AsyncClient) -> httpx.Response:
        return await client.get(resolved, headers=headers, follow_redirects=True)

    if http_client is not None:
        response = await _get(http_client)
    else:
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await _get(client)
    classified = classify_http_response(
        response.status_code, response.text, identity=identity
    )
    if classified is not None:
        raise classified
    response.raise_for_status()
    return response.json()


def _s3_creds_expired(creds: dict) -> bool:
    """True when a cached credentials payload's own ``expiration`` has passed."""
    expiration = creds.get("expiration")
    if not expiration:
        return False
    try:
        expires_at = datetime.fromisoformat(str(expiration).replace("Z", "+00:00"))
    except ValueError:
        return True
    if expires_at.tzinfo is None:
        expires_at = expires_at.replace(tzinfo=timezone.utc)
    return datetime.now(timezone.utc) >= expires_at


# -- failure classification ---------------------------------------------------

#: Signals in an authorization-failure body/message that the block is a
#: license-acceptance requirement, not a bad credential.
_EULA_PHRASES = ("eula", "approve_app", "license agreement", "end user license")

#: The EDL resolution URL NASA embeds in EULA-403 bodies.
_RESOLUTION_URL_RE = re.compile(r"https://[^\s\"'<>\\]*(?:approve_app|accept_eula)[^\s\"'<>\\]*")


def classify_http_response(
    status_code: int,
    body_text: str | None,
    *,
    identity: EDLIdentity | None = None,
) -> CredentialError | None:
    """Classify an HTTP auth failure, or ``None`` when it is not one.

    401 → a credential problem: ``TOKEN_EXPIRED`` when the identity's own
    ``exp`` claim has passed (the formerly-valid case), ``TOKEN_INVALID``
    otherwise. 403 → ``EULA_NOT_ACCEPTED`` when the body signals a license
    requirement (with the resolution URL passed through when NASA supplies
    one); a plain 403 is treated as a credential rejection like a 401.
    """
    if status_code not in (401, 403):
        return None
    text = body_text or ""
    lowered = text.lower()
    if status_code == 403 and any(phrase in lowered for phrase in _EULA_PHRASES):
        resolution_url = _extract_resolution_url(text)
        message = "NASA requires accepting this collection's license (EULA) first"
        if resolution_url:
            message += f" — accept it at {resolution_url}, then retry"
        return EulaNotAcceptedError(message, resolution_url=resolution_url)
    return _token_rejection(identity, f"NASA rejected the credential (HTTP {status_code})")


def classify_credential_failure(
    exc: BaseException, identity: EDLIdentity | None = None
) -> CredentialError | None:
    """Classify a caught provider exception, or ``None`` when it isn't one.

    Reached from the worker's task exception handlers (so a mid-job auth
    failure terminates the job with a first-class classification instead of a
    generic retrieval error, and never triggers the OPeNDAP fallback — silent
    identity substitution is worse than a classified failure) and from the
    tool boundary's planning-time provider calls. Understands:

    * :class:`CredentialError` raised anywhere below — passed through.
    * ``httpx.HTTPStatusError`` / ``requests.HTTPError`` — classified by
      status code + body via :func:`classify_http_response`.
    * harmony-py's two-arg ``Exception(response.reason, message)`` — by the
      HTTP reason phrase (``Unauthorized``/``Forbidden``) plus EULA phrases.
    """
    if isinstance(exc, CredentialError):
        return exc

    status, text = _status_and_text(exc)
    if status is not None:
        return classify_http_response(status, text, identity=identity)

    # harmony-py's JSON-error shape: Exception(reason_phrase, message).
    if type(exc) is Exception and len(exc.args) >= 2:
        reason = str(exc.args[0]).strip().lower()
        message = " ".join(str(a) for a in exc.args[1:])
        if reason == "unauthorized":
            return _token_rejection(identity, message)
        if reason == "forbidden":
            lowered = message.lower()
            if any(phrase in lowered for phrase in _EULA_PHRASES):
                resolution_url = _extract_resolution_url(message)
                eula_message = (
                    "NASA requires accepting this collection's license (EULA) first"
                )
                if resolution_url:
                    eula_message += f" — accept it at {resolution_url}, then retry"
                return EulaNotAcceptedError(eula_message, resolution_url=resolution_url)
            return _token_rejection(identity, message)
    return None


def _token_rejection(identity: EDLIdentity | None, message: str) -> CredentialError:
    if identity is not None and identity.is_expired():
        return TokenExpiredError(
            f"{message} — the token's own expiry has passed; renew it at "
            "Earthdata Login and retry"
        )
    return TokenInvalidError(message)


def _status_and_text(exc: BaseException) -> tuple[int | None, str | None]:
    """Pull ``(status_code, body_text)`` from an HTTP-error exception, if any."""
    response = getattr(exc, "response", None)
    status = getattr(response, "status_code", None)
    if not isinstance(status, int):
        return None, None
    try:
        text = response.text
    except Exception:  # pragma: no cover - a closed/streaming response body
        text = None
    return status, text


# -- durable credential encryption (Fernet/MultiFernet) -----------------------


def _fernet(settings: Settings | None = None):
    from cryptography.fernet import Fernet, MultiFernet

    settings = settings or get_settings()
    keys = [k.strip() for k in settings.earthdata_credential_key.split(",") if k.strip()]
    if not keys:
        raise CredentialKeyUnavailable()
    return MultiFernet([Fernet(k.encode()) for k in keys])


def ensure_credential_key(settings: Settings | None = None) -> None:
    """Raise :class:`CredentialKeyUnavailable` unless the Fernet key is set.

    Called at the tool boundary before any handle or job row exists, so a
    token-carrying call on an unconfigured deployment fails clean rather than
    leaving an orphan job the worker can never authenticate.
    """
    _fernet(settings)


def encrypt_credential(token: str, settings: Settings | None = None) -> str:
    """Encrypt a per-call token for the job row's credential column."""
    return _fernet(settings).encrypt(token.encode()).decode()


def decrypt_credential(blob: str, settings: Settings | None = None) -> str:
    """Decrypt a job row's credential column back to the raw token.

    Registers the plaintext with the redaction chokepoint before returning it,
    so a worker-decrypted credential is covered by the same scrub guarantees
    as one arriving through the tool boundary. Raises ``ValueError`` when no
    configured key can decrypt (key rotated away / tampered blob).
    """
    from cryptography.fernet import InvalidToken

    try:
        token = _fernet(settings).decrypt(blob.encode()).decode()
    except InvalidToken as exc:
        raise ValueError(
            "cannot decrypt the job's stored credential — "
            "EARTHDATA_CREDENTIAL_KEY does not match the key it was written under"
        ) from exc
    register_secret(token)
    return token


def _extract_resolution_url(text: str) -> str | None:
    """The EDL resolution URL from an EULA-403 body, when NASA supplies one."""
    try:
        body = json.loads(text)
    except (ValueError, TypeError):
        body = None
    if isinstance(body, dict):
        for key in ("resolution_url", "resolutionUrl"):
            value = body.get(key)
            if isinstance(value, str) and value:
                return value
    match = _RESOLUTION_URL_RE.search(text or "")
    return match.group(0) if match else None
