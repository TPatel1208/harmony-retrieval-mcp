"""Credential-identity cache/dedupe scoping (PRD 022).

The rule under test: collections CMR reports as **unrestricted** keep the
globally shared dedupe cache (the signature is byte-identical to the
pre-PRD-022 form); collections with access constraints — or whose restriction
status is unknown, or that a EULA-classified failure has durably flagged —
scope reuse to the fetching identity. Conservative default: unknown →
per-identity.
"""

from __future__ import annotations

import base64
import json

from earthdata_mcp.jobs import crud
from earthdata_mcp.providers._capabilities import CollectionCapabilities, umm_c_layer
from earthdata_mcp.providers.identity import parse_edl_token
from earthdata_mcp.providers.request_spec import compute_request_signature
from earthdata_mcp.tools.retrieval import _reuse_identity_scope

_BBOX = (-105.0, 37.0, -104.0, 38.0)
_TIME = "2024-01-01/2024-03-31"


def _jwt(uid: str) -> str:
    def seg(obj: dict) -> str:
        return base64.urlsafe_b64encode(json.dumps(obj).encode()).rstrip(b"=").decode()

    return f"{seg({'alg': 'RS256'})}.{seg({'uid': uid})}.sig"


def _caps(access_restricted: bool | None) -> CollectionCapabilities:
    return CollectionCapabilities(
        concept_id="C1-TEST",
        short_name="X",
        processing_level="3",
        output_shape="grid",
        native_formats=frozenset(),
        direct_s3=None,
        services=[],
        access_restricted=access_restricted,
    )


def _signature(scope: str | None) -> str:
    return compute_request_signature(
        concept_id="C1-TEST",
        bbox=_BBOX,
        time_range=_TIME,
        variables=("NDVI",),
        output_format="application/netcdf4",
        identity_scope=scope,
    )


# -- CMR restriction metadata parsing -----------------------------------------


def test_no_umm_c_record_means_unknown() -> None:
    assert umm_c_layer({})["access_restricted"] is None


def test_access_constraints_mean_restricted() -> None:
    umm = {"ShortName": "X", "AccessConstraints": {"Description": "restricted"}}
    assert umm_c_layer(umm)["access_restricted"] is True


def test_eula_identifiers_mean_restricted() -> None:
    umm = {"ShortName": "X", "UseConstraints": {"EULAIdentifiers": ["eula-1"]}}
    assert umm_c_layer(umm)["access_restricted"] is True


def test_not_free_and_open_means_restricted() -> None:
    umm = {"ShortName": "X", "UseConstraints": {"FreeAndOpenData": False}}
    assert umm_c_layer(umm)["access_restricted"] is True


def test_plain_record_means_unrestricted() -> None:
    umm = {"ShortName": "X", "UseConstraints": {"FreeAndOpenData": True}}
    assert umm_c_layer(umm)["access_restricted"] is False


def test_merged_capabilities_carry_the_flag() -> None:
    caps = CollectionCapabilities.from_harmony_capabilities(
        {"services": []}, {"ShortName": "X", "AccessConstraints": {"Description": "r"}}
    )
    assert caps.access_restricted is True


# -- the identity dimension in the signature -----------------------------------


def test_unscoped_signature_is_byte_identical_to_the_legacy_form() -> None:
    legacy = compute_request_signature(
        concept_id="C1-TEST",
        bbox=_BBOX,
        time_range=_TIME,
        variables=("NDVI",),
        output_format="application/netcdf4",
    )
    assert _signature(None) == legacy  # cross-user reuse preserved (story 11)


def test_identity_scope_changes_the_signature() -> None:
    assert _signature("alice") != _signature(None)
    assert _signature("alice") != _signature("bob")
    assert _signature("alice") == _signature("alice")


# -- the scoping decision (needs the durable collection_access flag) -----------


async def test_unrestricted_collection_shares_across_identities(session_factory) -> None:
    identity = parse_edl_token(_jwt("alice"))
    scope = await _reuse_identity_scope(_caps(False), "C-unflagged-1", identity, session_factory)
    assert scope is None  # shared cache: the common case, the value of dedupe


async def test_restricted_collection_scopes_to_the_token_identity(session_factory) -> None:
    identity = parse_edl_token(_jwt("alice"))
    scope = await _reuse_identity_scope(_caps(True), "C-unflagged-2", identity, session_factory)
    assert scope == "alice"


async def test_unknown_restriction_scopes_conservatively(session_factory) -> None:
    identity = parse_edl_token(_jwt("alice"))
    scope = await _reuse_identity_scope(_caps(None), "C-unflagged-3", identity, session_factory)
    assert scope == "alice"


async def test_env_credential_fetch_scopes_to_the_service_identity(session_factory) -> None:
    scope = await _reuse_identity_scope(_caps(True), "C-unflagged-4", None, session_factory)
    assert isinstance(scope, str) and scope  # the service identity, never None


async def test_eula_flag_overrides_cmr_unrestricted(session_factory) -> None:
    """A EULA-classified failure durably marks the collection restricted; from
    then on even a CMR-says-unrestricted collection scopes per identity."""
    concept_id = "C-eula-flagged"
    async with session_factory() as session:
        await crud.mark_collection_restricted(session, concept_id)
        assert await crud.collection_restriction(session, concept_id) is True

    identity = parse_edl_token(_jwt("alice"))
    scope = await _reuse_identity_scope(_caps(False), concept_id, identity, session_factory)
    assert scope == "alice"


async def test_mark_collection_restricted_is_idempotent(session_factory) -> None:
    async with session_factory() as session:
        await crud.mark_collection_restricted(session, "C-idem")
    async with session_factory() as session:
        await crud.mark_collection_restricted(session, "C-idem")
        assert await crud.collection_restriction(session, "C-idem") is True
