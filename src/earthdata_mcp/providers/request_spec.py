"""``RequestSpec`` — the durable, re-materializable request spec (PLAN.md §4.3/§4.5).

A retrieval's lifecycle splits across two halves: the planning half (the
``retrieve_*`` tools) builds this spec, and the durable worker reads it back on
every ``submit -> poll -> materialize`` transition and on restart resume.
``RequestSpec`` is the single owner of that contract: it knows how to be built
from a routed :class:`~earthdata_mcp.providers.base.RetrievalPlan`, how to
serialize to the JSONB stored in the ``jobs`` row and the provenance edge, how
to be reconstructed from that JSONB (tolerating already-persisted legacy
specs), how to yield back a ``RetrievalPlan``, and how to compute its
materialization :meth:`cache_key`.

``time_range`` and ``variables`` are accepted as explicit overrides on
:meth:`from_plan` rather than derived from ``plan`` alone: ``plan.time_range``
is a parsed :class:`~earthdata_mcp.providers.base.TimeRange`, and re-rendering
it via ``to_cmr()`` does not byte-for-byte round-trip a date-only input
(``"2024-01-01"`` becomes ``"2024-01-01T00:00:00Z"``); and ``plan.transform``
carries the *pre-resolution* variable names — the plan is built before OPeNDAP
variable-path resolution runs. The durable spec must carry the original raw
time range and the resolved variable names, so both are threaded in directly.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass

from earthdata_mcp.providers._capabilities import CollectionCapabilities
from earthdata_mcp.providers.base import AOI, RetrievalPlan, TimeRange, TransformSpec
from earthdata_mcp.providers.opendap import AxisGeometry, VarDimPlan
from earthdata_mcp.providers.router import RoutingDecision

__all__ = ["RequestSpec"]

_DEFAULT_FORMAT = "application/netcdf4"


@dataclass(frozen=True)
class RequestSpec:
    """The durable request spec. See module docstring."""

    concept_id: str | None
    short_name: str | None
    output_format: str
    output_shape: str | None
    needs_bbox: bool
    needs_variable: bool
    needs_temporal: bool
    needs_point_sample: bool
    aoi_bbox: tuple[float, float, float, float] | None
    time_range: str | None
    variables: tuple[str, ...]
    provider: str
    service_name: str | None
    opendap_urls: tuple[str, ...]
    coord_lat: str | None
    coord_lon: str | None
    lat_axis: AxisGeometry | None
    lon_axis: AxisGeometry | None
    var_dims: dict[str, VarDimPlan]
    workspace_id: str
    job_handle: str
    obs_handle: str
    cache_key_value: str

    # -- construction -------------------------------------------------------

    @classmethod
    def from_plan(
        cls,
        plan: RetrievalPlan,
        *,
        decision: RoutingDecision,
        caps: CollectionCapabilities,
        workspace_id: str,
        job_handle: str,
        obs_handle: str,
        time_range: str | None = None,
        variables: tuple[str, ...] | None = None,
        opendap_urls: list[str] | tuple[str, ...] = (),
        coord_lat: str | None = None,
        coord_lon: str | None = None,
        lat_axis: AxisGeometry | None = None,
        lon_axis: AxisGeometry | None = None,
        var_dims: dict[str, VarDimPlan] | None = None,
    ) -> RequestSpec:
        """Assemble from a routed plan plus the routing decision and OPeNDAP
        discovery outputs. ``time_range``/``variables`` override the plan's own
        (pre-resolution) values — see the module docstring for why."""
        service_name = decision.service.service_name if decision.service else None
        bbox = plan.aoi.bbox if plan.aoi is not None else None
        resolved_variables = tuple(
            variables
            if variables is not None
            else (plan.transform.variables if plan.transform else ())
        )
        key = _compute_cache_key(
            short_name=caps.short_name,
            output_format=plan.output_format,
            bbox=bbox,
            time_range=time_range,
            variables=resolved_variables,
            service_name=service_name,
            service_version=caps.capabilities_version,
        )
        return cls(
            concept_id=plan.concept_id,
            short_name=caps.short_name,
            output_format=plan.output_format,
            output_shape=caps.output_shape,
            needs_bbox=plan.needs_bbox,
            needs_variable=plan.needs_variable,
            needs_temporal=plan.needs_temporal,
            needs_point_sample=plan.needs_point_sample,
            aoi_bbox=bbox,
            time_range=time_range,
            variables=resolved_variables,
            provider=decision.path,
            service_name=service_name,
            opendap_urls=tuple(opendap_urls or ()),
            coord_lat=coord_lat,
            coord_lon=coord_lon,
            lat_axis=lat_axis,
            lon_axis=lon_axis,
            var_dims=dict(var_dims or {}),
            workspace_id=workspace_id,
            job_handle=job_handle,
            obs_handle=obs_handle,
            cache_key_value=key,
        )

    # -- durable (de)serialization -------------------------------------------

    def to_jsonb(self) -> dict:
        """The durable dict stored in the ``jobs`` row and the provenance edge."""
        return {
            "concept_id": self.concept_id,
            "short_name": self.short_name,
            "output_format": self.output_format,
            "output_shape": self.output_shape,
            "needs_bbox": self.needs_bbox,
            "needs_variable": self.needs_variable,
            "needs_temporal": self.needs_temporal,
            "needs_point_sample": self.needs_point_sample,
            "aoi_bbox": list(self.aoi_bbox) if self.aoi_bbox is not None else None,
            "time_range": self.time_range,
            "variables": list(self.variables),
            "provider": self.provider,
            "service_name": self.service_name,
            "opendap_urls": list(self.opendap_urls) if self.opendap_urls else None,
            "opendap_url": self.opendap_urls[0] if self.opendap_urls else None,
            "coord_lat": self.coord_lat,
            "coord_lon": self.coord_lon,
            "lat_axis": _serialize_axis(self.lat_axis),
            "lon_axis": _serialize_axis(self.lon_axis),
            "var_dims": _serialize_var_dims(self.var_dims),
            "workspace_id": self.workspace_id,
            "job_handle": self.job_handle,
            "obs_handle": self.obs_handle,
            "cache_key": self.cache_key_value,
        }

    @classmethod
    def from_jsonb(cls, data: dict) -> RequestSpec:
        """Reconstruct from stored JSONB, tolerant of already-persisted legacy specs."""
        opendap_urls = data.get("opendap_urls")
        if not opendap_urls and data.get("opendap_url"):
            opendap_urls = [data["opendap_url"]]
        bbox = data.get("aoi_bbox")
        return cls(
            concept_id=data.get("concept_id"),
            short_name=data.get("short_name"),
            output_format=data.get("output_format") or _DEFAULT_FORMAT,
            output_shape=data.get("output_shape"),
            needs_bbox=bool(data.get("needs_bbox")),
            needs_variable=bool(data.get("needs_variable")),
            needs_temporal=bool(data.get("needs_temporal")),
            needs_point_sample=bool(data.get("needs_point_sample")),
            aoi_bbox=tuple(bbox) if bbox else None,
            time_range=data.get("time_range"),
            variables=tuple(data.get("variables") or ()),
            provider=data.get("provider") or "harmony",
            service_name=data.get("service_name"),
            opendap_urls=tuple(opendap_urls or ()),
            coord_lat=data.get("coord_lat"),
            coord_lon=data.get("coord_lon"),
            lat_axis=_axis_from_jsonb(data.get("lat_axis")),
            lon_axis=_axis_from_jsonb(data.get("lon_axis")),
            var_dims=_var_dims_from_jsonb(data.get("var_dims")),
            workspace_id=data.get("workspace_id"),
            job_handle=data.get("job_handle"),
            obs_handle=data.get("obs_handle"),
            cache_key_value=data.get("cache_key") or "",
        )

    # -- worker-side reconstruction -------------------------------------------

    def to_plan(self) -> RetrievalPlan:
        """Reconstruct the :class:`RetrievalPlan` this spec was built from."""
        return RetrievalPlan(
            output_format=self.output_format,
            needs_bbox=self.needs_bbox,
            needs_variable=self.needs_variable,
            needs_temporal=self.needs_temporal,
            needs_point_sample=self.needs_point_sample,
            concept_id=self.concept_id,
            short_name=self.short_name,
            aoi=AOI(bbox=self.aoi_bbox) if self.aoi_bbox else None,
            time_range=TimeRange.from_cmr(self.time_range) if self.time_range else None,
            transform=TransformSpec(output_format=self.output_format, variables=self.variables)
            if self.variables
            else None,
        )

    def cache_key(self) -> str:
        """The materialization cache key computed at :meth:`from_plan` time."""
        return self.cache_key_value


# -- axis-geometry / var-dims (de)serialization ------------------------------


def _serialize_axis(axis: AxisGeometry | None) -> dict | None:
    if axis is None:
        return None
    return {"name": axis.name, "origin": axis.origin, "step": axis.step, "length": axis.length}


def _axis_from_jsonb(data: dict | None) -> AxisGeometry | None:
    if not data:
        return None
    return AxisGeometry(
        name=data["name"], origin=data["origin"], step=data["step"], length=data["length"]
    )


def _serialize_var_dims(var_dims: dict[str, VarDimPlan]) -> dict:
    if not var_dims:
        return {}
    return {var: [list(pair) for pair in dims] for var, dims in var_dims.items()}


def _var_dims_from_jsonb(data: dict | None) -> dict[str, VarDimPlan]:
    if not data:
        return {}
    return {var: tuple(tuple(pair) for pair in dims) for var, dims in data.items()}


# -- cache key -----------------------------------------------------------


def _compute_cache_key(
    *,
    short_name: str | None,
    output_format: str,
    bbox: tuple[float, float, float, float] | None,
    time_range: str | None,
    variables: tuple[str, ...],
    service_name: str | None,
    service_version: str | None,
) -> str:
    """Materialization cache key (§4.4): same inputs and hash as the pre-refactor
    ``tools.retrieval._cache_key``, so already-materialized results still resolve."""
    aoi_str = ",".join(str(c) for c in bbox) if bbox is not None else ""
    raw = ":".join(
        [
            short_name or "",
            output_format,
            aoi_str,
            time_range or "",
            ",".join(sorted(variables)),
            service_name or "",
            service_version or "",
        ]
    )
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:24]
