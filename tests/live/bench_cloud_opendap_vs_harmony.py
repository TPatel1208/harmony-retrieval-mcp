"""Live benchmark: Cloud OPeNDAP (DAP4) vs Harmony for equivalent subset plans.

Not a pytest test (no ``test_`` prefix, so it is never collected). Drives the real
``OPeNDAPProvider`` and ``HarmonyProvider`` code paths against production NASA
services and times the full durable seam (submit -> poll -> materialize) for each.

Run inside the container (needs EDL creds, already in the mcp service env):

    docker compose exec mcp python tests/live/bench_cloud_opendap_vs_harmony.py

Each case runs both providers on the *same* AOI + time window + variables, so the
wall-clock numbers are directly comparable. Results stream as they complete and a
summary table prints at the end.
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from datetime import datetime

from earthdata_mcp.config import Settings
from earthdata_mcp.providers.base import (
    AOI,
    JobRef,
    RetrievalPlan,
    TimeRange,
    TransformSpec,
)
from earthdata_mcp.jobs.state import JobState
from earthdata_mcp.providers.cmr import CMRProvider
from earthdata_mcp.providers.harmony import HarmonyProvider
from earthdata_mcp.providers.opendap import OPeNDAPProvider, _resolve_from_cmr
from earthdata_mcp.tools.retrieval import _opendap_url_of

NETCDF = "application/x-netcdf"
_GRANULE_CAP = 24          # bound OPeNDAP bundle + Harmony window for comparable cost
_POLL_INTERVAL_S = 4.0
_HARMONY_TIMEOUT_S = 600.0


@dataclass
class Case:
    name: str
    short_name: str
    provider_daac: str
    variables: tuple[str, ...]
    bbox: tuple[float, float, float, float]
    time_range: TimeRange


@dataclass
class Outcome:
    case: str
    provider: str
    status: str
    wall_s: float
    size_mb: float
    granules: int
    note: str = ""


def _mb(n: int | None) -> float:
    return round((n or 0) / 1_048_576, 2)


async def _run_opendap(
    caps, concept_id, urls, variables, coord_lat, coord_lon, tr, bbox
) -> Outcome:
    t0 = time.monotonic()
    try:
        prov = OPeNDAPProvider(
            caps,
            opendap_urls=urls,
            coord_lat=coord_lat,
            coord_lon=coord_lon,
            settings=Settings(),
        )
        plan = RetrievalPlan(
            output_format=NETCDF,
            needs_variable=True,
            needs_bbox=True,
            needs_temporal=True,
            concept_id=concept_id,
            aoi=AOI(bbox=bbox),
            time_range=tr,
            transform=TransformSpec(output_format=NETCDF, variables=variables),
        )
        if not prov.can_handle(plan):
            return Outcome("", "opendap", "SKIP", 0.0, 0.0, 0, "can_handle=False")
        ref = await prov.submit(plan)
        await prov.poll(ref)
        res = await prov.materialize(
            JobRef(
                provider="opendap",
                provider_job_url=ref.provider_job_url,
                job_handle=f"bench_od_{int(t0)}",
            )
        )
        return Outcome(
            "", "opendap", "OK", round(time.monotonic() - t0, 1),
            _mb(res.size_bytes), res.extra.get("granule_count", len(urls)),
        )
    except Exception as exc:  # noqa: BLE001 - benchmark records failures, never raises
        return Outcome(
            "", "opendap", "FAIL", round(time.monotonic() - t0, 1), 0.0, len(urls),
            f"{type(exc).__name__}: {str(exc)[:120]}",
        )


async def _run_harmony(caps, concept_id, variables, tr, bbox) -> Outcome:
    t0 = time.monotonic()
    plan = RetrievalPlan(
        output_format=NETCDF,
        needs_variable=True,
        needs_bbox=True,
        needs_temporal=True,
        concept_id=concept_id,
        aoi=AOI(bbox=bbox),
        time_range=tr,
        transform=TransformSpec(output_format=NETCDF, variables=variables),
    )
    svc = caps.find_service(plan)
    pin = svc.service_name if svc else "<server-default/unpinned>"
    try:
        prov = HarmonyProvider(caps, settings=Settings())
        ref = await prov.submit(plan)
        deadline = t0 + _HARMONY_TIMEOUT_S
        state = JobState.SUBMITTED
        while time.monotonic() < deadline:
            st = await prov.poll(ref)
            state = st.state
            if state in (JobState.READY, JobState.FAILED, JobState.CANCELLED):
                break
            await asyncio.sleep(_POLL_INTERVAL_S)
        if state is not JobState.READY:
            return Outcome(
                "", "harmony", "FAIL", round(time.monotonic() - t0, 1), 0.0, 0,
                f"ended={state.value} svc={pin}",
            )
        res = await prov.materialize(
            JobRef(
                provider="harmony",
                provider_job_id=ref.provider_job_id,
                provider_job_url=ref.provider_job_url,
                job_handle=f"bench_hm_{int(t0)}",
            )
        )
        return Outcome(
            "", "harmony", "OK", round(time.monotonic() - t0, 1),
            _mb(res.size_bytes), res.extra.get("granule_count", 1), f"svc={pin}",
        )
    except Exception as exc:  # noqa: BLE001
        return Outcome(
            "", "harmony", "FAIL", round(time.monotonic() - t0, 1), 0.0, 0,
            f"{type(exc).__name__}: {str(exc)[:100]} svc={pin}",
        )


async def _run_case(cmr: CMRProvider, case: Case) -> list[Outcome]:
    cols = await cmr.search_collections(
        short_name=case.short_name, provider=case.provider_daac, limit=1
    )
    if not cols:
        return [Outcome(case.name, "-", "SKIP", 0.0, 0.0, 0, "collection not found")]
    concept_id = cols[0]["concept_id"]
    caps = await cmr.collection_capabilities(concept_id)

    bbox_str = ",".join(str(c) for c in case.bbox)
    granules = await cmr.search_granules(
        concept_id, bounding_box=bbox_str, temporal=case.time_range.to_cmr(),
        limit=_GRANULE_CAP,
    )
    urls = [u for u in (_opendap_url_of(g) for g in granules) if u]
    coord_lat, coord_lon, resolved = await _resolve_from_cmr(
        cmr, concept_id, case.variables
    )

    print(f"\n=== {case.name} === {concept_id}  granules={len(urls)}  "
          f"vars={resolved} coords=({coord_lat},{coord_lon})", flush=True)

    od = await _run_opendap(
        caps, concept_id, urls, resolved, coord_lat, coord_lon,
        case.time_range, case.bbox,
    )
    od.case = case.name
    print(f"  opendap: {od.status} {od.wall_s}s {od.size_mb}MB "
          f"g={od.granules} {od.note}", flush=True)

    hm = await _run_harmony(caps, concept_id, resolved, case.time_range, case.bbox)
    hm.case = case.name
    print(f"  harmony: {hm.status} {hm.wall_s}s {hm.size_mb}MB "
          f"g={hm.granules} {hm.note}", flush=True)
    return [od, hm]


def _cases() -> list[Case]:
    d = lambda *a: datetime(*a)  # noqa: E731
    small = (-95.0, 38.0, -90.0, 43.0)      # ~5x5 deg over US Midwest
    large = (-125.0, 25.0, -66.0, 50.0)     # CONUS
    return [
        Case("GLDAS 3H · 1 var · small bbox · 1 day", "GLDAS_NOAH025_3H",
             "GES_DISC", ("Rainf_tavg",), small,
             TimeRange(d(2020, 1, 1), d(2020, 1, 2))),
        Case("GLDAS 3H · 1 var · small bbox · 3 days", "GLDAS_NOAH025_3H",
             "GES_DISC", ("Rainf_tavg",), small,
             TimeRange(d(2020, 1, 1), d(2020, 1, 4))),
        Case("GLDAS 3H · 3 vars · small bbox · 1 day", "GLDAS_NOAH025_3H",
             "GES_DISC", ("Rainf_tavg", "Tair_f_inst", "Qair_f_inst"), small,
             TimeRange(d(2020, 1, 1), d(2020, 1, 2))),
        Case("GLDAS 3H · 1 var · CONUS bbox · 1 day", "GLDAS_NOAH025_3H",
             "GES_DISC", ("Rainf_tavg",), large,
             TimeRange(d(2020, 1, 1), d(2020, 1, 2))),
        Case("GLDAS Monthly · 1 var · small bbox · 6 mo", "GLDAS_NOAH025_M",
             "GES_DISC", ("Rainf_tavg",), small,
             TimeRange(d(2020, 1, 1), d(2020, 7, 1))),
    ]


async def main() -> None:
    cmr = CMRProvider()
    rows: list[Outcome] = []
    for case in _cases():
        try:
            rows.extend(await _run_case(cmr, case))
        except Exception as exc:  # noqa: BLE001
            print(f"  case {case.name!r} errored: {exc}", flush=True)

    print("\n\n================  SUMMARY  ================", flush=True)
    hdr = f"{'case':<44}{'provider':<9}{'status':<7}{'wall_s':>8}{'MB':>8}{'gran':>6}"
    print(hdr)
    print("-" * len(hdr))
    for r in rows:
        print(f"{r.case:<44}{r.provider:<9}{r.status:<7}{r.wall_s:>8}"
              f"{r.size_mb:>8}{r.granules:>6}  {r.note}")


if __name__ == "__main__":
    asyncio.run(main())
