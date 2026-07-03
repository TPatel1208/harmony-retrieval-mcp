"""Format sniffing by magic number (PRD 008).

The single new seam that makes materialized-result media types honest: a pure
function of a file's leading bytes, with no I/O and no dependency on providers
or tools, so both layers can import it without a layering violation.
"""

from __future__ import annotations

#: HDF5 (also covers netCDF-4, which is HDF5 underneath).
_HDF5_MAGIC = b"\x89HDF\r\n\x1a\n"

#: netCDF classic (CDF-1) / 64-bit offset (CDF-2).
_NETCDF_CLASSIC_MAGICS = (b"CDF\x01", b"CDF\x02")

#: HDF4 (and HDF-EOS2, which is HDF4 underneath).
_HDF4_MAGIC = b"\x0e\x03\x13\x01"

#: sniff_format() labels that are genuinely netCDF-family — the one thing a
#: materialized result must sniff as to keep a netCDF media type (PRD 008).
NETCDF_FAMILY_FORMATS = frozenset({"hdf5", "netcdf-classic"})


def sniff_format(data: bytes) -> str:
    """Classify ``data`` by its leading magic bytes.

    Returns one of ``"hdf5"``, ``"netcdf-classic"``, ``"hdf4"``, ``"unknown"``.
    Empty or too-short input is ``"unknown"``, never an error — this function
    never raises.
    """
    if data.startswith(_HDF5_MAGIC):
        return "hdf5"
    if data.startswith(_NETCDF_CLASSIC_MAGICS):
        return "netcdf-classic"
    if data.startswith(_HDF4_MAGIC):
        return "hdf4"
    return "unknown"
