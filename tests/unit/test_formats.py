"""Format sniffing — bytes → format label (PRD 008 §"New pure format-sniffing module").

A pure function of the leading bytes of a file, table-driven over synthetic
headers. This is the single new seam both the Harmony provider (materialize)
and the data-I/O read path build honesty on top of.
"""

from __future__ import annotations

import pytest

from earthdata_mcp.formats import sniff_format


@pytest.mark.parametrize(
    ("header", "expected"),
    [
        (b"\x89HDF\r\n\x1a\n" + b"\x00" * 8, "hdf5"),
        (b"CDF\x01" + b"\x00" * 8, "netcdf-classic"),
        (b"CDF\x02" + b"\x00" * 8, "netcdf-classic"),
        (b"\x0e\x03\x13\x01" + b"\x00" * 8, "hdf4"),
        (b"not-a-real-format" + b"\x00" * 8, "unknown"),
    ],
)
def test_sniff_format_recognizes_magic_numbers(header: bytes, expected: str) -> None:
    assert sniff_format(header) == expected


def test_sniff_format_empty_input_is_unknown() -> None:
    assert sniff_format(b"") == "unknown"


def test_sniff_format_short_input_is_unknown() -> None:
    assert sniff_format(b"\x89H") == "unknown"
