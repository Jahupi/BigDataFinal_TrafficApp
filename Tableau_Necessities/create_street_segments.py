"""
Rebuild street_segments.csv from the cleaned crash heat map file.

This preserves the original Tableau source file used by big_data_tableau.twb.
"""

from __future__ import annotations

import csv

from export_to_tableau import (
    CRASH_HEAT_MAP_CSV,
    STREET_SEGMENTS_CSV,
    atomic_write_csv,
    build_street_segment_rows,
)


def rebuild_street_segments() -> int:
    with CRASH_HEAT_MAP_CSV.open("r", newline="", encoding="utf-8") as file:
        heat_map_rows = list(csv.DictReader(file))

    segment_rows = build_street_segment_rows(heat_map_rows)
    atomic_write_csv(
        STREET_SEGMENTS_CSV,
        [
            "Street",
            "Latitude",
            "Longitude",
            "Congestion",
            "Incidents",
            "Sequence",
            "Segment_Points",
        ],
        segment_rows,
    )
    return len(segment_rows)


if __name__ == "__main__":
    count = rebuild_street_segments()
    print(f"Rebuilt street_segments.csv with {count} sanitized segments.")
