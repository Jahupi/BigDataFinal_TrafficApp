"""
Sanitize crash heat map coordinates in place.

This keeps the original file name the same so Tableau can continue to refresh
the same data source.
"""

from __future__ import annotations

import csv
from pathlib import Path

from export_to_tableau import CRASH_HEAT_MAP_CSV, atomic_write_csv, is_nyc_coordinate, parse_float


def clean_crash_heat_map() -> int:
    rows: list[dict] = []
    with CRASH_HEAT_MAP_CSV.open("r", newline="", encoding="utf-8") as file:
        reader = csv.DictReader(file)
        for row in reader:
            latitude = parse_float(row.get("Latitude"))
            longitude = parse_float(row.get("Longitude"))
            if latitude is None or longitude is None:
                continue
            if not is_nyc_coordinate(latitude, longitude):
                continue

            row["Latitude"] = f"{latitude:.6f}"
            row["Longitude"] = f"{longitude:.6f}"
            rows.append(row)

    atomic_write_csv(
        CRASH_HEAT_MAP_CSV,
        [
            "Latitude",
            "Longitude",
            "Street",
            "Incident_Count",
            "Injured",
            "Killed",
            "Congestion_Index",
            "Last_Incident_Date",
        ],
        rows,
    )
    return len(rows)


if __name__ == "__main__":
    kept = clean_crash_heat_map()
    print(f"Cleaned crash_heat_map_data.csv and kept {kept} NYC rows.")
