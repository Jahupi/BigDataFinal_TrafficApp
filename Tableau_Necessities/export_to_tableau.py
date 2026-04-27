"""
Export MongoDB crash data into Tableau-ready CSV files for the crash workbook.

This script keeps the original Tableau file names in place:
- crash_heat_map_data.csv
- hourly_congestion_data.csv
- street_summary.csv
- street_segments.csv

It also guarantees that only valid NYC coordinates are written, so Tableau
refreshes cannot place marks in the ocean.
"""

from __future__ import annotations

import csv
import os
import tempfile
from collections import defaultdict
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv
from pymongo import MongoClient


BASE_DIR = Path(__file__).resolve().parent
ROOT_DIR = BASE_DIR.parent
ENV_PATH = ROOT_DIR / "secrets.env"

CRASH_HEAT_MAP_CSV = BASE_DIR / "crash_heat_map_data.csv"
HOURLY_CONGESTION_CSV = BASE_DIR / "hourly_congestion_data.csv"
STREET_SUMMARY_CSV = BASE_DIR / "street_summary.csv"
STREET_SEGMENTS_CSV = BASE_DIR / "street_segments.csv"

NYC_LAT_MIN = 40.45
NYC_LAT_MAX = 40.95
NYC_LON_MIN = -74.30
NYC_LON_MAX = -73.65
MAX_SEGMENTS_PER_STREET = 20


def load_collection():
    load_dotenv(ENV_PATH)
    mongodb_uri = os.getenv("MONGODB_URI")
    if not mongodb_uri:
        raise ValueError("MONGODB_URI not found in secrets.env")

    client = MongoClient(mongodb_uri)
    collection = client["traffic_data"]["crashes"]
    return client, collection


def parse_float(value) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def parse_int(value) -> int:
    if value in (None, ""):
        return 0
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return 0


def parse_crash_datetime(value) -> datetime | None:
    if not value:
        return None

    text = str(value).strip()
    for fmt in (
        "%Y-%m-%dT%H:%M:%S.%f",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d",
        "%m/%d/%Y",
        "%m/%d/%Y %H:%M",
        "%m/%d/%Y %I:%M:%S %p",
    ):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    return None


def format_crash_datetime(value) -> str:
    parsed = parse_crash_datetime(value)
    if not parsed:
        return ""
    return parsed.isoformat(timespec="milliseconds")


def is_nyc_coordinate(latitude: float, longitude: float) -> bool:
    return NYC_LAT_MIN <= latitude <= NYC_LAT_MAX and NYC_LON_MIN <= longitude <= NYC_LON_MAX


def atomic_write_csv(path: Path, fieldnames: list[str], rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        "w",
        newline="",
        encoding="utf-8",
        dir=path.parent,
        delete=False,
    ) as temp_file:
        writer = csv.DictWriter(temp_file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
        temp_name = temp_file.name

    os.replace(temp_name, path)


def load_valid_crashes(collection) -> list[dict]:
    projection = {
        "_id": 0,
        "latitude": 1,
        "longitude": 1,
        "on_street_name": 1,
        "number_of_persons_injured": 1,
        "number_of_persons_killed": 1,
        "crash_date": 1,
    }

    valid_rows: list[dict] = []
    for doc in collection.find({}, projection):
        latitude = parse_float(doc.get("latitude"))
        longitude = parse_float(doc.get("longitude"))
        if latitude is None or longitude is None:
            continue
        if not is_nyc_coordinate(latitude, longitude):
            continue

        valid_rows.append(
            {
                "latitude": round(latitude, 6),
                "longitude": round(longitude, 6),
                "street": (str(doc.get("on_street_name", "")).strip() or "Unknown"),
                "injured": parse_int(doc.get("number_of_persons_injured")),
                "killed": parse_int(doc.get("number_of_persons_killed")),
                "crash_date_raw": doc.get("crash_date"),
                "crash_date": parse_crash_datetime(doc.get("crash_date")),
            }
        )

    return valid_rows


def build_heat_map_rows(valid_crashes: list[dict]) -> list[dict]:
    grouped: dict[tuple[float, float, str], dict] = {}
    for crash in valid_crashes:
        key = (crash["latitude"], crash["longitude"], crash["street"])
        bucket = grouped.setdefault(
            key,
            {
                "Latitude": crash["latitude"],
                "Longitude": crash["longitude"],
                "Street": crash["street"],
                "Incident_Count": 0,
                "Injured": 0,
                "Killed": 0,
                "last_incident": None,
                "last_incident_raw": "",
            },
        )
        bucket["Incident_Count"] += 1
        bucket["Injured"] += crash["injured"]
        bucket["Killed"] += crash["killed"]
        if crash["crash_date"] and (
            bucket["last_incident"] is None or crash["crash_date"] > bucket["last_incident"]
        ):
            bucket["last_incident"] = crash["crash_date"]
            bucket["last_incident_raw"] = format_crash_datetime(crash["crash_date_raw"])

    grouped_rows = sorted(
        grouped.values(),
        key=lambda row: (-row["Incident_Count"], row["Street"], row["Latitude"], row["Longitude"]),
    )
    scale = max((row["Incident_Count"] for row in grouped_rows), default=1)

    output_rows: list[dict] = []
    for row in grouped_rows:
        congestion_index = min(100, round((row["Incident_Count"] / scale) * 100, 2))
        output_rows.append(
            {
                "Latitude": f'{row["Latitude"]:.6f}',
                "Longitude": f'{row["Longitude"]:.6f}',
                "Street": row["Street"],
                "Incident_Count": row["Incident_Count"],
                "Injured": row["Injured"],
                "Killed": row["Killed"],
                "Congestion_Index": congestion_index,
                "Last_Incident_Date": row["last_incident_raw"],
            }
        )

    return output_rows


def build_hourly_rows(valid_crashes: list[dict]) -> list[dict]:
    grouped: dict[tuple[float, float, str, int], dict] = {}
    for crash in valid_crashes:
        if not crash["crash_date"]:
            continue
        hour = crash["crash_date"].hour
        key = (crash["latitude"], crash["longitude"], crash["street"], hour)
        bucket = grouped.setdefault(
            key,
            {
                "Latitude": crash["latitude"],
                "Longitude": crash["longitude"],
                "Street": crash["street"],
                "Hour": hour,
                "Incident_Count": 0,
                "Total_Injured": 0,
            },
        )
        bucket["Incident_Count"] += 1
        bucket["Total_Injured"] += crash["injured"]

    return [
        {
            "Latitude": f'{row["Latitude"]:.6f}',
            "Longitude": f'{row["Longitude"]:.6f}',
            "Street": row["Street"],
            "Hour": row["Hour"],
            "Incident_Count": row["Incident_Count"],
            "Total_Injured": row["Total_Injured"],
        }
        for row in sorted(
            grouped.values(),
            key=lambda row: (row["Street"], row["Hour"], row["Latitude"], row["Longitude"]),
        )
    ]


def build_street_summary_rows(valid_crashes: list[dict]) -> list[dict]:
    grouped: dict[str, dict] = {}
    for crash in valid_crashes:
        if crash["street"] == "Unknown":
            continue
        bucket = grouped.setdefault(
            crash["street"],
            {
                "Street": crash["street"],
                "latitudes": [],
                "longitudes": [],
                "Total_Incidents": 0,
                "Total_Injured": 0,
                "Total_Killed": 0,
            },
        )
        bucket["latitudes"].append(crash["latitude"])
        bucket["longitudes"].append(crash["longitude"])
        bucket["Total_Incidents"] += 1
        bucket["Total_Injured"] += crash["injured"]
        bucket["Total_Killed"] += crash["killed"]

    grouped_rows = sorted(
        grouped.values(),
        key=lambda row: (-row["Total_Incidents"], row["Street"]),
    )[:500]
    max_incidents = max((row["Total_Incidents"] for row in grouped_rows), default=1)

    output_rows: list[dict] = []
    for row in grouped_rows:
        avg_lat = sum(row["latitudes"]) / len(row["latitudes"])
        avg_lon = sum(row["longitudes"]) / len(row["longitudes"])
        if not is_nyc_coordinate(avg_lat, avg_lon):
            continue
        danger_level = min(5, max(1, int((row["Total_Incidents"] / max_incidents) * 5)))
        output_rows.append(
            {
                "Street": row["Street"],
                "Latitude": f"{avg_lat:.6f}",
                "Longitude": f"{avg_lon:.6f}",
                "Total_Incidents": row["Total_Incidents"],
                "Total_Injured": row["Total_Injured"],
                "Total_Killed": row["Total_Killed"],
                "Danger_Level": danger_level,
            }
        )

    return output_rows


def build_street_segment_rows(heat_map_rows: list[dict]) -> list[dict]:
    street_points: dict[str, list[dict]] = defaultdict(list)
    for row in heat_map_rows:
        street = row["Street"].strip()
        if not street or street == "Unknown":
            continue

        latitude = parse_float(row["Latitude"])
        longitude = parse_float(row["Longitude"])
        congestion = parse_float(row["Congestion_Index"])
        incidents = parse_int(row["Incident_Count"])
        if latitude is None or longitude is None or congestion is None:
            continue
        if not is_nyc_coordinate(latitude, longitude):
            continue

        street_points[street].append(
            {
                "lat": latitude,
                "lon": longitude,
                "congestion": congestion,
                "incidents": incidents,
            }
        )

    output_rows: list[dict] = []
    sequence_num = 0

    for street in sorted(street_points):
        points = sorted(street_points[street], key=lambda point: (point["lon"], point["lat"]))
        if len(points) < 2:
            continue

        segment_size = max(2, len(points) // MAX_SEGMENTS_PER_STREET)
        for index in range(0, len(points), segment_size):
            segment_points = points[index:index + segment_size]
            if len(segment_points) < 2:
                continue

            avg_lat = sum(point["lat"] for point in segment_points) / len(segment_points)
            avg_lon = sum(point["lon"] for point in segment_points) / len(segment_points)
            if not is_nyc_coordinate(avg_lat, avg_lon):
                continue

            sequence_num += 1
            output_rows.append(
                {
                    "Street": street,
                    "Latitude": f"{avg_lat:.6f}",
                    "Longitude": f"{avg_lon:.6f}",
                    "Congestion": round(
                        sum(point["congestion"] for point in segment_points) / len(segment_points),
                        2,
                    ),
                    "Incidents": sum(point["incidents"] for point in segment_points),
                    "Sequence": sequence_num,
                    "Segment_Points": len(segment_points),
                }
            )

    return output_rows


def export_all() -> dict[str, int]:
    client, collection = load_collection()
    try:
        valid_crashes = load_valid_crashes(collection)
        if not valid_crashes:
            raise RuntimeError("No valid NYC crash data found in MongoDB.")

        heat_map_rows = build_heat_map_rows(valid_crashes)
        hourly_rows = build_hourly_rows(valid_crashes)
        street_summary_rows = build_street_summary_rows(valid_crashes)
        street_segment_rows = build_street_segment_rows(heat_map_rows)

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
            heat_map_rows,
        )
        atomic_write_csv(
            HOURLY_CONGESTION_CSV,
            [
                "Latitude",
                "Longitude",
                "Street",
                "Hour",
                "Incident_Count",
                "Total_Injured",
            ],
            hourly_rows,
        )
        atomic_write_csv(
            STREET_SUMMARY_CSV,
            [
                "Street",
                "Latitude",
                "Longitude",
                "Total_Incidents",
                "Total_Injured",
                "Total_Killed",
                "Danger_Level",
            ],
            street_summary_rows,
        )
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
            street_segment_rows,
        )

        return {
            "valid_crashes": len(valid_crashes),
            "heat_map_locations": len(heat_map_rows),
            "hourly_rows": len(hourly_rows),
            "street_summary_rows": len(street_summary_rows),
            "street_segments": len(street_segment_rows),
        }
    finally:
        client.close()


if __name__ == "__main__":
    counts = export_all()
    print(
        "Export complete: "
        f"{counts['valid_crashes']} valid crashes, "
        f"{counts['heat_map_locations']} heat map locations, "
        f"{counts['hourly_rows']} hourly rows, "
        f"{counts['street_summary_rows']} street summaries, "
        f"{counts['street_segments']} street segments."
    )
