"""
Export the latest MongoDB street-speed data into Tableau-ready CSV files.

Outputs:
- speed_segments_current.csv: one latest row per street segment with map-ready midpoint fields
- speed_segments_path.csv: ordered coordinate rows for drawing street segment lines in Tableau
- street_speed_summary.csv: grouped summary by borough and segment name
"""

from __future__ import annotations

import csv
import os
import tempfile
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterable

from dotenv import load_dotenv
from pymongo import MongoClient


BASE_DIR = Path(__file__).resolve().parent
ROOT_DIR = BASE_DIR.parent
ENV_PATH = ROOT_DIR / "secrets.env"

NYC_LAT_MIN = 40.45
NYC_LAT_MAX = 40.95
NYC_LON_MIN = -74.30
NYC_LON_MAX = -73.65

CURRENT_SEGMENTS_CSV = BASE_DIR / "speed_segments_current.csv"
PATH_SEGMENTS_CSV = BASE_DIR / "speed_segments_path.csv"
STREET_SUMMARY_CSV = BASE_DIR / "street_speed_summary.csv"


@dataclass
class SegmentRecord:
    link_id: str
    record_id: str
    street_segment_name: str
    borough: str
    owner: str
    transcom_id: str
    current_speed_mph: float | None
    travel_time_seconds: float | None
    data_as_of_raw: str
    data_as_of: datetime | None
    status_code: str
    status_label: str
    speed_category: str
    link_points_raw: str
    point_count: int
    start_latitude: float | None
    start_longitude: float | None
    end_latitude: float | None
    end_longitude: float | None
    mid_latitude: float | None
    mid_longitude: float | None


def load_collection():
    load_dotenv(ENV_PATH)
    mongodb_uri = os.getenv("MONGODB_URI")
    if not mongodb_uri:
        raise ValueError("MONGODB_URI not found in secrets.env")

    client = MongoClient(mongodb_uri)
    collection = client["traffic_data"]["speeds"]
    return client, collection


def parse_iso_datetime(value: str | None) -> datetime | None:
    if not value:
        return None

    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def parse_float(value: str | int | float | None) -> float | None:
    if value in (None, ""):
        return None

    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def parse_link_points(link_points: str | None) -> list[tuple[float, float]]:
    points: list[tuple[float, float]] = []
    if not link_points:
        return points

    for raw_pair in link_points.split():
        if "," not in raw_pair:
            continue

        lat_raw, lon_raw = raw_pair.split(",", 1)
        lat = parse_float(lat_raw)
        lon = parse_float(lon_raw)
        if lat is None or lon is None:
            continue
        if not (NYC_LAT_MIN <= lat <= NYC_LAT_MAX and NYC_LON_MIN <= lon <= NYC_LON_MAX):
            continue
        points.append((lat, lon))

    return points


def representative_point(points: list[tuple[float, float]]) -> tuple[float | None, float | None]:
    if not points:
        return None, None

    mid_index = len(points) // 2
    latitude, longitude = points[mid_index]
    return round(latitude, 6), round(longitude, 6)


def status_label(status_code: str | None) -> str:
    code = (status_code or "").strip()
    if code == "0":
        return "Normal"
    if code == "-101":
        return "No recent speed status"
    if code == "-1":
        return "Unknown"
    if not code:
        return "Missing"
    return f"Status {code}"


def speed_category(speed_mph: float | None) -> str:
    if speed_mph is None:
        return "Missing"
    if speed_mph < 10:
        return "Severe congestion"
    if speed_mph < 20:
        return "Heavy congestion"
    if speed_mph < 35:
        return "Moderate traffic"
    return "Free flow"


def latest_segment_documents(collection) -> list[dict]:
    pipeline = [
        {"$match": {"link_id": {"$exists": True, "$ne": None}}},
        {"$sort": {"link_id": 1, "data_as_of": -1, "_id": -1}},
        {
            "$group": {
                "_id": "$link_id",
                "doc": {"$first": "$$ROOT"},
            }
        },
        {"$replaceRoot": {"newRoot": "$doc"}},
        {"$sort": {"borough": 1, "link_name": 1, "link_id": 1}},
    ]
    return list(collection.aggregate(pipeline, allowDiskUse=True))


def normalize_documents(documents: Iterable[dict]) -> list[SegmentRecord]:
    records: list[SegmentRecord] = []

    for doc in documents:
        points = parse_link_points(doc.get("link_points"))
        mid_lat, mid_lon = representative_point(points)
        first_point = points[0] if points else (None, None)
        last_point = points[-1] if points else (None, None)
        current_speed = parse_float(doc.get("speed"))
        travel_time = parse_float(doc.get("travel_time"))
        data_as_of_raw = doc.get("data_as_of", "")

        records.append(
            SegmentRecord(
                link_id=str(doc.get("link_id", "")).strip(),
                record_id=str(doc.get("id", "")).strip(),
                street_segment_name=str(doc.get("link_name", "")).strip() or "Unknown",
                borough=str(doc.get("borough", "")).strip() or "Unknown",
                owner=str(doc.get("owner", "")).strip(),
                transcom_id=str(doc.get("transcom_id", "")).strip(),
                current_speed_mph=current_speed,
                travel_time_seconds=travel_time,
                data_as_of_raw=data_as_of_raw,
                data_as_of=parse_iso_datetime(data_as_of_raw),
                status_code=str(doc.get("status", "")).strip(),
                status_label=status_label(doc.get("status")),
                speed_category=speed_category(current_speed),
                link_points_raw=str(doc.get("link_points", "")).strip(),
                point_count=len(points),
                start_latitude=first_point[0],
                start_longitude=first_point[1],
                end_latitude=last_point[0],
                end_longitude=last_point[1],
                mid_latitude=mid_lat,
                mid_longitude=mid_lon,
            )
        )

    return records


def atomic_write_csv(path: Path, fieldnames: list[str], rows: Iterable[dict]) -> None:
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
        for row in rows:
            writer.writerow(row)
        temp_name = temp_file.name

    os.replace(temp_name, path)


def build_current_segment_rows(records: list[SegmentRecord]) -> list[dict]:
    rows: list[dict] = []
    for record in records:
        rows.append(
            {
                "Link_ID": record.link_id,
                "Record_ID": record.record_id,
                "Street_Segment_Name": record.street_segment_name,
                "Borough": record.borough,
                "Owner": record.owner,
                "Transcom_ID": record.transcom_id,
                "Current_Speed_MPH": record.current_speed_mph,
                "Travel_Time_Seconds": record.travel_time_seconds,
                "Data_As_Of": record.data_as_of_raw,
                "Segment_Status_Code": record.status_code,
                "Segment_Status_Label": record.status_label,
                "Speed_Category": record.speed_category,
                "Point_Count": record.point_count,
                "Start_Latitude": record.start_latitude,
                "Start_Longitude": record.start_longitude,
                "End_Latitude": record.end_latitude,
                "End_Longitude": record.end_longitude,
                "Mid_Latitude": record.mid_latitude,
                "Mid_Longitude": record.mid_longitude,
                "Link_Points": record.link_points_raw,
            }
        )
    return rows


def build_path_rows(records: list[SegmentRecord]) -> list[dict]:
    rows: list[dict] = []
    for record in records:
        points = parse_link_points(record.link_points_raw)
        for sequence, (latitude, longitude) in enumerate(points, start=1):
            rows.append(
                {
                    "Link_ID": record.link_id,
                    "Street_Segment_Name": record.street_segment_name,
                    "Borough": record.borough,
                    "Sequence": sequence,
                    "Latitude": latitude,
                    "Longitude": longitude,
                    "Current_Speed_MPH": record.current_speed_mph,
                    "Travel_Time_Seconds": record.travel_time_seconds,
                    "Data_As_Of": record.data_as_of_raw,
                    "Segment_Status_Label": record.status_label,
                    "Speed_Category": record.speed_category,
                }
            )
    return rows


def build_summary_rows(records: list[SegmentRecord]) -> list[dict]:
    grouped: dict[tuple[str, str], list[SegmentRecord]] = {}
    for record in records:
        grouped.setdefault((record.borough, record.street_segment_name), []).append(record)

    rows: list[dict] = []
    for (borough, street_segment_name), group in sorted(grouped.items()):
        speeds = [item.current_speed_mph for item in group if item.current_speed_mph is not None]
        travel_times = [item.travel_time_seconds for item in group if item.travel_time_seconds is not None]
        latest_record = max(
            group,
            key=lambda item: (
                item.data_as_of or datetime.min,
                item.link_id,
            ),
        )

        rows.append(
            {
                "Borough": borough,
                "Street_Segment_Name": street_segment_name,
                "Segment_Count": len(group),
                "Average_Speed_MPH": round(sum(speeds) / len(speeds), 2) if speeds else None,
                "Minimum_Speed_MPH": min(speeds) if speeds else None,
                "Maximum_Speed_MPH": max(speeds) if speeds else None,
                "Average_Travel_Time_Seconds": round(sum(travel_times) / len(travel_times), 2) if travel_times else None,
                "Latest_Data_As_Of": latest_record.data_as_of_raw,
                "Representative_Latitude": latest_record.mid_latitude,
                "Representative_Longitude": latest_record.mid_longitude,
                "Most_Recent_Speed_Category": latest_record.speed_category,
            }
        )

    return rows


def export_speed_data() -> dict[str, int]:
    client, collection = load_collection()
    try:
        documents = latest_segment_documents(collection)
        records = normalize_documents(documents)

        current_rows = build_current_segment_rows(records)
        path_rows = build_path_rows(records)
        summary_rows = build_summary_rows(records)

        atomic_write_csv(
            CURRENT_SEGMENTS_CSV,
            [
                "Link_ID",
                "Record_ID",
                "Street_Segment_Name",
                "Borough",
                "Owner",
                "Transcom_ID",
                "Current_Speed_MPH",
                "Travel_Time_Seconds",
                "Data_As_Of",
                "Segment_Status_Code",
                "Segment_Status_Label",
                "Speed_Category",
                "Point_Count",
                "Start_Latitude",
                "Start_Longitude",
                "End_Latitude",
                "End_Longitude",
                "Mid_Latitude",
                "Mid_Longitude",
                "Link_Points",
            ],
            current_rows,
        )
        atomic_write_csv(
            PATH_SEGMENTS_CSV,
            [
                "Link_ID",
                "Street_Segment_Name",
                "Borough",
                "Sequence",
                "Latitude",
                "Longitude",
                "Current_Speed_MPH",
                "Travel_Time_Seconds",
                "Data_As_Of",
                "Segment_Status_Label",
                "Speed_Category",
            ],
            path_rows,
        )
        atomic_write_csv(
            STREET_SUMMARY_CSV,
            [
                "Borough",
                "Street_Segment_Name",
                "Segment_Count",
                "Average_Speed_MPH",
                "Minimum_Speed_MPH",
                "Maximum_Speed_MPH",
                "Average_Travel_Time_Seconds",
                "Latest_Data_As_Of",
                "Representative_Latitude",
                "Representative_Longitude",
                "Most_Recent_Speed_Category",
            ],
            summary_rows,
        )

        return {
            "segments": len(current_rows),
            "path_points": len(path_rows),
            "summaries": len(summary_rows),
        }
    finally:
        client.close()


if __name__ == "__main__":
    counts = export_speed_data()
    print(
        "Export complete: "
        f"{counts['segments']} current segments, "
        f"{counts['path_points']} path points, "
        f"{counts['summaries']} summary rows."
    )
