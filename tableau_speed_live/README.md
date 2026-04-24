# Live Tableau Export for Street Speeds

This folder keeps Tableau-ready CSV files synced with the latest records in MongoDB's `traffic_data.speeds` collection.

## What it creates

- `speed_segments_current.csv`
  - One latest record per `link_id`
  - Includes exact segment name from MongoDB, speed, travel time, borough, status, and midpoint coordinates for map heat maps
- `speed_segments_path.csv`
  - One row per coordinate point in each street segment
  - Best for line maps in Tableau
- `street_speed_summary.csv`
  - Street-segment summary metrics for labels, filters, and side tables

## Why this is accurate

- It reads directly from MongoDB, not from stale local files
- It keeps only the newest record for each `link_id`
- It preserves the original `link_name`, `borough`, `speed`, `travel_time`, `data_as_of`, and `status`
- It derives map coordinates from the official `link_points` field in MongoDB
- It rewrites CSV files atomically so Tableau does not read half-written files

## Run one export

```powershell
cd tableau_speed_live
python export_speed_data.py
```

## Keep Tableau files updated automatically

```powershell
cd tableau_speed_live
python watch_speed_updates.py
```

Or on Windows:

```powershell
cd tableau_speed_live
.\start_speed_tableau_sync.bat
```

Leave the watcher running while your Kafka consumer continues inserting speed records into MongoDB.

## Files Tableau should use

- Start with `speed_segments_current.csv` for the main heat map
- Use `speed_segments_path.csv` if you want colored street lines
- Use `street_speed_summary.csv` for summary tables, KPIs, or filters

## Requirements

Your root project dependencies already include what this folder needs:

- `pymongo`
- `python-dotenv`

## Notes

- Best results come from MongoDB Atlas or another deployment that supports change streams
- If change streams are not available, the watcher automatically switches to a 60-second polling fallback
- Tableau refreshes these files from disk when you use `Data -> Refresh`
