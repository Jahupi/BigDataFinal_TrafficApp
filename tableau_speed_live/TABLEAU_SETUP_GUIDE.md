# Tableau Setup for Live Street-Speed Heat Map

This guide uses the files in `tableau_speed_live` so your Tableau workbook can stay aligned with the latest MongoDB street-speed data.

## 1. Start the live export

Open PowerShell in the project root and run:

```powershell
cd tableau_speed_live
python watch_speed_updates.py
```

Keep that terminal window open. It will regenerate the CSV files whenever the `speeds` collection changes.

## 2. Open Tableau Desktop

1. Start Tableau Desktop.
2. Click **Connect**.
3. Choose **Text file**.
4. Open `tableau_speed_live\speed_segments_current.csv`.

## 3. Confirm data types

On the Tableau data source page, make sure:

- `Current_Speed_MPH` is a **Number (decimal)**
- `Travel_Time_Seconds` is a **Number (decimal)**
- `Data_As_Of` is a **Date & Time**
- `Mid_Latitude` is a **Number (decimal)**
- `Mid_Longitude` is a **Number (decimal)**

If Tableau guesses wrong, click the column icon and change it manually.

## 4. Build the main street-speed heat map

1. Open **Sheet 1**.
2. Right-click `Mid_Latitude` and set **Geographic Role -> Latitude**.
3. Right-click `Mid_Longitude` and set **Geographic Role -> Longitude**.
4. Drag `Mid_Longitude` to **Columns**.
5. Drag `Mid_Latitude` to **Rows**.
6. Tableau will create a map.
7. In the **Marks** card, change the mark type to **Density** for a heat map.
8. Drag `Current_Speed_MPH` to **Color**.
9. Drag `Street_Segment_Name` to **Detail**.
10. Drag `Borough` to **Filters**.
11. Drag `Data_As_Of` to **Filters** if you want to limit the time shown.

## 5. Make the colors mean congestion clearly

Lower speed means more congestion, so use a reversed color scale:

1. Click **Color** on the Marks card.
2. Click **Edit Colors**.
3. Choose a palette such as **Red-Gold-Green**.
4. Check **Reversed** so:
   - Red = slower traffic
   - Green = faster traffic

## 6. Add the tooltip details you asked for

Drag these fields onto **Tooltip**:

- `Street_Segment_Name`
- `Current_Speed_MPH`
- `Travel_Time_Seconds`
- `Borough`
- `Data_As_Of`
- `Segment_Status_Label`
- `Link_ID`
- `Owner`

When you hover on the map, Tableau will show the street name, street speed, and supporting metadata.

## 7. Optional: build a street line map

If you want actual street segments drawn instead of density blobs:

1. Add a new data source in Tableau.
2. Connect to `tableau_speed_live\speed_segments_path.csv`.
3. Open a new sheet.
4. Set `Latitude` to **Geographic Role -> Latitude**.
5. Set `Longitude` to **Geographic Role -> Longitude**.
6. Drag `Longitude` to **Columns**.
7. Drag `Latitude` to **Rows**.
8. Change the **Marks** type to **Line**.
9. Drag `Link_ID` to **Detail**.
10. Drag `Sequence` to **Path**.
11. Drag `Current_Speed_MPH` to **Color**.
12. Drag `Street_Segment_Name` to **Tooltip**.

This view is often easier to read when you want each road segment colored by speed.

## 8. Optional: add a summary sheet

1. Add another data source: `tableau_speed_live\street_speed_summary.csv`.
2. Create a table or bar chart using:
   - `Street_Segment_Name`
   - `Average_Speed_MPH`
   - `Minimum_Speed_MPH`
   - `Maximum_Speed_MPH`
   - `Latest_Data_As_Of`

This is useful as a side panel next to the map.

## 9. Refresh when MongoDB updates

As long as `watch_speed_updates.py` is still running, the CSV files will keep updating.

Inside Tableau, refresh with:

1. **Data**
2. **[Your data source]**
3. **Refresh**

If you save the workbook, it can keep pointing to the same CSV paths.

## 10. Recommended dashboard layout

Use three sheets:

1. A heat map from `speed_segments_current.csv`
2. A line map from `speed_segments_path.csv`
3. A summary table from `street_speed_summary.csv`

Add shared filters for:

- `Borough`
- `Speed_Category`
- `Data_As_Of`

## What each key field means

- `Street_Segment_Name`: the exact segment name from MongoDB `link_name`
- `Current_Speed_MPH`: the latest speed value for that segment
- `Travel_Time_Seconds`: the latest travel time for that segment
- `Data_As_Of`: timestamp provided by the source data
- `Segment_Status_Label`: readable version of the source status code
- `Mid_Latitude` / `Mid_Longitude`: center point of the segment, used for map plotting
- `Link_ID`: unique segment identifier

## Important accuracy note

Your current producer inserts a fresh copy of speed rows into MongoDB on each run. This export handles that by choosing only the newest record for each `link_id`, so Tableau sees the latest segment speed instead of duplicate historical rows.
