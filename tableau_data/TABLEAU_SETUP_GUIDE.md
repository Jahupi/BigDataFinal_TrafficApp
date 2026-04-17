# Tableau Heat Map Setup Guide

This folder contains everything you need to create a heat map in Tableau showing street congestion in New York City.

## Overview

The heat map visualizes traffic congestion across NYC streets using:
- **Incident density** (number of crashes per location)
- **Location data** (latitude/longitude coordinates)
- **Congestion Index** (calculated metric based on incident frequency)

## Step 1: Export Data from MongoDB

Run the export script to extract your traffic data and format it for Tableau:

```bash
cd tableau_data
python export_to_tableau.py
```

This will generate three CSV files:
- **crash_heat_map_data.csv** - Aggregated incidents by location (recommended for heat map)
- **hourly_congestion_data.csv** - Time-based congestion analysis
- **street_summary.csv** - Street-level statistics with danger levels

## Step 2: Open Tableau

1. Launch Tableau Desktop
2. Click **File → New**
3. Select **Connect to Data**

## Step 3: Import CSV Data

1. Click **Text file** and select `crash_heat_map_data.csv`
2. Tableau will auto-detect the columns
3. Click **Sheet 1** at the bottom to start building the visualization

## Step 4: Create the Heat Map

### A. Set Up the Geographic Foundation

1. **Drag fields to shelves:**
   - Drag **Latitude** to the **Rows** shelf
   - Drag **Longitude** to the **Columns** shelf
   - Tableau will automatically create a map view

### B. Add Heat Map Coloring

1. **Drag Congestion_Index to Color:**
   - From the left panel, drag **Congestion_Index** onto the **Color** button in the Marks card
   - This will color each point based on congestion level

2. **Adjust color scheme:**
   - Click the **Color Legend**
   - Choose a color palette (Red-Yellow-Green or Red-Orange-Yellow recommended)
   - Red = High congestion
   - Yellow = Medium congestion
   - Green = Low congestion

### C. Add Size Dimension (Optional)

1. Drag **Incident_Count** to the **Size** button in Marks
   - This makes high-incident areas appear as larger bubbles

### D. Add Street Information

1. Drag **Street** to the **Detail** button in Marks
2. Right-click on the map and select **Add Tooltip**
3. Drag the following to the tooltip:
   - Street
   - Incident_Count
   - Injured
   - Killed
   - Congestion_Index

## Step 5: Customize the Visualization

### Adjust Opacity
- In Marks panel, click **Color**
- Drag the **Opacity** slider to ~60-70% to see overlapping heatmaps

### Zoom and Pan
- Use your mouse wheel to zoom
- Click and drag to pan across the map
- Double-click to reset the view

### Filters
1. Drag **Street** to the **Filters** shelf
2. You can now filter to view specific streets or neighborhoods

### Map Layers (Optional)
1. Click the map icon in the toolbar
2. Select **Map → Background Maps** to add streets and landmarks

## Step 6: Apply Geographic Settings

If Tableau doesn't automatically recognize your data as geographic:

1. Select the **Latitude** field in the data panel
2. Right-click → **Geographic Role** → **Latitude**
3. Do the same for **Longitude** → **Longitude**

## Step 7: View the Heat Map

- The map will show:
  - **Red areas** = High congestion hotspots
  - **Yellow areas** = Medium congestion
  - **Green areas** = Low congestion
  - **Hover** over any point to see street name, incident count, and injuries

## Advanced Features

### Time-Series Analysis
1. Open `hourly_congestion_data.csv` as a new data source
2. Drag **Hour** to filters to see congestion by time of day
3. Identify peak congestion hours

### Street-Level Dashboard
1. Import `street_summary.csv`
2. Create a bar chart with **Street** vs **Danger_Level**
3. Cross-filter with the heat map for detailed analysis

### Danger Level Clustering
1. Use `street_summary.csv`
2. Color by **Danger_Level** (1-5 scale)
3. Filter by danger level 4-5 to highlight the most dangerous streets

## Example Tableau Calculations

If you want to create additional metrics in Tableau:

**Injury Rate per Incident:**
```
SUM([Total_Injured]) / SUM([Total_Incidents])
```

**High Congestion Indicator:**
```
IF [Congestion_Index] > 50 THEN "High" ELSEIF [Congestion_Index] > 25 THEN "Medium" ELSE "Low" END
```

## Troubleshooting

| Issue | Solution |
|-------|----------|
| Map not showing | Verify Latitude/Longitude columns are set to Geographic Role |
| No data appears | Check that CSV has been properly imported and Latitude/Longitude are not null |
| Colors not showing gradient | Drag Congestion_Index to Color instead of another field |
| Tableau crashes with large data | Filter to top 1000-2000 locations, or use aggregation |
| Street names not appearing | Add Street to Detail marks, not Label |

## Data Interpretation

- **Red clusters** = High crash density → Areas requiring traffic management improvement
- **Yellow zones** = Moderate concern areas → Monitor for trends
- **Green areas** = Safer streets → Use as baseline for comparison

## Next Steps

1. **Create a dashboard** with the heat map + street rankings
2. **Add interactivity** with filters for time periods and neighborhoods
3. **Export as PDF** for presentations
4. **Publish to Tableau Server** for team sharing

---

**Note:** The data is updated hourly by the producer/consumer pipeline. Re-run `export_to_tableau.py` to refresh the CSV files with the latest data.
