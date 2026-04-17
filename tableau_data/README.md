# Quick Start Guide - Tableau Heat Map

## 📋 Files in This Folder

| File | Purpose |
|------|---------|
| `export_to_tableau.py` | ⚡ Main script - exports your MongoDB traffic data to Tableau-ready CSVs |
| `generate_sample_data.py` | 🧪 Optional - generates realistic sample data for testing (if you don't have enough real data yet) |
| `TABLEAU_SETUP_GUIDE.md` | 📖 Complete step-by-step guide for building the heat map |
| `requirements.txt` | 📦 Python dependencies |

## 🚀 Quick Start (5 Minutes)

### Option A: Using Your Real Data
```bash
# 1. Run the export script (make sure MongoDB is running)
python export_to_tableau.py

# 2. Look for these files:
#    - crash_heat_map_data.csv
#    - hourly_congestion_data.csv  
#    - street_summary.csv
```

### Option B: Using Sample Data (For Testing)
```bash
# Generate realistic test data
python generate_sample_data.py

# This creates the same CSV files without needing MongoDB
```

### 3. Open Tableau
1. Launch **Tableau Desktop**
2. **File → New → Connect to Data**
3. Select the **CSV file** you exported
4. Read the [TABLEAU_SETUP_GUIDE.md](TABLEAU_SETUP_GUIDE.md) for detailed instructions

## 🗺️ What You'll Get

A heat map showing:
- 🔴 **Red = High Congestion** (lots of traffic incidents)
- 🟡 **Yellow = Medium Congestion**
- 🟢 **Green = Low Congestion** (safest streets)

The map displays NYC streets, neighborhoods, and incident density to identify congestion hotspots.

## 📊 Data Metrics

The CSVs include:
- **Latitude & Longitude** - Precise street locations
- **Incident_Count** - Number of traffic incidents
- **Congestion_Index** - 0-100 scale (higher = more congestion)
- **Injured & Killed** - Human impact metrics
- **Street Names** - For filtering and labeling

## ✅ Checklist

- [ ] MongoDB running with traffic data
- [ ] Python dependencies installed (`pip install -r requirements.txt`)
- [ ] Ran `export_to_tableau.py` successfully
- [ ] CSV files generated (3 new files appear)
- [ ] Tableau Desktop installed
- [ ] Imported CSV into Tableau
- [ ] Created map with Latitude/Longitude
- [ ] Applied heat map coloring with Congestion_Index
- [ ] Adjusted map styling (colors, opacity)

## Need Help?

- **Heat map not showing?** → Set Latitude as Geographic Role → Latitude, Longitude as Geographic Role → Longitude
- **No data appearing?** → Verify CSV imported correctly and Lat/Lon columns have values
- **Tableau crashes?** → Your CSV is too large; filter top 2000 locations in export script
- **Want to refresh data?** → Run `export_to_tableau.py` again anytime

## Next Steps

After creating the heat map:
1. ✅ Create filters for specific streets or neighborhoods
2. ✅ Add time filters using `hourly_congestion_data.csv`
3. ✅ Build a dashboard combining map + statistics
4. ✅ Export as dashboard/report for presentations
5. ✅ Publish to Tableau Server for team access

## 🔄 Keeping Data Fresh

Your traffic data is updated hourly by the producer. To refresh:
```bash
python export_to_tableau.py  # Run this periodically
```

---

**Note:** This folder is separate from your main pipeline (producer/consumer/spark) to avoid any changes to existing data processing. All Tableau exports are in this dedicated directory.
