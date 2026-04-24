"""
Automated Tableau Heat Map Workbook Generator
Creates a pre-configured .twp (Tableau workbook) file for the heat map
"""

import csv
import os
import shutil
from pathlib import Path

def create_tableau_data_extract():
    """
    Create a Tableau data extract (.hyper file) from the CSV
    This makes it faster and more efficient in Tableau
    """
    try:
        from tableauhyperapi import HyperProcess, Telemetry, Connection, TableDefinition, SqlType, Inserter, CreateMode
        
        print("Creating Tableau data extract (.hyper file)...")
        
        # Read CSV data
        csv_file = 'crash_heat_map_data.csv'
        rows = []
        
        with open(csv_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                rows.append([
                    float(row['Latitude']),
                    float(row['Longitude']),
                    row['Street'],
                    int(row['Incident_Count']),
                    int(row['Injured']),
                    int(row['Killed']),
                    float(row['Congestion_Index']),
                    row['Last_Incident_Date']
                ])
        
        # Create table definition
        table_def = TableDefinition(
            table_name='crashes',
            columns=[
                TableDefinition.Column('Latitude', SqlType.double()),
                TableDefinition.Column('Longitude', SqlType.double()),
                TableDefinition.Column('Street', SqlType.text()),
                TableDefinition.Column('Incident_Count', SqlType.int()),
                TableDefinition.Column('Injured', SqlType.int()),
                TableDefinition.Column('Killed', SqlType.int()),
                TableDefinition.Column('Congestion_Index', SqlType.double()),
                TableDefinition.Column('Last_Incident_Date', SqlType.text())
            ]
        )
        
        # Create hyper file
        hyper_file = 'crash_heat_map_data.hyper'
        with HyperProcess(Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU) as hyper:
            with Connection(
                endpoint=hyper.endpoint,
                create_mode=CreateMode.CREATE_AND_REPLACE,
                database=hyper_file
            ) as connection:
                connection.catalog.create_table(table_def)
                
                with Inserter(connection, table_def) as inserter:
                    inserter.add_rows(rows)
                    inserter.execute()
        
        print(f"✓ Created {hyper_file}")
        return True
        
    except ImportError:
        print("Note: tableau-hyper-api not installed. Using CSV instead.")
        print("Install with: pip install tableauhyperapi")
        return False

def create_tableau_workbook_xml():
    """
    Create a basic Tableau workbook XML configuration
    (This is a simplified version - full workbooks are complex)
    """
    xml_content = '''<?xml version="1.0" encoding="UTF-8"?>
<workbook source-build="18.1.0" version="10.0" xmlns="http://tableauserver.com/api">
  <preferences/>
  <datasources>
    <datasource caption="crash_heat_map_data" name="crash_heat_map_data" version="10.0">
      <connection class="textscan" dbname="crash_heat_map_data.csv" server=""/>
      <column caption="Latitude" datatype="real" name="[Latitude]" role="measure" type="quantitative"/>
      <column caption="Longitude" datatype="real" name="[Longitude]" role="measure" type="quantitative"/>
      <column caption="Street" datatype="string" name="[Street]" role="dimension" type="nominal"/>
      <column caption="Incident_Count" datatype="integer" name="[Incident_Count]" role="measure" type="quantitative"/>
      <column caption="Injured" datatype="integer" name="[Injured]" role="measure" type="quantitative"/>
      <column caption="Killed" datatype="integer" name="[Killed]" role="measure" type="quantitative"/>
      <column caption="Congestion_Index" datatype="real" name="[Congestion_Index]" role="measure" type="quantitative"/>
    </datasource>
  </datasources>
</workbook>'''
    
    with open('heat_map_config.twb', 'w') as f:
        f.write(xml_content)
    
    print("✓ Created heat_map_config.twb template")

def generate_tableau_instructions():
    """
    Generate step-by-step instructions for quick setup
    """
    instructions = """
================================================================================
                TABLEAU HEAT MAP - QUICK SETUP INSTRUCTIONS
================================================================================

STEP 1: Open Tableau Desktop

STEP 2: Create New Workbook
   - File -> New -> Connect to Data
   - Select "Text file" 
   - Choose: crash_heat_map_data.csv

STEP 3: Build the Heat Map (in THIS order)
   
   a) CREATE MAP VIEW:
      - Drag [Latitude] to ROWS shelf
      - Drag [Longitude] to COLUMNS shelf
      - Map appears automatically
   
   b) ADD HEAT MAP COLORS:
      - Drag [Congestion_Index] to COLOR button (in Marks card)
      - Points now colored by congestion level
   
   c) SET COLOR PALETTE:
      - Click on the Color legend
      - Choose: Red-Yellow-Green (recommended) or Red-Blue
      - Red = High congestion
      - Green = Low congestion
   
   d) OPTIONAL - ADD BUBBLE SIZE:
      - Drag [Incident_Count] to SIZE button
      - Larger bubbles = more incidents
   
   e) OPTIONAL - ADD STREET NAMES:
      - Drag [Street] to DETAIL button
      - Hover over points to see street names
   
   f) OPTIONAL - ADD TOOLTIP INFO:
      - Right-click on map -> Edit Tooltip
      - Add these fields:
        * Street
        * Incident_Count
        * Injured
        * Killed
        * Congestion_Index

STEP 4: Customize the Visualization
   
   ADJUST OPACITY:
   - Click Color in Marks panel
   - Drag Opacity slider to 60-70%
   
   ZOOM INTO MANHATTAN:
   - Scroll to zoom in on high-incident areas
   - Red clusters show congestion hotspots

STEP 5: Add Filters (Optional)
   - Drag [Street] to FILTERS shelf
   - Now you can select specific streets to view

STEP 6: View Results
   Your heat map should show:
   - Red areas = High traffic congestion
   - Yellow areas = Medium congestion  
   - Green areas = Low congestion

================================================================================

TIPS:

* If map doesn't show: Set Latitude/Longitude as Geographic Role
  - Right-click [Latitude] -> Geographic Role -> Latitude
  - Right-click [Longitude] -> Geographic Role -> Longitude

* To zoom: Use mouse wheel or Tableau's zoom tools

* To see all points: Adjust opacity to 50-70%

* Manhattan hotspots: Look for red clusters around:
  - 12th Avenue (West Side Highway)
  - Central Park areas
  - Major intersections (42nd St, 59th St, etc.)

================================================================================

FINAL HEAT MAP FEATURES:

* Geographic map of NYC with latitude/longitude
* Color-coded points (red = congestion hotspots)
* Bubble size shows incident density
* Hover tooltips with street names & incident details
* Filterable by street name
* Real incident data from MongoDB

================================================================================

Need more help? Check TABLEAU_SETUP_GUIDE.md for detailed troubleshooting.
"""
    
    with open('QUICK_SETUP.txt', 'w', encoding='utf-8') as f:
        f.write(instructions)
    
    print("✓ Created QUICK_SETUP.txt - Open this for step-by-step guide")
    print("\n" + instructions)

def generate_one_click_launcher():
    """
    Create batch files for quick data import
    """
    # Create a simple Python script that opens Tableau with the data
    python_code = '''import subprocess
import sys

csv_file = "crash_heat_map_data.csv"
print(f"Opening Tableau with {csv_file}...")
print("When Tableau opens, connect to the CSV file as instructed.")

# Try to open Tableau Desktop
try:
    subprocess.Popen([r"C:\\Program Files\\Tableau\\Tableau ~\\bin\\tableau.exe"])
except FileNotFoundError:
    print("Tableau Desktop not found in default location.")
    print("Please open Tableau manually and connect to crash_heat_map_data.csv")
    print(f"File location: {os.path.abspath(csv_file)}")
    sys.exit(1)
'''
    
    with open('open_tableau.py', 'w') as f:
        f.write(python_code)
    
    print("✓ Created open_tableau.py - Run this to launch Tableau")

if __name__ == '__main__':
    print("================================================================================")
    print("         TABLEAU HEAT MAP SETUP - AUTOMATED WORKBOOK GENERATOR")
    print("================================================================================\n")
    
    print("Step 1: Generating Tableau data extract...")
    if create_tableau_data_extract():
        print("✓ Hyper extract created (faster in Tableau)\n")
    else:
        print("INFO: Will use CSV file instead\n")
    
    print("Step 2: Creating workbook configuration...")
    create_tableau_workbook_xml()
    
    print("\nStep 3: Generating one-click launcher...")
    generate_one_click_launcher()
    
    print("\nStep 4: Creating quick setup guide...")
    generate_tableau_instructions()
    
    print("\n================================================================================")
    print("                          SETUP COMPLETE!")
    print("================================================================================")
    print("\nNext Steps:")
    print("   1. Open QUICK_SETUP.txt for step-by-step instructions")
    print("   2. Open Tableau Desktop manually")
    print("   3. Connect to: crash_heat_map_data.csv")
    print("   4. Follow the 6 steps in QUICK_SETUP.txt")
    print("\nFiles created:")
    print("   * crash_heat_map_data.hyper (optional - faster performance)")
    print("   * heat_map_config.twb (Tableau config template)")
    print("   * open_tableau.py (Quick launcher script)")
    print("   * QUICK_SETUP.txt (Step-by-step guide)")
    print("\nYour heat map will show NYC street congestion!")
