#!/usr/bin/env python3
"""
Cambiar ETL Pipeline - Simple Version

This script runs the complete ETL pipeline:
1. Extract stations data from API
2. Clean and transform the data
3. Load the data into PostgreSQL database

Run with: python main.py
"""

from src.extract_data import extract_stations
from src.transform_data import clean_stations
from src.load_data import load_to_database, verify_data

def main():
    """Run the complete ETL pipeline"""
    print("🚲 Starting Bike Stations ETL Pipeline...")
    print("=" * 50)
    
    # Step 1: Extract data
    print("\n=== EXTRACTION ===")
    print("📥 Extracting data from sources...")
    
    # Call the extraction functions
    stations = extract_stations()
    
    # Step 2: Transform data
    print("\n=== TRANSFORMATION ===")
    print("🔄 Cleaning and transforming data...")
    
    # Call the transformation functions
    clean_stations_data = clean_stations(stations)
    
    # Step 3: Load data
    print("\n=== LOADING ===")
    print("💾 Loading data to database...")
    
    # Call the loading function
    load_to_database(clean_stations_data)
    
    # Step 4: Verify everything worked
    print("\n=== VERIFICATION ===")
    print("✅ Verifying data was loaded correctly...")
    
    # Call the verification function
    verify_data()
    
    print("\n🎉 ETL Pipeline completed!")
    print("=" * 50)

if __name__ == "__main__":
    main()
