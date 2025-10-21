"""
Data Transformation Module

This module handles cleaning and transforming the extracted data:
- Clean stations data (remove closed stations, handle missing values)
"""

import pandas as pd
import numpy as np
import json
import ast

def clean_stations(stations_df):
    """
    Clean and validate airport data
    
    Args:
        stations_df (pandas.DataFrame): Raw stations data from CSV
        
    Returns:
        pandas.DataFrame: Cleaned stations data
    """
    if stations_df.empty:
        print("⚠️  No stations data to clean")
        return stations_df
    
    print(f"🧹 Cleaning stations data...")
    print(f"Starting with {len(stations_df)} stations")
    
    # Make a copy to avoid modifying the original
    df = stations_df.copy()
    
    # Remove rows with missing position or adress
    df = df.dropna(subset=['position', 'address'])
    
    # Handle missing names (replace empty strings or 'N' with None)
    df['name'] = df['name'].replace(['', 'N', '\\N'], None) 

    df = df[(df['status'] == "OPEN")]

    # Extraire 'lat' et 'lng' depuis le dictionnaire 'position'
    df['lat'] = df['position'].apply(lambda x: x['lat'])
    df['lng'] = df['position'].apply(lambda x: x['lng'])
    df = df.drop('position', axis=1)
    
    # Print how many stations remain after cleaning
    print(f"After cleaning: {len(df)} stations remain")
    return df

def validate_data_quality(df):
    """
    Helper function to check data quality
    
    Args:
        df (pandas.DataFrame): Data to validate
    """
    if df.empty:
        print(f"⚠️  No stations data to validate")
        return
    
    print(f"📊 Stations quality report :")
    print(f"   Total records: {len(df)}")
    
    # Check for missing values
    missing_values = df.isnull().sum()
    if missing_values.any():
        print("   Missing values:")
        for col, count in missing_values[missing_values > 0].items():
            print(f"     {col}: {count}")
    else:
        print("   ✅ No missing values")

if __name__ == "__main__":
    """Test the transformation functions with sample data"""
    print("Testing transformation functions...\n")
    
    # Create sample airport data for testing
    df = pd.read_csv("ETL-Cambiar/data/stations_velo.csv")
    
    # Test airport cleaning
    cleaned_stations = clean_stations(df)
    validate_data_quality(cleaned_stations)
    cleaned_stations.to_csv('ETL-Cambiar/data/stations_velo_cleaned.csv')
    
    print("\nTransformation testing complete!")
