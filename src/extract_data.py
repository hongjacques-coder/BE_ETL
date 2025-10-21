"""
Data Extraction Module

This module handles extracting data from :
- Bike stations data from OpenSky JCDecaux API
"""

import pandas as pd
import requests
import time
import os


def extract_stations():
    """
    Extract bike stations data from JCDecaux API
    
    Returns:
        pandas.DataFrame: Stations data with current stations occupation
    """
    print("🌐 Fetching live stations data from API...")
    
    # API endpoint for JCDecaux
    url = "https://api.jcdecaux.com/vls/v1/stations?contract=toulouse&apiKey=ab168389738c556afb9ff7bd969c01c47b8c22f8"
    
    try:
        print("Making API request... (this may take a few seconds)")
        
        # Make the API request using requests.get()
        response = requests.get(url, timeout=30)
        
        # Check if the response is successful
        response.status_code == 200
        
        # Get the JSON data from the response
        data = response.json()
        
        # Convert to DataFrame
        df = pd.DataFrame(data)
        df.to_csv('data/stations_velo.csv')
        
        # Print how many stations were found
        print(f"Found {len(df)} stations")
        
        return df
        
    except requests.exceptions.RequestException as e:
        print(f"❌ Network error fetching stations data: {e}")
        return pd.DataFrame()
    except Exception as e:
        print(f"❌ Error processing stations data: {e}")
        return pd.DataFrame()

def test_api_connection():
    """
    Test function to check if the JCDecaux API is accessible
    Students can use this to debug connection issues
    """
    print("🔍 Testing API connection...")
    
    try:
        response = requests.get(
            "https://api.jcdecaux.com/vls/v1/stations?contract=toulouse&apiKey=ab168389738c556afb9ff7bd969c01c47b8c22f8",
            timeout=5
        )
        
        if response.status_code == 200:
            data = response.json()
            stations_count = len(data)
            print(f"✅ API connection successful! Found {stations_count} stations in test area")
            return True
        else:
            print(f"⚠️ API returned status code: {response.status_code}")
            return False
            
    except Exception as e:
        print(f"❌ API connection failed: {e}")
        return False

if __name__ == "__main__":
    """Test the extraction functions"""
    print("Testing extraction functions...\n")
    
    
    # Test API connection first
    if test_api_connection():
        # Test stations extraction
        stations = extract_stations()
        print(f"Stations extraction returned DataFrame with shape: {stations.shape}")
    else:
        print("Skipping stations extraction due to API issues")
