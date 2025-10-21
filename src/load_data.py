"""
Data Loading Module

This module handles loading cleaned data into PostgreSQL database:
- Load stations data to stations table
- Verify data was loaded correctly
"""

import pandas as pd
from sqlalchemy import create_engine, text
import psycopg2

# Database connection configuration
# Update these values with your actual database credentials
DATABASE_CONFIG = {
    'username': 'YOUR_USERNAME',
    'password': 'YOUR_PASSWORD', 
    'host': 'localhost',
    'port': '5432',
    'database': 'stations_db'
}

def get_connection_string():
    """Build PostgreSQL connection string"""
    return f"postgresql://{DATABASE_CONFIG['username']}:{DATABASE_CONFIG['password']}@{DATABASE_CONFIG['host']}:{DATABASE_CONFIG['port']}/{DATABASE_CONFIG['database']}"

def load_to_database(stations_df):
    """
    Load cleaned data into PostgreSQL database
    
    Args:
        stations_df (pandas.DataFrame): Cleaned stations data
    """
    print("💾 Loading data to PostgreSQL database...")
    
    # TODO: Create connection string using the function above
    connection_string = get_connection_string()
    
    try:
        # TODO: Create SQLAlchemy engine
        engine = create_engine(connection_string)
        
        # TODO: Load stations data
        # Use pandas to_sql method to insert data
        # Check if stations_df is not empty before loading
        if not stations_df.empty :
            stations_df.to_sql('stations', engine, if_exists='replace', index=False)
        else :
            print("stations_df est vide !")
        # 
        # Parameters explanation:
        # - 'stations': table name in database
        # - engine: database connection
        # - if_exists='replace': replace table if it exists (use 'append' to add to existing data)
        # - index=False: don't include pandas row index as a column
        
        # Print loading statistics
        if not stations_df.empty:
            print(f"✅ Loaded {len(stations_df)} stations to database")
        else:
            print("ℹ️  No stations data to load")
        
    except Exception as e:
        print(f"❌ Error loading data to database: {e}")
        print("💡 Make sure:")
        print("   - PostgreSQL is running")
        print("   - Database 'stations_db' exists") 
        print("   - Username and password are correct")
        print("   - Tables are created (run database_setup.sql)")

def verify_data():
    """
    Verify that data was loaded correctly by running some basic queries
    """
    print("🔍 Verifying data was loaded correctly...")
    
    connection_string = get_connection_string()
    
    try:
        # Create SQLAlchemy engine
        engine = create_engine(connection_string)
        
        # Count stations in database
        stations_count = pd.read_sql("SELECT COUNT(*) as count FROM stations", engine)
        print(f"📊 Stations in database: {stations_count.iloc[0]['count']}")
        
        # Show sample stations data
        sample_stations = pd.read_sql("SELECT name, address, available_bikes FROM stations LIMIT 3", engine)
        print("\n📋 Sample stations:")
        print(sample_stations.to_string(index=False))
        
    except Exception as e:
        print(f"❌ Error verifying data: {e}")

def run_sample_queries():
    """
    Run some interesting queries on the loaded data
    Students can use this to explore their data
    """
    print("📈 Running sample analysis queries...")
    
    connection_string = get_connection_string()
    
    try:
        engine = create_engine(connection_string)
        
        # Query 1: Available bikes
        print("\n🌍 Top 5 stations by number of available bikes:")
        available_query = """
        SELECT name, available_bikes
        FROM stations
        ORDER BY available_bikes DESC 
        LIMIT 5
        """
        available_results = pd.read_sql(available_query, engine)
        print(available_results.to_string(index=False))
        
        # Query 2: Show everything
        print("\n🌍 5 first stations:")
        available_query = """
        SELECT *
        FROM stations 
        LIMIT 5
        """
        available_results = pd.read_sql(available_query, engine)
        print(available_results.to_string(index=False))

    except Exception as e:
        print(f"❌ Error running sample queries: {e}")

def test_database_connection():
    """
    Test database connection without loading data
    Students can use this to debug connection issues
    """
    print("🔌 Testing database connection...")
    
    connection_string = get_connection_string()
    
    try:
        engine = create_engine(connection_string)
        
        # Try a simple query
        result = pd.read_sql("SELECT 1 as test", engine)
        
        if result.iloc[0]['test'] == 1:
            print("✅ Database connection successful!")
            
            # Check if our tables exist
            tables_query = """
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_name IN ('stations')
            ORDER BY table_name
            """
            tables = pd.read_sql(tables_query, engine)
            
            if len(tables) == 1:
                print("✅ Required table stations_db exist")
            else:
                print(f"⚠️  Found {len(tables)} tables, expected 1")
                print("💡 Run database_setup.sql to create tables")
            
            return True
        else:
            print("❌ Database connection test failed")
            return False
            
    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        print("💡 Check your connection settings in DATABASE_CONFIG")
        return False

if __name__ == "__main__":
    """Test the loading functions"""
    print("Testing database loading functions...\n")
    
    # Test database connection first
    if test_database_connection():
        print("\nDatabase connection OK. Ready for data loading!")
        
        sample_stations = pd.read_csv("ETL-Cambiar/data/stations_velo_cleaned.csv") 
        
        # Test loading (won't work until students implement it)
        load_to_database(sample_stations)
        run_sample_queries()
    else:
        print("Fix database connection before testing loading functions")
