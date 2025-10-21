-- AirLife Database Setup
-- Run this script to create the necessary tables for the ETL pipeline

-- Drop tables if they exist (for clean restart)
DROP TABLE IF EXISTS stations;

-- Airports table - stores airport information from CSV data
CREATE TABLE stations (
    number INTEGER,
    contract_name VARCHAR(200) NOT NULL,
    name VARCHAR(200) NOT NULL,
    adress VARCHAR(100),
    banking BOOLEAN DEFAULT FALSE,
    bonus BOOLEAN DEFAULT FALSE,
    bike_stands INTEGER,
    available_bike_stands INTEGER,
    available_bikes INTEGER,
    status BOOLEAN DEFAULT FALSE,
    last_update INTEGER,
    lat DECIMAL (8,6),
    lng DECIMAL (8,6),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Verify tables were created
\dt

SELECT 'Database setup complete!' as status;
