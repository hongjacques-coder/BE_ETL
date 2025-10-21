# BE ETL Pipeline 


Welcome to the BE ETL Pipeline! 


### Setup


1. **Fork this repository** to your GitHub account
2. **Clone your fork** locally:
  ```bash
  git clone https://github.com/YOUR_USERNAME/BE_ETL.git
  cd BE_ETL
  ```


3. **Install Python dependencies**:
  ```bash
  pip install -r requirements.txt
  ```


4. **Create PostgreSQL database**:
  ```bash
  # Connect to PostgreSQL
  psql -U your_username -d postgres
 
  # Create database
  CREATE DATABASE stations_db;
 
  # Exit and reconnect to new database
  \q
  psql -U your_username -d stations_db
 
  # Create tables
  \i database_setup.sql
  ```


### 3. Configure Database Connection


Edit the database configuration in `src/load_data.py`:


```python
DATABASE_CONFIG = {
   'username': 'your_username',      # Replace with your PostgreSQL username
   'password': 'your_password',      # Replace with your PostgreSQL password
   'host': 'localhost',
   'port': '5432',
   'database': 'stations_db'
}
```
