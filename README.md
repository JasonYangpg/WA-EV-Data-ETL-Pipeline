# Washington EV Data ETL Pipeline

This repository implements an ETL pipeline to process and load electric and alternative fuel vehicle registration data from Washington State into a data warehouse. The design follows dimensional modeling best practices and is built using Python and Pandas.

## Project Objectives

- Extract and explore electric vehicle (EV) registration data  
- Clean and transform key features  
- Design a dimensional model (star schema) centered around registration behavior  
- Generate surrogate keys and link dimensions to a fact table  
- Load the final tables into a SQL Server data warehouse  

## Star Schema Design

The central fact table `fact_ev` represents individual **vehicle registration events**. Each fact record is linked to multiple dimensions via surrogate keys.

### Fact Table: `fact_ev`

| Field              | Description                                      |
|--------------------|--------------------------------------------------|
| `ev_id`          | Surrogate key                                    |
| `vin`              | Vehicle Identification Number                    |
| `dol_vehicle_id`   | WA Department of Licensing vehicle record ID     |
| `vehicle_model_id` | FK → `dim_vehicle_model`                         |
| `location_id`      | FK → `dim_location`                              |
| `utility_id`       | FK → `dim_utility`                               |
| `policy_id`        | FK → `dim_policy_eligibility`                    |
| `district_id`      | FK → `dim_district`                              |

### Dimensions

#### `dim_vehicle_model`
Describes the structural and pricing aspects of each vehicle model.

| Field           | Description               |
|----------------|---------------------------|
| `vehicle_model_id` | Surrogate key          |
| `make`          | Manufacturer              |
| `model`         | Model name                |
| `model_year`    | Year of manufacture       |
| `ev_type`       | Electric vehicle type     |
| `electric_range`| Electric range in miles   |
| `base_msrp`     | Suggested retail price    |

#### `dim_location`
Describes the administrative and spatial registration location.

| Field         | Description             |
|---------------|--------------------------|
| `location_id` | Surrogate key            |
| `state`       | State (e.g., WA)         |
| `county`      | County name              |
| `city`        | City                     |
| `postal_code` | Postal code              |
| `latitude`    | Latitude (parsed)        |
| `longitude`   | Longitude (parsed)       |

#### `dim_utility`
Describes the electric utility company responsible for the location.

| Field         | Description              |
|---------------|---------------------------|
| `utility_id`  | Surrogate key             |
| `electric_utility`| Name of electric utility  |

#### `dim_policy_eligibility`
Captures policy status for clean alternative fuel vehicle (CAFV) eligibility.

| Field         | Description                              |
|---------------|-------------------------------------------|
| `policy_id`   | Surrogate key                             |
| `cafv_eligibility` | Eligibility status (e.g., 1: Yes / 2: No / 3: Unknown)|

#### `dim_district`
Captures the regional legislative and census designations.

| Field               | Description                        |
|---------------------|------------------------------------|
| `district_id`       | Surrogate key                      |
| `legislative_district` | Washington legislative district |
| `census_tract`      | 2020 census tract code             |

## ETL Process

The ETL pipeline performs the following steps:

1. **Extract & Explore**  
   - The source dataset is read from a CSV file into a Pandas DataFrame.
   - As per the assignment requirement, only records where the `State` equals `'WA'` (Washington) are retained. Approximately 583 records from other states (e.g., AK, BC, WY) are excluded during this step.
   - The script explores the dataset structure: 250,076 rows and 17 columns, examining column names and data types.
   - Numerical features such as `Electric Range` and `Base MSRP` are analyzed for their distributions.
   - The central tendency of the `Model Year` is evaluated using descriptive statistics.
   - The dispersion of categorical features such as `Electric Vehicle Type` and `CAFV Eligibility` is explored to understand value frequency and variability.
   
2. **Clean & Transform**  
   - Column names are renamed and standardized to follow a consistent naming convention (e.g., lowercase with underscores).
   - Missing values in numerical features (`electric_range`, `base_msrp`) are addressed:
     - Zero values are treated as missing.
     - Values are imputed using the median within groups defined by `make` and `model`, preserving contextual relevance.
   - Categorical fields such as `ev_type` and `cafv_eligibility` are encoded to support efficient storage and lookup.
   - The `vehicle_location` field is split into two separate numeric columns: `latitude` and `longitude`.
   - Duplicate records are removed when constructing dimension tables, and surrogate keys are generated to ensure referential integrity and enable dimensional modeling.

3. **Load**  
   - Creates fact and dimension DataFrames  
   - Loads each table into SQL Server using SQLAlchemy and `to_sql()`

## How to Run

> **Environment**  
> This project was developed and tested in **Google Colab**, a cloud-based Python notebook environment.  
> It is compatible with **Python 3.10+** and requires the following packages:

### 1. Install Dependencies

```bash
pip install pandas numpy sqlalchemy configparser pyodbc
```

### 2. Update your database configuration in the `db_config.ini` file (see below).

#### Configuration File (`db_config.ini`)
  
The database connection settings are stored in a separate configuration file for better security and maintainability.

#### Sample: `db_config.ini`
```ini
[sqlserver]
server = your_sql_server
database = your_database
username = your_username
password = your_password
driver = ODBC Driver 17 for SQL Server
```

Place this file in the same directory as your Python scripts.

> **Important**: Add `db_config.ini` to your `.gitignore` to avoid accidentally uploading sensitive credentials.

### 3. Run the ETL script:

```bash
python etl_ev_population.py
```

## Load to SQL Server (Code Snippet)

```python
from configparser import ConfigParser
from sqlalchemy import create_engine

config = ConfigParser()
config.read('db_config.ini')
db = config['sqlserver']

conn_str = f"mssql+pyodbc://{db['username']}:{db['password']}@{db['server']}/{db['database']}?driver={db['driver']}"
engine = create_engine(conn_str)

fact_ev.to_sql('fact_ev', engine, if_exists='replace', index=False)
```

> Make sure ODBC Driver 17+ for SQL Server is installed.

## Why This Design?

- **Behavior-centric Fact Table**: fact_ev captures each registration as an event, enabling trend analysis over time
- **Surrogate Keys**: All dimension tables use surrogate keys (id) to ensure data integrity and decouple from source formats 
- **Modular Dimensions**: Vehicle, location, policy, and specification information are modularized for reusability and extensibility
- **Spatial & Policy Awareness**: Dimensions such as dim_location (with city, county, postal code, state, lat/lon) and dim_legislative_district/dim_census_tract support policy, spatial, and demographic integration  
- **Scalable**: Supports future extension with time-based snapshots, additional fact tables (e.g., charging sessions), and cross-state datasets
 
## Recommended Project Structure

```
WA-EV-Warehouse/
├── etl_ev_population.py       # Main ETL script
├── db_config.ini              # Local config file (sample)
├── README.md
├── Electric_Vehicle_Population_Data.csv                      # Source CSV
└── output/                    # Processed tables (optional)
```

## Future Enhancements

- Integrate time dimension (registration_date) and possibly build a dedicated dim_date  
- Enhance dim_location with hierarchical levels (e.g., FSA, province/state, region)  
- Incorporate SCD (Slowly Changing Dimensions) strategy if source data includes historical changes  
- Create Power BI or Tableau dashboards for analytics
- Explore integration with real-time streaming vehicle event data (e.g., charging or maintenance)
  
> **Note on Data Scope**:  
> Although the dataset is described as containing vehicles registered in Washington State, it includes a small portion of records from other states (e.g., AK, BC, WY).  
> These entries are filtered out during extraction based on the `state` field.

## Data Source

Note: The original data files have been sanitized to remove any sensitive content. Only column headers are retained for reference and structure demonstration.

## Author

This project was developed as part of a technical assessment for a Lead Solutions Architect role. For questions, contact Jason Yang.
