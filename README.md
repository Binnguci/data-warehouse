# Data Warehouse Staging Loader

This script is designed to load data from CSV files into staging tables in a MySQL database. It uses the `LOAD DATA LOCAL INFILE` command to efficiently transfer data.

---

## Features

- Load data from CSV files to MySQL staging tables.
- Flexible configuration for file paths and table names.
- Error handling with logging for debugging.

---

## Requirements

- Python 3.x
- MySQL database
- Required Python libraries (see `requirements.txt`):
  - `mysql-connector-python`
  - `logging`

---

## Usage

1. **Set Up Environment**:
   - Create a virtual environment:
     ```bash
     python3 -m venv .venv
     source .venv/bin/activate
     ```
   - Install dependencies:
     ```bash
     pip install -r requirements.txt
     ```

2. **Run the Script**:
   - Use the following command to execute the script:
     ```bash
     python analyses/load_to_staging.py
     ```

3. **File Processing**:
   - Ensure the CSV files are available in the specified directory.
   - The script processes files in the following order:
     1. `tgdd_data.csv`
     2. `cellphones_data.csv`
     3. `fptshop_data.csv`

4. **Configuration**:
   - File paths and table names can be adjusted in the code logic:
     ```python
     load_data_to_staging(connection, '/path/to/file.csv', 'table_name')
     ```

---

## Function Details

### `load_data_to_staging`

This function loads a CSV file into a specified staging table.

#### Parameters:
- `connection`: MySQL database connection object.
- `file_path`: Path to the CSV file.
- `staging_table`: Name of the target staging table.

#### Example:
```python
load_data_to_staging(connection, '/path/to/tgdd_data.csv', 'staging_tgdd')
```
#### SQL Logic:
```sql
LOAD DATA LOCAL INFILE '/path/to/file.csv'
INTO TABLE staging_table
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;
```
### DBT Configuration:
The DBT profile is configured in the .dbt/profiles.yml file:
```yaml
data_warehouse_profile:
  outputs:
    dev:
      type: mysql
      host: 127.0.0.1
      port: 3306
      user: <your_username>
      password: <your_password>
      schema: data_staging
      database: data_staging
  target: dev
```
### DBT Model:
The DBT model is defined in the `models` directory:
```sql
{{ config(
    materialized='table',
    schema='staging'
) }}

SELECT
    ROW_NUMBER() OVER (ORDER BY field_1) AS id,
    field_1 AS name,
    CAST(field_2 AS INT) AS price,
    CAST(field_3 AS DATE) AS release_date,
    CAST(field_4 AS DOUBLE) AS screen_size,
    field_5 AS gpu,
    field_6 AS front_camera,
    field_7 AS rear_camera,
    field_8 AS color,
    field_9 AS security,
    field_10 AS bluetooth,
    field_11 AS battery_capacity,
    field_12 AS communication_port,
    field_13 AS brand,
    field_14 AS ram,
    field_15 AS cpu,
    field_16 AS operating_system,
    field_17 AS memory,
    field_18 AS chip,
    field_19 AS image,
    field_20 AS date
FROM {{ ref('staging_phone_tgdd_temp') }}
```
### .gitignore:
The `.gitignore` file is used to exclude sensitive information:
```
target/
dbt_packages/
../logs/
../.env
.env
.venv
.idea
.dbt

```