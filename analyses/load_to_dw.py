# -*- coding: utf-8 -*-
"""Untitled0.ipynb

Automatically generated by Colab.

Original file is located at
    https://colab.research.google.com/drive/1xrm7W0K7rV7tj7OgHVmmcxvH13qS7HLW
"""

import mysql.connector
from mysql.connector import Error
import logging
from colorlog import ColoredFormatter
from datetime import datetime

# Cấu hình logging
formatter = ColoredFormatter(
    "%(log_color)s%(asctime)s - %(levelname)s - %(message)s",
    log_colors={
        "DEBUG": "purple",
        "INFO": "green",
        "WARNING": "yellow",
        "ERROR": "red",
        "CRITICAL": "red,bg_white",
    },
)

handler = logging.StreamHandler()
handler.setFormatter(formatter)

logger = logging.getLogger()
logger.addHandler(handler)
logger.setLevel(logging.DEBUG)

# Kết nối với data_control
def connect_to_data_control():
    try:
        connection = mysql.connector.connect(
            host='127.0.0.1',
            port=3306,
            user='root',
            password='tran1505',
            database='data_control',
            allow_local_infile=True
        )
        if connection.is_connected():
            logger.info("Connection to data_control successful!")
            return connection
    except Error as e:
        logger.error(f"Error while connecting to data_control: {e}")
        return None

# Kết nối với data_staging
def connect_to_data_staging():
    try:
        connection = mysql.connector.connect(
            host='127.0.0.1',
            port=3306,
            user='root',
            password='tran1505',
            database='data_staging',
            allow_local_infile=True
        )
        if connection.is_connected():
            logger.info("Connection to data_staging successful!")
            return connection
    except Error as e:
        logger.error(f"Error while connecting to data_staging: {e}")
        return None

# Kết nối với data_warehouse
def connect_to_database():
    try:
        connection = mysql.connector.connect(
            host='127.0.0.1',
	    port=3306,
            database='data_warehouse',
            user='root',
            password='tran1505',
	    allow_local_infile=True
        )
        if connection.is_connected():
            print("Connected to the database")
            return connection
    except Error as e:
        print("Error while connecting to database:", e)
        return None

def execute_query(connection, query, params=None):
    cursor = connection.cursor()
    try:
        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)
        connection.commit()
    except Error as e:
        print(f"Error executing query: {e}")
        connection.rollback()

def call_procedure(connection, procedure_name, params=None):
    """Call a stored procedure."""
    cursor = connection.cursor()
    try:
        if params:
            cursor.callproc(procedure_name, params)
        else:
            cursor.callproc(procedure_name)
        connection.commit()
        print(f"Procedure {procedure_name} executed successfully.")
    except Error as e:
        print(f"Error calling procedure {procedure_name}: {e}")
        connection.rollback()

def main():

        if connection.is_connected():
            print("Connected to MySQL database.")

            # Create tables
            create_dw_phones_temp_table = """
            CREATE TABLE IF NOT EXISTS dw_phones_temp (
                id INT NOT NULL PRIMARY KEY,
                brand_name VARCHAR(60),
                model_name VARCHAR(60) UNIQUE,
                os VARCHAR(60),
                popularity INT,
                best_price DOUBLE,
                lowest_price DOUBLE,
                highest_price DOUBLE,
                sellers_amount INT,
                screen_size DOUBLE,
                memory_size DOUBLE,
                battery_size DOUBLE,
                release_date DATE,
                date_id INT NOT NULL,
                is_active TINYINT DEFAULT 1,
                is_deleted TINYINT DEFAULT 0,
                dt_expired DATE DEFAULT '9999-12-31',
                dt_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """

            create_dw_phones_table = """
            CREATE TABLE IF NOT EXISTS dw_phones (
                sk INT AUTO_INCREMENT PRIMARY KEY,
                brand_name VARCHAR(60),
                model_name VARCHAR(60) UNIQUE,
                os VARCHAR(60),
                popularity INT,
                best_price DOUBLE,
                lowest_price DOUBLE,
                highest_price DOUBLE,
                sellers_amount INT,
                screen_size DOUBLE,
                memory_size DOUBLE,
                battery_size DOUBLE,
                release_date DATE,
                date_id INT NOT NULL,
                is_active TINYINT,
                is_deleted TINYINT,
                dt_expired DATE,
                dt_updated TIMESTAMP
            );
            """

            execute_query(connection, create_dw_phones_temp_table)
            execute_query(connection, create_dw_phones_table)

            # Enable event scheduler
            enable_event_scheduler = "SET GLOBAL event_scheduler = ON;"
            execute_query(connection, enable_event_scheduler)

            # Ensure daily_etl_procedure exists
            create_daily_etl_procedure = """
            CREATE PROCEDURE daily_etl_procedure()
            BEGIN
                IF EXISTS (SELECT 1 FROM staging_table WHERE status = 'L_SE' AND timestamp >= NOW() - INTERVAL 1 DAY) THEN
                    CALL MoveDataToDW(1);
                    UPDATE staging_table SET status = 'L_CE' WHERE status = 'L_SE';
                END IF;
            END;
            """
            ensure_procedure_exists(connection, "daily_etl_procedure", create_daily_etl_procedure)

            # Create daily_etl_event
            create_daily_etl_event = """
            CREATE EVENT IF NOT EXISTS daily_etl_event
            ON SCHEDULE EVERY 1 DAY
            STARTS CURRENT_DATE + INTERVAL 21 HOUR
            DO
                CALL daily_etl_procedure();
            """
            execute_query(connection, create_daily_etl_event)

            # Show events
            show_events_query = "SHOW EVENTS FROM data_warehouse;"
            execute_query(connection, show_events_query)

    except Error as e:
        print(f"Error while connecting to MySQL: {e}")
    finally:
        if connection.is_connected():
            connection.close()
            print("MySQL connection is closed.")

if __name__ == "__main__":
    main()