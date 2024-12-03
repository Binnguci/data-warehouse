import csv
import logging
from colorlog import ColoredFormatter
import mysql.connector
from mysql.connector import Error
import os
from datetime import datetime
import smtplib
from config.email_config import EMAIL_HOST, EMAIL_PORT, EMAIL_USE_TLS, EMAIL_USERNAME, EMAIL_PASSWORD, EMAIL_RECIPIENT

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
            password='binnguci@220121',
            database='data_control',
            allow_local_infile = True
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
            password='binnguci@220121',
            database='data_staging',
            allow_local_infile = True
        )
        if connection.is_connected():
            logger.info("Connection to data_staging successful!")
            return connection
    except Error as e:
        logger.error(f"Error while connecting to data_staging: {e}")
        return None


# Lấy các file cần load từ data_control
def get_pending_files(connection):
    query = """
    SELECT config_id, file_name, tble_staging 
    FROM configs
    WHERE config_id NOT IN (
        SELECT config_id FROM file_logs WHERE status = 'L_SE'
    )
    """
    cursor = connection.cursor(dictionary=True)
    cursor.execute(query)
    result = cursor.fetchall()
    cursor.close()
    return result


# Load dữ liệu từ CSV vào data_staging
def load_data_to_staging(connection, file_path, staging_table):
    cursor = None  # Khởi tạo cursor để tránh lỗi
    try:
        query = f"""
        LOAD DATA LOCAL INFILE '{file_path}'
        INTO TABLE {staging_table}
        FIELDS TERMINATED BY ',' 
        OPTIONALLY ENCLOSED BY '"'
        LINES TERMINATED BY '\\n'
        IGNORE 1 ROWS;
        """
        cursor = connection.cursor()
        cursor.execute(query)
        connection.commit()
        logger.info(f"Data loaded successfully to {staging_table}")
        return True
    except Error as e:
        logger.error(f"Error while loading data: {e}")
        connection.rollback()
        return False
    finally:
        if cursor:
            cursor.close()

def truncate_table(connection, table_name):
    cursor = None
    try:
        query = f"TRUNCATE TABLE {table_name}"
        cursor = connection.cursor()
        cursor.execute(query)
        connection.commit()
    except Exception as e:
        logger.error(f"Error while truncating table {table_name}: {e}", exc_info=True)
        if connection:
            connection.rollback()
    finally:
        if cursor:
            cursor.close()


def log_file_status(connection, config_id, file_path, status, file_size):
    cursor = None

    try:
        # Đếm số lượng bản ghi trong file
        with open(file_path, 'r') as file:
            reader = csv.reader(file)
            row_count = sum(1 for row in reader)

        query = """
        INSERT INTO file_logs (config_id, time, file_path, status, start_time, end_time, file_size, count, update_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        now = datetime.now()
        cursor = connection.cursor()
        cursor.execute(query, (config_id, now, file_path, status, now, now, file_size, row_count, now))
        connection.commit()
        logger.info(f"Log inserted for file: {file_path} with {row_count} rows")
    except Exception as e:
        logger.error(f"Error while logging file status: {e}")
        if connection:
            connection.rollback()
    finally:
        if cursor:
            cursor.close()

def send_email(subject, message):
    server = None
    try:
        server = smtplib.SMTP(EMAIL_HOST, EMAIL_PORT)
        if EMAIL_USE_TLS:
            server.starttls()
        server.login(EMAIL_USERNAME, EMAIL_PASSWORD)
        server.sendmail(
            from_addr=EMAIL_USERNAME,
            to_addrs=[EMAIL_RECIPIENT],
            msg=f"Subject: {subject}\n\n{message}"
        )
        logger.info("Email sent successfully!")
    except Exception as e:
        logger.error(f"Failed to send email: {e}")
    finally:
        if server:
            server.quit()


def main():
    # Kết nối tới cả 2 cơ sở dữ liệu
    connection_data_control = connect_to_data_control()
    connection_data_staging = connect_to_data_staging()

    if not connection_data_control or not connection_data_staging:
        return

    try:
        # 1. Lấy danh sách file cần xử lý
        pending_files = get_pending_files(connection_data_control)

        if not pending_files:
            send_email(
                subject="No Pending Files",
                message="There are no pending files to process. The program will now exit."
            )
            logger.info("No pending files found. Exiting program.")
            return

        # 2. Duyệt qua từng file và thực hiện load dữ liệu
        for file in pending_files:
            config_id = file['config_id']
            file_name = file['file_name']
            staging_table = file['tble_staging']
            file_path = os.path.join('/home/binnguci/Source/data-warehouse/seeds/', file_name)
            file_size = os.path.getsize(file_path)
            logger.info(f"Processing file: {file_name}")

            if not os.path.exists(file_path):
                logger.warning(f"File not found: {file_path}")
                log_file_status(connection_data_control, config_id, file_path, 'L_FE', file_size)
                continue

            retry_count = 0
            success = False

            while retry_count < 3 and not success:
                truncate_table(connection_data_staging, staging_table)
                success = load_data_to_staging(connection_data_staging, file_path, staging_table)
                retry_count += 1

            if success:
                log_file_status(connection_data_control, config_id, file_path, 'L_SE', file_size)
                logger.info(f"File processed successfully: {file_name}")
            else:
                logger.error(f"Failed to process file after {retry_count} attempts: {file_name}")
                log_file_status(connection_data_control, config_id, file_path, 'L_FE', file_size)
                send_email(
                    subject=f"Error loading file {file_name}",
                    message=f"File {file_name} could not be loaded to table {staging_table} after 3 attempts."
                )
    finally:
        if connection_data_control:
            connection_data_control.close()
        if connection_data_staging:
            connection_data_staging.close()


if __name__ == "__main__":
    main()
