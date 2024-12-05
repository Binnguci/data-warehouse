import logging
from colorlog import ColoredFormatter
import mysql.connector
from mysql.connector import Error
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

# Kết nối với data_staging
def connect_to_data_staging():
    try:
        connection = mysql.connector.connect(
            host='127.0.0.1',
            port=3306,
            user='root',
            password='Trandat123',
            database='data_staging',
            allow_local_infile=True
        )
        if connection.is_connected():
            logger.info("Connection to data_staging successful!")
            return connection
    except Error as e:
        logger.error(f"Error while connecting to data_staging: {e}")
        return None

# Kết nối với data_control
def connect_to_data_control():
    try:
        connection = mysql.connector.connect(
            host='127.0.0.1',
            port=3306,
            user='root',
            password='Trandat123',
            database='data_control',
            allow_local_infile=True
        )
        if connection.is_connected():
            logger.info("Connection to data_control successful!")
            return connection
    except Error as e:
        logger.error(f"Error while connecting to data_control: {e}")
        return None

def log_transformation_status(connection, config_id, status, start_time, end_time, count):
    cursor = None
    try:
        # Danh sách các giá trị hợp lệ cho cột 'status'
        valid_statuses = ['C_RE', 'C_E', 'C_SE', 'C_FE', 'L_RE', 'L_P', 'L_SE', 'L_FE', 'L_CE']
        
        # Kiểm tra xem 'status' có hợp lệ hay không
        if status not in valid_statuses:
            logger.error(f"Invalid status value: {status}")
            return  # Nếu không hợp lệ, thoát ra mà không chèn dữ liệu
        
        query = """
        INSERT INTO file_logs (config_id, status, start_time, end_time, count)
        VALUES (%s, %s, %s, %s, %s)
        """
        now = datetime.now()  
        cursor = connection.cursor()
        cursor.execute(query, (config_id, status, start_time, end_time, count))
        connection.commit()
        logger.info(f"Transformation log inserted: config_id={config_id}, status={status}, count={count}")
    except Error as e:
        logger.error(f"Error while logging transformation status: {e}")
        if connection:
            connection.rollback()
    finally:
        if cursor:
            cursor.close()




def get_data_from_staging(connection, staging_table):
    try:
        query = f"SELECT * FROM {staging_table}"
        cursor = connection.cursor(dictionary=True)
        cursor.execute(query)
        result = cursor.fetchall()
        cursor.close()
        logger.info(f"Fetched {len(result)} rows from {staging_table}")
        return result
    except Error as e:
        logger.error(f"Error fetching data from {staging_table}: {e}")
        return []
    

def convert_date(date_str):
    try:
        # Loại bỏ các ký tự dư thừa như \r, \n, và khoảng trắng
        date_str = date_str.strip()
        
        # Kiểm tra nếu ngày tháng có dấu '-'
        if '-' in date_str:
            parts = date_str.split('-')
            if len(parts) == 2 and parts[0].isdigit() and parts[1].isdigit():
                month = parts[0].zfill(2)  # Đảm bảo tháng có 2 chữ số
                year = parts[1]
                return f"{year}-{month}-01"
            else:
                logger.error(f"Invalid date format: {date_str}")
                return "2020-10-01"  # Trả về mặc định nếu không hợp lệ
        else:
            logger.error(f"Invalid date format: {date_str}")
            return "2020-10-01"  # Trả về mặc định nếu không tìm thấy dấu '-'
    except Exception as e:
        logger.error(f"Error converting date: {date_str}, Error: {e}")
        return "2020-10-01"

def clean_data(data):
    cleaned_data = []
    for row in data:
        row['field_1'] = row['field_1'].strip() if row['field_1'] else 'OK'
        row['field_2'] = row['field_2'].strip() if row['field_2'] else 'OK'
        row['field_3'] = row['field_3'].strip() if row['field_3'] else 'OK'
        row['field_4'] = row['field_4'].strip() if row['field_4'] else 'OK'
        row['field_5'] = float(row['field_5']) if row['field_5'] and row['field_5'] not in [None, ''] else 0.0
        row['field_6'] = float(row['field_6']) if row['field_6'] and row['field_6'] not in [None, ''] else 0.0
        row['field_7'] = float(row['field_7']) if row['field_7'] and row['field_7'] not in [None, ''] else 0.0
        row['field_8'] = float(row['field_8']) if row['field_8'] and row['field_8'] not in [None, ''] else 0.0
        row['field_9'] = int(row['field_9']) if row['field_9'] and row['field_9'] not in [None, ''] else 0
        row['field_10'] = float(row['field_10']) if row['field_10'] and row['field_10'] not in [None, ''] else 0.0
        row['field_11'] = float(row['field_11']) if row['field_11'] and row['field_11'] not in [None, ''] else 0.0
        row['field_12'] = float(row['field_12']) if row['field_12'] and row['field_12'] not in [None, ''] else 0.0
        
        # Kiểm tra và xử lý ngày phát hành (field_13)
        if row.get('field_13'):
            release_date = convert_date(row['field_13'])
            row['release_date'] = release_date

        else:
            row['release_date'] = "2020-10-01"  # Set default if no release date
        
        cleaned_data.append(row)
    return cleaned_data

def check_data_in_fact_table(connection, fact_table):
    try:
        query = f"SELECT * FROM {fact_table} LIMIT 10"  # Lấy 10 dòng dữ liệu đầu tiên
        cursor = connection.cursor(dictionary=True)
        cursor.execute(query)
        result = cursor.fetchall()
        if result:
            logger.info(f"Preview of data from {fact_table}:")
            for row in result:
                logger.info(row)
        else:
            logger.info(f"No data found in {fact_table}.")
        cursor.close()
    except Error as e:
        logger.error(f"Error fetching data from {fact_table}: {e}")

def load_data_into_fact_table(connection_staging, connection_control, staging_table, fact_table):
    try:
        cursor_staging = connection_staging.cursor(dictionary=True)
        cursor_control = connection_control.cursor()

        # Lấy dữ liệu từ bảng tạm trong data_staging
        rows = get_data_from_staging(connection_staging, staging_table)

        if not rows:
            logger.info(f"No data to load from {staging_table}.")
            return

        # Làm sạch dữ liệu
        cleaned_data = clean_data(rows)

        if not cleaned_data:
            logger.info("No valid data after cleaning.")
            return

        # Đếm số lượng bản ghi sau khi làm sạch
        rows_transformed = len(cleaned_data)
        logger.info(f"Loading {rows_transformed} cleaned records from {staging_table} to {fact_table}.")

        # Bắt đầu quá trình ghi dữ liệu vào bảng chính thức
        start_time = datetime.now()
        for row in cleaned_data:
            date_id = row.get('date_id', 20201023)
            if 'release_date' not in row or not row['release_date']:
                logger.error(f"Missing or invalid release_date for row: {row}")
                continue
            
            try:
                release_date = datetime.strptime(row['release_date'], '%Y-%m-%d').date()
            except ValueError:
                logger.error(f"Invalid release_date format for row: {row}")
                continue
            
            query_insert = f"""
            INSERT INTO {fact_table} (
                brand_name, model_name, os, popularity, best_price, lowest_price, highest_price,
                sellers_amount, screen_size, memory_size, battery_size, release_date, date_id
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            cursor_staging.execute(query_insert, (
                row['field_2'], row['field_3'], row['field_4'], row['field_5'], row['field_6'],
                row['field_7'], row['field_8'], row['field_9'], row['field_10'], row['field_11'],
                row['field_12'], release_date, date_id
            ))

        # Commit dữ liệu vào data_staging (bảng chính thức trong data_staging)
        connection_staging.commit()
        end_time = datetime.now()
        log_transformation_status(connection_control, 1, 'L_SE', start_time, end_time, rows_transformed)
        logger.info(f"Data loaded into {fact_table} successfully, {rows_transformed} rows inserted.")

        # Kiểm tra dữ liệu trong bảng fact sau khi load
        check_data_in_fact_table(connection_staging, fact_table)

    except Error as e:
        logger.error(f"Error during data loading: {e}")
        if connection_staging:
            connection_staging.rollback()
        log_transformation_status(connection_control, 1, 'L_FE', datetime.now(), datetime.now(), len(cleaned_data))

    finally:
        cursor_staging.close()
        cursor_control.close()


def main():
    # Kết nối đến các cơ sở dữ liệu
    connection_staging = connect_to_data_staging()
    connection_control = connect_to_data_control()

    if not connection_staging or not connection_control:
        return

    try:
         # Chuyển dữ liệu từ bảng tạm sang bảng chính thức cho TGDD
        load_data_into_fact_table(connection_staging, connection_control, "staging_phone_tgdd_temp", "fact_phone_tgdd_daily")

        # Chuyển dữ liệu từ bảng tạm sang bảng chính thức cho FPTShop
        load_data_into_fact_table(connection_staging, connection_control, "staging_phone_fptshop_temp", "fact_phone_fptshop_daily")

        # Chuyển dữ liệu từ bảng tạm sang bảng chính thức cho Cellphones
        load_data_into_fact_table(connection_staging, connection_control, "staging_phone_cellphones_temp", "fact_phone_cellphones_daily")

    finally:
        if connection_staging:
            connection_staging.close()
        if connection_control:
            connection_control.close()


if __name__ == "__main__":
    main()
