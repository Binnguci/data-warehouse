create database IF NOT EXISTS data_warehouse;
use data_warehouse; 

DELIMITER //

CREATE PROCEDURE MoveDataToDW(IN parameter_config_id INT)
BEGIN
    DECLARE var_file_log_id INT DEFAULT NULL;
    DECLARE var_file_path VARCHAR(255) DEFAULT NULL;

    -- 1. Lấy một hàng từ bảng file_logs có config_id và status = 'L_SE'
SELECT 
    file_log_id, file_path
INTO var_file_log_id , var_file_path 
FROM data_control.file_logs
WHERE
    config_id = parameter_config_id
        AND status = 'L_SE'
		LIMIT 1;
    IF file_log_id IS NULL THEN
        SIGNAL SQLSTATE '45000' 
        SET MESSAGE_TEXT = 'No matching rows in file_logs with the given config_id and status L_SE';
    END IF;

    -- 2. Load dữ liệu từ bảng staging vào bảng tạm (phones_temp)
    SET @staging_table = (SELECT tble_staging FROM data_control.configs WHERE config_id = parameter_config_id);
    SET @load_data_query = CONCAT(
        'INSERT INTO dw_phones_temp (brand_name, model_name, os, popularity, best_price, lowest_price, highest_price, 
        sellers_amount, screen_size, memory_size, battery_size, release_date , date_id)  SELECT * FROM ', @staging_table
    );

    PREPARE stmt FROM @load_data_query;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
    
    -- So sánh từng dòng trong bảng tạm với bảng DW chính thức
    -- Bảng chính thức: dw_phones
    -- So sánh theo businessKey hoặc naturalKey (giả định là "model_name" trong trường hợp này)
    
	-- 4. Cập nhật trạng thái các bản ghi đã cũ
    UPDATE dw_phones dw
    JOIN dw_phones_temp temp
    ON dw.model_name = temp.model_name
    SET dw.is_active = 0,
        dw.is_deleted = 1,
        dw.dt_expired = NOW(),
        dw.dt_updated = NOW()
    WHERE  temp.brand_name <> dw.brand_name 
        OR temp.os <> dw.os
        OR temp.popularity <> dw.popularity
        OR temp.best_price <> dw.best_price
        OR temp.lowest_price <> dw.lowest_price
        OR temp.highest_price <> dw.highest_price
        OR temp.sellers_amount <> dw.sellers_amount
        OR temp.screen_size <> dw.screen_size
        OR temp.memory_size <> dw.memory_size
        OR temp.battery_size <> dw.battery_size
        OR temp.release_date <> dw.release_date; 
       
	-- 5. Chèn nếu không có dòng trùng ID trong DW hoặc có thay đổi giữa các dòng trùng ID
    INSERT INTO dw.phones (
        brand_name, model_name, os, popularity, best_price, lowest_price, highest_price, 
        sellers_amount, screen_size, memory_size, battery_size, release_date , date_id , 
        is_active, is_deleted, dt_expired, dt_updated
    )
    SELECT
		temp.brand_name, temp.model_name, temp.os, temp.popularity, temp.best_price, 
        temp.lowest_price, temp.highest_price, temp.sellers_amount, temp.screen_size, temp.memory_size, 
        temp.battery_size, temp.release_date , temp.date_id , temp.is_active, temp.is_deleted, 
		temp.dt_expired, temp.dt_updated
    FROM dw_phones_temp temp
    LEFT JOIN dw_phones dw
    ON temp.model_name = dw.model_name 
    WHERE dw.model_name IS NULL 
		OR temp.brand_name <> dw.brand_name 
        OR temp.os <> dw.os
        OR temp.popularity <> dw.popularity
        OR temp.best_price <> dw.best_price
        OR temp.lowest_price <> dw.lowest_price
        OR temp.highest_price <> dw.highest_price
        OR temp.sellers_amount <> dw.sellers_amount
        OR temp.screen_size <> dw.screen_size
        OR temp.memory_size <> dw.memory_size
        OR tempconfigsPRIMARYdw_phonesdw_phones_temp.battery_size <> dw.battery_size
        OR temp.release_date <> dw.release_date; 
       
	-- 6. Cập nhật trạng thái trong bảng file_logs
    UPDATE file_logs
    SET status = 'L_CE', 
        update_at = NOW()
    WHERE file_log_id = var_file_log_id;

    -- 7. Xóa dữ liệu trong bảng staging và bảng tạm 
    SET @truncate_staging = CONCAT('TRUNCATE TABLE ', @staging_table);
    PREPARE stmt FROM @truncate_staging;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
     
     TRUNCATE TABLE dw_phones_temp;
END //

DELIMITER ;

CALL MoveDataToDW(12);

SET GLOBAL event_scheduler = ON;
SHOW VARIABLES LIKE 'event_scheduler';


DELIMITER //

CREATE PROCEDURE daily_etl_procedure()
BEGIN
    -- Kiểm tra trạng thái trong staging
    IF EXISTS (SELECT 1 FROM staging_table WHERE status = 'L_SE' AND timestamp >= NOW() - INTERVAL 1 DAY) THEN
        -- Gọi procedure MoveDataToDW
        CALL MoveDataToDW(1);

        -- Cập nhật trạng thái trong staging
        UPDATE staging_table 
        SET status = 'L_CE' 
        WHERE status = 'L_SE';
    END IF;
END //

DELIMITER ;

DELIMITER //

CREATE EVENT IF NOT EXISTS daily_etl_event
ON SCHEDULE EVERY 1 DAY
STARTS CURRENT_DATE + INTERVAL 21 HOUR
DO
    CALL daily_etl_procedure();

DELIMITER ;

SHOW EVENTS FROM data_warehouse;


