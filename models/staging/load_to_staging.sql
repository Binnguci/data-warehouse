LOAD DATA INFILE '/path/to/your_file.csv'
INTO TABLE data_staging.your_staging_table
FIELDS TERMINATED BY ',' -- Ký tự phân cách
OPTIONALLY ENCLOSED BY '"' -- Ký tự bao quanh
LINES TERMINATED BY '\n' -- Ký tự kết thúc dòng
IGNORE 1 ROWS -- Bỏ qua dòng header
(column1, column2, column3, ...)
SET config_id = (SELECT config_id FROM data_control.configs WHERE file_name = 'your_file.csv');