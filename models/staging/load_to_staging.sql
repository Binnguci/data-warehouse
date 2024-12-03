LOAD DATA INFILE '/path/to/your_file.csv'
INTO TABLE data_staging.your_staging_table
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(column1, column2, column3, ...)
SET config_id = (SELECT config_id FROM data_control.configs WHERE file_name = 'your_file.csv');