SELECT *
FROM file_logs
WHERE file_path = '/path/to/your_file.csv' AND status = 'L_SE';

---
INSERT INTO file_logs (config_id, time, file_path, count, start_time, end_time, file_size, status)
VALUES (
    (SELECT config_id FROM data_control.configs WHERE file_name = 'your_file.csv'),
    NOW(),
    '/path/to/your_file.csv',
    (SELECT COUNT(*) FROM data_staging.your_staging_table),
    NOW(),
    NOW() + INTERVAL 2 MINUTE, -- Thời gian xử lý (giả định 2 phút)
    (SELECT ROUND(SUM(data_length + index_length) / 1024 / 1024, 2)
     FROM information_schema.TABLES
     WHERE table_schema = 'data_staging' AND table_name = 'your_staging_table'),
    'L_SE'
);
