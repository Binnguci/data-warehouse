{% macro load_csv_to_staging(file_path, staging_table, config_id) %}
    {% set log_count = run_query("SELECT COUNT(*) FROM data_control.file_logs WHERE file_path = '{{ file_path }}' AND status = 'L_SE'") %}

    {% if log_count == 0 %}
        -- Load dữ liệu vào bảng staging
        LOAD DATA INFILE '{{ file_path }}'
        INTO TABLE {{ staging_table }}
        FIELDS TERMINATED BY ','
        OPTIONALLY ENCLOSED BY '"'
        LINES TERMINATED BY '\n'
        IGNORE 1 ROWS;

        -- Ghi log
        INSERT INTO data_control.file_logs (config_id, time, file_path, count, start_time, end_time, file_size, status)
        VALUES (
            {{ config_id }},
            NOW(),
            '{{ file_path }}',
            (SELECT COUNT(*) FROM {{ staging_table }}),
            NOW(),
            NOW() + INTERVAL 2 MINUTE,
            (SELECT ROUND(SUM(data_length + index_length) / 1024 / 1024, 2)
             FROM information_schema.TABLES
             WHERE table_schema = 'data_staging' AND table_name = '{{ staging_table }}'),
            'L_SE'
        );
    {% else %}
        -- File đã được xử lý
        {{ log("File {{ file_path }} đã được xử lý trước đó.") }}
    {% endif %}
{% endmacro %}
