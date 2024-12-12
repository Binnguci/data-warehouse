create database IF NOT EXISTS data_warehouse;
use data_warehouse; 

CREATE TABLE dw_phones_temp
(
		id                 INT NOT NULL PRIMARY KEY,
		brand_name         VARCHAR(60),
		model_name        VARCHAR(60) UNIQUE,
		os 				   VARCHAR(60),
		popularity         INT,
		best_price         DOUBLE,
		lowest_price       DOUBLE,
		highest_price      DOUBLE,
		sellers_amount     INT,
		screen_size        DOUBLE,
		memory_size        DOUBLE,
		battery_size       DOUBLE,
		release_date   	   DATE, 
		date_id            INT NOT NULL,
        is_active 			TINYINT DEFAULT 1,
        is_deleted 			TINYINT DEFAULT 0,
        dt_expired 			DATE DEFAULT '9999-12-31',
        dt_updated 			TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);