create database IF NOT EXISTS data_warehouse;
use data_warehouse; 

CREATE TABLE dw_phones
(
		sk					INT AUTO_INCREMENT PRIMARY KEY,
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
		is_active 			TINYINT,
		is_deleted 			TINYINT,
		dt_expired 			DATE,
		dt_updated 			TIMESTAMP
);
