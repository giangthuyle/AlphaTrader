//Create warehouse
CREATE OR REPLACE WAREHOUSE alpha_trader_warehouse
WITH WAREHOUSE_SIZE = 'SMALL'
AUTO_SUSPEND = 300
AUTO_RESUME = TRUE;

USE WAREHOUSE alpha_trader_warehouse;

//Create database
CREATE OR REPLACE DATABASE stock_data;
//Create schema
CREATE OR REPLACE SCHEMA stock_data.public;
//Create table
-- Create the company table
CREATE OR REPLACE TABLE stock_data.public.company (
    company_id NUMBER PRIMARY KEY,
    symbol STRING,
    name STRING,
    sector STRING
);

-- Create the time_dimension table with TINYINT types and CHECK constraints
CREATE OR REPLACE TABLE stock_data.public.time_dimension (
    date DATE PRIMARY KEY,
    day TINYINT,
    month TINYINT,
    quarter TINYINT,
    year SMALLINT 
);

-- Create the stock_price table with foreign keys
CREATE OR REPLACE TABLE stock_data.public.stock_price (
    id NUMBER PRIMARY KEY,
    company_id NUMBER,
    date DATE,
    open FLOAT,
    high FLOAT,
    low FLOAT,
    close FLOAT,
    volume FLOAT,
    FOREIGN KEY (company_id) REFERENCES stock_data.public.company(company_id),
    FOREIGN KEY (date) REFERENCES stock_data.public.time_dimension(date)
);

-- Count the number of rows in table stock_price
SELECT COUNT(*) FROM stock_data.public.stock_price;
