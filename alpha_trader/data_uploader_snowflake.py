import snowflake.connector
import pandas as pd
from snowflake import SnowflakeGenerator
import os
from dotenv import load_dotenv

load_dotenv()

def get_date_components(date):
    date = date.split('-')
    day = date[2]
    month = date[1]
    year = date[0]
    quarter = (int(month) - 1) // 3 + 1
    return day, month, quarter, year



data = pd.read_csv('NVDA_stock_data.csv')
conn = snowflake.connector.connect(
    user=os.getenv('SNOWFLAKE_USER'),
    password=os.getenv('SNOWFLAKE_PASSWORD'),
    account=os.getenv('SNOWFLAKE_ACCOUNT'),
    warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
)

cur = conn.cursor()

gen = SnowflakeGenerator(42)
company_id = next(gen)
symbol = 'NVDA'
name = 'Nvidia Corporation'
sector = 'Technology'

#Insert into company table
insert_company_table = f"INSERT INTO stock_data.public.company (company_id, symbol, name, sector) VALUES ({company_id},'{symbol}','{name}','{sector}');"

cur.execute(insert_company_table)

#Insert into time_dimension table
insert_time_table = "INSERT INTO stock_data.public.time_dimension (date, day, month, quarter, year) VALUES "
values_time_table = ", ".join(
    [
        f"('{row['date']}', {get_date_components(row['date'])[0]}, {get_date_components(row['date'])[1]} ,{get_date_components(row['date'])[2]}, {get_date_components(row['date'])[3]})"
        for index, row in data.iterrows()
    ]
)
full_insert_time_table = insert_time_table + values_time_table + ";"

cur.execute(full_insert_time_table)


#Insert into stock_price table
insert_stmt = "INSERT INTO stock_data.public.stock_price (id, company_id, date, open, high, low, close, volume) VALUES "
values_str = ", ".join(
    [
        f"({next(gen)}, {company_id}, '{row['date']}', {row['1. open']}, {row['2. high']}, {row['3. low']}, {row['4. close']}, {row['5. volume']})"
        for index, row in data.iterrows()
    ]
)
full_insert_stmt = insert_stmt + values_str + ";"

cur.execute(full_insert_stmt)

conn.commit()
    
cur.close()
conn.close()
