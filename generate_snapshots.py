import pandas as pd
from sqlalchemy import create_engine

# Connect to the SQLite database
engine = create_engine('sqlite:///accident_data_warehouse.db', echo=False)

# List of tables to query
tables = ['Dim_Location', 'Dim_Vehicle', 'Dim_RoadCondition', 'Dim_Date', 'Fact_Accidents']

# Query and display the first 10 rows of each table
for table in tables:
    print(f"\n--- {table} (First 10 Rows) ---")
    df = pd.read_sql(f"SELECT * FROM {table} LIMIT 10", engine)
    print(df.to_string(index=False))