from tableauhyperapi import HyperProcess, Connection, Telemetry, SqlType
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import os
from datetime import datetime

# Define file paths
hyper_file_path = '/Users/jpetrides/Downloads/FAA_Monthly_Flights/Data/Extracts/FAA_Monthly_Flights.hyper'
parquet_file_path = '/Users/jpetrides/Downloads/FAA_Monthly_Flights/Data/Extracts/FAA_Monthly_Flights.parquet'

# Start Hyper process with telemetry
with HyperProcess(telemetry=Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU) as hyper:
    with Connection(hyper.endpoint, hyper_file_path) as connection:
        # Execute the query
        with connection.execute_query('SELECT * FROM "Extract"."Extract"') as result:
            # Get column names and types from the schema
            columns = [col.name for col in result.schema.columns]
            column_types = [col.type for col in result.schema.columns]
            
            # Identify date columns
            date_cols = [i for i, t in enumerate(column_types) if t == SqlType.date()]
            date_col_names = [columns[i] for i in date_cols]
            print(f"Date columns detected: {date_col_names}")

            # Fetch and transform rows
            rows = []
            for row in result:
                # Convert each row to a list, handling date columns
                transformed_row = []
                for i, value in enumerate(row):
                    if i in date_cols and value is not None:
                        # Convert tableauhyperapi.Date to string, then to datetime
                        value = pd.to_datetime(str(value)).date()  # e.g., "2001-12-01" -> datetime.date
                    transformed_row.append(value)
                rows.append(transformed_row)

        # Create pandas DataFrame
        df = pd.DataFrame(rows, columns=columns)

        # Ensure date columns are properly typed in pandas
        for col in date_col_names:
            df[col] = pd.to_datetime(df[col], errors='coerce')

# Convert to PyArrow Table
table = pa.Table.from_pandas(df)

# Write to Parquet
pq.write_table(table, parquet_file_path)

# Verify
if os.path.exists(parquet_file_path):
    print(f"Successfully converted Hyper file to Parquet file at: {parquet_file_path}")
    # Optional: Check the schema
    parquet_table = pq.read_table(parquet_file_path)
    print("Parquet schema:")
    print(parquet_table.schema)
else:
    print("Conversion failed.")