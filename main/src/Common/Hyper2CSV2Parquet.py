#converting from hyper directly to parquet can be done, but there are so many data type issues.
#it is easier (but less efficient) to convert first to csv and then to parquet.

from tableauhyperapi import HyperProcess, Connection, Telemetry
import csv
import os
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# Define file paths
hyper_file_path = '/Users/jpetrides/Downloads/FAA_Monthly_Flights/Data/Extracts/FAA_Monthly_Flights.hyper'
csv_file_path = '/Users/jpetrides/Downloads/FAA_Monthly_Flights/Data/Extracts/FAA_Monthly_Flights.csv'

# Start Hyper process with telemetry
with HyperProcess(telemetry=Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU) as hyper:
    with Connection(hyper.endpoint, hyper_file_path) as connection:
        with connection.execute_query('SELECT * FROM "Extract"."Extract"') as result:
            # Get column names
            columns = [col.name for col in result.schema.columns]
            
            # Write to CSV
            with open(csv_file_path, 'w', newline='') as csvfile:
                writer = csv.writer(csvfile)
                # Write header
                writer.writerow(columns)
                # Write rows
                for row in result:
                    writer.writerow(row)

# Verify
if os.path.exists(csv_file_path):
    print(f"Successfully exported Hyper file to CSV at: {csv_file_path}")
else:
    print("Export to CSV failed.")



# Define file paths
csv_file_path = '/Users/jpetrides/Downloads/FAA_Monthly_Flights/Data/Extracts/FAA_Monthly_Flights.csv'
parquet_file_path = '/Users/jpetrides/Downloads/FAA_Monthly_Flights/Data/Extracts/FAA_Monthly_Flights.parquet'

# Read CSV, parsing "FLIGHT DATE" as a datetime
df = pd.read_csv(csv_file_path)

# Print dtypes to inspect what pandas inferred
print("Data types inferred by pandas:")
print(df.dtypes)

# Convert to PyArrow Table
table = pa.Table.from_pandas(df)

# Write to Parquet
pq.write_table(table, parquet_file_path)

# Verify
if os.path.exists(parquet_file_path):
    print(f"Successfully converted CSV to Parquet file at: {parquet_file_path}")
else:
    print("Conversion failed.")