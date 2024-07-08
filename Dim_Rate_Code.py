import random
import datetime
from tableauhyperapi import HyperProcess, Connection, Telemetry, TableDefinition, SqlType, Inserter, CreateMode


# Define the column names and their corresponding data types
columns = [
    ("Rate_Code", SqlType.text(), lambda: f"Rate_Code{random.randint(1, 500)}"),
    ("Rate_Code_Name", SqlType.text(), lambda: f"Rate_Code_Name{random.randint(1, 500)}"),
    ("Travel_Manger", SqlType.text(), lambda: f"Travel_Manager{random.randint(1, 50)}"),
    ("Rate_Code_Type", SqlType.text(), lambda: random.choice(["Corporate", "Agency","Group Sales"])),

]

# Define the path and filename for the Hyper file
hyper_file_path = "/Users/jpetrides/Documents/Demo Data/Hotels/Dim_Rate_Code.hyper"

# Define the number of rows to generate
num_rows = 100

# Create the Tableau Hyper file
with HyperProcess(telemetry=Telemetry.SEND_USAGE_DATA_TO_TABLEAU) as hyper:
    with Connection(endpoint=hyper.endpoint, database=hyper_file_path, create_mode=CreateMode.CREATE_AND_REPLACE) as connection:
        # Define columns properly for the TableDefinition
        table_definition = TableDefinition(
            table_name='Extract',
            columns=[TableDefinition.Column(name, type) for name, type, _ in columns]
        )
        connection.catalog.create_table(table_definition)
        
        with Inserter(connection, table_definition) as inserter:
            for _ in range(num_rows):
                row = [func() for _, _, func in columns]
                inserter.add_row(row)
            inserter.execute()

print("Hyper file created successfully at", hyper_file_path)