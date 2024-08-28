import random
import datetime
from tableauhyperapi import HyperProcess, Connection, Telemetry, TableDefinition, SqlType, Inserter, CreateMode


# Define the column names and their corresponding data types
columns = [
    ("Customer_ID", SqlType.text(), lambda: f"Customer_{random.randint(1, 100000)}"),
    ("Rewards_Level", SqlType.text(), lambda: random.choice(["Blue", "Bronze","Silver","Gold","Non Member"])),
    ("Customer_Name", SqlType.text(), lambda: f"CustomerName{random.randint(1, 6000)}"),
    ("Loyalty Number", SqlType.text(), lambda: f"Loyalty_{random.randint(1, 100000000)}"),
    ("Email", SqlType.text(), lambda: f"email_{random.randint(1, 100000000)}_@marketingclouddemo.com"),
    ("Phone",SqlType.big_int(),lambda: random.randint(2010000000, 9890000000)),

]

# Define the path and filename for the Hyper file
hyper_file_path = "/Users/jpetrides/Documents/Demo Data/Hotels/Jeeves Idea/data/Dim_Customer.hyper"

# Define the number of rows to generate
num_rows = 160000

# Create the Tableau Hyper file
with HyperProcess(telemetry=Telemetry.SEND_USAGE_DATA_TO_TABLEAU) as hyper:
    with Connection(endpoint=hyper.endpoint, database=hyper_file_path, create_mode=CreateMode.CREATE_AND_REPLACE) as connection:
        # Define columns properly for the TableDefinition
        table_definition = TableDefinition(
            table_name='D_Customer',
            columns=[TableDefinition.Column(name, type) for name, type, _ in columns]
        )
        connection.catalog.create_table(table_definition)
        
        with Inserter(connection, table_definition) as inserter:
            for _ in range(num_rows):
                row = [func() for _, _, func in columns]
                inserter.add_row(row)
            inserter.execute()

print("Hyper file created successfully at", hyper_file_path)