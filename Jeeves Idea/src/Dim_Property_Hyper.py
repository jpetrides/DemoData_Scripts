import random
import datetime
from tableauhyperapi import HyperProcess, Connection, Telemetry, TableDefinition, SqlType, Inserter, CreateMode


# Define the column names and their corresponding data types
columns = [
 
    ("Property_Code", SqlType.text(), lambda: f"Property_Code_{random.randint(1, 600000)}"),
    ("Year_Opened",SqlType.big_int(),lambda: random.randint(2005, 2024)),
    ("Property", SqlType.text(), lambda: f"Property_{random.randint(1, 6000)}"),
    ("Region", SqlType.text(), lambda: f"Region_{random.randint(1, 10)}"),
    ("Sub_Region", SqlType.text(), lambda: f"Sub_Region_{random.randint(1, 10)}"),
    ("Business_Unit", SqlType.text(), lambda: f"Business_Unit_{random.randint(1, 10)}"),
    ("Market",  SqlType.text(),lambda: f"Market_{random.randint(1, 10)}"),
    ("Country",  SqlType.text(), lambda: f"Country_{random.randint(1, 10)}"),
    ("State", SqlType.text(), lambda: f"State_{random.randint(1, 50)}"),
    ("City", SqlType.text(), lambda: f"City_{random.randint(1, 100)}"),
    ("LocationType", SqlType.text(), lambda: random.choice(["Airport", "City Center", "Suburb", "Professional Park", "Highway","Convention"])),
    ("Management_Type", SqlType.text(), lambda: random.choice(["Owned", "Managed", "Local"])),
    ("Management_Company", SqlType.text(), lambda: f"Mgmt_Company{random.randint(1, 100)}"),
    ("Owner", SqlType.text(), lambda: f"Owner{random.randint(1, 100)}"),
    ("RevManager", SqlType.text(), lambda: f"RevManager{random.randint(1, 100)}"),
    ("RevManagerL1",SqlType.text(), lambda: f"RevManagerL1{random.randint(1, 10)}"),
    ("RevManagerL2", SqlType.text(), lambda: f"RevManagerL2{random.randint(1, 5)}"),
    ("OpsManager", SqlType.text(), lambda: f"OpsManager{random.randint(1, 100)}"),
    ("OpsManagerL1", SqlType.text(), lambda: f"OpsManagerL1{random.randint(1, 10)}"),
    ("OpsManagerL2", SqlType.text(), lambda: f"OpsManagerL2{random.randint(1, 5)}"),
    ("Brand", SqlType.text(), lambda: random.choice(["Brand1", "Brand2", "Brand3", "Brand4", "Brand6", "Brand7"])),
    ("ADR_Comp",SqlType.int(), lambda: random.randint(150, 300)),
    ("Rooms_Available",SqlType.int(), lambda: random.randint(40, 300))
]

# Define the path and filename for the Hyper file
hyper_file_path = "/Users/jpetrides/Documents/Demo Data/Hotels/Jeeves Idea/data/Hotel_Dim_Property.hyper"

# Define the number of rows to generate
num_rows = 5000

# Create the Tableau Hyper file
with HyperProcess(telemetry=Telemetry.SEND_USAGE_DATA_TO_TABLEAU) as hyper:
    with Connection(endpoint=hyper.endpoint, database=hyper_file_path, create_mode=CreateMode.CREATE_AND_REPLACE) as connection:
        # Define columns properly for the TableDefinition
        table_definition = TableDefinition(
            table_name='D_Property',
            columns=[TableDefinition.Column(name, type) for name, type, _ in columns]
        )
        connection.catalog.create_table(table_definition)
        
        with Inserter(connection, table_definition) as inserter:
            for _ in range(num_rows):
                row = [func() for _, _, func in columns]
                inserter.add_row(row)
            inserter.execute()

print("Hyper file created successfully at", hyper_file_path)