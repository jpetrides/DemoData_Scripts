from tableauhyperapi import HyperProcess, Connection, Telemetry

# Define the Hyper file path
hyper_file_path = '/Users/jpetrides/Downloads/FAA_Monthly_Flights/Data/Extracts/FAA_Monthly_Flights.hyper'

# Start Hyper process with telemetry
with HyperProcess(telemetry=Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU) as hyper:
    with Connection(hyper.endpoint, hyper_file_path) as connection:
        # Get all schema names in the catalog
        catalog = connection.catalog
        schema_names = catalog.get_schema_names()
        print("Available schemas in the Hyper file:")
        for schema in schema_names:
            print(f"Schema: {schema}")
            # Get table names within this schema
            try:
                table_names = catalog.get_table_names(schema=schema)
                print("  Tables:")
                for table in table_names:
                    print(f"    {table}")
            except Exception as e:
                print(f"  No tables found or error: {e}")