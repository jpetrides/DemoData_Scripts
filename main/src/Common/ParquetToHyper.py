import pandas as pd
from tableauhyperapi import HyperProcess, Connection, SqlType, TableDefinition, TableName, Inserter, CreateMode, Telemetry
import os

def parquet_to_hyper(parquet_file_path, hyper_file_path, table_name="Extract"):
    """
    Convert a Parquet file to Tableau Hyper format.
    
    Args:
        parquet_file_path (str): Path to the input Parquet file
        hyper_file_path (str): Path where the output Hyper file will be saved
        table_name (str): Name of the table in the Hyper file (default: "Extract")
    """
    print(f"Converting {parquet_file_path} to {hyper_file_path}...")
    
    # Load the Parquet file into a pandas DataFrame
    df = pd.read_parquet(parquet_file_path)
    
    # Remove the Hyper file if it already exists
    if os.path.exists(hyper_file_path):
        os.remove(hyper_file_path)
    
    # Map pandas dtypes to Tableau SqlType
    def map_dtype_to_sqltype(dtype):
        if pd.api.types.is_integer_dtype(dtype):
            return SqlType.big_int()
        elif pd.api.types.is_float_dtype(dtype):
            return SqlType.double()
        elif pd.api.types.is_bool_dtype(dtype):
            return SqlType.bool()
        elif pd.api.types.is_datetime64_any_dtype(dtype):
            return SqlType.timestamp()
        elif pd.api.types.is_timedelta64_dtype(dtype):
            return SqlType.interval()
        else:
            return SqlType.text()
    
    # Create a HyperProcess
    with HyperProcess(telemetry=Telemetry.SEND_USAGE_DATA_TO_TABLEAU) as hyper:
        print("Connected to Hyper Process")
        
        # Create a connection to the Hyper file
        with Connection(hyper.endpoint, hyper_file_path, CreateMode.CREATE_AND_REPLACE) as connection:
            print("Connected to Hyper file")
            
            # Create table definition
            columns = []
            for col_name, dtype in df.dtypes.items():
                sql_type = map_dtype_to_sqltype(dtype)
                columns.append(TableDefinition.Column(col_name, sql_type))
            
            table_def = TableDefinition(TableName(table_name), columns)
            
            # Create the table
            connection.catalog.create_table(table_def)
            print(f"Created table: {table_name}")
            
            # Insert data into the table
            with Inserter(connection, table_def) as inserter:
                for row in df.itertuples(index=False):
                    # Process data to handle NaN values
                    processed_row = []
                    for val in row:
                        if pd.isna(val):
                            processed_row.append(None)
                        else:
                            processed_row.append(val)
                    
                    inserter.add_row(processed_row)
                
                inserter.execute()
            
            # Get row count
            row_count = connection.execute_scalar_query(f'SELECT COUNT(*) FROM {table_name}')
            print(f"Successfully inserted {row_count} rows into the Hyper file")

# Example usage
if __name__ == "__main__":
    parquet_file = "/Users/jpetrides/Documents/Customers/Hotel/Page_View_Sankey.parquet"
    hyper_file = "/Users/jpetrides/Documents/Customers/Hotel/.hyper"
    
    parquet_to_hyper(parquet_file, hyper_file)
    print("Conversion complete!")