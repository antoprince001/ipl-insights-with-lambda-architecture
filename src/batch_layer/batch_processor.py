from utils.duckdb_handler import setup_duckdb_connection, create_table

def process_batch(db_path, file_path):
    connection = setup_duckdb_connection(db_path)
    # Load to DuckDB
    create_table(connection, file_path) 
    # transform

    # write to where we want
    
