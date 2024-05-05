import duckdb


def setup_duckdb_connection(db_path):
    con = duckdb.connect(db_path)
    return con


def create_table(duckdb_connection, file_path):
    duckdb_connection.sql(f"CREATE TABLE tweet_data_batch AS SELECT * FROM read_csv('{file_path}')")
    duckdb_connection.sql('DESCRIBE tweet_data_batch')
