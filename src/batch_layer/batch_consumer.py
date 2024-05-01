import pandas as pd
from utils.write_functions import write_to_csv

def write_to_batch_landing_zone(data):
    file_name = 'data/raw/ipl_data.csv'
    write_to_csv(data, file_name)

def process_batch()
    # load to duckdb etl
    # transform
    # write to where we want
