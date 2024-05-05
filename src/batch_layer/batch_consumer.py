from src.utils.write_functions import write_to_csv
import os


def write_to_batch_landing_zone(data):
    file_name = './ipl_data.csv'
    if not os.path.exists(file_name):
        write_to_csv(data.keys(), file_name, 'w+')
    write_to_csv(data.values(), file_name, 'a')
