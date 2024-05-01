import csv
import os
from write_functions import write_to_csv


def test_write_to_csv():
    csv_filename = "test.csv"
    if os.path.exists(csv_filename):
        os.remove(csv_filename)
    test_data = {'a': 1, 'b': 2}

    write_to_csv(test_data, csv_filename)

    with open(csv_filename, 'r') as f:
        reader = csv.reader(f)
        assert next(reader) == ['1', '2']
    os.remove(csv_filename)
