import unittest
import pandas as pd
from io import StringIO
import os

from utils.write_functions import write_to_csv

def test_write_to_csv(self):
    data = {'col1': [1, 2], 'col2': [3, 4]}
    test_filename = 'test_file.csv'
    if os.path.exists(test_filename):
        os.remove(test_filename)
    
    write_to_csv(data, test_filename)
    
    expected_df = pd.DataFrame(data)
    written_df = pd.read_csv(test_filename)

    pd.testing.assert_frame_equal(written_df, expected_df)
    
    os.remove(test_filename)
