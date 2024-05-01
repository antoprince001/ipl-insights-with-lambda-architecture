import csv


def write_to_csv(data, file_name, mode):
    if data is not None:
        with open(file_name, mode, newline='') as f:
            writer = csv.writer(f)
            writer.writerow(data)
