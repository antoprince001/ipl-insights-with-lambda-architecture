import csv


def write_to_csv(data, filename):
    print(data.values())
    with open(filename, 'a', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(data.values())
