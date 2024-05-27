from confluent_kafka import Producer
import csv
import json
import time

p = Producer({
    'bootstrap.servers': 'localhost:9092,localhost:9093,localhost:9094'
})

print(p.list_topics().topics)


def receipt(err, msg):
    if err is not None:
        print('Error : {}'.format(err))
    else:
        print('Message on topic on partition {}  with value of {}'.format(msg.partition(), msg.value()))


# file_name = './src/data/335982.csv'
file_name = './ipl_data.csv'

with open(file_name, 'r', newline='') as csvfile:
    reader = csv.reader(csvfile)
    headers = next(reader)

    for row in reader:
        data = {}
        for i, value in enumerate(row):
            data[headers[i]] = value
        print(data)
        print('produced')
        m = json.dumps(data)
        p.poll(0)
        p.produce('ipl_event', m.encode('utf-8'), callback=receipt)
        p.flush()
        time.sleep(10)
