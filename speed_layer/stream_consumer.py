from confluent_kafka import Consumer
import signal

running = True


def stop_consumer(signal, frame):
    global running
    print("Stopping consumer...")
    running = False


# Register signal handler for SIGINT (Ctrl+C)
signal.signal(signal.SIGINT, stop_consumer)

c = Consumer({
    'bootstrap.servers': 'localhost:9092,localhost:9093,localhost:9094',
    'group.id': 'python-consumer',
    'auto.offset.reset': 'earliest'
})

print(c.list_topics().topics)

c.subscribe(['log'])

while running:
    msg = c.poll(1.0)

    if msg is None:
        continue

    if msg.error():
        print('Error : {}'.format(msg.error()))

    data = msg.value().decode('utf-8')
    print(data)

c.close()
