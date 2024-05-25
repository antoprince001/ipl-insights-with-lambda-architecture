from confluent_kafka import Consumer
import signal

running = True


def stop_consumer(signal, frame):
    global running
    print("Stopping consumer...")
    running = False


# Register signal handler for SIGINT (Ctrl+C)
signal.signal(signal.SIGINT, stop_consumer)


def setup_consumer(topic):
    c = Consumer({
        'bootstrap.servers': 'localhost:9092,localhost:9093,localhost:9094',
        'group.id': 'python-consumer',
        'auto.offset.reset': 'earliest'
    })

    print(c.list_topics().topics)

    c.subscribe([topic])
    return c


def ingestion_consumer(consumer):
    msg = consumer.poll(1.0)
    print('in ingestion')
    print(msg)
    if msg is not None:
        data = msg.value().decode('utf-8')
        return data

    if msg is None or msg.error():
        return None
