import signal
from src.ingestion_layer.kafka_consumer import setup_consumer, ingestion_consumer
from src.batch_layer.batch_consumer import write_to_batch_landing_zone
from src.speed_layer.stream_consumer import stream_job
import json

running = True


def stop_consumer(signal, frame):
    global running
    print("Stopping consumer...")
    running = False


signal.signal(signal.SIGINT, stop_consumer)

topic = 'ipl_event'

consumer = setup_consumer(topic)

while running:
    print('consumed')
    data = ingestion_consumer(consumer)
    if data is not None:
        print('data'+str(data))
        data = json.loads(data)
        write_to_batch_landing_zone(data)
        stream_job(data)

# ingestion - kafka consumer

# batch - write to some file

# stream - spark streaming job ?

# batch - trigger sometime, transform and then warehouse

# stream - deicd what to do
