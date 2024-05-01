from confluent_kafka import Producer
import json


p = Producer({
    'bootstrap.servers': 'localhost:9092,localhost:9093,localhost:9094'
})

print(p.list_topics().topics)


def receipt(err, msg):
    if err is not None:
        print('Error : {}'.format(err))
    else:
        print('Message on topic on partition {}  with value of {}'.format(msg.topic(), msg.partition(), msg.value()))


for i in range(10):
    data = {
        'name': f'name-{i}',
        'city': f'city-{i}',
        'message': f'message-{i}'
    }
    print('produced')
    m = json.dumps(data)
    p.poll(0)
    p.produce('log', m.encode('utf-8'), callback=receipt)
    p.flush()
