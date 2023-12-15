from confluent_kafka.avro import AvroProducer
from confluent_kafka import avro
from confluent_kafka import Producer
from datetime import datetime
import random
import json
import uuid
from config import TOPIC

avro_schema = {
    "type": "record",
    "name": "StockData",
    "fields": [
        {"name": "symbol", "type": "string"},
        {"name": "price", "type": "float"},
        {"name": "timestamp", "type": "long"}
    ]
}

def produce():
    # Configure the Producer
    p = Producer({
        'bootstrap.servers': 'localhost:19092',  # Assuming you're running this on the same machine as the compose
        'client.id': 'python-producer',
        'schema.registry.url': 'http://schema-registry:8081'
    },default_value_schema=avro_schema)

    # Produce a message
    try:
        while True:
            stock = {
                'event_time': datetime.now().isoformat(),
                'ticker': random.choice(['AAPL', 'AMZN', 'MSFT', 'INTC', 'TBV']),
                'price': round(random.random() * 100, 2)
            }
            p.produce(TOPIC, key=str(uuid.uuid4), value=json.dumps(stock), callback=delivery_report)
    except Exception as e:
        print(str(e))

    # Wait for any outstanding messages to be delivered
    p.flush()

def delivery_report(err, msg):
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))

def main():
    produce()

if __name__ == "__main__":
    main()