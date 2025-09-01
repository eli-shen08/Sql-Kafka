import os
import time
import json
from confluent_kafka import DeserializingConsumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import StringDeserializer

# Define Kafka Cluster configuration
kafka_config = {
    'bootstrap.servers': 'pkc-l7pr2.ap-south-1.aws.confluent.cloud:9092',
    'sasl.mechanisms': 'PLAIN',
    'security.protocol': 'SASL_SSL',
    'sasl.username': 'MFNKCJU7RCEUJ5CN',
    'sasl.password': 'cfltckib0yrAQLtEbVUyb+tHGqCjLLQ7C9mv9M4K5Oh7spCZP6QKjYs1A/6nIWEw',
    'group.id': 'G1',
    'auto.offset.reset': 'earliest'
}

# Kafka Schema Registry API configuration
schema_registry_client = SchemaRegistryClient({
  'url': 'https://psrc-777rw.asia-south2.gcp.confluent.cloud',
  'basic.auth.user.info': '{}:{}'.format('7U62UQDHOI3KPWRT', 'cfltVg34voBGpAvZ+QKJU1bHaVZ5zVfReGjNLhBRiOJlehxdMFyJWfjaIBg8qECA')
})

# Fetch the latest schema dynamically from kafka and create Avro Deserializer
def get_latest_schema(subject):
    schema = schema_registry_client.get_latest_version(subject).schema.schema_str
    return AvroDeserializer(schema_registry_client, schema)


key_deserializer = StringDeserializer('utf_8')  # Deserialize keys as UTF-8 strings

# consumer
record_consumer = DeserializingConsumer({**kafka_config,
                                       'key.deserializer': key_deserializer, 
                                       'value.deserializer': get_latest_schema('product_updates-value')
                                       }
                                    )

# Subscribe to the 'retail_data_topic' topic
record_consumer.subscribe(['product_updates'])

#Continually read messages from Kafka
try:
    # Open and load records from user.json to add new records to them
    if os.path.exists('users1.json'):
        try:
            with open('users1.json','r',encoding='utf-8') as f:
                all_rec = json.load(f)
        except json.JSONDecodeError:  # Empty or invalid file
            all_rec = []   

    while True:
        msg = record_consumer.poll(2.0) # How many seconds to wait for message

        if msg is None:
            print('No new messages found.')
            continue
        
        if msg.error():
            print('Consumer error: {}'.format(msg.error()))
            continue

        if msg:
            all_rec.append(msg.value())   # Adds new records to the end of the already loaded records in users.json
            with open('users1.json', 'w', encoding='utf-8') as f:        # opens the file in write format and dumps all_Rec into the file
                json.dump(all_rec, f, indent=4, ensure_ascii=False)
        
        print('Successfully consumed record from partition {} and offset {} and saved in users.json'.format(msg.partition(), msg.offset()))
        print('Key {} and Value {}'.format(msg.key(), msg.value()))
        print("*"*100)
        time.sleep(2)   # after polling wait 2 seconds

except KeyboardInterrupt:
    pass
finally:
    record_consumer.close()
