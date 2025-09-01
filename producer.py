from decimal import *
import os
import time
import mysql.connector
from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer


def delivery_report(err, msg):
    """
    Reports the failure or success of a message delivery.

    Args:
        err (KafkaError): The error that occurred on None on success.

        msg (Message): The message that was produced or failed.
    """
    if err is not None:
        print(f"Delivery failed for User record {msg.key()}: {err}")
        return
    print(f"Record key: {msg.key()} successfully produced to topic: {msg.topic()} at partition: [{msg.partition()}] at offset {msg.offset()}")
    print("-"*100)

# Kafka Cluster API configuration
kafka_config = {
    'bootstrap.servers': 'pkc-l7pr2.ap-south-1.aws.confluent.cloud:9092',
    'sasl.mechanisms': 'PLAIN',
    'security.protocol': 'SASL_SSL',
    'sasl.username': 'ZVR5F4QJJLBOOS5R',
    'sasl.password': 'cfltjqyVxxE2NTgOQ4kNJMvOFmMr9V6lJl/lNgctXDpo8rRAI83mubq2VyA9r+9w'
}

# Kafka Schema Registry API configuration
schema_registry_client = SchemaRegistryClient({
  'url': 'https://psrc-777rw.asia-south2.gcp.confluent.cloud',
  'basic.auth.user.info': '{}:{}'.format('JVUDCFGBP4XGPZTO', 'cfltgtILnYDx08Z7jj+Tp3vfN5CgKlOnn5EykU7UBGV4YEUILFGJKwdpbkgmLHqA')
})

# Fetch the latest schema dynamically from kafka and create Avro Serializer
def get_latest_schema(subject):
    schema = schema_registry_client.get_latest_version(subject).schema.schema_str
    return AvroSerializer(schema_registry_client, schema)

key_serializer = StringSerializer('utf_8')  # Serialize keys as UTF-8 strings

# Producers
record_producer = SerializingProducer({**kafka_config,
                                       'key.serializer': key_serializer, 
                                       'value.serializer': get_latest_schema('product_updates-value')
                                       }
                                    )

# path to last time stamp
time_path = 'last_time_stamp.txt'

# Load local time stamp file.
def load_last_timestamp() ->str:
  if os.path.exists(time_path):
    with open(time_path, 'r') as f:
      res = f.read().split()
      if res:
        return str(res[0]+" "+res[1]), res[2]
  return "1970-01-01 00:00:00", '0'        # Date H:M:S

# save last read timestamp    
def save_last_read_timestamp(ts: str, idx: str) -> None:
  with open(time_path, 'w') as f:
    f.write(f'{ts} {idx}')

# Creating infinite loop
while True:

  # Connet to MySql database and table
  cnx = mysql.connector.connect(user='root', password="deez4nuts", database='buyonline')
  cursor = cnx.cursor()

  # Loading last read time stamp before querying
  last_read_ts, idx = load_last_timestamp()
  print(f'Last read Time Stamp : {last_read_ts} and ID : {idx}')
  
  query = f"SELECT * from products1 where last_updated >= %s and id > %s"

  cursor.execute(query, (last_read_ts,idx))

  #fetchall retrieves all rows that have not been retrieved yet by select query
  new_rows = cursor.fetchall()

  if new_rows:

    for row in new_rows:
        record_producer.produce(
            topic='product_updates',
            key=str(row[0]),
            value={
            "id": row[0],
            "name": row[1],
            "category": row[2],
            "price": row[3],
            "last_updated": row[4].strftime("%Y-%m-%d %H:%M:%S")
            },
            on_delivery=delivery_report
            )
        record_producer.flush()
        # Assigning last read time stamp to same var where we loaded the time stamp
        if new_rows:
            last_read_ts = row[4].strftime("%Y-%m-%d %H:%M:%S") # coverts date time upto seconds
            last_idx = row[0]
        save_last_read_timestamp(last_read_ts, last_idx)
        time.sleep(1)           # After publishing a Message wait 1 seconf before publishing another

    
    print(f'Published all Data and saved last read Time Stamp : {last_read_ts} and ID : {last_idx}')
  else:
      print('No new Data found')

  time.sleep(3) # Waits 5 seconds then again polls MySQL

