# Ecommerce Data Generator/Producer.py
from confluent_kafka import Producer, KafkaException  # Kafka client for Python
import csv   # To read CSV files
import json  # To convert Python dictionaries to JSON strings
import time  # To simulate streaming with delays
from configparser import ConfigParser  # To read configuration files

# -------------------------------
# Reading configuration from 'variables.ini'
# -------------------------------
config = ConfigParser()
config.read('variables.ini')

# Get Kafka topic name and bootstrap servers from config file
KAFKA_TOPIC_NAME_CONS = config["KAFKA"]["KAFKA_TOPIC_NAME_CONS"]  # Topic to send messages
KAFKA_BOOTSTRAP_SERVERS_CONS = config["KAFKA"]["KAFKA_BOOTSTRAP_SERVERS_CONS"]  # Kafka server address

# -------------------------------
# Kafka Producer setup
# -------------------------------
conf = {
    'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS_CONS  # Defines Kafka cluster to connect
}
producer = Producer(**conf)  # Initialize Kafka producer

# -------------------------------
# Callback function to confirm message delivery
# -------------------------------
def delivery_report(err, msg):
    """
    Key point: Confirms if message delivery succeeded or failed.
    - err: Error info if delivery failed
    - msg: Topic and partition info of the sent message
    Explanation: Useful for monitoring and logging message delivery.
    """
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

# -------------------------------
# Function to send messages to Kafka
# -------------------------------
def kafka_producer(data):
    """
    Key point: Converts dictionary to JSON and sends it to Kafka topic.
    Explanation: Ensures reliable delivery by flushing producer buffer and handling exceptions.
    """
    try:
        producer.produce(
            KAFKA_TOPIC_NAME_CONS,
            key=None,  # No specific key; Kafka partitions messages automatically
            value=json.dumps(data),  # Convert dict to JSON string
            callback=delivery_report
        )
        producer.flush()  # Wait for all messages in buffer to be sent
    except KafkaException as e:
        print(f"Kafka error: {e}")

# -------------------------------
# Read CSV and send each row as Kafka message
# -------------------------------
with open("./dataset.csv") as f:
    fdict = csv.DictReader(f, delimiter=",")  # Read CSV as dictionary
    for row in fdict:
        data = dict(row)  # Convert OrderedDict to dict
        kafka_producer(data)  # Send row to Kafka
        time.sleep(5)  # Simulate real-time streaming
        print(data)  # Log sent data

# -------------------------------
# Key Points Summary:
# 1. Configuration file allows flexible topic/server management.
# 2. Producer setup connects to Kafka cluster using bootstrap servers.
# 3. delivery_report callback confirms message delivery for reliability.
# 4. kafka_producer function handles JSON conversion, sending, and error handling.
# 5. CSV reading loop simulates streaming of real-time data with a delay.


