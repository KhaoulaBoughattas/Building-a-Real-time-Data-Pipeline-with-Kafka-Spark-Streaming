# streamAddUsers.py (avec confluent-kafka)
from confluent_kafka import Producer  # Kafka client moderne
from faker import Faker  # Génère des données aléatoires
import json
import time
import random

# -------------------------------
# Kafka Configuration
# -------------------------------
KAFKA_TOPIC = "topicA"
KAFKA_BOOTSTRAP = "kafka:9092"

# -------------------------------
# Kafka Producer Setup
# -------------------------------
producer = Producer({'bootstrap.servers': KAFKA_BOOTSTRAP})

def delivery_report(err, msg):
    """Callback pour confirmer l'envoi des messages"""
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

# -------------------------------
# Faker setup
# -------------------------------
fake = Faker()
print("StreamAddUsers Producer started")

# -------------------------------
# Infinite loop to continuously send messages
# -------------------------------
while True:
    message = {
        "id": random.randint(1, 100000),
        "first_name": fake.first_name(),
        "last_name": fake.last_name(),
        "country": fake.country(),
        "product_name": fake.word(),
        "gender": random.choice(["Male", "Female"]),
        "quantity": random.randint(1, 10)
    }
    # Envoi du message en JSON
    producer.produce(KAFKA_TOPIC, json.dumps(message).encode('utf-8'), callback=delivery_report)
    producer.flush()
    time.sleep(5)  # Envoie toutes les 5 secondes
