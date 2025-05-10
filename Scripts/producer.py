import pandas as pd
from kafka import KafkaProducer
import json
import time

KAFKA_TOPIC = "happiness_data"
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"

# Inicializar producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

df = pd.read_csv("data/merged.csv")

print(f"Enviando {len(df)} registros al topic '{KAFKA_TOPIC}'...")
for idx, row in df.iterrows():
    data = row.to_dict()
    producer.send(KAFKA_TOPIC, value=data)
    time.sleep(0.5)

producer.flush()
print("Envío completado.")