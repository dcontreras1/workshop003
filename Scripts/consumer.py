import json
import pandas as pd
from kafka import KafkaConsumer
import joblib
import psycopg2

with open(r"config\credentials.json", encoding="utf-8") as f:
    creds = json.load(f)

conn_str = f"host={creds['host']} port={creds['port']} dbname={creds['database']} user={creds['user']} password={creds['password']}"

conn = psycopg2.connect(conn_str)
cursor = conn.cursor()

cursor.execute("""
CREATE TABLE IF NOT EXISTS predictions (
    id SERIAL PRIMARY KEY,
    country TEXT,
    year INTEGER,
    gdp_per_capita REAL,
    social_support REAL,
    life_expectancy REAL,
    freedom REAL,
    corruption REAL,
    predicted_happiness REAL
)
""")
conn.commit()

cursor.execute("DELETE FROM predictions")
conn.commit()
print("tabla 'predictions' reiniciada")

# Cargar modelo
model = joblib.load("models/random_forest_model.pkl")

# Configuración de Kafka
KAFKA_TOPIC = "happiness_data"
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"

consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='happiness_group'
)

print("Esperando mensajes desde Kafka...\n")

# Consumir mensajes
for message in consumer:
    data = message.value

    features = ["gdp_per_capita", "social_support", "life_expectancy", "freedom", "corruption"]
    X = pd.DataFrame([data])[features]
    prediction = model.predict(X)[0]

    cursor.execute("""
        INSERT INTO predictions (
            country, year, gdp_per_capita, social_support, life_expectancy,
            freedom, corruption, predicted_happiness
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """, (
        data["country"], data["year"], data["gdp_per_capita"], data["social_support"],
        data["life_expectancy"], data["freedom"],
        data["corruption"], float(prediction)
    ))
    conn.commit()

    print(f"{data['country']} ({data['year']}): prediccion = {prediction:.3f}")