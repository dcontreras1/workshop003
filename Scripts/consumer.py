import json
import pandas as pd
import numpy as np
import joblib
import psycopg2
from kafka import KafkaConsumer
from sklearn.preprocessing import StandardScaler, PolynomialFeatures


# 1. Cargar credenciales

with open("config/credentials.json", encoding="utf-8") as f:
    creds = json.load(f)

conn_str = f"host={creds['host']} port={creds['port']} dbname={creds['database']} user={creds['user']} password={creds['password']}"
conn = psycopg2.connect(conn_str)
cursor = conn.cursor()


# 2. Reiniciar tabla
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
print("Tabla 'predictions' reiniciada.")


# 3. Cargar modelo y transformadores
model = joblib.load("models/stacking_tuned_meta_model.pkl")
scaler = joblib.load("models/scaler.pkl")
poly = joblib.load("models/poly_features.pkl")
features_extended = joblib.load("models/features_extended.pkl")  # lista exacta de columnas del entrenamiento


# 4. Función de transformación
def prepare_features(raw_data: dict) -> np.ndarray:
    df = pd.DataFrame([raw_data])

    # Derivadas
    df["gdp_life_combo"] = df["gdp_per_capita"] * df["life_expectancy"]
    df["support_per_gdp"] = df["social_support"] / (df["gdp_per_capita"] + 1e-5)

    # Polinómicas
    poly_input = df[["gdp_per_capita", "social_support", "life_expectancy"]]
    X_poly = poly.transform(poly_input)
    poly_feature_names = poly.get_feature_names_out(poly_input.columns)
    df_poly = pd.DataFrame(X_poly, columns=poly_feature_names)

    df_final = pd.concat([df, df_poly], axis=1)

    # Selección exacta de columnas usadas en entrenamiento
    X = df_final[features_extended]
    X_scaled = scaler.transform(X)

    return X_scaled


# 5. Configurar Kafka
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


# 6. Consumir y predecir
for message in consumer:
    data = message.value
    try:
        X = prepare_features(data)
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

        print(f"{data['country']} ({data['year']}): predicción = {prediction:.3f}")
    except Exception as e:
        print(f"Error al procesar mensaje: {e}")
