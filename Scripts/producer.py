import pandas as pd
from kafka import KafkaProducer
import json
import time
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import PolynomialFeatures
import os

# Cargar el dataset completo
file_path = r"C:\Users\Danie\OneDrive\Escritorio\Workshop03\data\merged.csv"

try:
    df = pd.read_csv(file_path)
    print("Archivo cargado correctamente.")
except FileNotFoundError:
    print(f"Error: El archivo no se encontró en la ruta: {file_path}")
    exit()
except Exception as e:
    print(f"Error al leer el archivo CSV: {e}")
    exit()

# Nuevas características
df["gdp_life_combo"] = df["gdp_per_capita"] * df["life_expectancy"]
df["support_per_gdp"] = df["social_support"] / (df["gdp_per_capita"] + 1e-5)

# Expansión polinómica
features_original = ["gdp_per_capita", "social_support", "life_expectancy"]
poly = PolynomialFeatures(degree=2, interaction_only=True, include_bias=False)
X_poly = poly.fit_transform(df[features_original])
poly_feature_names = poly.get_feature_names_out(features_original)
df_poly = pd.DataFrame(X_poly, columns=poly_feature_names)
df_extended = pd.concat([df, df_poly], axis=1)

features_extended = [col for col in df_extended.columns if col not in ["happiness_score", "country"]]
target = "happiness_score"

X = df_extended[features_extended]
y = df_extended[target]

# Dividir el conjunto para obtener el 20% de prueba
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# Inicializar el productor de Kafka
KAFKA_TOPIC = "happiness_data"
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

print(f"Enviando {len(X_test)} registros del conjunto de prueba al topic '{KAFKA_TOPIC}'...")
for idx in range(len(X_test)):
    data = X_test.iloc[idx].to_dict()
    data['country'] = df.iloc[X_test.index[idx]]['country']  # Agregar el país correspondiente
    data['year'] = 2023  # Puedes ajustar el año según sea necesario
    producer.send(KAFKA_TOPIC, value=data)
    time.sleep(0.5)  # Delay entre envíos para simular streaming

producer.flush()
print("Envío completado.")
