# producer.py
import pandas as pd
import numpy as np
import os
import joblib
import xgboost as xgb
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler, PolynomialFeatures
from sklearn.linear_model import LinearRegression, Ridge
from sklearn.ensemble import GradientBoostingRegressor, StackingRegressor
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
from kafka import KafkaProducer
import json
import time

# Configuración de Kafka
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
PREDICTION_TOPIC = 'happiness_predictions'

# Ruta del archivo de datos
file_path = r"C:\Users\Danie\OneDrive\Escritorio\Workshop03\data\merged.csv"
model_dir = "models"  # Asegúrate de que esta carpeta exista

# Cargar datos
try:
    df = pd.read_csv(file_path)
    print("Archivo cargado correctamente.")
except FileNotFoundError:
    print(f"Error: El archivo no se encontró en la ruta: {file_path}")
    exit()
except Exception as e:
    print(f"Error al leer el archivo CSV: {e}")
    exit()

# Ingeniería de características (replicando el training)
df["gdp_life_combo"] = df["gdp_per_capita"] * df["life_expectancy"]
df["support_per_gdp"] = df["social_support"] / (df["gdp_per_capita"] + 1e-5)

features_original = ["gdp_per_capita", "social_support", "life_expectancy"]
poly = PolynomialFeatures(degree=2, interaction_only=True, include_bias=False)
X_poly = poly.fit_transform(df[features_original])
poly_feature_names = poly.get_feature_names_out(features_original)
df_poly = pd.DataFrame(X_poly, columns=poly_feature_names)

df_extended = pd.concat([df, df_poly], axis=1)

features_extended = list(dict.fromkeys(
    [col for col in df_extended.columns if col not in ["happiness_score", "country"]]
))
target = "happiness_score"

X = df_extended[features_extended].copy() # Usar una copia para evitar SettingWithCopyWarning

# Cargar el modelo XGBoost, scaler y poly features
try:
    xgboost_model = joblib.load(os.path.join(model_dir, "xgboost.pkl"))
    scaler = joblib.load(os.path.join(model_dir, "scaler.pkl"))
    poly_loaded = joblib.load(os.path.join(model_dir, "poly_features.pkl"))
    loaded_features = joblib.load(os.path.join(model_dir, "features_extended.pkl"))
    print("Modelo XGBoost, scaler y características cargados correctamente.")
except FileNotFoundError as e:
    print(f"Error al cargar artefactos del modelo: {e}")
    exit()

# Asegurarse de que las columnas en X coincidan con las esperadas por el modelo
missing_cols = set(loaded_features) - set(X.columns)
if missing_cols:
    print(f"Error: Faltan columnas en los datos para la predicción: {missing_cols}")
    exit()
extra_cols = set(X.columns) - set(loaded_features)
if extra_cols:
    X = X[loaded_features].copy() # Mantener solo las columnas necesarias

# Escalar y transformar las características
X_scaled = scaler.transform(X)

# Realizar predicciones con el modelo XGBoost
predictions = xgboost_model.predict(X_scaled)

# Crear un DataFrame con las predicciones y el país
predictions_df = pd.DataFrame({'country': df['country'], 'predicted_happiness': predictions})

# Inicializar el productor de Kafka
try:
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )
    print(f"Productor de Kafka conectado a {KAFKA_BOOTSTRAP_SERVERS}")
except Exception as e:
    print(f"Error al conectar con Kafka: {e}")
    exit()

# Enviar las predicciones a Kafka
for index, row in predictions_df.iterrows():
    data = {
        'country': row['country'],
        'predicted_happiness': float(row['predicted_happiness'])
    }
    try:
        producer.send(PREDICTION_TOPIC, value=data)
        print(f"Enviando predicción para {row['country']}: {row['predicted_happiness']:.4f}")
        time.sleep(1) # Simular un flujo de datos en tiempo real
    except Exception as e:
        print(f"Error al enviar mensaje a Kafka: {e}")

# Cerrar el productor de Kafka
producer.close()
print("Productor de Kafka cerrado.")