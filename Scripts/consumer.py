import streamlit as st
import pandas as pd
import psycopg2
import json
import time
from threading import Thread
from queue import Queue
from kafka import KafkaConsumer

# Configuración de Kafka
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
PREDICTION_TOPIC = 'happiness_predictions'
GROUP_ID = 'happiness_dashboard_group'

# Leer credenciales
@st.cache_resource
def get_connection_params():
    with open("config/credentials.json", encoding="utf-8") as f:
        return json.load(f)

def connect_db():
    creds = get_connection_params()
    return psycopg2.connect(
        host=creds['host'],
        port=creds['port'],
        dbname=creds['database'],
        user=creds['user'],
        password=creds['password']
    )

def create_table(conn):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS predictions (
                id SERIAL PRIMARY KEY,
                country VARCHAR(255) NOT NULL,
                gdp_per_capita FLOAT,
                social_support FLOAT,
                life_expectancy FLOAT,
                freedom FLOAT,
                corruption FLOAT,
                predicted_happiness FLOAT NOT NULL,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        conn.commit()

def insert_prediction(conn, data):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO predictions (country, gdp_per_capita, social_support, life_expectancy, freedom, corruption, predicted_happiness)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (
            data.get('country'),
            data.get('gdp_per_capita'),
            data.get('social_support'),
            data.get('life_expectancy'),
            data.get('freedom'),
            data.get('corruption'),
            data.get('predicted_happiness')
        ))
        conn.commit()

# Hilo para escuchar Kafka y llenar la cola
def kafka_consumer_thread(queue):
    consumer = KafkaConsumer(
        PREDICTION_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=GROUP_ID,
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    for message in consumer:
        queue.put(message.value)

# Streamlit UI
st.set_page_config(page_title="Happiness Stream", layout="wide")
st.title("Predicción del Índice de Felicidad en Tiempo Real (con Kafka)")

# Inicialización
if "data_queue" not in st.session_state:
    st.session_state.data_queue = Queue()

if "kafka_thread" not in st.session_state:
    t = Thread(target=kafka_consumer_thread, args=(st.session_state.data_queue,), daemon=True)
    t.start()
    st.session_state.kafka_thread = t

if "dashboard_data" not in st.session_state:
    st.session_state.dashboard_data = pd.DataFrame(columns=[
        "country", "gdp_per_capita", "social_support",
        "life_expectancy", "freedom", "corruption", "predicted_happiness"
    ])

# Conexión a la BD
conn = connect_db()
create_table(conn)

# Procesar nuevos datos de Kafka
while not st.session_state.data_queue.empty():
    new_data = st.session_state.data_queue.get()
    insert_prediction(conn, new_data)
    new_row = pd.DataFrame([new_data])
    st.session_state.dashboard_data = pd.concat([st.session_state.dashboard_data, new_row], ignore_index=True)

# Mostrar datos
if not st.session_state.dashboard_data.empty:
    st.subheader("Datos de predicciones recibidas:")
    st.dataframe(st.session_state.dashboard_data, use_container_width=True)

    st.subheader("Gráfico de Felicidad Promedio por País:")
    grouped = st.session_state.dashboard_data.groupby("country")["predicted_happiness"].mean().sort_values(ascending=False)
    st.bar_chart(grouped)

# Actualizar cada segundo
time.sleep(1)
st.rerun()
