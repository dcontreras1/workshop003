# consumer.py
from kafka import KafkaConsumer
import json
import psycopg2
import time

# Configuración de Kafka
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
PREDICTION_TOPIC = 'happiness_predictions'
GROUP_ID = 'happiness_consumer_group'

# Configuración de la base de datos
DATABASE_CONFIG_FILE = 'config/credentials.json'

def get_db_credentials():
    try:
        with open(DATABASE_CONFIG_FILE, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"Error: No se encontró el archivo de credenciales en {DATABASE_CONFIG_FILE}")
        exit()
    except json.JSONDecodeError:
        print(f"Error: El archivo de credenciales en {DATABASE_CONFIG_FILE} no es un JSON válido.")
        exit()

def connect_db():
    credentials = get_db_credentials()
    try:
        conn = psycopg2.connect(
            host=credentials['host'],
            port=credentials['port'],
            dbname=credentials['database'],
            user=credentials['user'],
            password=credentials['password']
        )
        conn.autocommit = True
        return conn
    except psycopg2.Error as e:
        print(f"Error al conectar a la base de datos: {e}")
        exit()

def create_table(conn):
    cur = conn.cursor()
    try:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS predictions (
                id SERIAL PRIMARY KEY,
                country VARCHAR(255) NOT NULL,
                predicted_happiness FLOAT NOT NULL,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        print("Tabla 'predictions' creada o ya existente.")
    except psycopg2.Error as e:
        print(f"Error al crear la tabla: {e}")
        conn.rollback()

def insert_prediction(conn, country, predicted_happiness):
    cur = conn.cursor()
    try:
        cur.execute("""
            INSERT INTO predictions (country, predicted_happiness)
            VALUES (%s, %s)
        """, (country, predicted_happiness))
    except psycopg2.Error as e:
        print(f"Error al insertar la predicción: {e}")
        conn.rollback()

if __name__ == '__main__':
    consumer = KafkaConsumer(
        PREDICTION_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=GROUP_ID,
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    db_conn = connect_db()
    create_table(db_conn)

    print(f"Consumidor de Kafka escuchando el topic: {PREDICTION_TOPIC}")
    try:
        for message in consumer:
            prediction_data = message.value
            country = prediction_data.get('country')
            predicted_happiness = prediction_data.get('predicted_happiness')

            if country is not None and predicted_happiness is not None:
                insert_prediction(db_conn, country, predicted_happiness)
                print(f"Predicción recibida y guardada: Country={country}, Happiness_score={predicted_happiness:.4f}")
            else:
                print(f"Mensaje de Kafka con formato incorrecto: {prediction_data}")
    except KeyboardInterrupt:
        print("Consumidor detenido por el usuario.")
    finally:
        if db_conn:
            db_conn.close()
            print("Conexión a la base de datos cerrada.")
        consumer.close()
        print("Consumidor de Kafka cerrado.")