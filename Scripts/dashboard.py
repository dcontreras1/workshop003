import streamlit as st
import pandas as pd
import psycopg2
import json
import time

# Leer credenciales
@st.cache_resource(ttl=600)
def get_connection_params():
    with open("config/credentials.json", encoding="utf-8") as f:
        return json.load(f)

def get_connection():
    creds = get_connection_params()
    conn_str = (
        f"host={creds['host']} port={creds['port']} dbname={creds['database']} "
        f"user={creds['user']} password={creds['password']}"
    )
    return psycopg2.connect(conn_str)

def fetch_all_data():
    conn = get_connection()
    try:
        return pd.read_sql("SELECT * FROM predictions ORDER BY id ASC", conn)
    finally:
        conn.close()

# Configuración general
st.set_page_config(page_title="Happiness Stream", layout="wide")
st.title("Predicción del Índice de Felicidad en Tiempo Real")

# Inicializar estado solo si no está
if "full_data" not in st.session_state:
    st.session_state.full_data = fetch_all_data()
if "shown_data" not in st.session_state:
    st.session_state.shown_data = pd.DataFrame(columns=["country", "predicted_happiness"])
if "index" not in st.session_state:
    st.session_state.index = 0

# Zonas dinámicas
placeholder_chart = st.empty()
placeholder_table = st.empty()

# Mostrar tabla y gráfico actualizados
def update_display():
    df = st.session_state.shown_data
    if not df.empty:
        grouped = df.groupby("country")["predicted_happiness"].mean().sort_values(ascending=False)
        placeholder_chart.bar_chart(grouped)
        placeholder_table.dataframe(df, use_container_width=True)

# Simular flujo fila por fila
if st.session_state.index < len(st.session_state.full_data):
    next_row = st.session_state.full_data.iloc[st.session_state.index]
    st.session_state.shown_data = pd.concat(
        [st.session_state.shown_data, pd.DataFrame([next_row])],
        ignore_index=True
    )
    st.session_state.index += 1

    update_display()

    time.sleep(1)
    st.rerun()

else:
    st.success("Todos los datos han sido visualizados")
    update_display()
