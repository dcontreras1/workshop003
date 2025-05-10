import streamlit as st
import pandas as pd
import psycopg2
import time
import json

with open("config/credentials.json", encoding="utf-8") as f:
    creds = json.load(f)

def get_data():
    conn = psycopg2.connect(
        host=creds['host'],
        port=creds['port'],
        dbname=creds['database'],
        user=creds['user'],
        password=creds['password']
    )
    query = """
        SELECT country, year, gdp_per_capita, social_support, life_expectancy,
               freedom, generosity, corruption, predicted_happiness
        FROM predictions
        ORDER BY id DESC
    """
    df = pd.read_sql(query, conn)
    conn.close()
    return df

# Streamlit App
st.set_page_config(page_title="Happiness predictions", layout="wide")
st.title("Happiness predictions")

placeholder = st.empty()

while True:
    with placeholder.container():
        df = get_data()

        st.subheader("Last predictions")
        st.dataframe(df.head(10), use_container_width=True)

        st.subheader("Happiness predictions distribution by country")
        st.bar_chart(df.groupby("country")["predicted_happiness"].mean())

        st.subheader("Yearly happiness trend")
        st.line_chart(df.groupby("year")["predicted_happiness"].mean())

    time.sleep(5)