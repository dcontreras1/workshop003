# Workshop03 - Predicción de Felicidad con Machine Learning y Streaming

Este proyecto implementa un pipeline completo de datos para predecir el índice de felicidad de los países utilizando datos históricos del World Happiness Report, aprendizaje automático y visualización en tiempo real a través de Kafka y Streamlit.

---

## Contenido

- [Objetivo](#objetivo)
- [Estructura del Proyecto](#estructura-del-proyecto)
- [Tecnologías](#tecnologías)
- [Pipeline ETL](#pipeline-etl)
- [EDA (Análisis Exploratorio de Datos)](#eda-análisis-exploratorio-de-datos)
- [Entrenamiento de Modelos](#entrenamiento-de-modelos)
- [Streaming con Kafka](#streaming-con-kafka)
- [Dashboard Interactivo](#dashboard-interactivo)
- [Conclusiones](#conclusiones)
- [Archivos Clave](#archivos-clave)

---

## Objetivo

Desarrollar un sistema predictivo y visual que integre:

- Limpieza, transformación y análisis de datos
- Entrenamiento de modelos de machine learning
- Simulación de datos en tiempo real con Kafka
- Visualización en vivo usando Streamlit

---

## Tecnologías

- **Python**
- **Apache Kafka** (Streaming)
- **PostgreSQL** (Almacenamiento)
- **Scikit-learn, XGBoost** (ML)
- **Pandas, Matplotlib, Seaborn** (EDA)
- **Streamlit** (Dashboard)
- **Docker** (Contenedores Kafka)

---

## Pipeline ETL

El proceso ETL se ejecuta en `merge_data.py`:

1. **Extracción**: Carga de datasets por año (2015–2019)
2. **Transformación**:
   - Estandarización de columnas
   - Limpieza de valores inconsistentes
   - Nuevas características:
     - `gdp_life_combo = gdp_per_capita * life_expectancy`
     - `support_per_gdp = social_support / gdp_per_capita`
   - Expansión polinómica de 2º grado
3. **Unificación**: Concatenación de todos los datos y guardado en `merged.csv`

---

## EDA (Análisis Exploratorio de Datos)

- Se detectaron fuertes correlaciones entre:
  - `gdp_per_capita`, `life_expectancy`, `social_support` <-> `happiness_score`
- Se evaluó la distribución de la felicidad y se graficó la importancia de variables

> ![Matriz de correlación](C:\Users\Danie\Downloads\matriz_corre.png)
> ![Características más importantes](C:\Users\Danie\Downloads\top caracteristicas.png)

---

## Entrenamiento de Modelos

Se probaron varios modelos:

| Modelo              | MAE   | R2    |
|---------------------|-------|-------|
| Linear Regression   | 0.52  | 0.73  |
| Ridge Regression    | 0.46  | 0.76  |
| Gradient Boosting   | 0.37  | 0.81  |
| **XGBoost**         | **0.35**  | **0.83**  |

**Modelo final**: `XGBoost`  
**Transformaciones aplicadas**: `StandardScaler`, `PolynomialFeatures`, variables derivadas  
Guardado en: `models/xgboost.pkl`

---

## Streaming con Kafka

### Producer
- Envía registros del 20% de test set
- Intervalo de envío: 0.5s
- Topic: `happiness_data`

### Consumer
- Recibe mensaje JSON
- Realiza transformación con `scaler` y `poly`
- Predice felicidad y guarda en PostgreSQL

> ![terminal del consumer](C:\Users\Danie\OneDrive\Escritorio\Workshop03\pictures\image.png)

---

## Dashboard Interactivo

Con `Streamlit`, se diseñó un dashboard que:

- Muestra las últimas predicciones
- Visualiza felicidad promedio por país
- Gráfica de evolución histórica

> ![Streaming Dashboard](C:\Users\Danie\OneDrive\Escritorio\Workshop03\pictures\image-1.png)
---

## Conclusiones

- El modelo XGBoost predice con un R² > 0.83
- Las variables socioeconómicas tienen un alto impacto en la felicidad
- El sistema completo simula un entorno de producción con streaming real
- El uso de Kafka y Streamlit permite construir soluciones escalables y visibles

---

## Archivos Clave

| Archivo                | Descripción                                 |
|------------------------|---------------------------------------------|
| `merge_data.py`        | ETL y estandarización de datos              |
| `02_model_training.py` | Entrenamiento y evaluación de modelos       |
| `producer.py`          | Kafka producer con simulación de streaming  |
| `consumer.py`          | Kafka consumer con predicción y almacenamiento |
| `dashboard.py`         | Dashboard Streamlit                         |
| `docker-compose.yml`   | Infraestructura Kafka + Zookeeper           |

---

## Cómo Ejecutar

1. Inicia Docker:  
   `docker-compose up -d`

2. Ejecuta el entrenamiento del modelo manualmente:

    - Abre el archivo notebooks/02_model_training.ipynb

    - Ejecuta todas las celdas para entrenar el modelo y guardar los artefactos (xgboost.pkl, scaler.pkl, etc.) en la carpeta models/

3. Lanza la aplicación de Streamlit (consumer):
    `streamlit run python scripts/consumer.py`

4. Ejecuta el producer.py para simular el flujo:
    `python scripts/producer.py`
