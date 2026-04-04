# Au lieu de python:3.9-slim, on part de l'image officielle Airflow avec Python 3.10
FROM apache/airflow:2.7.1-python3.10

# On copie le fichier requirements.txt
COPY requirements.txt .

# On installe les librairies (requests, pandas...)
# Le 'airflow' user est celui par défaut dans cette image
RUN pip install --no-cache-dir -r requirements.txt
RUN pip install dbt-bigquery
