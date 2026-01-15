# Au lieu de python:3.9-slim, on part de l'image officielle Airflow
FROM apache/airflow:2.7.1

# On copie le fichier requirements.txt
COPY requirements.txt .

# On installe les librairies (requests, pandas...)
# Le 'airflow' user est celui par défaut dans cette image
RUN pip install --no-cache-dir -r requirements.txt
