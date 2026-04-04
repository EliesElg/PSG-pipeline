# ⚽ PSG Analytics & RAG Pipeline

Ce projet est un pipeline End-to-End. 
Il extrait les données de matchs du Paris Saint-Germain via une API, les transforme dans le Cloud, et les rend accessibles via un Chatbot (RAG) basé sur gemini.

## Archi

1. **Extraction (Python)** : Scraping de données sportives via `football-data.org`.
2. **Data Warehouse (Google BigQuery)** : Stockage brut des données dans le cloud.
3. **Analytics Engineering (DBT)** : Nettoyage, structuration (Staging/Marts) et Tests de Qualité (`schema.yml`).
4. **Orchestration (Apache Airflow)** : Automatisation quotidienne du pipeline CI/CD (Extract >> Load >> Run >> Test).
5. **AI Vector Database (Qdrant & Gemini Embeddings)** : Recherche sémantique vectorielle.
6. **Interface Utilisateur (Streamlit & Langchain)** : Chatbot RAG avec mémoire de session.

##  Comment lancer le projet

### 1. Prérequis & Clés d'API
- Créez un fichier `.env` à la racine contenant :
  ```env
  API_KEY=votre_cle_football_data
  GOOGLE_API_KEY=votre_cle_gemini_ai_studio
  GOOGLE_PROJECT=nom_de_votre_projet_gcp
  GOOGLE_RAG_DATASET=nom_du_dataset_bq
  GOOGLE_RAG_TABLE=nom_de_la_table_bq
  ```
- Placez votre fichier de compte de service GCP (`google_key.json`) dans le dossier `secrets/`.

### 2. Lancer l'Orchestrateur (Airflow & DBT)
- Construisez les conteneurs (qui intègrent dbt-bigquery) et lancez l'environnement :
  ```bash
  docker-compose up -d --build
  ```
- Allez sur `http://localhost:8080` pour déclencher le pipeline de données (Extraction -> BigQuery -> DBT).

### 3. Lancer le Chatbot IA (Interface RAG)
Le fichier `test_rag.py` était un brouillon de développement. Le vrai produit final se lance via cette commande :
```bash
pip install -r requirements.txt
streamlit run src/app.py
```
L'interface s'ouvrira dans votre navigateur. Demandez à Gemini tous les détails de la saison du PSG !
