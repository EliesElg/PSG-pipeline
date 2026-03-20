import streamlit as st
import os
from google.cloud import bigquery
from dotenv import load_dotenv
from langchain_google_genai import GoogleGenerativeAIEmbeddings, ChatGoogleGenerativeAI
from langchain_qdrant import QdrantVectorStore
from langchain_core.documents import Document
from langchain_core.prompts import ChatPromptTemplate

load_dotenv()

st.set_page_config(page_title="Chatbot PSG RAG", page_icon="⚽")
st.title("⚽ Assistant Data PSG")
st.write("Pose-moi tes questions sur la saison 2025-2026 !")


# --- ETAPE 1 : MISE EN CACHE DU SYSTEME RAG ---
# st.cache_resource permet de ne pas relancer BigQuery et les Embeddings à chaque fois qu'on tape un message !
@st.cache_resource(show_spinner="Chargement de l'intelligence artificielle en cours...")
def init_rag_system():
    project_name = os.getenv("GOOGLE_PROJECT")
    dataset_name = os.getenv("GOOGLE_RAG_DATASET")
    table_name = os.getenv("GOOGLE_RAG_TABLE")
    gemini_api = os.getenv("GOOGLE_API_KEY")

    # 1. Extraction (JSON Service Account)
    bq_client = bigquery.Client.from_service_account_json(
        r"C:\Users\elies\Desktop\projet DE\secrets\google_key.json"
    )
    query = (
        f"SELECT contexte_rag FROM {project_name}.{dataset_name}.{table_name} LIMIT 100"
    )
    result = bq_client.query(query)

    rag_chunks = []
    for row in result:
        rag_chunks.append(Document(page_content=row[0]))

    # 2. Vectorisation
    embeddings = GoogleGenerativeAIEmbeddings(
        model="gemini-embedding-001", google_api_key=gemini_api
    )

    # 3. Base de données
    db = QdrantVectorStore.from_documents(
        rag_chunks, embeddings, location=":memory:", collection_name="base_matches"
    )

    # 4. Préparation du LLM et des Règles
    llm = ChatGoogleGenerativeAI(
        model="gemini-flash-latest", temperature=0.1, google_api_key=gemini_api
    )
    retriever = db.as_retriever(search_kwargs={"k": 5})

    prompt = ChatPromptTemplate.from_template("""
    Reponds en francais en utilisant UNIQUEMENT ce contexte.
    Si autre contexte invite l'utlisateur a poser des questions uniquement sur le contexte (saison 2025-2026 du PSG). Reformule naturellement.
     
    CONTEXTE : {context}
     
    QUESTION : {input}
     
    REPONSE : """)

    return retriever, llm, prompt


# Execution unique du chargement (mise en cache)
retriever, llm, prompt = init_rag_system()


# --- ETAPE 2 : GESTION DE LA FENETRE DE CHAT (Session State) ---
# On crée une liste vide de messages si elle n'existe pas
if "messages" not in st.session_state:
    st.session_state.messages = []

# On affiche tous les messages de l'historique sur la page
for msg in st.session_state.messages:
    with st.chat_message(msg["role"]):
        st.write(msg["content"])


# --- ETAPE 3 : INTERACTION AVEC L'UTILISATEUR ---
user_question = st.chat_input("Ex: Contre qui a gagné le PSG en février ?")

if user_question:
    # 1. L'utilisateur pose la question (Affichage UI + Sauvegarde Session)
    with st.chat_message("user"):
        st.write(user_question)
    st.session_state.messages.append({"role": "user", "content": user_question})

    # 2. L'IA réfléchit et répond
    with st.chat_message("assistant"):
        # Petit cercle de chargement visuel
        with st.spinner("Je cherche dans mes fiches..."):
            # --- NOTRE CODE RAG DE TOUT A L'HEURE ---
            matchs_trouves = retriever.invoke(user_question)
            contexte_brut = "\n".join([doc.page_content for doc in matchs_trouves])

            messages_prepares = prompt.format_messages(
                context=contexte_brut, input=user_question
            )
            reponse = llm.invoke(messages_prepares)

            # ---------------------------------------

            # On affiche la vraie réponse textuelle
            st.write(reponse.content)

    # 3. On sauvegarde la réponse de l'IA dans l'historique
    st.session_state.messages.append({"role": "assistant", "content": reponse.content})
