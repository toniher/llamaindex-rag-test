import os

from rich.console import Console
from rich.markdown import Markdown

console = Console()
import duckdb
from llama_index.core import (
    Settings,
    SimpleDirectoryReader,
    StorageContext,
    VectorStoreIndex,
)
from llama_index.embeddings.ollama import OllamaEmbedding
from llama_index.llms.ollama import Ollama
from llama_index.vector_stores.duckdb import DuckDBVectorStore

con = duckdb.connect("datacamp.duckdb")
con.execute("""
    CREATE TABLE IF NOT EXISTS bank AS 
    SELECT * FROM read_csv('bank.csv')
""")
con.execute("SHOW ALL TABLES").fetchdf()
print(con.execute("SELECT * FROM bank WHERE duration < 100 LIMIT 5").fetchdf())

bank_duck = duckdb.read_csv("bank.csv", sep=",")
bank_duck.filter("duration < 100").limit(3).df()

rel = con.table("bank")
print(rel.columns)

print(
    rel.filter("duration < 100")
    .project("job,education,loan")
    .order("job")
    .limit(3)
    .df()
)

res = duckdb.query("""SELECT 
                            job,
                            COUNT(*) AS total_clients_contacted,
                            AVG(duration) AS avg_campaign_duration,
                        FROM 
                            'bank.csv'
                        WHERE 
                            age > 30
                        GROUP BY 
                            job
                        ORDER BY 
                            total_clients_contacted DESC;""")
res2 = con.query(
    """SELECT 
                            job,
                            COUNT(*) AS total_clients_contacted,
                            AVG(duration) AS avg_campaign_duration,
                        FROM 
                            bank
                        WHERE 
                            age > 30
                        GROUP BY 
                            job
                        ORDER BY 
                            total_clients_contacted DESC;"""
)


print(res.df())
print(res2.df())

# llm = Ollama(model="llama3.2:latest", request_timeout=120.0)
# ollama_embedding = OllamaEmbedding(
#     model_name="all-minilm",
# )
#
# Settings.llm = llm
# Settings.embed_model = ollama_embedding
#
# documents = SimpleDirectoryReader("data").load_data()
# print(documents)
# vector_store = DuckDBVectorStore(
#     database_name="datacamp.duckdb", table_name="blog", persist_dir="./"
# )
# print(vector_store)
# storage_context = StorageContext.from_defaults(vector_store=vector_store)
#
# index = VectorStoreIndex.from_documents(documents, storage_context=storage_context)
# print(con.execute("SHOW ALL TABLES").fetchdf())
# # Query the database
# query_engine = index.as_query_engine()
# response = query_engine.query("What is the best way to organize spreadsheets?")
# console.print(Markdown(f"<b>{response}</b>"))
#
# from llama_index.core.chat_engine import CondensePlusContextChatEngine
# from llama_index.core.memory import ChatMemoryBuffer
#
# memory = ChatMemoryBuffer.from_defaults(token_limit=3900)
#
# chat_engine = CondensePlusContextChatEngine.from_defaults(
#     index.as_retriever(), memory=memory, llm=llm
# )
#
# response = chat_engine.chat("What is the best way to organize spreadsheets?")
# console.print(Markdown(response.response))
#
# response = chat_engine.chat("Could you please provide more details about the naming?")
# console.print(Markdown(response.response))
#
con.close()
