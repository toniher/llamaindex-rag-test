import os

from rich.console import Console
from rich.markdown import Markdown

console = Console()
from llama_index.core import (
    Settings,
    SimpleDirectoryReader,
    StorageContext,
    VectorStoreIndex,
)
from llama_index.embeddings.ollama import OllamaEmbedding
from llama_index.llms.ollama import Ollama
from llama_index.vector_stores.duckdb import DuckDBVectorStore

llm = Ollama(model="llama3.2:latest", request_timeout=120.0)
ollama_embedding = OllamaEmbedding(
    model_name="all-minilm",
)
import duckdb

Settings.llm = llm
Settings.embed_model = ollama_embedding

documents = SimpleDirectoryReader("data").load_data()
print(documents)
con = duckdb.connect(database="datacamp.duckdb")

# Check if the table exists
table_exists = con.execute(
    "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'blog'"
).fetchone()[0]

print(table_exists)
con.close()

# This handles existing stores. Careful if contents already added
if table_exists > 0:
    vector_store = DuckDBVectorStore.from_local(
        database_path="./datacamp.duckdb", table_name="blog"
    )
else:
    vector_store = DuckDBVectorStore(
        database_name="datacamp.duckdb",
        table_name="blog",
        persist_dir="./",
    )

print(vector_store)
storage_context = StorageContext.from_defaults(vector_store=vector_store)

index = VectorStoreIndex.from_documents(documents, storage_context=storage_context)
# Query the database
query_engine = index.as_query_engine()
response = query_engine.query("What is the best way to organize spreadsheets?")
console.print(Markdown(f"<b>{response}</b>"))

from llama_index.core.chat_engine import CondensePlusContextChatEngine
from llama_index.core.memory import ChatMemoryBuffer

memory = ChatMemoryBuffer.from_defaults(token_limit=3900)

chat_engine = CondensePlusContextChatEngine.from_defaults(
    index.as_retriever(), memory=memory, llm=llm
)

response = chat_engine.chat("What is the best way to organize spreadsheets?")
console.print(Markdown(response.response))

response = chat_engine.chat("Could you please provide more details about the naming?")
console.print(Markdown(response.response))
