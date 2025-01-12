from sqlalchemy import create_engine

engine = create_engine("duckdb:///datacamp.duckdb")
with engine.connect() as connection:
    cursor = connection.exec_driver_sql("SELECT * FROM bank LIMIT 3")
    print(cursor.fetchall())

from llama_index.core import SQLDatabase

sql_database = SQLDatabase(engine, include_tables=["bank"])

from llama_index.core.query_engine import NLSQLTableQueryEngine
from llama_index.llms.ollama import Ollama

llm_model = Ollama(model="llama3.2:latest", request_timeout=120.0)
query_engine = NLSQLTableQueryEngine(sql_database=sql_database, llm=llm_model)

response = query_engine.query("Which is the longest running campaign?")

print(response.response)

response = query_engine.query("Which type of job has the most housing loan?")
print(response.response)
print(response.metadata)
with engine.connect() as connection:
    cursor = connection.exec_driver_sql(response.metadata["sql_query"])
    print(cursor.fetchall())
