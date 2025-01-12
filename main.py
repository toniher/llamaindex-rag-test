import duckdb

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

con.close()
