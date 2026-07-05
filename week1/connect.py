import psycopg2

conn = psycopg2.connect(
    host="localhost",
    database="Practise",
    user="postgres",
    password="Ugash"
)

cursor = conn.cursor()
cursor.execute("SELECT * FROM test_table;")
rows = cursor.fetchall()

for row in rows:
    print(row)

cursor.close()
conn.close()

print("Connection successful!")