import mysql.connector
import sys

mydb = mysql.connector.connect(
    host="localhost",
    port=7712,
    user=sys.argv[1],
    password=sys.argv[2],
    database="recipe",
)

cursor = mydb.cursor()

cursor.execute("SELECT link, domain, priority FROM waiting_link")

result = cursor.fetchall()

print(result.rowcount)
for row in result:
    print(row[0])

