import redis
import mysql.connector
import sys

mydb = mysql.connector.connect(
    host="localhost",
    port=7712,
    user=sys.argv[1],
    password=sys.argv[2],
    database="recipe",
)

r = redis.Redis(
    host='localhost', 
    port=6379, 
    db=0,
)

cursor = mydb.cursor()

cursor.execute("SELECT link, domain, priority FROM waiting_link")
result = cursor.fetchall()

r.flushall()

for row in result:
    link = str(row[0])
    domain = str(row[1])
    priority = float(row[2])
    
    r.zadd("link:links_by_status:waiting", link)
    r.hset("link:status", link, "waiting")
    r.hset("link:priority", link, priority)
    r.hset("link:domain", link, domain)
    r.zadd("link:waiting_links_by_domain:" + domain, link, priority)
    r.sadd("link:waiting_domains", domain)
    
