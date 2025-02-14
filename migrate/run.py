import redis
import mysql.connector
import sys
import tldextract

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

def migrate_waiting():
    cursor.execute("SELECT link, domain, priority FROM waiting_link")
    result = cursor.fetchall()
    
    for row in result:
        link = str(row[0])
        domain = str(row[1])
        priority = float(row[2])
    
        r.zadd("link:links_by_status:waiting", { link: priority})
        r.hset("link:status", link, "waiting")
        r.hset("link:priority", link, str(priority))
        r.hset("link:domain", link, domain)
        r.zadd("link:waiting_links_by_domain:" + domain, { link: priority })
        r.sadd("link:waiting_domains", domain)
 
def migrate_download_failed():
    cursor.execute("SELECT link FROM download_failed_link")
    result = cursor.fetchall()
    
    for row in result:
        link = str(row[0])
        domain = tldextract.extract(link).domain
        priority = 0
    
        r.zadd("link:links_by_status:download_failed", { link: priority })
        r.hset("link:status", link, "download_failed")
        r.hset("link:priority", link, str(priority))
        r.hset("link:domain", link, domain)
 
def migrate_extraction_failed():
    cursor.execute("SELECT link, content_size FROM extraction_failed_link")
    result = cursor.fetchall()
    
    for row in result:
        link = str(row[0])
        content_size = str(row[1])
        domain = tldextract.extract(link).domain
        priority = 0
    
        r.zadd("link:links_by_status:extraction_failed", { link: priority})
        r.hset("link:status", link, "extraction_failed")
        r.hset("link:priority", link, str(priority))
        r.hset("link:domain", link, domain)
        r.hset("link:content_size", link, content_size)
 
def migrate_processed():
    cursor.execute("SELECT link, content_size FROM processed_link")
    result = cursor.fetchall()
    
    for row in result:
        link = str(row[0])
        content_size = str(row[1])
        domain = tldextract.extract(link).domain
        priority = 0
    
        r.zadd("link:links_by_status:processed", { link: priority})
        r.hset("link:status", link, "processed")
        r.hset("link:priority", link, str(priority))
        r.hset("link:domain", link, domain)
        r.hset("link:content_size", link, content_size)
 
def migrate_word():
    cursor.execute("SELECT word, parent_new, priority, status FROM word")
    result = cursor.fetchall()
    
    for row in result:
        word = str(row[0])
        parent = str(row[1])
        priority = str(row[2])
        status = str(row[3]).lower()
    
        r.zadd("word:words_by_status:" + status, { word: priority})
        r.hset("word:status", word, status)
        r.hset("word:priority", word, str(priority))

        if parent != "None": # lol
            print(parent)
            r.hset("word:parent", parent)
 

r.flushall()
migrate_word()
migrate_waiting()
migrate_download_failed()
migrate_extraction_failed()
migrate_processed()

