import redis
import mysql.connector
import sys
import tldextract

mydb = mysql.connector.connect(
    host="localhost",
    port=3306,
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

cursor.execute("SELECT link, title, description, date, rating, rating_count, prep_time_seconds, cook_time_seconds, total_time_seconds, servings, calories, carbohydrates, cholesterol, fat, fiber, protein, saturated_fat, sodium, sugar, id FROM word")
result = cursor.fetchall()

for row in result:
    link = str(row[0])
    title = str(row[1])
    description = str(row[2])
    date = str(row[3])
    rating = str(row[4])
    rating_count = str(row[5])
    prep_time_seconds = str(row[6])
    cook_time_seconds = str(row[7])
    total_time_seconds = str(row[8])
    servings = str(row[9])
    calories = str(row[10])
    carbohydrates = str(row[11])
    cholesterol = str(row[12])
    fat = str(row[13])
    fiber = str(row[14])
    protein = str(row[15])
    saturated_fat = str(row[16])
    sodium = str(row[17])
    sugar = str(row[18])
    id = str(row[19])

    print(servings)

    #r.zadd("word:words_by_status:" + status, { word: priority})

    #if parent is not None:
        #r.hset("word:parent", word, str(parent))
 

