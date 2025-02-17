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



r.delete("recipe:recipes")
r.delete("recipe:link")
r.delete("recipe:title")
r.delete("recipe:description")
r.delete("recipe:date")
r.delete("recipe:rating")
r.delete("recipe:rating_count")
r.delete("recipe:prep_time_seconds")
r.delete("recipe:cook_time_seconds")
r.delete("recipe:total_time_seconds")
r.delete("recipe:servings")
r.delete("recipe:calories")
r.delete("recipe:carbohydrates")
r.delete("recipe:cholesterol")
r.delete("recipe:fat")
r.delete("recipe:fiber")
r.delete("recipe:protein")
r.delete("recipe:saturated_fat")
r.delete("recipe:sodium")
r.delete("recipe:sodium")

cursor.execute("SELECT link, title, description, date, rating, rating_count, prep_time_seconds, cook_time_seconds, total_time_seconds, servings, calories, carbohydrates, cholesterol, fat, fiber, protein, saturated_fat, sodium, sugar, id FROM recipe")
result = cursor.fetchall()

for row in result:
    id = str(row[19])
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

    r.sadd("recipe:recipes", id)
    if link != "None":
        r.hset("recipe:link", id, link)
    if title != "None":
        r.hset("recipe:title", id, title)
    if description != "None":
        r.hset("recipe:description", id, description)
    if date != "None":
        r.hset("recipe:date", id, date)
    if rating != "None":
        r.hset("recipe:rating", id, rating)
    if rating_count != "None":
        r.hset("recipe:rating_count", id, rating_count)
    if prep_time_seconds != "None":
        r.hset("recipe:prep_time_seconds", id, prep_time_seconds)
    if cook_time_seconds != "None":
        r.hset("recipe:cook_time_seconds", id, cook_time_seconds)
    if total_time_seconds != "None":
        r.hset("recipe:total_time_seconds", id, total_time_seconds)
    if servings != "None":
        r.hset("recipe:servings", id, servings)
    if calories != "None":
        r.hset("recipe:calories", id, calories)
    if carbohydrates != "None":
        r.hset("recipe:carbohydrates", id, carbohydrates)
    if cholesterol != "None":
        r.hset("recipe:cholesterol", id, cholesterol)
    if fat != "None":
        r.hset("recipe:fat", id, fat)
    if fiber != "None":
        r.hset("recipe:fiber", id, fiber)
    if protein != "None":
        r.hset("recipe:protein", id, protein)
    if saturated_fat != "None":
        r.hset("recipe:saturated_fat", id, saturated_fat)
    if sodium != "None":
        r.hset("recipe:sodium", id, sodium)
    if sugar != "None":
        r.hset("recipe:sodium", id, sugar)

print("finished migrating main recipe table")



# TODO cleanup script

cursor.execute("SELECT recipe_keyword.recipe, keyword.keyword FROM keyword JOIN recipe_keyword on keyword.id = recipe_keyword.keyword")
result = cursor.fetchall()

for row in result:
    recipe_id = str(row[0])
    keyword = str(row[1])
    
    r.sadd("recipe:keywords", keyword)
    r.sadd(f"recipe:{recipe_id}:keywords", keyword)



# TODO cleanup script

cursor.execute("SELECT recipe_author.recipe, author.name FROM author JOIN recipe_author ON author.id = recipe_author.author")
result = cursor.fetchall()

for row in result:
    recipe_id = str(row[0])
    author = str(row[1])
    
    r.sadd("recipe:authors", author)
    r.sadd(f"recipe:{recipe_id}:authors", author)



# TODO cleanup script
cursor.execute("SELECT recipe_image.recipe, image.image FROM image JOIN recipe_image ON image.id = recipe_image.image")
result = cursor.fetchall()

for row in result:
    recipe_id = str(row[0])
    image = str(row[1])
    
    r.sadd("recipe:images", image)
    r.sadd(f"recipe:{recipe_id}:images", image)



# TODO cleanup script
cursor.execute("SELECT recipe_ingredient.recipe, ingredient.ingredient FROM ingredient JOIN recipe_ingredient ON ingredient.id = recipe_ingredient.ingredient")
result = cursor.fetchall()

for row in result:
    recipe_id = str(row[0])
    ingredient = str(row[1])
    
    r.sadd("recipe:ingredients", ingredient)
    r.sadd(f"recipe:{recipe_id}:ingredients", ingredient)



# TODO cleanup script
cursor.execute("SELECT recipe_instruction.recipe, instruction.instruction FROM instruction JOIN recipe_instruction ON instruction.id = recipe_instruction.instruction")
result = cursor.fetchall()

for row in result:
    recipe_id = str(row[0])
    instruction = str(row[1])
    
    r.sadd("recipe:instructions", instruction)
    r.sadd(f"recipe:{recipe_id}:instructions", instruction)
