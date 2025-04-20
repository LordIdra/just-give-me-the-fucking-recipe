import csv
import redis

#r = redis.Redis(
#    host='localhost', 
#    port=6383, 
#    db=0,
#)

nutrients = {}
with open("nutrient.csv", "r") as file:
    csv_reader = csv.reader(file)
    fields = next(csv_reader)
    for row in csv_reader:
        id = row[3]
        name = row[1]
        unit = row[2]
        nutrients[id] = name
        #r.hset("nutrient:unit", name, unit)
x = ""
for (k, v) in nutrients:
    x += v + ", "
print(x)

ingredients = {}
with open("fndds_ingredient_nutrient_value.csv", "r") as file:
    csv_reader = csv.reader(file)
    fields = next(csv_reader)
    for row in csv_reader:
        description = row[1]
        nutrient = nutrients[row[2]]
        value = row[3]
        kind = row[4]
        if kind != "Foundation" and kind != "SR Legacy":
            continue
        if not description in ingredients:
            ingredients[description] = { "type": kind }
        ingredients[description][nutrient] = value

id = 0
for ingredient, nutrient_map in ingredients.items():
    #print(id, ingredient)
    #for nutrient, value in nutrient_map.items():
        #r.hset(f"ingredient:{id}:nutrients", nutrient, value)
        #print(f"ingredient:{id}:nutrients " + nutrient + " " + value)
    #r.set(f"ingredient:{id}:type", nutrient_map["type"])
    #print(f"ingredient:{id}:type", nutrient_map["type"])
    id += 1

