from googlesearch import search

with open("links.txt", "r") as file:
    results = file.readlines()

for link in search("special fried rice recipe", num=40, stop=40, pause=2):
    line = link + "\n"
    if line not in results:
        with open("links.txt", "a") as file:
            file.write(line)

