from bs4 import BeautifulSoup
import requests
import os.path

headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}

with open("links.txt", "r") as file:
    links = file.readlines()

for link in links:
    try:
        name = link.replace("\n", "").replace("https://", "").replace("http://", "").replace("/", ".").replace("www.", "")
        path = f"pages/{name}"
        if os.path.isfile(path):
            continue
        print(f"Downloading {name}...")
        r = requests.get(link, headers = headers)
        if r.status_code == 200:
            content = BeautifulSoup(str(r.content), features = "lxml").prettify()
            with open(path, "w+") as file:
                file.write(content)
        else:
            print(f"Failed to download with status {r.status_code}")
    except Exception as e:
        print(e)

