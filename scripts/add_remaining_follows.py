import redis

r = redis.Redis(
    host='localhost', 
    port=6379, 
    db=0,
)

keys = r.hkeys("link:status")

for key in keys:
    status = r.hget("link:status", key)

    if status == "waiting":
        pass
    elif status == "processing":
        pass
    elif status == "download_failed":
        pass
    elif status == "extraction_failed":
        pass
    elif status == "processed":
        pass
    else:
        print(status)

