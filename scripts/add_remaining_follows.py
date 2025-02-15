import redis

r = redis.Redis(
    host='localhost', 
    port=6379, 
    db=0,
)

keys = r.hkeys("link:status")

for key in keys:
    status = r.hget("link:status", key)

    if status == b"waiting":
        pass
    elif status == b"processing":
        pass
    elif status == b"download_failed":
        pass
    elif status == b"extraction_failed":
        pass
    elif status == b"processed":
        pass
    else:
        print(status)

