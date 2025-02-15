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
        r.hset("link:remaining_follows", key, str(2))
    elif status == b"processing":
        r.hset("link:remaining_follows", key, str(2))
    elif status == b"download_failed":
        pass
    elif status == b"extraction_failed":
        pass
    elif status == b"processed":
        pass

