import redis

r = redis.Redis(
    host='localhost', 
    port=6379, 
    db=0,
)

keys = r.hkeys("link:status")
print(keys)

