import redis as Redis

r = Redis.Redis(host='redis-14092.re-cluster1.ps-redislabs.org', port=14092)
#res = r.execute_command("RG.TRIGGER", "count", 0)
#res = r.execute_command("RG.TRIGGER", "dist", 0)
#res = r.execute_command("RG.TRIGGER", "range", 0)
res = r.execute_command("RG.TRIGGER", "upsert", 1, 1, 2, 1, 5)
print(res)