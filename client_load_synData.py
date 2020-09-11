import redis as Redis

r = Redis.Redis(host='redis-14092.re-cluster1.ps-redislabs.org', port=14092)

f = open("data/synData.csv", "r")

while True:
    l = f.readline().strip()
    if not l:
        break
    a = l.split(",")
    assert len(a) == 5, "Line does not have three columns"
    
    rix=a[0]
    tix=a[1]
    aix=a[2]
    vix=a[3]
    ts=a[4]
    r.execute_command("RG.TRIGGER", "upsert", rix, aix, tix, vix, ts)
    print("%s:%s:%s:%s" % (rix, aix, ts, vix))

f.close()