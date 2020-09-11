import json
import redis as Redis

r = Redis.Redis(host='redis-14092.re-cluster1.ps-redislabs.org', port=14092)

f = open("data/dump.txt", "r")

while True:
    l = f.readline().strip()
    if not l:
        break
    a = l.split("|")
    assert len(a) == 3, "Line does not have three columns"
    
    rid=a[0]
    questions=json.loads(a[1])
    for qid in questions:
        ts = questions[qid]['ts']
        v = questions[qid]['v']
        r.execute_command("RG.TRIGGER", "upsert", rid, qid, 0, v, ts)
        print("%s:%s:%s:%s" % (rid, qid, ts, v))

f.close()