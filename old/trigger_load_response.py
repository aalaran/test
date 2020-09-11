def load_response(args):
    rid = args[1]
    qid = args[2]
    tid = args[3]
    vid = args[4]
    ts = args[5]

    if vid == 0:
        #even if t_ does not exist delete it
        execute("hdel", "p:%s" % rid, "v:%s" % qid, "ts:%s" % qid, "t:%s" % qid)
    else:
        execute("hset", "p:%s" % rid, "v:%s" % qid, vid, "ts:%s" % qid, ts)
        execute("sadd", "v:%s:{%s}" % (qid, hashtag()), rid)
        if tid != 0:
            execute("hset", "p:%s" % rid, "t:%s" % qid, tid)
            execute("sadd", "t:%s:{%s}" % (qid, hashtag()), rid)
        

GB('CommandReader',desc="load_response rid qid tid vid ts").map(load_response).register(trigger='load_response', mode='sync')
