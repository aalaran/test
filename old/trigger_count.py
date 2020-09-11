
def count(args):
    execute('SINTERSTORE', "tmpCond:leafA:{%s}" % hashtag(),  "v:%s:{%s}" % (298, hashtag()),  "nullSet:{%s}" % hashtag())
    execute('EXPIRE', "tmpCond:leafA:{%s}" % hashtag(),  60)
    execute('SINTERSTORE', "tmpCond:leafB:{%s}" % hashtag(),  "v:%s:{%s}" % (2803, hashtag()),  "nullSet:{%s}" % hashtag())
    execute('EXPIRE', "tmpCond:leafB:{%s}" % hashtag(),  60)
    execute('SINTERSTORE', "tmpCond:hashOfCond:{%s}" % hashtag(),  "tmpCond:leafA:{%s}" % hashtag(),  "tmpCond:leafB:{%s}" % hashtag())
    execute('EXPIRE', "tmpCond:hashOfCond:{%s}" % hashtag(),  60)
    return execute('SCARD', "tmpCond:hashOfCond:{%s}" % hashtag())

GB('CommandReader',desc="count").map(count).aggregate(0,
             lambda a, r: a + r,
             lambda a, r: a + r).register(trigger='count', mode='async')