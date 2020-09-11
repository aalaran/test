
def myLog(msg):
	execute('publish', 'log', msg)

def condition(args):
    execute('SUNIONSTORE', f"c:subcondA:{{{ hashtag()}}}",  f"a:1:{{{hashtag()}}}")
    execute('EXPIRE', f"c:subcondA:{{{hashtag()}}}",  60)
    subcondA = execute('SCARD',  f"c:subcondA:{{{hashtag()}}}")
    myLog(f"{hashtag()} subcondA {subcondA}")
    execute('SUNIONSTORE', f"c:subcondB:{{{hashtag()}}}",  f"a:2:{{{hashtag()}}}")
    execute('EXPIRE', f"c:subcondB:{{{hashtag()}}}",  60)
    subcondB = execute('SCARD', f"c:subcondB:{{{hashtag()}}}")
    myLog(f"{hashtag()} subcondB {subcondB}")
    execute('SINTERSTORE', f"c:subcondC:{{{hashtag()}}}",   f"c:subcondA:{{{hashtag()}}}",   f"c:subcondB:{{{hashtag()}}}")
    execute('EXPIRE', f"c:subcondC:{{{hashtag()}}}",  60)
    return  f"c:subcondC:{{{hashtag()}}}"

def count(args):
    matchingRespondentsSetName = condition(args)
    size = execute('SCARD', matchingRespondentsSetName)
    myLog(f"{hashtag()} matchingRespondentsSize {size}")
    return size

def dist(args):
    selectors = ["1:v","2:v"]
    matchingRespondentsSetName = condition(args)
    respondents = execute('SMEMBERS', matchingRespondentsSetName)
    tuples = []
    for r in respondents:
        selectorValues = execute("hmget", f"p:{r}:{{{hashtag()}}}", *selectors)
        myLog(f"r[{r}] selectorValues[{selectorValues}]")
        fixedValues = ["0" if x is None else x for x in selectorValues]
        myLog(f"r[{r}] fixedValues[{fixedValues}]")
        key = ",".join(fixedValues)
        myLog(f"key[{key}]")
        tuples.append(key)
    return tuples

def range(args):
    column = "1:ts"
    matchingRespondentsSetName = condition(args)
    respondents = execute('SMEMBERS', matchingRespondentsSetName)
    min = None
    max = None
    for r in respondents:
        value = execute("hget", f"p:{r}:{{{hashtag()}}}", column)
        myLog(f"r[{r}] value[{value}]")
        if value is not None:
            value = int(value)
            if min is None:
                min = value
            elif min > value:
                min = value
            if max is None:
                max = value
            elif max < value:
                max = value
    return {"min":min, "max":max}

def upsert(args):
    rix = args[1]
    aix = args[2]
    tix = args[3]
    vix = args[4]
    ts = args[5]

    if vix == "0":
        res = execute("hdel", f"p:{rix}:{{{hashtag()}}}", f"{aix}:v", f"{aix}:ts", f"{aix}:t")
        res = execute("srem", f"a:{aix}:{{{hashtag()}}}", rix)
    else:
        oldTs = execute("hget", f"p:{rix}:{{{hashtag()}}}", f"{aix}:ts")
        if oldTs is None or int(oldTs) < int(ts):
            res = execute("hset", f"p:{rix}:{{{hashtag()}}}", f"{aix}:v", vix, f"{aix}:ts", ts, f"{aix}:t", tix)
            res = execute("sadd", f"a:{aix}:{{{hashtag()}}}", rix)

GB('CommandReader',desc="count").map(count).aggregate(
            0,
            lambda a, r: a + r,
            lambda a, r: a + r
            ).register(trigger='count', mode='async')        

GB('CommandReader',desc="dist").map(dist).flatmap(lambda r: r).countby(lambda r : r).register(trigger='dist', mode='async')    

GB('CommandReader',desc="range").map(range).aggregate(
            {"min":None, "max":None},
            lambda a, r: 
                {
                "min":None if a["min"] is None  and r["min"] is None else a["min"] if r["min"] is None else r["min"] if a["min"] is None else min(a["min"], r["min"]), 
                "max":None if a["max"] is None  and r["max"] is None else a["max"] if r["max"] is None else r["max"] if a["max"] is None else max(a["max"], r["max"])
                }
            ,
            lambda a, r: 
                {
                "min":None if a["min"] is None  and r["min"] is None else a["min"] if r["min"] is None else r["min"] if a["min"] is None else min(a["min"], r["min"]), 
                "max":None if a["max"] is None  and r["max"] is None else a["max"] if r["max"] is None else r["max"] if a["max"] is None else max(a["max"], r["max"])
                }
            ,
            ).register(trigger='range', mode='async')    

GB('CommandReader',desc="upsert rix aix tix vix ts").map(upsert).register(trigger='upsert', mode='sync')
