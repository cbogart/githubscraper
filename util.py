
def fixtime(t):
    return t #return t.replace(tzinfo=pytz.utc) if t is not None else None

def forceString(v):
    try:
        return str(v)
    except:
        return v.encode('utf-8')

def get_sample_set_project_names(setname):
    return [r.strip() for r in open(config["sample_set"], "r").readlines()]


