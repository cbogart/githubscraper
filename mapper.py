from collections import defaultdict
import csv

usermap = defaultdict(set)

def add_alias(mapping, a1, a2):
    if "@" in a1 and "@" not in a2:
        mapping[a1].add(a2)
    elif "@" in a2 and "@" not in a1:
        mapping[a2].add(a1)

def find_close_enough(emailfront, candidates):
    stripped = emailfront.replace(".","").replace(" ","").lower()
    for c in candidates:
        if c.lower() == stripped:
            print emailfront, "matches", c
            return c
    return None

def get_usermap(confg, include_pushes = False):
    if (include_pushes):
        pushcsv = csv.DictReader(open(confg["data_dir"]+"/aliases_pushes.csv", "r"))
        print "Reading pushes"
        for p in pushcsv:
            add_alias(usermap, p["alias2"], p["alias1"])
    print "Reading pulls"
    pushcsv = csv.DictReader(open(confg["data_dir"]+"/aliases_prs.csv", "r"))
    for p in pushcsv:
        add_alias(usermap, p["alias2"], p["alias1"])
    print "Erasing duplicates"

    # Forget any learned ambiguities
    forgets = set()
    for u in usermap:
        if len(usermap[u]) > 1: 
            closest = find_close_enough(u.split("@")[0], usermap[u])
            if closest is not None:
                usermap[u] = closest
            else: 
                forgets.add(u)
        else:
            usermap[u] = list(usermap[u])[0]
    for f in forgets: 
        del usermap[f]
    print "Done with heuristic"

