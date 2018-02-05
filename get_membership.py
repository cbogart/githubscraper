import sys
import re
import pdb
from pymongo import MongoClient
from dateutil.parser import parse
from datetime import timedelta
import datetime
from collections import defaultdict
import glob
import csv
from config import Config
from mapper import get_usermap, usermap
from util import forceString

confg = Config(sys.argv[1])
r_db = confg.proc_db()
 

end_of_days = parse("2970-01-01")
participation = defaultdict(lambda: end_of_days)
first_part = defaultdict(str)
membership = defaultdict(lambda: end_of_days)
first_memb = defaultdict(str)
projects = set()
at_count = defaultdict(set)
user_count = defaultdict(set)
def register(r):
    actor = r["actor"]
    if actor is None: return   
    if "@" in actor and actor in usermap:
        actor = usermap[actor]
    rectype = r["rectype"]
    owner = r["project_owner"] 
    pname = r["project_name"] 
    dt = r["time"]
    project = owner + "/" + pname
    key = (owner, pname, actor)
    if owner + "/" + pname not in projects:
        projects.add(project)
    if dt < participation[key]:
        participation[key] = dt
        first_part[key] = rectype
    if actor==owner and dt < membership[key]:
        membership[key] = dt
        first_memb[key] = "owner"
    elif dt < membership[key] and rectype in ["pull_request_merged", "commit_messages", "pull_request_commit"]:
        membership[key] = dt
        first_memb[key] = rectype
    if "@" in forceString(actor):
        at_count[project].add(forceString(actor))
        at_count["all"].add(forceString(actor))
    user_count[project].add(forceString(actor))
    user_count["all"].add(forceString(actor))

interesting_projects = [k.lower() for k in confg.get_sample_set_project_names()]
#interesting_projects = set()
#burstsf = csv.DictReader(open("micro_burst_congruence.csv","r"))
#for burstday in burstsf:
    #interesting_projects.add((burstday["project_owner"] , burstday["project_name"]))
get_usermap(confg)

for (ix, pr) in enumerate(interesting_projects):
    p = pr.split("/")
    if ix % 100 == 0:
        print ix, "of", len(interesting_projects), ":", p
    for r in r_db.issue_events.find({"project_owner": p[0], "project_name": p[1]}):
        register(r)
    for r in r_db.project_events.find({"project_owner": p[0], "project_name": p[1]}):
        register(r)


csvf = csv.writer(open(confg["data_dir"] + "/participation.csv", "w"))
csvf.writerow(["owner","project","actor","first_participation_date","first_participation_action","first_member_date","first_member_action"])
for k in sorted(participation.keys()):
  try:
    [owner, project, actor] = k
    row = [owner, project, forceString(actor), participation[k].isoformat(), first_part[k] ,
           membership[k].isoformat() if membership[k] != end_of_days else "",
           first_memb[k]]
    csvf.writerow(row)
  except Exception, e:
    print "Problem: skipping ", owner, project, forceString(actor), e

csvf2 = csv.writer(open(confg["data_dir"] + "/atcount2.csv","w"))
csvf2.writerow(["project","Users","Users with @ in name"])
for p in user_count:
    csvf2.writerow([p, len(user_count[p]), len(at_count[p])])
