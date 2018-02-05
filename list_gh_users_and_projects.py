import os
import json
import csv
import sys
import config
from collections import defaultdict
from pymongo import MongoClient



#
#   Scan to get list of github users, and another list of projects
#
confg = config.Config(sys.argv[1])
client = MongoClient("mongodb://127.0.0.1:27017")
r_db = client[confg.config["mongodb_proc"]]


from signal import signal, SIGPIPE, SIG_DFL
signal(SIGPIPE, SIG_DFL) 
csv.field_size_limit(10000000)

users = set()
projects = set()
 
for f in r_db.issue_events.find():   # TODO
    users.add(f["actor"])
    projects.add(f["project_owner"] + "/" + f["project_name"])
for f in r_db.project_events.find():   # TODO
    users.add(f["actor"])
    projects.add(f["project_owner"] + "/" + f["project_name"])

csvwriter = csv.writer(open(confg.config["git_users"], "w"))
for u in sorted(users): csvwriter.writerow([u])
csvwriter = csv.writer(open(confg.config["git_projects"], "w"))
for p in sorted(projects): csvwriter.writerow([p])
