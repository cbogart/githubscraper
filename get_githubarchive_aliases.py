import read_ga
import pdb
import os
from dateutil.parser import parse
from datetime import timedelta
import simplejson
import csv
import gzip
import traceback
import sys
import json
import datetime
import jsonpath_rw
from githubarchive import GithubArchive, GithubArchiveFormatError

import pymongo
from pymongo import MongoClient

configfile = sys.argv[1]

config = json.load(open(configfile, "r"))
client = MongoClient("mongodb://127.0.0.1:27017")
r_db = client["githubarchive-aliases"]

from signal import signal, SIGPIPE, SIG_DFL
signal(SIGPIPE, SIG_DFL) 

class MongoWriter:
    def __init__(self, collection):
        self.written = False
        self.coll = collection
    def set_cols(self, cols):
        self.column = cols
    def writerow(self, data):
        record = dict(zip(self.column, data))
        r_db[self.coll].update(record, record, upsert = True)
        if not self.written:
            r_db[self.coll].create_index([("full_name",1)])
            r_db[self.coll].create_index([("github_project_id",1)])
        self.written = True

csvf = dict()
csvf["RepoAliases"] = MongoWriter("RepoAliases")
csvf["RepoAliases"].set_cols(["github_project_id","full_name"])

errorf = open(config["events"] + "/errors_aliases.txt", "a")

ch = GithubArchive("{}")

def safe_utf8(k):
    if k==None: return k
    return k.encode("utf-8")

decoder = json.JSONDecoder()
for f in sorted(os.listdir(sys.argv[2])):
 if f.endswith(".gz"):
  print f
  for line in gzip.open(sys.argv[2] + "/" + f,"r").readlines():
    pos = 0
    lin = line.strip().decode("utf-8","ignore")
    leng = len(lin)
    while not pos == leng:
        try:
            (one_record, json_len) = decoder.raw_decode(lin[pos:].strip())
        except Exception, e:
            pdb.set_trace()
        pos += json_len
        ch = GithubArchive(one_record)
        if ch.repo_id() is not None:
            csvf["RepoAliases"].writerow([ch.repo_id(), ch.repo_full_name().lower()])


