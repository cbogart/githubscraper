import mysql.connector
from dateutil.parser import parse
from collections import defaultdict
import traceback
import datetime
import urllib2
import pdb
from config import Config
import re
import clone_all
import clone_analyzer
import pytz
import base64
import json
import time
import os
import datetime
import sys
import csv
import git_comment_conventions
from dateutil.relativedelta import relativedelta


def get_dotted(dic, name, default=None):
    names = name.split(".")
    sofar = dic
    for n in names:
        if sofar == default or sofar is None or n not in sofar: return default
        sofar = sofar[n]
    return sofar
        
def get_fallback(dic, name, default=None):
    names = name.split(",")
    for n in names:
        r = get_dotted(dic, n, default=default)
        if r is not default: return r
    return default

fd = { "a": { "b": 1, "c": 1 }, "d": { "e": 1 }, "f": 2 }
assert fd["a"]["b"] == 1, "regular dict access"
assert get_dotted(fd,"f") == 2, "dotted 0"
assert get_dotted(fd,"a.b") == 1, "dotted 1"
assert get_dotted(fd,"b") == None, "dotted 0 fail"
assert get_dotted(fd,"b",default=44) == 44, "dotted 0 fail default"
assert get_dotted(fd,"a.q") == None, "dotted 1 fail"
assert get_fallback(fd, "f") == 2, "fallback 0"
assert get_fallback(fd, "a.b") == 1, "fallback 1"
assert get_fallback(fd, "a.q") == None, "fallback 1 fail"
assert get_fallback(fd, "a.q", default="None") == "None", "fallback 1 fail default"
assert get_fallback(fd, "a.q,a.b") == 1, "fallback 1 ok"
assert get_fallback(fd, "a.b,a.q") == 1, "fallback 1 ok irr"

def now(): datetime.datetime.now(pytz.timezone("GMT")).isoformat()

def fixtime(t):
    return t #return t.replace(tzinfo=pytz.utc) if t is not None else None

def fixUnicode(v):
    if type(v) is str:
        return unicode(v,'utf-8',errors="replace")
    else:
        return v

def forceString(v):
    try:
        return str(v)
    except:
        return v.encode('utf-8', errors="replace") #unicode(v, 'utf-8', errors="replace")

class MyMongoDb:
    def __init__(self, confg, owner, project):
        self.indexed = dict()
        self.confg = confg
        self.owner = owner
        self.project = project
    def db(self): return None   # override me
    def collection(self, name): return self.db()[name]
    def __getitem__(self, name): return self.db()[name]
    def upsert(self, collection, row, anno=None):
        if collection not in self.table_keys:
            print collection, " NOT IN self.table_keys  Fix and paste in:"
            print ' "' + collection + '": ["project_owner", "project_name",',
            print '","'.join(row.keys()), "],"
            pdb.set_trace()
        keys = self.table_keys[collection]
        row2 = { k:fixUnicode(row[k]) for k in row}
        row2["project_owner"] = self.owner
        row2["project_name"] = self.project
        if anno is not None:
            anno(row2) 
        key = { k: row2[k] for k in keys }
        self.db()[collection].update(key, row2, upsert=True)
        if collection not in self.indexed:
            self.indexed[collection] = 1
            self.db()[collection].create_index([(k, 1) for k in keys])
            

class ProcDb(MyMongoDb):
    table_keys = {
       "issue_events": ["project_owner", "project_name", "rectype", "issueid", "uid"],
       "project_events":["project_owner", "project_name", "rectype", "uid", "time","actor"],
       "readmes": ["project_owner", "project_name", "title"]
    }
    def db(self): return self.confg.proc_db()

    def list_issues(self):
        return self.db()["issue_events"].find({
                   "project_owner": self.owner,
                   "project_name": self.project,
                   "rectype": "issue_title",
                   }).sort("time",1)

    def summarize_all(self):
        for i in self.list_issues():
            print "********", i["issue_type"], " #", i["issueid"]
            self.summarize_issue(i["issueid"])

    def summarize_issue(self, issueid):
        for row in self.db()["issue_events"].find({
                   "project_owner": self.owner,
                   "project_name": self.project,
                   "issueid": int(issueid)}).sort("time",1):
           try:
            print row["rectype"], row["time"], forceString(row["actor"]), \
                   (forceString(row.get("title","")) + forceString(row.get("text",""))[:30]) \
                   .replace("\n"," ").replace("\r"," ")
           except:
            pdb.set_trace()
            print row["time"]

class RawDb(MyMongoDb):
    table_keys = {
       "git_commits": ["project_owner", "project_name", "sha"], 
       "gh_commits": ["project_owner", "project_name", "sha"], 
       "issue_info": ["project_owner", "project_name", "number"], 
       "issue_info_no_closer": ["project_owner", "project_name", "number"], 
       "pull_request_info": ["project_owner", "project_name", "number"], 
       "pull_request_info_no_closer": ["project_owner", "project_name", "number"], 
       "pr_commits": ["project_owner", "project_name", "issueid", "sha"],
       "pr_commit_comment": ["project_owner", "project_name", "issueid", "commit_id", "created_at", "id"],
       "commit_comments": ["project_owner", "project_name", "id", "commit_id"],
       "issue_comments": ["project_owner", "project_name", "id", "issueid"],
       "repo_info": ["project_owner", "project_name", "id"],
       "label_info": ["project_owner","project_name","issueid","id"],
       "milestones": ["project_owner","project_name","number","id"],
       "releases": ["project_owner", "project_name", "id", "tag_name"],
       "forks": ["project_owner", "project_name", "id", "full_name"],
       "deployments": ["project_owner", "project_name", "id"],
       "requested_reviewers": ["project_owner", "project_name", "issueid"],
       "reviews": ["project_owner", "project_name", "issueid"],
       "assignees": ["project_owner", "project_name", "login"],
       "readme": ["project_owner", "project_name", "path"] 
    }
    def db(self): return self.confg.raw_db()

    def fork_owner(self, issueid):
        rec = self.db()["pull_request_info"].find_one({
             "project_owner": self.owner,
             "project_name": self.project,
             "number": int(issueid)})
        return get_dotted(rec, "head.repo.owner.login")

    def list_issues(self):
        return [int(i["number"]) for i in self.db()["issue_info"].find({
                   "project_owner": self.owner,
                   "project_name": self.project
                   }).sort("number",1)]

    def get_pr_commits(self, pr):
        return self.db()["pr_commits"].find({
               "project_owner": self.owner,
               "project_name": self.project,
               "issueid": int(pr)})

    def is_pr(self, issuenum): 
        return self.issue_type(issuenum) == "pull"

    def issue_type(self, issuenum):
        rec1 = self.db()["issue_info_no_closer"].find_one({
               "project_owner": self.owner,
               "project_name": self.project,
               "number": int(issuenum)}) 
        return "pull" if rec1 is not None and "pull_request" in  rec1 else "issue"

    def get_prs(self):
        return list({r["number"] for r in 
            self.db()["pull_request_info"].find({
               "project_owner": self.owner,
               "project_name": self.project})})

