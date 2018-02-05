import pdb
import csv
import json
import os
import sys
import pymongo
from pymongo import MongoClient
from config import Config
from util import forceString
import datetime

confg = Config(sys.argv[1])
confg.ensure_dir("data_dir")
confg.ensure_dir("issues_dir")
csv.field_size_limit(sys.maxsize)

client = confg.mongo_client()
r_db = client[confg["events-db"]]
r_db_proc = client[confg["mongodb_proc"]]

def issuecount(proj_owner, proj_name):
    issues = [int(row["issueid"]) for row in r_db_proc.issue_events.find({"rectype": "issue_title", "project_owner": proj_owner, "project_name": proj_name, "time": {"$lt": datetime.datetime(2017, 10, 1, 0, 0, 0)}})]
    if len(issues) > 0: return max(issues)
    return 0

def find_title_commit_matches(o,p,issueid):
    aliases = set()
    coltypes = ["pull_request_commit", "pull_request_title"] 
    for row in r_db_proc.issue_events.find({"project_owner": o, "project_name": p, "issueid": int(issueid)}).sort("time"):
        if row["rectype"] in coltypes and ("@" in row["actor"] or row["rectype"] != "pull_request_commit") and (row["actor"] != "ghost"):
            aliases.add(row["actor"])
            coltypes.remove(row["rectype"])   # We only want the first instance of each type
    return aliases

def match_pushes():
    outf = csv.writer(open(confg["data_dir"] + "/aliases_pushes.csv","w"))
    outf.writerow(["provenance", "alias1","alias2"])
    commit_authors = {}    # c_a[project][sha] = email
    for p in r_db.PushEvents.find():
        if ";" not in p["shas"]:
            if p["project"] not in commit_authors:
                mapping = {}
                
                for c in r_db_proc.project_events.find({"project_owner": p["project"].split("/")[0],
                             "project_name": p["project"].split("/")[1]}):
                    if "@" in c["actor"]:
                        mapping[c["action"]] = c["actor"]
                commit_authors[p["project"]] = mapping
            if p["shas"] in commit_authors[p["project"]]:
                outf.writerow(["push event", p["actor"], commit_authors[p["project"]][p["shas"]]])

def match_pull_requests():
    outf = csv.writer(open(confg["data_dir"] + "/aliases_prs.csv","w"))
    outf.writerow(["provenance", "alias1","alias2"])
    matches = 0
    for project in confg.get_sample_set_project_names():
        [po,pn] = project.split("/")
        maxissue = issuecount(po,pn)
        if maxissue > 0:
            for i in range(1,maxissue+1):
                aliases = find_title_commit_matches(po,pn,i)
                if len(aliases) > 1:
                    matches += 1
                    outf.writerow([project + ":issue:" + str(i)] + 
                        [forceString(k) for k in list(aliases)])

class Aliasmerge:
    def __init__(self, savefile):
        self.savefile = savefile
        self.load()
    def lookup_canonical(self, user):
        if user in self.matches_rev:
            key = self.matches_rev[user]
            nonemail = sorted([a for a in self.matches[key]["aliases"] if "@" not in a])
            if len(nonemail) > 0: return nonemail[0]
            return sorted(list(self.matches[key]["aliases"]))[0]
        else:
            return user
    def add_alias(self, alias1, alias2, provenance):
        if alias1 in self.matches_rev and alias2 in self.matches_rev and self.matches_rev[alias1] != self.matches_rev[alias2]:
            self.handle_conflict(alias1, alias2, provenance)
        elif alias1 in self.matches_rev and alias2 in self.matches_rev and self.matches_rev[alias1] == self.matches_rev[alias2]:
            pass # Nothing to do here
        else:
            key = self.matches_rev.get(alias1, self.matches_rev.get(alias2, len(self.matches)))
            self.matches_rev[alias1] = key
            self.matches_rev[alias2] = key
            if key not in self.matches: self.matches[key] = { "aliases": [],  "provenances": [] }
            self.matches[key]["aliases"].append(alias1)
            self.matches[key]["aliases"].append(alias2)
            self.matches[key]["aliases"] = list(set(self.matches[key]["aliases"]))
            self.matches[key]["provenances"].append(provenance + " (" + alias1 + "=" + alias2 + ")")
            self.matches[key]["provenances"] = list(set(self.matches[key]["provenances"]))
    def handle_conflict(self, alias1, alias2, provenance):
        """If some source says 2 aliases are the same, but they're in separate sets already"""
        key1 = self.matches_rev[alias1]
        key2 = self.matches_rev[alias2]
        merged = set(self.matches[key1]["aliases"] + self.matches[key2]["aliases"])
        non_at = {al for al in merged if "@" not in al}
        if len(non_at) < 2:   # If there's only one non-email address in both alias sets
            self.matches[key1]["aliases"] = list(merged)
            self.matches[key1]["provenances"].append(provenance)
            self.matches[key1]["provenances"].extend(self.matches[key2]["provenances"])
            self.matches[key1]["provenances"] = list(set(self.matches[key1]["provenances"]))
            del self.matches[key2]
            for a in self.matches[key1]["aliases"]:
                self.matches_rev[a] = key1
        else:
            print "Not aliasing", alias1, alias2, "despite suggestion by", provenance
            print "   ",alias1,"->",self.aliases_of(alias1),self.provenance_of(alias1)
            print "   ",alias2,"->",self.aliases_of(alias2),self.provenance_of(alias2)
    def provenance_of(self,alias1):
        return self.matches[self.matches_rev[alias1]]["provenances"]
    def aliases_of(self,alias1):
        return self.matches[self.matches_rev[alias1]]["aliases"]
    def load(self):
        if os.path.exists(self.savefile):
            self.matches = json.load(open(self.savefile,'rb'))
            self.matches_rev = dict()
            for m in self.matches:
                for a in self.matches[m]["aliases"]:
                    self.matches_rev[a]=m
        else:
            self.matches = dict()
            self.matches_rev = dict()
    def save(self):
        json.dump(self.matches, open(self.savefile,'wb'), indent=4)

def merge_alias_files():
    am = Aliasmerge(confg["data_dir"] + "/aliases.json")
    csvr = csv.DictReader(open(confg["data_dir"] + "/actor_info.csv","r"))
    for r in csvr:
        if r["login"].strip() != "" and r["email"].strip() != "" and r["fake"] != "1":
            am.add_alias(r["login"],r["email"],"Github profile")
        #am.add_alias(r["login"],r["name"],"Github profile")    Risky ambiguity
    csvr = csv.DictReader(open(confg["data_dir"] + "/aliases_prs.csv","r"))
    for r in csvr:
        am.add_alias(r["alias1"],r["alias2"],r["provenance"])
    csvr = csv.DictReader(open(confg["data_dir"] + "/aliases_pushes.csv","r"))
    for r in csvr:
        am.add_alias(r["alias1"],r["alias2"],r["provenance"])
    am.save()

if __name__=="__main__":
    match_pushes()
    match_pull_requests()
    #merge_alias_files()

