import read_ga
import pdb
import os
from dateutil.parser import parse
from datetime import timedelta
from collections import defaultdict
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
indexed = set()

config = json.load(open(configfile, "r"))
client = MongoClient("mongodb://127.0.0.1:27017")
r_db = client[config["events-db"]]
raw_scraped = client[config["mongodb_raw"]]
github_aliases = client['githubarchive-aliases']
canonical_project_list = r_db["CanonList"]
try:
    os.makedirs(config["events"])
except:
    pass

from signal import signal, SIGPIPE, SIG_DFL
signal(SIGPIPE, SIG_DFL) 

projects = set([p.strip().lower() for p in open(config["sample_set"], "r").readlines()])

def expand_to_include_aliases():
    project_raw_ids = defaultdict(list)
    project_aliases = set()
    canonicals = dict()
    rev_ids = dict()
    for row in canonical_project_list.find():
        project_raw_ids[row["id"]] = row["aliases"]
        project_aliases.update(row["aliases"])
        canonicals[row["id"]] = row["canonical"]
        rev_ids[row["canonical"]] = row["id"]
        
    return (project_aliases, project_raw_ids, canonicals, rev_ids)


class MongoWriter:
    def __init__(self, collection, upsert=[]):
        """Upsert is list of columns to use as unique ID"""
        self.coll = collection
        self.indexed = False
        self.upsert = upsert
    def set_cols(self, cols):
        self.column = cols
    def mkindex(self):
        if not self.indexed:
            print "Ix"
            r_db[self.coll].create_index([("github_project_id",1)])
            r_db[self.coll].create_index([("canonical_project",1)])
            r_db[self.coll].create_index([("project",1)])
            if len(self.upsert):
                r_db[self.coll].create_index([(k,1) for k in self.upsert])
            self.indexed = True
    def writerow(self, data, upsert = False):
        record = dict(zip(self.column, data))
        if len(self.upsert) > 0:
            try: self.mkindex()
            except: pass
            r_db[self.coll].replace_one({k: record[k] for k in self.upsert}, record, True)
        else:
            print "No index used for ", self.coll
            r_db[self.coll].insert(record);
            self.mkindex()

if __name__ == "__main__":
   print "Starting with", len(projects), "projects"
   (projects, project_ids, canonicals, rev_ids) = expand_to_include_aliases()
   print "Expanded to", len(projects), "aliases"

   #project_ids = {int(p["github_id"]):p["project"] for p in csv.DictReader(open("get_project_ids/project2github_id_map.csv", "r"))}
   #assert "openfl/openfl" in project_ids[8869463], "Didn't read project_ids in OK"
   #assert len(project_ids) == 34, "Wrong number of projects" + str(len(project_ids))
   project_keys = project_ids.keys()

   csvf = dict()
   csvf["PushEvent"] = MongoWriter("PushEvents", ["created_at","project"])
   csvf["GollumEvent"] = MongoWriter("GollumEvents", ["created_at", "project"])
   csvf["ForkEvent"] = MongoWriter("ForkEvents", ["actor", "created_at", "project"])
   csvf["CommitCommentEvent"] = MongoWriter("CommitCommentEvents", ["sha"])
   csvf["CreateEvent"] = MongoWriter("CreateEvents", ["project","created_at","what_type"])
   csvf["DeleteEvent"] = MongoWriter("DeleteEvents", ["project","created_at","what_type"])
   #csvf["WatchEvent"]  = MongoWriter("WatchEvents")
   
   csvf["PullRequestReviewCommentEvent"]  = MongoWriter("PullRequestReviewCommentEvent",["project","created_at","actor","position"])
   csvf["PublicEvent"]  = MongoWriter("PublicEvent", ["project","created_at"])
   csvf["MemberEvent"]  = MongoWriter("MemberEvent",["member","project","created_at"])
   csvf["MemberStatusEvent"]  = MongoWriter("MemberStatusEvent")
   csvf["ReleaseEvent"]  = MongoWriter("ReleaseEvent", ["project","created_at","tag","action"])
   errorf = open(config["events"] + "/errors.txt", "a")
   
   csvf["PushEvent"].set_cols(["event_type","github_project_id", "canonical_project", "project","actor","created_at","branch","shas","id"]);
   csvf["GollumEvent"].set_cols(["event_type","github_project_id", "canonical_project", "project","actor","created_at","title","action","html_url","sha","page_name","summary","id"]);
   csvf["ForkEvent"].set_cols(["event_type","github_project_id", "canonical_project", "project","actor","created_at","forked_to","id"]);
   csvf["CommitCommentEvent"].set_cols(["event_type","github_project_id", "canonical_project", "project","actor","created_at","commit_comment", "commit_comment_url", "sha","id"]);
   csvf["CreateEvent"].set_cols(["event_type","github_project_id", "canonical_project", "project","actor","created_at","what","what_type","id"]);
   csvf["DeleteEvent"].set_cols(["event_type","github_project_id", "canonical_project", "project","actor","created_at","what","what_type","id"]);
   #csvf["WatchEvent"].set_cols(["event_type","github_project_id", "canonical_project", "project","actor","created_at","action","id"]);
   
   csvf["PullRequestReviewCommentEvent"].set_cols(["event_type","github_project_id", "canonical_project", "project","actor","created_at","action","path","position","original_position","commit_sha","original_commit_sha", "body", "html_url", "pr_num","id"])
   csvf["PublicEvent"].set_cols(["event_type","github_project_id", "canonical_project", "project","actor","created_at","id"]);
   csvf["MemberEvent"].set_cols(["event_type","github_project_id", "canonical_project", "project","actor","created_at","action","member","id"]);
   csvf["MemberStatusEvent"].set_cols(["event_type","github_project_id", "canonical_project", "project","actor","created_at","action","manager","id"]);
   csvf["ReleaseEvent"].set_cols(["event_type","github_project_id", "canonical_project", "project","actor","created_at","action", "tag", "author", "target_commitish", "branch","id"]);
   
   ch = GithubArchive("{}")
   
   def safe_utf8(k):
       if k==None: return k
       return k.encode("utf-8")
   
   decoder = json.JSONDecoder()
   for f in sorted(os.listdir(sys.argv[2])):
     print f
     for line in gzip.open(sys.argv[2] + "/" + f,"r").readlines():
       pos = 0
       lin = line.strip().decode("utf-8","ignore")
       leng = len(lin)
       while pos < leng:
           try:
               (one_record, json_len) = decoder.raw_decode(lin[pos:].strip())
           except Exception, e:
               print "ERROR In decoding", e, pos, leng, json_len, len(one_record)
               if json_len == 0: json_len = 1
           pos += json_len
           try:
                   ch = GithubArchive(one_record)
                   if (ch.type() not in ["FollowEvent","GistEvent"] and (ch.repo_full_name().lower() in projects or ch.repo_id() in project_keys) and not ch.badrecord()):
                       canonical = ch.repo_full_name().lower()
                       if canonical not in projects: 
                           projects.add(canonical.lower())
                       ghid = -2
                       if ch.repo_id() is not None: 
                           ghid = int(ch.repo_id())
                           canonical = project_ids[ghid]
                       if ghid == -2 and canonical in canonicals:
                           canonical = canonicals[canonical]
                           ghid = rev_ids[canonical]
                       if ch.type() == "PushEvent":
                           csvf[ch.type()].writerow(["PushEvent", ghid, canonical, ch.repo_full_name(), ch.actor(), ch.created_at(), ch.into_branch(), ";".join(ch.pushed_shas()), ch.event_id()])
                       elif ch.type() == "GollumEvent":
                           for p in ch.wiki_pages():
                               csvf[ch.type()].writerow(["GollumEvent", ghid, canonical, ch.repo_full_name(), ch.actor(), ch.created_at(), safe_utf8(p["title"]), p["action"], 
                                     p.get("html_url",""), p["sha"], safe_utf8(p["page_name"]), safe_utf8(p["summary"]), ch.event_id()])
                       elif ch.type() == "ForkEvent":
                           csvf[ch.type()].writerow(["ForkEvent", ghid, canonical, ch.repo_full_name(), ch.actor(), ch.created_at(), ch.repo_forked_to(), ch.event_id()])
                       elif ch.type() == "CommitCommentEvent":
                           csvf[ch.type()].writerow(["CommitCommentEvent", ghid, canonical, ch.repo_full_name(), ch.actor(), ch.created_at(), safe_utf8(ch.commit_comment()), ch.commit_comment_url(), ch.commit_comment_sha(), ch.event_id()])
                       elif ch.type() == "CreateEvent":
                           csvf[ch.type()].writerow(["CreateEvent", ghid, canonical, ch.repo_full_name(), ch.actor(), ch.created_at(), safe_utf8(ch.create_what()), ch.create_what_type(), ch.event_id()])
                       elif ch.type() == "DeleteEvent":
                           csvf[ch.type()].writerow(["DeleteEvent", ghid, canonical, ch.repo_full_name(), ch.actor(), ch.created_at(),safe_utf8( ch.create_what()), ch.create_what_type(), ch.event_id()])
                       #elif ch.type() == "WatchEvent":
                           #csvf[ch.type()].writerow(["WatchEvent", ghid, canonical, ch.repo_full_name(), ch.actor(), ch.created_at(), ch.action()])
       
                       elif ch.type() == "PullRequestReviewCommentEvent":
                           csvf[ch.type()].writerow(["PullRequestReviewCommentEvent", ghid, canonical, ch.repo_full_name(), ch.actor(), ch.created_at(), "created",
                                                 ch.pr_review_comment("path"), ch.pr_review_comment("position"), ch.pr_review_comment("original_position"),
                                                 ch.pr_review_comment("commit_id"), ch.pr_review_comment("original_commit_id"), safe_utf8(ch.pr_review_comment("body")),
                                                 ch.pr_review_comment("html_url"), ch.pr_number(), ch.event_id()])
                       elif ch.type() == "PublicEvent":
                           csvf[ch.type()].writerow(["PublicEvent", ghid, canonical, ch.repo_full_name(), ch.actor(), ch.created_at(), ch.event_id()])
                       elif ch.type() == "MemberEvent":
                           csvf[ch.type()].writerow(["MemberEvent", ghid, canonical, ch.repo_full_name(), ch.actor(), ch.created_at(), ch.action(), ch.member(), ch.event_id()])
                       elif ch.type() == "ReleaseEvent":
                           csvf[ch.type()].writerow(["ReleaseEvent", ghid, canonical, ch.repo_full_name(), ch.actor(), ch.created_at(), ch.action(), safe_utf8(ch.tag_name()), ch.author(), ch.target_commitish(), ch.into_branch(), ch.event_id()])
                       else:
                           try:
                               j1 = ch.j.copy()
                               try:
                                  actor = ch.actor()
                               except:
                                  actor = ""
                               j1.update({"event_type": ch.type(),
                                      "project": ch.repo_full_name(),
                                      "github_project_id": ch.repo_id(),
                                      "canonical_project": canonical,
                                      "actor": actor,
                                      "created_at": ch.created_at()})
                               r_db[ch.type()].replace_one({"id":j1["id"]}, j1, True)
                               if ch.type() not in indexed:
                                   r_db[ch.type()].create_index([("id",1)])
                                   indexed.add(ch.type())
                           except Exception, e:
                               print "Skip record ", ch.type(), " at ", ch.created_at(), e
   
   
           except Exception, e:
               errorf.write("----------------")
               errorf.write("Skipping: " + str(type(e)))
               traceback.print_exc(file=errorf)
               errorf.write(line)
               errorf.write("================ " + str(e))
               errorf.write(ch.pp())

