import mysql.connector
import pickle
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
from git_mongo import *
import os
import datetime
import sys
import csv
import git_comment_conventions
from github_auth import GhAuth
from dateutil.relativedelta import relativedelta

now_time = now()

def parse_header_links(value):
    """Return a dict of parsed link headers proxies.

    i.e. Link: <http:/.../front.jpeg>; rel=front; type="image/jpeg",<http://.../back.jpeg>; rel=back;type="image/jpeg"
    Adapted from https://github.com/kennethreitz/requests/  utils.py
    """

    links = []
    rels = {}
    replace_chars = " '\""

    for val in re.split(", *<", value):
        try:
            url, params = val.split(";", 1)
        except ValueError:
            url, params = val, ''
        link = {}
        link["url"] = url.strip("<> '\"")
        for param in params.split(";"):
            try:
                key, value = param.split("=")
            except ValueError:
                break
            link[key.strip(replace_chars)] = value.strip(replace_chars)

        if ("rel" in link): rels[link["rel"]] = link["url"]
        links.append(link)
    return rels

assert parse_header_links('<http://xyzzy.com>; rel="smash"')["smash"] == "http://xyzzy.com", "parse_header_links test"

class Scraper:
    def __init__(self, confg, owner, project, ghauth):
        self.confg = confg
        self.owner = owner
        self.project = project
        self.ghauth = ghauth
        self.rawdb = RawDb(confg, owner, project)
        self.procdb = ProcDb(confg, owner, project)
      

    def err_log(self, context, e):
        self.rawdb["errors"].insert(
          {"project_owner": self.owner,
           "project_name": self.project,
           "context": context,
           "error": traceback.format_exc(),
           "time": now_time})

    def get_readme_like_files_from_github(self):
        url = "https://api.github.com/repos/%s/%s/contents" % (self.owner, self.project)
        site = self.ghauth.geturl(url)
        content = site.read()
        rateinfo = site.info()
        filesinfo = json.loads(content)

        for parsed in filesinfo:
            if (parsed["type"] == "file" and (parsed["path"].split(".")[-1] in ["txt","rst","md"] or "." not in parsed["path"])
                        and not(parsed["path"].endswith("LICENSE") or parsed["path"].endswith("Makefile"))):
                url2 = "https://api.github.com/repos/%s/%s/commits?path=%s" % (self.owner, self.project, parsed["path"])
                content2 = self.ghauth.geturl(url2).read()
                parsed2 = json.loads(content2)
    
                url3 = "https://raw.githubusercontent.com/%s/%s/%s/%s" % (self.owner, self.project, parsed2[0]["sha"], parsed["path"])
                content3 = self.ghauth.geturl(url3).read()
            
                row =        { "rectype": "readme",
                               "project_owner": self.owner,
                               "project_name":self.project, 
                               "title":parsed["name"],
                               "cmu_last_retrieval": now_time,
                               "actor": get_fallback(parsed2[0], "author.login,commit.author.email"),
                               "time":parse(get_dotted(parsed2[0],"commit.author.date", default="1970-01-01")),
                               "action":parsed["sha"],
                               "sha":parsed["sha"],
                               "provenance":"api.github.com",
                               "text":content3}
                self.procdb.upsert("readmes", row, anno=self.annotate)
                self.ghauth.awaitThrottle(rateinfo)
    

    def register_commit_comments(self, pr, row):
        self.register_commit_comments_raw(pr, row)
        self.register_commit_comments_proc(pr, row)

    def register_commit_comments_raw(self, pr, row):
        if pr is not None: row["issueid"] = int(pr)
        self.rawdb.upsert("commit_comments", row)

    def register_commit_comments_proc(self, pr, row):
        procrow = { "project_owner": self.owner,
                "project_name":self.project, 
                "id":row["id"], 
                "rectype": "commit_comments",
                "sha":row["commit_id"],
                "action":row["commit_id"],
                "actor":get_dotted(row,"user.login", default="ghost"),
                "time":parse(row["created_at"]),
                "title":str((row["position"],row["line"],row["path"])),
                "provenance":"api.github.com",
                "uid": row["id"],
                "text":row["body"]}
        if pr is None:
            self.procdb.upsert("project_events", procrow, anno=self.annotate)
        else:
            procrow["issueid"] = int(pr)
            self.procdb.upsert("issue_events", procrow, anno=self.annotate)

    
    def load_commit_info_from_clone(self):
        if not hasattr(self, "clone_shas"):
            clone_all.clone(self.confg, self.owner + "/" + self.project)
            repo = clone_analyzer.get_repo_object(self.confg, self.owner + "/" + self.project)
            self.clone_shas = dict()
            for (ix, cmt) in clone_analyzer.yield_all_commits(repo):
                key = {"project_owner":self.owner, "project_name":self.project, "sha":cmt.hexsha}
                row = key.copy()
                row["parent_count"] = len(cmt.parents)
                row["parents"] = [c.hexsha for c in cmt.parents]
                row["message"] = cmt.message
                row["cmu_last_retrieval"] = now_time
                stats = repo.commit(row["sha"]).stats
                row["paths"] = json.dumps(stats.files.keys()),
                row["insertions"] = stats.total["insertions"]
                row["deletions"] = stats.total["deletions"]
                self.rawdb.upsert("git_commits", row)
                self.clone_shas[row["sha"]] = row
        return self.clone_shas

    def get_commit_info_from_github(self):
        self.get_query("https://api.github.com/repos/%s/%s/commits?per_page=100" 
              % (self.owner, self.project), 
                 self.register_commit, "gh_commits")

    def register_commit(self, row):
        clone_sha =  self.load_commit_info_from_clone().get(row["sha"],{})
        paths = clone_sha.get("paths",["(unknown)"])
        row_title = {"rectype": "commit_messages",
                      "project_owner": self.owner,
                      "project_name":self.project, 
                      "actor": get_fallback(row, "author.login,commit.author.email"),
                      "commit_author": get_dotted(row, "commit.author"),
                      "commit_committer": get_dotted(row,"commit.committer"),
                      "committer_login": get_fallback(row, "committer.login,commit.committer.email"),
                      "author_login": get_fallback(row, "author.login,commit.author.email"),
                      "time":parse(get_dotted(row,"commit.author.date",default="1970-01-01")),
                      "paths": paths,
                      "insertions":clone_sha.get("insertions",None),
                      "deletions":clone_sha.get("deletions",None),
                      "action":row["sha"],
                      "sha":row["sha"],
                      "uid": row["sha"],
                      "provenance":"api.github.com,git_clone",
                      "text":row["commit"]["message"],
                      "message":row["commit"]["message"]};
        self.procdb.upsert("project_events", row_title, anno=self.annotate)

    def get_query_single_page(self, url, callee, context, more_args=[]):
            print url
            self.ghauth.query_page(more_args, url, callee)

    def get_too_long_query(self, url, callee, context, more_args=[]):
        print url
        try:
            self.ghauth.query_pages(more_args, url, callee)
        except ValueError, ve:
            print ve
            print "Retrying once!"

    # TODO: add an TOO_MANY_PAGES exception
    def get_query(self, url, callee, context, more_args=[]):
        #try:
        print url
        try:
            self.ghauth.query_pages(more_args, url, callee)
        except ValueError, ve:
            print ve
            print "Retrying once!"
            self.ghauth.query_pages(more_args, url, callee)
        #except Exception, e:
        #    print context, e
        #    pdb.set_trace()
        #    self.err_log(context, e)

    def get_all_pr_info_from_github(self):
        self.get_query("https://api.github.com/repos/%s/%s/pulls?state=all&per_page=100" % (self.owner, self.project), 
                 self.register_pr_info, "allpulls")
        # now get commit info  
        # now get commit comment info    get_all_pr_comments_from_github

    def get_all_pr_comments_from_github(self):
        self.get_query("https://api.github.com/repos/%s/%s/pulls/comments?per_page=100" % (self.owner, self.project), 
            self.register_pr_commit_comment, "allpull_comments", more_args=[None])

    def get_issue_count_from_github(self):
        self.highest = 0
        def save_highest(row): 
            self.highest=int(row["number"])
        self.get_query_single_page("https://api.github.com/repos/%s/%s/issues?state=all&per_page=1" % (self.owner, self.project), 
           save_highest, "issuecount") 
        print "Issue count is ", self.highest
        return self.highest

    def get_project_info_from_github(self):
        self.get_query("https://api.github.com/repos/%s/%s" % (self.owner, self.project), 
           self.register_repo_info, "project_info")

    def get_all_pr_info_from_github_no_closer(self):
        self.get_query("https://api.github.com/repos/%s/%s/pulls?state=all&per_page=100" % (self.owner, self.project), 
           self.register_pr_info_no_closer, "pulls_lite")

    def get_all_issue_info_from_github_no_closer(self):
        self.get_query("https://api.github.com/repos/%s/%s/issues?state=all&per_page=100" % (self.owner, self.project), 
           self.register_issue_info_no_closer, "issues_lite")

    def get_all_issue_info_from_github(self):
        self.get_query("https://api.github.com/repos/%s/%s/issues?state=all&per_page=100" % (self.owner, self.project), 
           self.register_issue_info, "allissues")

    def get_issue_info_from_github(self, pr): 
        self.get_query("https://api.github.com/repos/%s/%s/issues/%s" % (self.owner, self.project, pr), 
            self.register_issue_info,
            "issue" + str(pr))

    def get_pr_info_from_github(self, pr):
        self.get_query("https://api.github.com/repos/%s/%s/pulls/%s" % (self.owner, self.project, pr), 
            self.register_pr_info, "pull" + str(pr))

    def get_pr_commit_comments_from_fork(self, fork_owner, sha, pr):
        self.get_query("https://api.github.com/repos/%s/%s/commits/%s/comments?per_page=100" % (fork_owner, self.project, sha), 
            self.register_pr_commit_comment, "pullf" + str(pr), more_args=[pr])

    def get_pr_commit_comments_from_head(self, head_owner):
        self.get_query("https://api.github.com/repos/%s/%s/comments?per_page=100" % (head_owner, self.project), 
            self.register_pr_commit_comment, "pull_head", more_args=[None])

    def get_issue_comments_from_github(self, pr):
        self.get_query("https://api.github.com/repos/%s/%s/issues/%s/comments?per_page=100" % (self.owner, self.project, pr), 
            self.register_issue_comments, "iss_comm_" + str(pr))

    def get_all_issue_comments_from_github(self):
        self.get_query("https://api.github.com/repos/%s/%s/issues/comments?per_page=100" % (self.owner, self.project), 
            self.register_issue_comments, "allissue_comments")

    def get_pr_comments_from_github(self, pr):
        self.get_query("https://api.github.com/repos/%s/%s/pulls/%s/comments?per_page=100" % (self.owner, self.project, pr), 
            self.register_pr_commit_comment,"pull_comm_"+str(pr), more_args=[pr])


    def get_all_pr_commits_from_github(self):
        #self.get_query("https://api.github.com/repos/%s/%s/pulls/commits?per_page=100" % (self.owner, self.project), 
        #    self.register_pr_commit, "allpull_comments", more_args=[None])
        for prid in self.rawdb.get_prs():
            self.get_pr_commits_from_github(prid)


    def get_pr_commits_from_github(self, pr):
        self.get_query("https://api.github.com/repos/%s/%s/pulls/%s/commits?per_page=100" % (self.owner, self.project, pr), 
            self.register_pr_commit,"pr_commits_" + str(pr), more_args=[pr])

    def get_commit_comments_sha_from_github(self, sha, pr=None):
        self.get_query("https://api.github.com/repos/%s/%s/commits/%s/comments?per_page=100" % (self.owner, self.project, sha), 
            self.register_commit_comments, "sha_comments pr:" + str(pr) + " :" + sha, more_args=[pr])

    def get_commit_comments_from_github(self):
        self.get_query("https://api.github.com/repos/%s/%s/comments?per_page=100" % (self.owner, self.project), 
            self.register_commit_comments, "all_comments", more_args=[None])

    def register_repo_info(self, row):
        self.rawdb.upsert("repo_info", row)

    def register_issue_info(self, row):
        if row["closed_at"] != None and "closed_by" not in row:
            self.get_query(row["url"], self.register_issue_info, "issue:" + str(row["number"]))
        else:
            self.rawdb.upsert("issue_info", row)
            self.register_issue_info_proc(row)

    # Do a quick scan just to see how many issues and PRs and when they closed
    def register_issue_info_no_closer(self, row):
        self.rawdb.upsert("issue_info_no_closer", row)

    def register_issue_info_proc(self, row):
            self.procdb.upsert("issue_events", 
                              {"rectype": "issue_title",
                               "issueid": int(row["number"]),
                               "project_owner": self.owner,
                               "project_name":self.project, 
                               "issue_type": "pull" if "pull_request" in row else "issue",
                               "actor": get_dotted(row,"user.login", default="ghost"),
                               "time":parse(row["created_at"]),
                               "uid": row["id"],
                               "title":row["title"],
                               "action":"start issue",
                               "provenance":"api.github.com",
                               "text":row["body"]}, anno=self.annotate)
            if "closed_by" in row and row["closed_at"] != None:
                self.procdb.upsert("issue_events", 
                              {"rectype": "issue_closed",
                               "issueid": row["number"],
                               "project_owner": self.owner,
                               "project_name":self.project, 
                               "issue_type": "pull" if "pull_request" in row else "issue",
                               "actor": get_dotted(row, "closed_by.login"),
                               "time":parse(row["closed_at"]),
                               "uid": row["id"],
                               "title":row["title"],
                               "action":"closed issue",
                               "provenance":"api.github.com"}, anno=self.annotate);

    def register_pr_info(self, row):
        if ("merged_at" in row and row["merged_at"] != None) and "merged_by" not in row:
            self.get_query(row["url"], self.register_pr_info, "pr: " + str(row["number"]))
        else:
            self.rawdb.upsert("pull_request_info", row)
            self.register_pr_info_proc(row)
    
    def register_pr_info_proc(self, row):
        row_title = {"rectype": "pull_request_title", 
                     "issueid": int(row["number"]),
                     "issue_type": "pull",
                     "project_owner": self.owner,
                     "project_name":self.project, 
                     "actor": get_dotted(row,"user.login", default="ghost"),
                     "time":parse(row["created_at"]),
                     "title":row["title"],
                     "sha":row["head"]["sha"],
                     "uid": row["id"],
                     "action":row["head"]["sha"],
                     "provenance":"api.github.com",
                     "text":row["body"]}
        if "comments" in row:
                row_title.update({"status": {
                     "was_correct_at_date": row["cmu_last_retrieval"],
                     "comments": row["comments"],
                     "additions": row["additions"],
                     "deletions": row["deletions"],
                     "changed_files": row["changed_files"]}})
        self.procdb.upsert("issue_events", row_title, anno=self.annotate)

        if "merged_by" in row:
                try:
                    headreponame = get_dotted(row, "head.repo.full_name", default="Unknown")
                    row_merge = row_title.copy()
                    row_merge.update({"rectype": "pull_request_merged",
                               "actor":get_dotted(row,"merged_by.login"),
                               "time":parse(row["merged_at"]),
                               "title":"Pull request merge from " + headreponame + ":" + 
                                     row["head"]["ref"] + " to " + 
                                     row["base"]["repo"]["full_name"] + ":" + 
                                     row["base"]["ref"],
                               "action":row["merge_commit_sha"],
                               "uid": row["merge_commit_sha"],
                               "sha":row["merge_commit_sha"],
                               "provenance":"api.github.com",
                               "text":row["title"] });
                    self.procdb.upsert("issue_events", row_merge, anno=self.annotate)
                except Exception, e:
                    self.err_log("merge record missing for pr "+str(row["number"]), e)
                
    #def get_pr_commit_comments_from_github(self, pr):
    #   for commit in self.rawdb.get_pr_commits(pr):
    #       self.get_commit_comments_sha_from_github(commit["sha"], pr=pr)
           #self.get_commit_comments_sha_from_github(commit["head"]["repo"]["owner"]["login"], commit["head"]["repo"]["name"], 


    def register_issue_comments(self, row):
        row["issueid"] = int(row["issue_url"].split("/")[-1])
        self.rawdb.upsert("issue_comments", row)
        self.register_issue_comments_proc(row)

    def register_issue_comments_proc(self, row):
        the_row = {"rectype": "issue_comment",
                   "issueid": int(row["issue_url"].split("/")[-1]),
                   "project_owner": self.owner,
                   "project_name":self.project, 
                   "actor": get_dotted(row,"user.login", default="ghost"),
                   "time":parse(row["created_at"]),
                   "provenance":"api.github.com",
                   "uid": row["id"],
                   "text":row["body"]};
        self.procdb.upsert("issue_events", the_row, anno=self.annotate)
    
    def register_pr_commit(self, pr, row):
        row["issueid"] = int(pr)
        self.rawdb.upsert("pr_commits", row)
        self.register_pr_commit_proc(pr, row)

    def register_pr_commit_proc(self, pr, row):
        the_row = {"rectype": "pull_request_commit",
                   "issueid": int(pr),
                   "project_owner": self.owner,
                   "project_name":self.project, 
                   "actor": get_fallback(row, "author.login,commit.author.email"),
                   "time":parse(get_dotted(row,"commit.author.date", default="1970-01-01")),
                   "action":row["sha"],   # this is the SHA
                   "provenance":"api.github.com",
                   "uid": row["sha"],
                   "text":row["commit"]["message"],
                   "status": { "comment_count": row["commit"]["comment_count"],
                               "as_of": row["cmu_last_retrieval"] }  };
        self.procdb.upsert("issue_events", the_row, anno=self.annotate)        


    def register_pr_commit_comment(self, pr, row):
        if pr is None:
            if "pull_request_url" in row:
                row["issueid"] = int(row["pull_request_url"].split("/")[-1]),
            else:
                row["issueid"] = None
        else:
            row["issueid"] = int(pr)
        print "Pr commit comment ===========> ", pr, self.owner, "=>",row["url"].split("/")[4],self.project
        self.rawdb.upsert("pr_commit_comment", row)
        self.register_pr_commit_comment_proc(pr, row)

    def register_pr_commit_comment_proc(self, pr, row):
        the_row = {"rectype": "pull_request_commit_comment",
                   "issueid": row["issueid"],
                   "project_owner": self.owner,
                   "project_name":self.project, 
                   "actor": get_dotted(row,"user.login", default="ghost"),
                   "time":parse(row["created_at"]),
                   "title":str((row["position"],row.get("line",""),row["path"])),
                   "action":row["commit_id"],   # this is the SHA
                   "sha":row["commit_id"],   # this is the SHA
                   "uid":row["id"],
                   "provenance":"api.github.com",
                   "text":row["body"]}
        self.procdb.upsert("issue_events", the_row, anno=self.annotate)        

    def query_lite(self):
        print "Querying", self.owner + "/" + self.project
        self.get_all_pr_info_from_github_no_closer()

    def query_all(self, carefully=False):
        print "Querying", self.owner + "/" + self.project
        ##self.query_readmes()
        self.query_commits()
        self.get_project_info_from_github()
        self.get_all_issue_info_from_github()
        self.get_all_pr_info_from_github()
        #TODO: check for TOO_MANY_PAGES exception
        self.get_all_issue_comments_from_github()
        self.get_all_pr_comments_from_github()
        self.get_all_pr_commits_from_github()
        self.query_fork_comments()
        for i in range(1,self.get_issue_count_from_github()+1): 
            self.query_issue(i, full=True)   
            if carefully:
                self.get_issue_comments_from_github(i)
                if self.rawdb.is_pr(i):
                    self.get_pr_comments_from_github(i)

    def list_all_pr_originators(self):
        return {self.rawdb.fork_owner(issid) for issid in self.rawdb.get_prs()}

    def query_issue(self, issue, full=True):
        print "Querying", self.owner + "/" + self.project, "#", issue
        if full:
            self.get_issue_info_from_github(issue)
            self.get_issue_comments_from_github(issue)
        if self.rawdb.is_pr(issue):
           if full:
               self.get_pr_info_from_github(issue)
               self.get_pr_comments_from_github(issue)
           self.get_pr_commits_from_github(issue)
           forkowner = self.rawdb.fork_owner(issue)
           if forkowner is not None:
               # in some cases github stores commit comments under the account
               # of the owner of the head of a PR rather than the base.
               for commit in self.rawdb.get_pr_commits(issue):
                   if commit["commit"]["comment_count"] > 0:
                       self.get_pr_commit_comments_from_fork(forkowner, commit["sha"], issue)

    def query_fork_comments(self):
        for issid in self.rawdb.get_prs():
            forkowner = self.rawdb.fork_owner(issid)
            for commit in self.rawdb.get_pr_commits(issid):
                if commit["commit"]["comment_count"] > 0:
                    self.get_pr_commit_comments_from_fork(forkowner, commit["sha"], issid)

    def query_commits(self):
         self.get_commit_info_from_github()
         print "WARNING -- might fail on big projects -- query commit by commit"
         self.get_commit_comments_from_github()

    def query_readmes(self):
        self.get_readme_like_files_from_github()

    def annotate(self, result):
        features = {}
        git_comment_conventions.find_special(features, result.get("title",""))    
        git_comment_conventions.find_special(features, result.get("text",""))    
        if "issues" in features:
            for i in features["issues"]:
                i["parts"] = list(i["parts"])
                i["parts"].append("rev" if result["provenance"] == "issue_crossref" else "")
                if i["parts"][0] == "%OWNER%": i["parts"][0] = self.owner
                if i["parts"][1] == "%PROJECT%": i["parts"][1] = self.project
        result.update(features)


if __name__=="__main__":
    confg = Config(sys.argv[1])
    confg.ensure_dir("issues_dir")
    ghauth = GhAuth(confg["github_authtokens"])
    if len(sys.argv) == 2:
        for (ix, proj) in enumerate(confg.get_sample_set_project_names()):
            print ix, proj
            scraper = Scraper(confg, proj.split("/")[0], proj.split("/")[1], ghauth)
            scraper.query_all(True)
        quit()
    if len(sys.argv) < 5:
        print "usage: python", sys.argv[0], "<config> <owner> <project> <issue-or-PR-number>\n     or\n   python ", sys.argv[0], "<config>\n  or\n   python ", sys.argv[0], "<config> <owner> <project> <all|repo|commits|lite|issue_commits>";
        quit();
    scraper = Scraper(confg, sys.argv[2], sys.argv[3], ghauth)
    if sys.argv[4] == "all":
        scraper.query_all()
        scraper.procdb.summarize_all()
    elif sys.argv[4] == "repo":
        scraper.get_project_info_from_github()
    elif sys.argv[4] == "commits":
        scraper.query_commits()
    elif sys.argv[4] == "lite":
        scraper.query_lite()
    elif sys.argv[4] == "issue_comments":
        scraper.get_all_issue_comments_from_github()
    else:
        scraper.query_issue(sys.argv[4])
        scraper.procdb.summarize_issue(sys.argv[4])

