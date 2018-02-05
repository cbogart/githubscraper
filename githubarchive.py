import json
import pdb

class GithubArchiveFormatError(Exception):
    pass

class GithubArchive:
    def __init__(self, line):
        try:
            if type(line) is str or type(line) is unicode:
                self.j = json.loads(line)
            elif type(line) is dict or type(line) is list:
                self.j = line
        except Exception, jde:
            try:
                print "Decoding " + line
                line1 = line.decode('utf-8', 'replace').encode('utf-8')
                print "   to " + line1
                self.j = json.loads(line1)
                print "   success"
            except Exception, jde:
                print "   failure " + str(jde)
                raise GithubArchiveFormatError(str(jde) + "\n" + line1)

    def type(self): return self.j["type"]

    def badrecord(self):
        """Some records have missing user information; skip these"""
        return (("actor" in self.j
                   and type(self.j["actor"]) is dict
                   and "url" in self.j["actor"]
                   and self.j["actor"]["url"] == "https://api.github.dev/users/")
               or
                  ("actor" in self.j
                   and "actor_attributes" in self.j
                   and False)) # second condition disabled for now; don't recall why it's here

    def person(self, persontype):
        if persontype + "_attributes" in self.j: return self.j[persontype + "_attributes"]["login"]
        elif persontype in self.j and "login" in self.j[persontype]: return self.j[persontype]["login"]
        elif persontype in self.j and not (type(self.j[persontype]) is dict): return self.j[persontype]
        elif persontype in self.j["payload"] and "login" in self.j["payload"][persontype]: return self.j["payload"][persontype]["login"]
        elif persontype in self.j["payload"] and not (type(self.j["payload"][persontype]) is dict): return self.j["payload"][persontype]
        else: raise GithubArchiveFormatError("No " + persontype + "/login info")
    def actor(self): return self.person("actor")

    def repo_id(self):
        if "repo" in self.j and "id" in self.j["repo"]:
            return self.j["repo"]["id"]
        elif "repository" in self.j:
            return self.j["repository"].get("id",None)
        else:
            return None
    def event_id(self): return self.j["id"]
    def repo_full_name(self):
        rfn = ""
        if "payload" in self.j and "repo" in self.j["payload"]:
            rfn = self.j["payload"]["repo"]
        elif "repo" in self.j:
            rfn= self.j["repo"]["name"]
        elif "repository" in self.j:
            rfn= self.j["repository"]["owner"] + "/" + self.j["repository"]["name"]
        elif "url" in self.j:
            rfn= "/".join(self.j["url"].split("/")[3:5])
        elif "repo" in self.j["payload"]:
            rfn= self.j["payload"]["repo"]
        if len(rfn) > 1 and rfn[0] == "/":
            if "url" in self.j and "github.com/" in self.j["url"]:
                parts = self.j["url"].split("github.com/")[1].split("/")
                rfn = parts[0] + "/" + parts[1]
        if rfn == "" or rfn.startswith("/"):
            raise GithubArchiveFormatError("No repository info")
        return rfn

    def repo_owner(self): return self.repo_full_name().split("/")[0]
    def repo_name(self): return self.repo_full_name().split("/")[1]
    def created_at(self): return self.j["created_at"]

    def pp(self): 
        if "_id" in self.j: del self.j["_id"]
        return json.dumps(self.j, indent=4)
    def __str__(self): return json.dumps(self.j)

    def wiki_pages(self):
        if "pages" in self.j["payload"]:
            return self.j["payload"]["pages"]
        else:
            return [self.j["payload"]]

    def pushed_shas(self):
        if "shas" in self.j["payload"]:
            return [sha[0] for sha in self.j["payload"]["shas"]]
        elif "commits" in self.j["payload"]:
            return [commit["sha"] for commit in self.j["payload"]["commits"]]
        else: raise GithubArchiveFormatError("No commit shas")

    def repo_forked_from(self): return self.repo_full_name()
    def repo_forked_to(self):
        try:
           return self.j["payload"]["forkee"]["full_name"]
        except TypeError:
           return self.actor() + "/" + self.repo_name()
        except KeyError:
           return self.actor() + "/" + self.repo_name()

    def commit_comment_url(self):
        return "https://api.github.com/repos/" + self.repo_full_name() + "/comments/" + self.commit_comment_id()

    def commit_comment_id(self):
        if "comment" in self.j["payload"]: return str(self.j["payload"]["comment"]["id"])
        else: return str(self.j["payload"]["comment_id"])

    def commit_comment_sha(self):
        if "comment" in self.j["payload"]: return self.j["payload"]["comment"]["commit_id"]
        else: return self.j["payload"]["commit"]

    def commit_comment(self):
        if "comment" in self.j["payload"]: return self.j["payload"]["comment"]["body"]
        else: return ""

    def create_what(self):
        if "ref" in self.j["payload"] and self.j["payload"]["ref"] is not None:
            return self.j["payload"]["ref"]
        elif "object_name" in self.j["payload"]:
            return self.j["payload"]["object_name"]
        elif self.create_what_type() == "repository":
            return self.repo_full_name()
    def create_what_type(self):
        if "ref_type" in self.j["payload"]: return self.j["payload"]["ref_type"]
        else: return self.j["payload"]["object"]

    def action(self): return self.j["payload"]["action"]

    def pr_review_comment(self, field): return self.j["payload"]["comment"][field]
    def pr_review_comment_html_url(self):
        try:
            return self.j["payload"]["comment"]["html_url"]
        except KeyError, e:
            return self.j["payload"]["comment"]["_links"]["html"]["href"]
    def pr_number(self):
        try:
            return self.j["payload"]["pull_request"]["number"]
        except KeyError, e:
            return self.j["payload"]["comment"]["pull_request_url"].split("/")[-1]

    def pr_merged(self):
        return self.j["payload"]["pull_request"]["merged_at"]
    def pr_title(self): return self.j["payload"]["pull_request"]["title"]
    def issue_number(self):
        try:
            return self.j["payload"]["issue"]["number"]
        except TypeError, e:
            return self.j["payload"]["number"]
    def issue_title(self): return self.j["payload"]["issue"]["title"]
    def issue_body(self): return self.j["payload"]["issue"]["body"]
    def member(self): return self.person("member")
    def author(self):
        try:
            return self.j["payload"]["release"]["author"]["login"]
        except KeyError, e:
            return self.j["actor_attributes"]["login"]

    def tag_name(self): return self.j["payload"]["release"]["tag_name"]
    def target_commitish(self): return self.j["payload"]["release"]["target_commitish"]
    def into_branch(self):
        if (self.type() == "PullRequestEvent"):
            return self.j["payload"]["pull_request"]["base"]["ref"]
        elif (self.type() == "PushEvent"):
            return self.j["payload"]["ref"]
    def from_branch(self):
        if (self.type() == "PullRequestEvent"):
            return self.j["payload"]["pull_request"]["head"]["ref"]
        elif (self.type() == "PushEvent"):
            return self.j["payload"]["ref"]
