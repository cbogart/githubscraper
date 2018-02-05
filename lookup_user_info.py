import urllib2
import pdb
from github_auth import GhAuth
from pymongo import MongoClient
import pytz
import json
from config import Config
import time
import os
import datetime
import sys
import csv

#
#  From a plain list of git users (git_users.csv)
#  look up names in github.com's api to get metadata for each user
#  like email and location.  Put results in actor_info_2016.csv
#


def awaitThrottle(rateinfo):
    if int(rateinfo.getheader("X-RateLimit-Remaining")) < 10:
        print "SLEEPING for ", 5 + int(rateinfo.getheader("X-Ratelimit-Reset")) - time.time(), " seconds"
        time.sleep(5 + int(rateinfo.getheader("X-Ratelimit-Reset")) - time.time())
    else:
        print "NOT SLEEPING -- ", rateinfo.getheader("X-RateLimit-Remaining")

old_actor_info_cache = {}
def get_user_info_from_old_actor_info(old_act_inf, user):
    if len(old_actor_info_cache) == 0:
        for rec in csv.DictReader(open(old_act_inf,"r")):
            old_actor_info_cache[rec["login"]] = rec
    if user in old_actor_info_cache:
        return [old_actor_info_cache[user]]
    else:
        return []


def get_user_info_from_ghtorrent(connection, user):
    result = []
    try:
        cur = connection.cursor(dictionary=True);

        cur.execute("""select * from users where login=%s;""", (user,))
        rows = cur.fetchall()

        for row in rows:
           dt = row["created_at"]
           if dt.tzinfo is None or dt.tzinfo.utcoffset(d) is None:
               dt.replace(tzinfo=pytz.UTC)
           result.append({
               "location": row["location"],
               "login": row["login"],
               #"name": row["name"],
               #"email": row["email"],
               "company": row["company"],
               "type": row["type"],
               "fake": row["fake"],
               "deleted": row["deleted"],
               "created_at": datetime.datetime.isoformat(dt)
                          });
        if len(rows) > 0: print "Found " + user + " in database";

    except Exception, e:
         print "DB error " + str(e) + " for user " + user
         raise
    finally:
        cur.close();

    return result;


def get_user_info_file_from_github(gh_auth, user):
    url = "https://api.github.com/users/%s" % (user,)
    print url
    try:
        if "@" in user:
            raise Exception("Not a github username")
        returned = {} 
        def set_returned(x):
            global returned
            returned = x
        gh_auth.query_page([], url, set_returned)
        print returned["login"]
        return returned
    except Exception, e:
        print type(e), e
        return {"type": "deleted", "login": user, "error": str(e) }

def safe_utf8(v):
    if v is None: return None
    if type(v) is unicode: return v.encode("utf-8")
    return str(v)

if __name__=="__main__":
    confg = Config(sys.argv[1])
    mongo_db = MongoClient()[confg["mongodb_proc"]]
    gh_auth = GhAuth(confg["github_authtokens"])
    connection = confg.get_ghtorrent_connection()
    confg.ensure_dir("data_dir")

    users = set()
    for issue in mongo_db.project_events.find():
        users.add(issue["actor"])
    for issue in mongo_db.issue_events.find():
        users.add(issue["actor"])
    print "Found", len(users), "actors"
    confg.ensure_dir("data_dir")

    outp = csv.writer(open(confg["data_dir"] + "/" + "actor_info.csv", "w"))
    fields = ["location", "login", "name", "email", "company", "email", "blog", "fake",
                      "name", "type", "site_admin", "bio", "public_repos", "public_gists",
                      "followers", "following", "created_at", "updated_at", "error"]
    outp.writerow(fields)
    for user in users:
        print "-----Checking user", user
        info = get_user_info_from_old_actor_info("/usr2/scratch/giterator/data/pypi_arun_16k_v3/actor_info.csv", user)
        if len(info) == 0:
            info = get_user_info_from_ghtorrent(connection, user)
            print "from db returned " + str(len(info)) + " rows"
        if len(info) > 0:
            info = info[0]
        else:
            print "Querying github instead"
            info = get_user_info_file_from_github(gh_auth, user)
        outp.writerow([safe_utf8(info.get(f,None)) for f in fields])

