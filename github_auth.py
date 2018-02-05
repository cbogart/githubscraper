import time
import pytz
import re
import urllib2
import sys
import json
import datetime
import pdb

def now(): 
    return datetime.datetime.now(pytz.timezone("GMT")).isoformat()

class GhAuth:
    def __init__(self, tokens):
        self.tokens = tokens;
        self.sleepuntil = [time.time() for t in self.tokens]
        self.remaining = [3600 for t in self.tokens]
        self.which = 0
        self.probeAllTokens()
    def probeAllTokens(self):
        for self.which in range(len(self.tokens)):
            site = self.geturl("https://api.github.com/rate_limit")
            rateinfo = site.info()
            self.setLimits(rateinfo)
        if self.remaining[self.which] < 30: print "TOKEN LIMITS: ", self.remaining
        self.findFreshToken()
    def token(self):
        return self.tokens[self.which]
    def geturl(self, url):
        req = urllib2.Request(url, headers = {"Authorization": 'token %s' % self.token()})
        retries = 10
        while retries > 0:
            try:
                return urllib2.urlopen(req)
            except urllib2.URLError, e:
                if e.code == 404 or e.code == 410:
                    raise e
                elif "Bad Request" in e.reason:
                    print "Something wrong with ", url
                    raise e
                print "Connection problem; pausing for a little bit (", e.reason, ")"
                retries -= 1
                print url
                try: print req.info()
                except: print "Can't print request info"
                print e
                sys.stdout.flush()
                time.sleep(120)

    def findFreshToken(self):
        self.which = self.remaining.index(max(self.remaining))
        if self.remain() < 20:
            self.which = self.sleepuntil.index(min(self.sleepuntil))
        print "TOKEN LIMITS: ", self.remaining
        print "SLEEPTIMES: ", [s-time.time() for s in self.sleepuntil]
        print "SWITCHING TO TOKEN ", self.which

    def remain(self): return self.remaining[self.which]
    def sleeptime(self): return self.sleepuntil[self.which] - time.time()

    def setLimits(self, rateinfo):
        self.sleepuntil[self.which] = 15 + int(rateinfo.getheader("X-Ratelimit-Reset")) 
        self.remaining[self.which] = int(rateinfo.getheader("X-Ratelimit-Remaining"))

    def awaitThrottle(self, rateinfo):
        self.setLimits(rateinfo)
        if self.remain() < 10 and self.sleeptime() > 0:
            self.probeAllTokens()
            if self.remain() < 10 and self.sleeptime() > 0:
                print "SLEEPING for ", self.sleeptime(), " seconds with", \
                         self.remain(), " queries remaining"
                print "(until ", str(datetime.datetime.fromtimestamp(self.sleepuntil[self.which])), ")"
                sys.stdout.flush()
                time.sleep(self.sleeptime())
                self.probeAllTokens()
        else:
            pass
            #print "NOT SLEEPING -- ", self.remain(), "queries remain. Sleeptime=",self.sleeptime()

    def query_page(self, args, url, registry):
        try:
            site = self.geturl(url)
        except Exception, e:
            print "Failed to read url ", url, e
            return
        content = site.read()
        rateinfo = site.info()
        raw = json.loads(content)
        if len(raw) > 0:
            row = raw[0]
            if type(row) is dict:
                row["cmu_last_retrieval"] = now()
                registry(*(args + [row]))
        self.awaitThrottle(rateinfo)

    def query_pages(self, args, url, registry):
        lasturl = "??"
        while(url != ""):
            if "page=400" in url:
                print "INCOMPLETE QUERY", url
            try:
                print url
                site = self.geturl(url)
            except Exception, e:
                print "Failed to read url ", url, e
                return
            content = site.read()
            rateinfo = site.info()
            try:
                links = parse_header_links(rateinfo.getheader("Link"))
                nextpage = links.get("next", "")
                lasturl = links.get("last", lasturl)
            except Exception, e:
                #print "Looking for next page: ", e
                nextpage = ""
            parsed = json.loads(content)
            if not (type(parsed) is list):
                parsed = [parsed]
            for row in parsed:
                # Watch out: sometimes row is None when interface returns "null",
                # e.g. see https://api.github.com/repos/skyscanner/pyfailsafe/pulls/comments
                if type(row) is dict:
                    row["cmu_last_retrieval"] = now()
                    registry(*(args + [row]))
            self.awaitThrottle(rateinfo)
            url = nextpage
    
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
    
