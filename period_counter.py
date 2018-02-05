import csv
import re
import datetime
import resource
from collections import defaultdict
from dateutil.parser import parse

def enc(s):
    try:  
        return s.encode("utf-8")
    except:
        return str(s)

class PeriodCounter:
    """Tally events by package and by period"""
    def __init__(self):
        self.perperiod = defaultdict(lambda: defaultdict(lambda: defaultdict(set))) #  project -> period -> measure -> list of unique members
        self.allperiods = set(["total"])
        self.allmeasures = set()
        self.output_columns = set()
        self.uniquecounter = 0
        self.invariant()
 
    def set_measures(self, kinds):
        self.allmeasures = kinds
        self.output_columns = sorted(kinds)

    def wipe(self, reg):
        r = re.compile(reg)
        dellist = []
        for proj in self.perperiod:
            if r.search(proj) is not None:
                dellist.append(proj)
        for d in dellist: del self.perperiod[d]
    def uuid(self):
        self.uniquecounter += 1
        return self.uniquecounter-1

    def chk_measures(self, kind):
        if kind not in self.allmeasures:
            print "Add " + kind + " to qc.set_measures"
        assert kind in self.allmeasures, "Add " + kind + " to qc.set_measures"

    def derive_running_tally(self, opening, closing, opened, closed):
        """Provide columns indicating the opening and closing of some resources; this function
           adds new columns called opened and closed, that tally opened and closed resources to date"""
        self.chk_measures(opening)
        self.chk_measures(closing)
        self.chk_measures(opened)
        self.chk_measures(closed)
        numopen = 0
        numclosed = 0
        for proj in self.perperiod.keys():
            seen = set()
            for quar in sorted(self.allperiods):
                if quar != "total":
                    numopen += len(self.perperiod[proj][quar][opening]) - len(self.perperiod[proj][quar][closing])
                    numclosed += len(self.perperiod[proj][quar][closing])
                    self.count_last(proj, quar, opened, numopen)
                    self.count_max(proj, quar, closed, numclosed)

    def derive_arrivers(self, fromkind, newkind):
        """In a measure derived from count_unique events, count how many were brand new in each period"""
        self.chk_measures(fromkind)
        self.chk_measures(newkind)
        for proj in self.perperiod.keys():
            seen = set()
            for quar in sorted(self.allperiods):
                if quar != "total":
                    for item in  self.perperiod[proj][quar][fromkind]:
                        if item not in seen:
                            self.perperiod[proj][quar][newkind].add(item)
                            seen.add(item)
            self.perperiod[proj]["total"][newkind] = seen
        self.invariant()

    def derive_leavers(self, fromkind, newkind):
        """In a measure derived from count_unique events, count how many were seen for the last time in each period"""
        self.chk_measures(fromkind)
        self.chk_measures(newkind)
        for proj in self.perperiod.keys():
            seen = set()
            for quar in sorted(self.allperiods, reverse=True):
                if quar != "total":
                    for item in  self.perperiod[proj][quar][fromkind]:
                        if item not in seen:
                            self.perperiod[proj][quar][newkind].add(item)
                            seen.add(item)
            self.perperiod[proj]["total"][newkind] = seen
        self.invariant()

    def persist_values(self, kind, startfrom):
        """Assume that values for missing periods are the same as previous periods, or startfrom if there are no previous"""
        self.chk_measures(kind)
        for proj in self.perperiod.keys():
            previous = startfrom
            for quar in sorted(self.allperiods):
                if quar != "total":
                    if quar not in self.perperiod[proj] or kind not in self.perperiod[proj][quar]:
                        self.perperiod[proj][quar][kind] = previous
                    else:
                        previous = self.perperiod[proj][quar][kind] 
        self.invariant()

    def has(self, proj, quar, kind):
        ithas = proj in self.perperiod and quar in self.perperiod[proj] and kind in self.perperiod[proj][quar]
        return ithas

    def count_max(self, proj, quar, kind, num):
        """Assert this many items in the period, but just keep the maximum ever asserted"""
        self.allperiods.add(quar)
        self.chk_measures(kind)
        if num > len(self.perperiod[proj][quar][kind]):
            uniques = set(range(0,num))
            self.perperiod[proj][quar][kind] = uniques
            if num > len(self.perperiod[proj]["total"][kind]):
                self.perperiod[proj]["total"][kind] = uniques
        self.invariant()

    def count_average(self, proj, quar, kind, num):
        self.count_sum(proj, quar,kind+".sum",num)
        self.count_unique(proj, quar, kind+".count", self.uuid())

    def count_last(self, proj, quar, kind, num):
        """Add this many items for the period, and have the total be just
           the same as the most recent value"""
        self.allperiods.add(quar)
        self.chk_measures(kind)
        self.perperiod[proj]["total"][kind] = set()
        for i in range(0,num):
            dummy = self.uuid()
            self.perperiod[proj][quar][kind].add(dummy)
            self.perperiod[proj]["total"][kind].add(dummy)
        self.invariant()

    def count_sum(self, proj, quar, kind, num):
        """Add this many items for the period, without worrying about uniqueness"""
        self.allperiods.add(quar)
        self.chk_measures(kind)
        """A kludge that keeps other stuff simple: add this many non-unique items
           rather than having a separate count for unique and non-unique items"""
        for i in range(0,num):
            dummy = self.uuid()
            self.perperiod[proj][quar][kind].add(dummy)
            self.perperiod[proj]["total"][kind].add(dummy)
        self.invariant()


    def count_unique(self, proj, quar, kind, element):
        """Add this element for the period, not counting it if it is not unique"""
        self.chk_measures(kind)
        self.allperiods.add(quar)
        self.perperiod[proj][quar][kind].add(element)
        self.perperiod[proj]["total"][kind].add(element)
        self.invariant()

    def set_period(self, period):
        self.period_funcs = defaultdict(dict)
        if period == "day":
            self.period_funcs["datestr2period"] = lambda (datestr): self.datetime2day(parse(datestr))
            self.period_funcs["unixtime2period"] = lambda (unixtime): self.datetime2day(datetime.datetime.fromtimestamp(int(unixtime)))
            self.period_funcs["datetime2period"] = lambda (dt): self.datetime2day(dt)
        elif period == "month":
            self.period_funcs["datestr2period"] = lambda (datestr): self.datetime2month(parse(datestr))
            self.period_funcs["unixtime2period"] = lambda (unixtime): self.datetime2month(datetime.datetime.fromtimestamp(int(unixtime)))
            self.period_funcs["datetime2period"] = lambda (dt): self.datetime2month(dt)

    # This is where "period" becomes specific, e.g. to break into days, point to datetime2day
    def datestr2period(self, datestr): return self.period_funcs["datestr2period"](datestr)
    def unixtime2period(self, unixtime): return self.period_funcs["unixtime2period"](unixtime)
    def datetime2period(self, dt): return self.period_funcs["datetime2period"](dt)
    #def datestr2period(self, datestr): return self.datetime2day(parse(datestr))
    #def unixtime2period(self, unixtime): return self.datetime2day(datetime.datetime.fromtimestamp(int(unixtime)))
    #def datetime2period(self, dt): return self.datetime2day(dt)

    def datetime2quarter(self, dt):
        y = dt.year
        m = dt.month
        q = "%04d%02d" % (y, (3*((m-1) / 3) + 1))
        return q

    def datetime2day(self, dt):
        y = dt.year
        m = dt.month
        q = "%04d%02d%02d" % (y, m, dt.day)
        return q

    def datetime2month(self, dt):
        y = dt.year
        m = dt.month
        q = "%04d%02d" % (y, m)
        return q

    def flush_periodic_data(self, proj_regex, filename, periods=False, hide_zeroes=False):
        startusage = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        outv = csv.writer(open(filename,"a"))

        r = re.compile(proj_regex)
        #print "About to flush projects matching", proj_regex
        for p in sorted(self.perperiod.keys(), key=lambda k: enc(k)): 
            if r.search(p) is not None:
                #print "Flushing",p 
                for q in sorted(self.allperiods):
                    columnvals = [len(self.perperiod[p][q][m]) for m in self.output_columns]
                    if not (hide_zeroes and sum(columnvals[:-2]) == 0):
                        outv.writerow([enc(p),q] + [len(self.perperiod[p][q][m]) for m in self.output_columns])
                del self.perperiod[p]
        #self.wipe(proj_regex)
        endusage = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        #print "Flush reduced memory", startusage,"->",endusage
        
    def set_output_columns(self, outcols):
        self.output_columns = outcols

    def start_write_periodic_data(self, filename, periods=False, hide_zeroes=False):
        usage = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        print "Usage:",usage
        outv = csv.writer(open(filename,"w"))

        outv.writerow(["project","period"]+self.output_columns)

    # Enable and use for testing, but avoid for real scraping.
    def invariant(self):
        #assert not ("andycasey/ads" in self.perperiod and "201601" in self.perperiod["andycasey/ads"] and "num_files_existing" in self.perperiod["andycasey/ads"]["201601"])
        return
        for p in self.perperiod.keys():
            for q in self.allperiods:
                for m in self.allmeasures:
                    assert p not in self.perperiod or q not in self.perperiod[p] or m not in self.perperiod[p][q] or type(self.perperiod[p][q][m]) is set, "Problem with " + str(p,q,m)


