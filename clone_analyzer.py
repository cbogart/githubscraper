import json
import pdb
from config import Config
import os
import datetime
from git import Repo,Blob
from period_counter import PeriodCounter
import csv


def comparisonStats(commit1, commit2):
       diff = cmt.diff(cs[ix+1])
       return { "numChanges": len(diff) }

def numfiles(t):
    return len(t.blobs) + sum([numfiles(t1) for t1 in t.trees])

def allfiles(t):
    return [b.path for b in t.blobs] + [b for t1 in t.trees for b in allfiles(t1)]

test_directories = ["test","tests","spec"]
def likely_test(filepath):
    pathparts = filepath.split("/")
    return any([testdir in pathparts for testdir in test_directories])

def yield_all_commits(repo):
    ix = -1
    for cmt in repo.iter_commits():
        ix += 1
        yield (ix, cmt)

def yield_mainline_commits(repo):
    cmt = repo.iter_commits().next()
    ix = -1
    while (cmt is not None):
        ix += 1
        yield (ix, cmt)
        if len(cmt.parents) > 0: cmt = cmt.parents[0]
        else: cmt = None

def get_repo_object(confg, packagename):
    confg.ensure_dir("clone_dir")
    fn = confg["clone_dir"] + "/" + packagename
    return Repo(fn)

def scan_git_clones_for_commit_sizes(confg, p, header=False):
    confg.ensure_dir("data_dir")
    csvf = csv.writer(open(confg["data_dir"] + "/commit_sizes.csv","a"))
    if header:
        csvf.writerow(["project","commit_sha", "committed_date", "insertions", "deletions", "files", "total_lines"])
    for commit in yield_all_commits(get_repo_object(confg, p)):
        c = commit[1]
        dt = datetime.datetime.utcfromtimestamp(c.committed_date)
        csvf.writerow([p, c.hexsha, dt.isoformat() + "Z", c.stats.total["insertions"],
                       c.stats.total["deletions"],c.stats.total["files"],c.stats.total["lines"]])
            

def scan_git_clones_for_periodic_data(confg, qc, packages, aliases):
    confg.ensure_dir("clone_dir")
    errf = open("periodic_errors.txt", "w")
    for package in packages:
        fn = confg["clone_dir"] + "/" + package
        print "Scanning git clone for ", package
        if os.path.exists(fn):
            repo = Repo(fn)
            try:
                for (ix, cmt) in yield_all_commits(repo):
                    try:
                        when = cmt.committed_date
                    except Exception, e:
                        print "BAD COMMIT: ", package, cmt, cmt.message, ix
                        continue
                    if ix%1000 == 0:
                        print "Commit #", ix
                    quar = qc.unixtime2period(when)
                    qc.count_unique(package, quar, "commits_recheck", cmt.hexsha)
                    #qc.count_unique(package, quar, "commit_authors_name_recheck", cmt.author.name )
                    #qc.count_unique(package, quar, "commit_committers_name_recheck", cmt.committer.name)
                    try:
                        qc.count_unique(package, quar, "commit_authors_email", aliases.lookup_canonical(cmt.author.email))
                    except:
                        errf.write(package + ": Weirdly, author missing for commit "+ str(cmt))
                        qc.count_unique(package, quar, "commit_authors_email", aliases.lookup_canonical(cmt.committer.email))
                        
                    qc.count_unique(package, quar, "commit_committers_email", aliases.lookup_canonical(cmt.committer.email))
                    if len(cmt.parents) > 1:
                        not_all_me = True
                        try:
                            # Just count commits that combine someone else's work -- it shows you're a coordinator, not
                            # just merging your own branches or something
                            me = aliases.lookup_canonical(cmt.committer)
                            not_all_me = me != aliases.lookup_canonical(cmt.parents[0].committer)  or me != aliases.lookup_canonical(cmt.parents[1].committer)
           
                            #print cmt.committer.name.encode("utf-8"), "<-", cmt.parents[0].committer.name.encode("utf-8"), "+",cmt.parents[1].committer.name.encode("utf-8")
                        except AttributeError:
                            #In rare cases, looking at parent committer fails
                            errf.write(package + ": In rare cases, looking at parent committer fails " + str(cmt))
                            pass
                        if not_all_me:
                            qc.count_unique(package, quar, "mergers_email", aliases.lookup_canonical(cmt.committer.email))
                            qc.count_sum(package, quar, "merges", 1)
    
                    # Assumes the commits are in reverse order, and these values all increase over time.
                    #  Neither is strictly true, so the result is an approximation
                    if not qc.has(package, quar, "num_files_existing"):
                        allfiles = [f.path for f in cmt.tree.traverse() if f.type == Blob.type]
                        qc.count_max(package, quar, "num_files_existing", len(allfiles))
                        qc.count_max(package, quar, "num_py_files_existing", len([f for f in allfiles if f.endswith(".py")]))
                        qc.count_max(package, quar, "num_rb_files_existing", len([f for f in allfiles if f.endswith(".rb")]))
                        qc.count_max(package, quar, "num_test_files_existing", len([f for f in allfiles if likely_test(f)]))
    
                    # The following is interesting but quite expensive to compute
                    # because of the cmt.diff call
                    """
                    if ix < len(cs) -1:  # for all but first commit, analyze the diff w/ previous commit
                        diff = cmt.diff(cs[ix+1])
                        for d in diff:
                           qc.count_unique(package, quar, "files_changed_in_period", d.a_path)
                           if d.a_path.endswith(".py"):
                               qc.count_unique(package, quar, "py_files_changed_in_period", d.a_path)
                           if d.a_path.endswith(".rb"):
                               qc.count_unique(package, quar, "rb_files_changed_in_period", d.a_path)
                           if likely_test(d.a_path):
                               qc.count_unique(package, quar, "test_files_changed_in_period", d.a_path)
                        qc.count_sum(package, quar, "distinct_file_changes", len(diff))
                    else:  # if it's the first commit, analyze the commit itself
                        for path in allfiles:
                           qc.count_unique(package, quar, "files_changed_in_period", path)
                           if path.endswith(".py"):
                               qc.count_unique(package, quar, "py_files_changed_in_period", path)
                           if path.endswith(".rb"):
                               qc.count_unique(package, quar, "rb_files_changed_in_period", path)
                           if likely_test(path):
                               qc.count_unique(package, quar, "test_files_changed_in_period", path)
                        qc.count_sum(package, quar, "distinct_file_changes", len(allfiles))
                    """
                print "Scanned ", ix, "commits"
            except Exception, e:
                errf.write(package + ": Skipping the rest of the project" + str(e))

    qc.derive_arrivers("commit_authors_email", "arriving_commit_authors_email")
    qc.derive_arrivers("commit_committers_email", "arriving_commit_committers_email")
    qc.derive_leavers("commit_authors_email", "leaving_commit_authors_email")
    qc.derive_leavers("commit_committers_email", "leaving_commit_committers_email")
    qc.derive_arrivers("mergers_email", "arriving_mergers_email")
    qc.derive_leavers("mergers_email", "leaving_mergers_email")
    qc.persist_values("num_files_existing", [])
    qc.persist_values("num_py_files_existing", [])
    qc.persist_values("num_rb_files_existing", [])
    qc.persist_values("num_test_files_existing", [])

       # This is for a file difference, but diff is a tree difference.
       #from difflib import Differ
       #differ = Differ()
       #from cStringIO import StringIO
       #bloba = StringIO()
       #blobb = StringIO()
       #diff.a_blob.stream_data(bloba)
       #diff.a_blob.stream_data(blobb)
       #differences = list(differ.compare(bloba, blobb))




