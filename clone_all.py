import sys
from config import Config
import os
import subprocess
import json

userPass = "x:y"  # Not a real user; just prevents prompting from terminal

def clone(confg, project):
    confg.ensure_dir("clone_dir")
    print "Cloning " + project
    assert " " not in project and len(project) > 0 and len(project.split("/")) == 2
    [owner, projname] = project.split("/")
    clonedir = confg["clone_dir"]
    print("git clone http://" + userPass + "@github.com/" + project + " " + clonedir + "/" + project)
    try:
        os.makedirs(clonedir + "/" + owner)
    except:
        pass
    if os.path.exists(clonedir + "/" + project):
        pass #subprocess.call(["git", "pull"], cwd=clonedir + "/" + project)
        print "NOT PULLING LATEST VERSION"
    else:
        subprocess.call(["git", "clone", "http://" + userPass + "@github.com/" + project], cwd= clonedir + "/" + owner)

if __name__=="__main__":
    confg = Config(sys.argv[1])
    for a in open(confg["sample_set"], "r").readlines():
        clone(confg, a.strip())
