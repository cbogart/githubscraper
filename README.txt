
To install this package, mysql.connector can be troublesome; what worked for
me was installing with:

   pip install --egg mysql-connector

How to use it to download a github dataset:
*  Download all githubarchive files into a directory for each year
*  hardcode the appropriate directory in get_githubarchive_aliases.py
*  run get_githubarchive_aliases.py 
   * Creates a RepoAliases mongo collection capturing how repos names have changed over time

Then for each scrape you want to do:
*  List projects (owner/project) in a text file, CR-separated, called sample_set_(whatever).txt
*  Fill in config.json with directories for saving stuff, plus a pointer to the sample_set file
*  python extract_issue.py config.json
   * Pipe into a log file, and check log for errors
   * Rescrape any projects that failed, if the problem was temporary (e.g. network down)
*  edit get_githubarchive_events_mongo.sh to use the right names/directories
*  bash get_githubarchive_events_mongo.sh
*  python make_canonical_project_list.py
*  python list_gh_users_and_projects.py
*  python lookup_user_info.py      
   * creates actor_info.csv with github users' name & location
* python lookup_user_info.py      
   * creates actor_info.csv with github users' name & location
* python username_match_mongo.py config.json
   * writes to data directory
* python get_membership.py config.json
   * reads from data directory

