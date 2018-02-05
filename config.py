import mysql.connector
import json
import pdb
import os
import sys
from pymongo import MongoClient

class Config:
    def __init__(self, config_file):
        try:
            self.config = json.load(open(config_file,"r"))
            self.mongo_cli = None
            self.mongo_db = None
            self.mongo_db_raw = None
            self.mongo_db_proc = None
        except Exception, e:
            raise(ValueError("Can't read from configuration file " + config_file + ":\n" + str(e)))

    def __getitem__(self, key): 
        return self.config[key]

    def ensure_dir(self, d):
        try:
            os.makedirs(self.config[d])
        except OSError as e:
            pass
        return self.config[d]

    def mongo_client(self):
        if self.mongo_cli is None: self.mongo_cli = MongoClient(self.config["mongourl"])
        return self.mongo_cli
    
    def raw_db(self):
        """Store json records literally as scraped from github here"""
        if self.mongo_db_raw is None: self.mongo_db_raw = self.mongo_client()[self.config["mongodb_raw"]]
        return self.mongo_db_raw

    def proc_db(self):
        """Store researcher-friendly processed records here"""
        if self.mongo_db_proc is None: self.mongo_db_proc = self.mongo_client()[self.config["mongodb_proc"]]
        return self.mongo_db_proc
        
    def get_ghtorrent_connection(self):
        return mysql.connector.connect(
            host= self.config["db_host"],
            user= self.config["db_user"],
            password= self.config["db_password"],
            time_zone= self.config["db_time_zone"],
            database= self.config["db_ghtorrent_database"]
        );

    def get_sample_set_project_names(self):
        return [r.strip() for r in open(self.config["sample_set"], "r").readlines()]

