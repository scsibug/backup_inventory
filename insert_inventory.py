#!/usr/bin/python
# Insert inventory data into PostgreSQL database.

# Usage:
# ./insert_inventory.py database-name database-user inventory-directory

# This will iterate through all inventory/metadata pairs in the
# inventory-directory, inserting their contents into the database-name
# (connecting as database-user).
# Completed files are moved into inventory-directory/completed (which is created if it does not already exist)

import os
import sys
import psycopg2
import json
import re
import datetime
import csv

database_name = sys.argv[1]
database_user = sys.argv[2]
inventory_directory = os.path.abspath(sys.argv[3])

# Ensure inventory-directory exists
if (not os.path.isdir(inventory_directory)):
    print "%s is not a directory" % inventory_directory
# Create completed directory if needed
inventory_completed_directory = os.path.join(inventory_directory,"completed")
if (not os.path.isdir(inventory_completed_directory)):
    print "inventory completed directory does not exist, creating it."
    os.mkdir(inventory_completed_directory)
# Find all the inventory metadata files
all_inv_files = [ f for f in os.listdir(inventory_directory) if os.path.isfile(os.path.join(inventory_directory,f)) ]
# we want all the files which begin with "inv_md" and end with json
all_inv_md_files = [f for f in all_inv_files if (f.startswith("inv_md") and f.endswith(".json"))]

# Get a connection
conn = psycopg2.connect(database=database_name, user=database_user)
# from http://stackoverflow.com/questions/127803/how-to-parse-iso-formatted-date-in-python
def from_utc(utcTime,fmt="%Y-%m-%dT%H:%M:%S.%f+00:00"):
    """
    Convert UTC time string to time.struct_time
    """
    # TODO: properly handle timezone data
    # change datetime.datetime to time, return time.struct_time type
    return datetime.datetime.strptime(utcTime, fmt)

def create_inventory_run(conn, root_id, ts_utc, duration, version):
    try:
        cur = conn.cursor()
        # Check if this run already exists
        cur.execute("SELECT root_path FROM inventory_run WHERE root_path = %s AND tstamp = %s", (root_id, ts_utc))
        existing_run = cur.fetchone()
        if (existing_run is None):
            cur.execute("INSERT INTO inventory_run (root_path, tstamp, duration, version) VALUES (%s, %s, %s, %s) RETURNING id",
                        (root_id,
                         ts_utc,
                         duration,
                         version))
            new_run = cur.fetchone()
            conn.commit()
            return new_run[0]
        else:
            print "Run %s already exists" % existing_run[0]
            return None
    except psycopg2.Error as e:
        print e

def create_inventory_root(conn, inventory):
    # Try to find an existing root
     try:
         cur = conn.cursor()
         print inventory["uuid"]
         cur.execute("SELECT id FROM inventory_root WHERE uuid=%s", (inventory["uuid"],))
         found_root = cur.fetchone()
         if (found_root is None):
             print "need to create root"
             cur.execute("INSERT INTO inventory_root (uuid, hostname, name, path, description, type, rep_factor) VALUES (%s, %s, %s, %s, %s, %s, %s) RETURNING id",
                         (inventory["uuid"],
                          inventory["hostname"],
                          inventory["name"],
                          inventory["root"],
                          inventory["description"],
                          inventory["type"],
                          inventory["replication_factor"]))
             new_root = cur.fetchone()
             conn.commit()
             return new_root[0]
         else: 
             return found_root[0]
     except psycopg2.Error as e:
         print e

def add_file_ref(cur,run_id,fp,fhash,fsize,fmodified):
    #print "adding file: %s" % (fp,)
    cur.execute("INSERT INTO hashes(hash) SELECT decode(%s,'hex') WHERE NOT EXISTS (SELECT 1 FROM hashes WHERE hash=decode(%s,'hex'))",(fhash,fhash))
    cur.execute("INSERT INTO file_references(rel_path) SELECT %s WHERE NOT EXISTS (SELECT 1 FROM file_references WHERE rel_path=%s)",(fp,fp))
    cur.execute("INSERT INTO inventory_items(inventory_run, hash, file, modified, filesize) (SELECT %s, h.id, f.id, %s, %s from hashes h, file_references f where h.hash = decode(%s,'hex') and f.rel_path = %s)",
                (run_id, datetime.datetime.utcfromtimestamp(float(fmodified)), fsize, fhash,fp))

for inv_md_rel in all_inv_md_files:
    print "======== importing '%s' ========" %inv_md_rel
    inv_md = os.path.join(inventory_directory,inv_md_rel)
    #print "full path is %s" %inv_md
    # find inventory filename from metadata filename
    inv_filename_rel = re.sub("inv_md_","inv_",inv_md_rel)
    inv_filename_rel = re.sub("\\.json",".csv",inv_filename_rel)
    inv_filename = os.path.join(inventory_directory,inv_filename_rel)
    # Read the metadata
    inv_md_f = open(inv_md)
    inv = json.loads(inv_md_f.read())
    uuid = inv["uuid"]
    print "inventory file: %s" % inv_filename
    # Create inventory root record if needed.
    root_id = create_inventory_root(conn, inv)
    #print "found inventory root ID: %s" % root_id
    # Create inventory run record (always).
    ts = inv["timestamp"]
    duration = inv["duration_sec"]
    try:
        version = inv["bkupinv_version"]
    except KeyError:
        version = "unknown"
    ts_utc = from_utc(ts)
    #print ts_utc
    run_id = create_inventory_run(conn, root_id, ts_utc, duration, version)
    if (run_id is None):
        print "ignoring this run, already in database."
        continue
    # Insert hash values & filenames
    with open(inv_filename, 'r') as inv_file:
        with conn.cursor() as cur:
            inv_reader = csv.reader(inv_file, delimiter=',', quotechar='\"')
            for row in inv_reader:
                fp = row[0]
                fhash = row[1]
                fsize = row[2]
                fmodified = row[3]
                add_file_ref(cur,run_id,fp,fhash,fsize,fmodified)
    conn.commit()

    
