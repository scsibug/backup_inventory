#!/usr/bin/python
# Create incremental backups based on an inventory.

# The goal is to fill up as much of a piece of media (like a blu-ray
# disc) with content.

import sys
reload(sys)
sys.setdefaultencoding("utf-8")
import csv
import json
import os
from datetime import datetime, MINYEAR
import time
import re
import pytz
import shutil
import uuid
import codecs
#from operator import attrgetter

# CSV Dialect
csv.register_dialect('Inventory',
                     delimiter=',',
                     doublequote=False,
                     quotechar='"',
                     lineterminator='\n',
                     escapechar='\\',
                     quoting=csv.QUOTE_ALL)

# TODO: read the inventory AND metadata to determine the parent path for inventory items.

# Or, just scrap the attempt to reconcile the inventory files and the current backup, and just
# write items as we find them, who cares what the inventory says....

# This contains data for the latest snapshot of files paths/hashes/modification dates we are trying to backup.
inv_raw_filename = sys.argv[1]
inv_filename = os.path.abspath(inv_raw_filename)
# This contains the description of the backup (containing the parent path as the "root" element of the json file)
inv_description_filename = sys.argv[2]
inv_description = os.path.abspath(inv_description_filename)
# Where to look for incremental backup log history (one file per backup)
log_dir = os.path.abspath(sys.argv[3])
# how much to backup in this run
backup_size = int(sys.argv[4])
# stop when we are this close to the end.  Prevents piling on all the small files every time.
backup_limit = 100000000
# where to store the copies of backup files prior to burning
temp_parent_dir = os.path.abspath(sys.argv[5])
# human-readable name of backup
backup_name = sys.argv[6]
# This backups uuid
backup_uuid = str(uuid.uuid4())
# Full/unique backup name
unique_backup_name = backup_name+"-"+backup_uuid

class BackupFile:
    """A file that needs to be backed up"""
    def __repr__(self):
        return "BackupFile(%s, %d, %d)" % (self.filename, self.modified, self.size)
    def __init__(self, filename, hash, modified, size):
        self.count = 0
        self.last_backup_date = None
        self.filename = filename
        self.hash = hash
        self.modified = modified
        self.size = size
    def updateBackupDate(new_date):
        if ((new_date > self.last_backup_date) or
            (self.last_backup_date is None)):
            self.last_backup_date = new_date

# Read each line of the inventory file
print("Parsing inventory: %s" % inv_filename)

# We want to build a dictionary of hashes to date/count info
# This structure will be:
# 'hash' => {count: 0, date: None}
# count is how many times we have written to this to backups in the past.
# date is the most recent time it appears in a backup (nil if never backup up)
# filename, size, and timestamp are from the original inventory
incr_history = []

# TODO: CSV module doesn't support unicode??
with open(inv_filename, "r") as inv_file:
    inv_reader = csv.reader(inv_file, dialect='Inventory')
    for row in inv_reader:
        # this inserts one entry per hash, which won't allow us to reconstruct the filesystem.
        # instead we should maintain an array of items that we can then sort by date.
        incr_history.append(BackupFile(row[0],row[1],int(row[3]),int(row[2])))

# Determine the root prefix of the files described in the inventory
print("Parsing inventory description: %s" % inv_description)
inv_desc_f = codecs.open(inv_description,encoding='utf-8')
inv_json = json.loads(inv_desc_f.read())
root_path = inv_json["root"]
print("Root path of all files is: %s" % root_path)
inv_name = inv_json["name"]
inv_hostname = inv_json["hostname"]
print("Backup source name is: %s" % inv_name)
print("Backup source hostname is: %s" % inv_hostname)

# Prepare the directory which will contain inventory and metadata subdirs
temp_inv_dir = temp_parent_dir+os.path.sep+backup_name+os.path.sep+"inventory"
if (not os.path.exists(temp_inv_dir)):    os.makedirs(temp_inv_dir)
dt = datetime.now(pytz.timezone('UTC'))
short_ts = dt.strftime("%Y-%m-%d_%H%M%S")
iso_ts = dt.isoformat()
# inventory this path
media_inv_filename = "inv_"+inv_hostname+"_"+inv_name.replace(" ","_")+"_"+short_ts+".csv"
csv_file = codecs.open(os.path.join(temp_inv_dir,media_inv_filename), 'w',encoding='utf-8')
csv_writer = csv.writer(csv_file, 'Inventory')
temp_md_dir = temp_parent_dir+os.path.sep+backup_name+os.path.sep+"metadata"
if (not os.path.exists(temp_md_dir)):    os.makedirs(temp_md_dir)
# copy metadata
shutil.copy2(inv_description,temp_md_dir)
# Prepare the directory we will backup to
temp_dir = temp_parent_dir+os.path.sep+backup_name+os.path.sep+inv_hostname+os.path.sep+inv_name
if (not os.path.exists(temp_dir)):
    os.makedirs(temp_dir)
else:
    sys.exit("This backup directory already exists!")

# Create a dict ordered by hash
file_by_hash = {}
for f in incr_history:
    h = f.hash
    found = file_by_hash.get(h)
    if (found == None):
        file_by_hash[h] = [f]
    else:
        found.append(f)

# Now, read through all of the incremental logs

# Incremental log format should be a list of hashes.
# The filename gives the backup date.


# We would need to be able to order incr_history by date.
# And, look up values by hash.


#TODO: if a log directory doesn't exist, create it
#find all files in the log directory
all_log_files = [ f for f in os.listdir(log_dir) ]
for log_filename in all_log_files:
    log_path = os.path.join(log_dir,log_filename)
    log_time = datetime.strptime((re.sub("\\.txt","",log_filename)),"%Y-%m-%d_%H%M%S")
    print "======== checking '%s' ========" %log_time
    # TODO process incremental logs
    # from filename determine the backup date
    with codecs.open(log_path, "r",encoding='utf-8') as log_file:
        for line in log_file:
            hash_entry = line.rstrip()
            # Lookup hash from incr_history, and set the date if this is more recent
            found = file_by_hash.get(hash_entry)
            if (found == None):
                pass
               #"hash not found.. this file is no longer in the current snapshot we are backing up"
            else:
                for entry in found:
                    if (entry.last_backup_date == None or entry.last_backup_date < log_time):
                        entry.last_backup_date = log_time
                
# TODO: Get the current log file ready
dt = datetime.now(pytz.timezone('UTC'))
short_ts = dt.strftime("%Y-%m-%d_%H%M%S")
curr_log_filename = short_ts+".txt"
log_file = codecs.open(os.path.join(log_dir,curr_log_filename), "w",encoding='utf-8')


# Sort incr_history by last_backup_date, then modified date. (oldest first).
mindate = datetime(MINYEAR, 1, 1)
def getBackupDate(x):
    return x.last_backup_date or mindate
incr_history = sorted(incr_history, key=getBackupDate, reverse=False)


# Now that we have annotations on all the items in our incr_history,
# we should pick out files in order to maximize the number of backups
# for a file.

# Using number of backups has bad properties when our churn between backups exceeds the amount of space.  We could keep backing up temp files!
# If instead we backup based on modification date, we do much better.


# While we have space left; we should find items with exactly N backups and add them to our to-backup-list.

# Start with the earliest file modification date.

current_size = 0

# There are race conditions we are trying to prevent here:
# Check the size of the file to determine if we can move it.
# If the filesize and modification date do not match, ignore.
#  -- This means we don't pick up very frequently-modified files.
# Write the file hash to the current log file as well

# Set of hashes that we did backup.
backup_set = set()

# Files we did backup
backed_up = []

# Files we did not backup
backup_omitted = []

for f in incr_history:
    print f
    print "current size is %d" % current_size
    print "checking %s" % (f.filename)
    print "last backed up date: %s" % (f.last_backup_date)
    print "Currently we are at %.2f %% capacity" % (100.0*current_size/backup_size)

    if ((current_size + f.size) <= backup_size):
        print "...adding file"
        current_size = current_size + f.size
        # TODO: copy files to temp location
        # Need to know where this file is in order to copy it!
        # Possibly should try to verify that the modification date/hash are the same
        #print f.filename.encode('utf8').decode('ascii',errors='ignore')
        x = f.filename.encode('utf-8')
        print x
        full_filepath = os.path.abspath(root_path.encode('utf-8')+os.sep.encode('utf-8')+f.filename.encode('utf-8'))
        dest_filepath = os.path.abspath(temp_dir.encode('utf-8')+os.sep.encode('utf-8')+f.filename.encode('utf-8'))
        dest_dir = os.path.dirname(dest_filepath)
        # Check if modification date matches
        mtime = int(os.path.getmtime(full_filepath))
        size = os.path.getsize(full_filepath)
        # Proceed if the modification timestamp and filesize match
        if (mtime == f.modified and size == f.size):
            if (not os.path.exists(dest_dir)):
                print "making directory %s" % (dest_dir)
                os.makedirs(dest_dir)
            print "Backing up %s" % (full_filepath)
            shutil.copy2(full_filepath,dest_filepath)
            # Add log entry for hash
            print "  hash: %s" % f.hash
            # Write CSV entry for file
            csv_writer.writerow( (f.filename.encode('utf-8'), f.hash, f.size, f.modified))
            backed_up.append(f)
            if (f not in backup_set):
                backup_set.add(f.hash)
                log_file.write(f.hash+"\n")
        else :
            print "file metadata has changed since inventory, ignoring."
            backup_omitted.append(f)
    elif (backup_size - current_size <= backup_limit):
        print "Not enough room for more files, finishing up"
        backup_omitted.append(f)
        break
    else:
        print "(skipping large file: %s)" % f.filename
        backup_omitted.append(f)
print "===================="
if (len(backed_up) > 0):
    # Analyze files that are set to be backed up
    print "backed up %i files" % len(backed_up)
    backed_up = sorted(backed_up, key=getBackupDate, reverse=False)
#    print "Newest file we did backup: %s"% backed_up[0].last_backup_date
    print "Newest file we did backup:"
    print "    name       : %s"% backed_up[0].filename
    print "    modified   : %s"% time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(backed_up[0].modified))
    print "    last backup: %s"% backed_up[0].last_backup_date
    print "    size       : %s"% backed_up[0].size
else:
    print "No files were backed up."
# Analyze files that were omitted
print "===================="
if (len(backup_omitted) > 0):
    backup_omitted = sorted(backup_omitted, key=getBackupDate, reverse=False)
    print "omitted up %i files" % len(backup_omitted)
    print "Oldest file we did not backup:"
    print "    name       : %s"% backup_omitted[0].filename
    print "    modified   : %s"% time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(backup_omitted[0].modified))
    print "    last backup: %s"% backup_omitted[0].last_backup_date
    print "    size       : %s"% backup_omitted[0].size
else:
    print "All files were backed up."
# What is the newest file we did backup, and the oldest file we did not?
# Oldest file we did not backup

# close log file
log_file.close()
#close CSV inventory
csv_file.close()

# TODO
# Generate inventory config
with codecs.open(backup_name, "w",encoding='utf-8') as inventory_config:
    inv_config_json = {'global': {'hostname': inv_hostname},
                       'paths': [
                           {
                               'description': backup_name,
                               'uuid': backup_uuid,
                               'name': backup_name,
                               'path': root_path,
                               'excludes': [],
                               'type': 'archive',
                               'image_hash': '',
                               'image_size': '',
                               'image_block_size': '',
                               'device_hash': '',
                               'inventory_file': ''
                           }
                        ]
                       }
    inventory_config.write(json.dumps(inv_config_json,indent=4))

# Put the bkupinv style metadata in the output directory
# Create a disc image!
