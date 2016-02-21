#!/usr/bin/python
# Create incremental backups based on an inventory.

# The goal is to fill up as much of a piece of media (like a blu-ray
# disc) with content.

import sys
import csv
import os
from operator import attrgetter

# CSV Dialect
csv.register_dialect('Inventory', delimiter=',',doublequote=False,quotechar='"',lineterminator='\n',escapechar='\\',quoting=csv.QUOTE_ALL)

# TODO: read the inventory AND metadata to determine the parent path for inventory items.

# Or, just scrap the attempt to reconcile the inventory files and the current backup, and just
# write items as we find them, who cares what the inventory says....

inv_filename = os.path.abspath(sys.argv[1])
log_dir = os.path.abspath(sys.argv[2])
backup_size = int(sys.argv[3])
temp_dir = os.path.abspath(sys.argv[4])

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

with open(inv_filename, "r") as inv_file:
    inv_reader = csv.reader(inv_file, dialect='Inventory')
    for row in inv_reader:
        # this inserts one entry per hash, which won't allow us to reconstruct the filesystem.
        # instead we should maintain an array of items that we can then sort by date.
        incr_history.append(BackupFile(row[0],row[1],int(row[3]),int(row[2])))

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
    print "======== checking '%s' ========" %log_path
    # TODO process incremental logs


# Sort incr_history by last_backup_date, then modified date. (oldest first).
incr_history = sorted(incr_history, key=attrgetter('last_backup_date'))
incr_history = sorted(incr_history, key=attrgetter('modified'))
print incr_history



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

for f in incr_history:
    print "current size is %d" % current_size
    print "checking %s" % (f.filename)
    print "Currently we are at %.2f %% capacity" % (100*current_size/backup_size)
    if (current_size + f.size <= backup_size):
        print "...adding file"
        current_size = current_size + f.size
    else:
        print "...file is too large, skipping"

