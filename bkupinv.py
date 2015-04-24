#!/usr/bin/python
# Backup inventory script
# Create a description of the files that exist in a given path.

import sys
import os
import hashlib
import socket
import time
from datetime import datetime
import json

# Create a unique identifier for the snapshot.
# suggestions: hostname-yyyy-mm-dd-hh-mm-ss

# Write two files:
# Inventory Metadata
#  This would consist of metadata about the time the snapshot was made,
#  The host it was taken from
#  The time taken
#  Checksum type
#  etc.

# Inventory Contents
#   This would consist of a file listing and cryptographic hash of the
#   file contents of any directories that were included in the run.
#  quoted full file path , size in bytes, checksum, modification timestamp

def hash_file(filename):
   """Compute and return the SHA-256 hash of a given file by name."""
   h = hashlib.sha256()
   with open(filename,'rb') as file:
       chunk = 0
       while chunk != b'':
           # read 128K chunks
           chunk = file.read(128*1024)
           h.update(chunk)
   return h.hexdigest()

def read_config(file):
    """Read the given configuration file that describes the paths to inventory."""
    with open(file,"r") as configfile:
        config = configfile.read()
        c = json.loads(config)
        return c

# Create inventory
# directory is an absolute path
# excludes is a list of relative paths from absolute that we should discard.
def inventory_dir(directory, excludes_rel, output_file):
    """Create the inventory for a given directory, writing results to the given output file."""
    # expand excludes into full paths?
    excludes = map((lambda x: os.path.join(directory,x)), excludes_rel)
    for root, dirs, files in os.walk(directory):
        if (root in excludes):
#            print "excluding %s" % root
#            print "dirs are %s" % dirs
            dirs[:] = []
        else:
            for file in files:
                full_path = os.path.join(root, file)
                try:
                    if ((not os.path.islink(full_path)) and os.path.isfile(full_path)):
                        mtime = os.path.getmtime(full_path)
                        size = os.path.getsize(full_path)
                        fhash = hash_file(full_path)
                        output_file.write('%s,"%s",%d,%d\n' % 
                                          (json.dumps(full_path),fhash,size,mtime))
                except IOError as e:
                    print "I/O error({0}): {1}".format(e.errno, e.strerror)

def inventory_config(config):
    """Inventory items from a given configuration."""
    # get global configuration options
    hostname = socket.gethostname()
    try:
        hostname = config['global']['hostname']
    except Exception:
        pass
    # Read paths list
    for path in config['paths']:
        path_name = path['name']
        base_path = path['path']
        excl = path['excludes']
        t = path['type']
        print "type is %s" % (t)
        dt = datetime.now()
        short_ts = dt.strftime("%Y-%m-%d_%H:%M:%S")
        iso_ts = dt.isoformat("T")
        # inventory this path
        inventory_prefix = hostname+"_"+path_name.replace(" ", "_")+"_"+short_ts
        start = time.time()
        with open("inv_"+inventory_prefix+".csv",'w') as inv_output:
            inv = inventory_dir(base_path, excl, inv_output)
        end = time.time()
        # create inventory metadata
        with open("inv_md_"+inventory_prefix+".json",'w') as md_output:
            # metadata records:
            # hostname, timestamp, duration, path
            dur = end-start
            inv_md = {'hostname':hostname,
                      'bkupinv_version':'0.0.1',
                      'name': path_name,
                      'timestamp':iso_ts,
                      'duration_sec': round(dur,2),
                      'type': t}
            md_output.write(json.dumps(inv_md))
        print "Completed inventory of "+path_name


# Read config from command line
config = read_config(sys.argv[1])
# Perform the inventory
inventory_config(config)
print "Completed inventory of all paths."
