#!/usr/bin/python
# Pack as much stuff from a given directory root into a destination,
# not exceeding the provided filesize.

import os, sys
import shutil

# Usage:
# ./pack.py [max-size-bytes] source destination [inventory_file]

# pack will not take everything from the source, files will be skipped
# once they cause the destination to exceed the max size.  Anything
# that is selected will be appended to the inventory_file and will
# always be skipped.  This allows you to keep running the script with
# a new destination, and the inventory_file will prevent inclusion of
# duplicates.

# Taken from http://stackoverflow.com/questions/1392413/calculating-a-directory-size-using-python
def directory_size(path):
    total_size = 0
    seen = set()
    
    for dirpath, dirnames, filenames in os.walk(path):
        for f in filenames:
            fp = os.path.join(dirpath, f)
            try:
                stat = os.stat(fp)
            except OSError:
                continue
            if stat.st_ino in seen:
                continue
            seen.add(stat.st_ino)
            total_size += stat.st_size
    return total_size  # size in bytes

def get_file_sizes(path):
    # Returns an array of pairs (rel-path,size)
    seen = set()
    sizes = set()
    for dirpath, dirnames, filenames in os.walk(path):
        for f in filenames:
            this_relpath = os.path.join(path,f)
            fp = os.path.join(dirpath, f)
            try:
                stat = os.stat(fp)
            except OSError:
                continue
            if stat.st_ino in seen:
                continue
            seen.add(stat.st_ino)
#            if (not this_relpath == '.'):
            sizes.add( (fp, stat.st_size) )
    return sizes


# Determine the existing size of the destination.
max_bytes = int(sys.argv[1])
source = sys.argv[2]
destination = sys.argv[3]
print "using destination %s" % destination
dest_size = directory_size(destination)
print "destination size at start is %d" % dest_size

# allowed padding: stop skipping items in order to pack if we have
# less than this many bytes.  This prevents attempts to shove tons of
# small files (like .txt,.DS_Store,m3u, etc.) files onto the end of
# every archive.
allowed_padding = int(max_bytes*0.01)

# Find all the already-copied files
inventory = set()
write_inventory = False
if len(sys.argv) > 4:
    inv_filename = sys.argv[4]
    # Create inventory file if needed
    if (!os.path.isfile(inv_file))):
        i_empty = open(inv_file,'w');
        i_empty.write("")
        i_empty.close()
    inventory_file = open(inv_filename,'rw')
    inventory = set(inventory_file.read().splitlines())
    inventory_file.close()
    inventory_file = open(sys.argv[4],'a')
    write_inventory = True
    print "Found %d items in inventory" % len(inventory)

# Find filepaths and sizes for the source directory
all_sizes = get_file_sizes(source)

# Iterate through sizes, skip anything that puts us over the max size.
curr_size = 0
stop_skip = False
for (fp,bytes) in all_sizes:
    if (fp in inventory):
        continue
    if (not stop_skip and (curr_size + allowed_padding > max_bytes)):
        # We now have less than allowed_padding space left
        print "switching to stop skip mode!"
        stop_skip = True
    if ((bytes+curr_size) <= max_bytes):
        print "have enough size for this one"
        curr_size += bytes
        if '\n' in fp:
            print "filename contains a newline, skipping writing this to inventory."
        else:
            #print "copying %s" % fp
            prefix = os.path.commonprefix([source, fp])
            relpath = os.path.relpath(fp, prefix)
            full_dest = os.path.join(destination,relpath)
            print "copying %s to %s" % (fp, full_dest)
            if not os.path.exists(os.path.dirname(full_dest)):
                os.makedirs(os.path.dirname(full_dest))
            shutil.copy2(fp,full_dest)
            if write_inventory:
                inventory_file.write(fp+"\n")
    elif stop_skip:
        print "this one would have been skipped: %s" % fp
        break
print "final size was %d, %f%% utilized" % (curr_size, 100.0*curr_size/max_bytes)
