#!/usr/bin/python
import psycopg2
import sys
database_name = sys.argv[1]
database_user = sys.argv[2]

conn = psycopg2.connect(database=database_name, user=database_user)

##### Summary #####
# How many files, hashes, and what is the overall size of the latest
# snapshot across all roots?
with conn.cursor() as cur:
    cur.execute("select sum(filesize) from (select items.id, root.path, files.rel_path, items.modified, items.filesize as filesize, root.rep_factor  from latest_inventory_runs runs, inventory_items items, file_references files, inventory_roots root where items.inventory_run = runs.id and items.file=files.id and runs.root_path=root.id ) fs")
    result = cur.fetchone()
    total_size = result[0]
    print "Total Size: %.2f GB" % (total_size/(1024*1024*1024))
