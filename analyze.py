#!/usr/bin/python
import psycopg2
import sys
import locale

locale.setlocale(locale.LC_ALL, 'en_US.UTF-8')

database_name = sys.argv[1]
database_user = sys.argv[2]

conn = psycopg2.connect(database=database_name, user=database_user)

##### Summary #####
# How many files, hashes, and what is the overall size of the latest
# snapshot across all roots?
with conn.cursor() as cur:
    cur.execute("""SELECT sum(filesize), count(distinct(hash)), count(distinct(file)), count(distinct(i.id)) FROM
                latest_inventory_runs lr INNER JOIN inventory_items i ON lr.id=i.inventory_run""")
    results = cur.fetchone()
    total_size = results[0]
    distinct_hashes = results[1]
    distinct_files = results[2]
    total_items = results[3]
    print "Total Size: %.2f GB" % (total_size/(1024*1024*1024))
    print "Total Items: %s" % locale.format("%d", total_items, grouping=True)
    print "Distinct Relative Paths: %s" % locale.format("%d", distinct_files, grouping=True)
    print "Distinct Hashes: %s" % locale.format("%d", distinct_hashes, grouping=True)

    
