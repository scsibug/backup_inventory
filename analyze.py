#!/usr/bin/python
import psycopg2
import sys
import locale
from datetime import datetime

locale.setlocale(locale.LC_ALL, 'en_US.UTF-8')

database_name = sys.argv[1]
database_user = sys.argv[2]

conn = psycopg2.connect(database=database_name, user=database_user)

def pg_utcnow():
    import psycopg2
    return datetime.utcnow().replace(
        tzinfo=psycopg2.tz.FixedOffsetTimezone(offset=0, name=None))

def header(s):
    sys.stdout.write("========== %s ==============\n" % s)


##### Summary #####
# How many files, hashes, and what is the overall size of the latest
# snapshot across all roots?
with conn.cursor() as cur:
    # Roots and runtimes
    header("Roots and Recent Runtimes")
    cur.execute("""select tstamp,duration,hostname,name,type from latest_inventory_runs lr INNER JOIN inventory_roots r
                ON lr.root_path=r.id order by tstamp""")
    results = cur.fetchall()
    for r in results:
        last_checked = r[0]
        duration = r[1]
        host = r[2]
        name = r[3]
        type = r[4]
        sys.stdout.write("  %s@%s (%s)" % (name,host,type))
        n = pg_utcnow()
        sys.stdout.write(" (last update: %s)" % str(n - last_checked))
        sys.stdout.write("\n")
    header("Sizes")
    cur.execute("""SELECT sum(filesize), count(distinct(hash)), count(distinct(file)), count(distinct(i.id)) FROM
                latest_inventory_runs lr INNER JOIN inventory_items i ON lr.id=i.inventory_run""")
    results = cur.fetchone()
    total_size = results[0]
    distinct_hashes = results[1]
    distinct_files = results[2]
    total_items = results[3]
    print "Total Size: %s GB (%s TB)" % (locale.format("%.2f",(total_size/(1024*1024*1024)),grouping=True),
                                        locale.format("%.2f",(total_size/(1024*1024*1024*1024)),grouping=True))
    print "Total Items: %s" % locale.format("%d", total_items, grouping=True)
    print "Distinct Relative Paths: %s" % locale.format("%d", distinct_files, grouping=True)
    print "Distinct Hashes: %s" % locale.format("%d", distinct_hashes, grouping=True)

