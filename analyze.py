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
    return datetime.utcnow().replace(
        tzinfo=psycopg2.tz.FixedOffsetTimezone(offset=0, name=None))

# take byte count and return a human readable string with units (MiB, GiB, TiB)
def hr_size(bytes):
    if (bytes > pow(1024,4)):
        return locale.format("%.2f",(bytes/(pow(1024,4))),grouping=True) + " TiB"
    elif (bytes > pow(1024,3)):
        return locale.format("%.2f",(bytes/(pow(1024,3))),grouping=True) + " GiB"
    elif (bytes > pow(1024,2)):
        return locale.format("%.2f",(bytes/(pow(1024,2))),grouping=True) + " MiB"
    elif (bytes > 1024):
        return locale.format("%.2f",(bytes/1024),grouping=True) + " KiB"
    else:
        return locale.format("%d",(bytes),grouping=True) + " bytes"

def header(s):
    sys.stdout.write("========== %s ==============\n" % s)

##### Summary #####
# How many files, hashes, and what is the overall size of the latest
# snapshot across all roots?
with conn.cursor() as cur:
    # Roots and runtimes
    header("Roots and Recent Runtimes")
    cur.execute("""select now()""")
    pg_now = cur.fetchone()[0]
    cur.execute("""select tstamp,duration,hostname,name,type from latest_inventory_runs lr INNER JOIN inventory_roots r
                ON lr.root_path=r.id order by tstamp""")
    results = cur.fetchall()
    for r in results:
        last_checked = r[0]
        duration = r[1]
        host = r[2]
        name = r[3]
        type = r[4]
        fmt = '   {0:.<30}{1:.<15}'
        host_info = "%s / %s" % (host,name)
        sys.stdout.write(fmt.format(host_info,type))
        n = pg_utcnow()
        sys.stdout.write(" (last update: %s)" % str(abs(last_checked-n)))
        sys.stdout.write("\n")
    header("Sizes")
    cur.execute("""SELECT sum(filesize), count(distinct(hash)), count(distinct(file)), count(distinct(i.id)) FROM
                latest_inventory_runs lr INNER JOIN inventory_items i ON lr.id=i.inventory_run""")
    results = cur.fetchone()
    total_size = results[0]
    distinct_hashes = results[1]
    distinct_files = results[2]
    total_items = results[3]
    print "Total Size: %s" % hr_size(total_size)
    print "Total Items: %s" % locale.format("%d", total_items, grouping=True)
    print "Distinct Relative Paths: %s" % locale.format("%d", distinct_files, grouping=True)
    print "Distinct Hashes: %s" % locale.format("%d", distinct_hashes, grouping=True)
    header("Distinct Size")
    # Compute the size of all unique hashes seen in the latest imports
    cur.execute("""select sum(filesize) from (select distinct on (hash)  hash, filesize from inventory_items i INNER JOIN all_latest_inventory_runs r ON i.inventory_run=r.id ORDER BY hash) x;""")
    results = cur.fetchone()
    total_size = results[0]
    print "De-duped Size: %s" % hr_size(total_size)

    header("Status of Master Volumes")
    # Determine how much of our masters exist in some backup
    cur.execute("""SELECT i.id, r.name, r.hostname from latest_inventory_runs i inner join inventory_roots r on i.root_path=r.id where r.type='master' order by r.hostname,r.name""")
    results = cur.fetchall()
    for (run_id, root_name, hostname) in results:
        # We want to find de-duped size for each master
        cur.execute("""select sum(filesize) from (select distinct on (hash)  hash, filesize from inventory_items i INNER JOIN all_latest_inventory_runs r ON i.inventory_run=%s and i.inventory_run=r.id ORDER BY hash) x""", (run_id,))
        r = cur.fetchone()
        fmt = '   {0:.<30}{1:<15}'
        host_info = "%s / %s" % (hostname, root_name)
        sys.stdout.write(fmt.format(host_info,hr_size(r[0])) + "\n")
    header("Corruptions")
    print "  todo"
    

