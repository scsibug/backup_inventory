import sqlite3

db_version = 1

# Table definitions
# Metadata about the current version
backup_md_t = "backup_md"
# Top-level paths to backup
inv_roots_t = "inventory_roots"
# Inventory runs (full lists of paths/hashes)
inventory_runs_t = "inventory_runs"
# Hashes
hashes_t = "hashes"
# Filenames
file_refs_t = "file_references"
# Mapping between an inventory run, and filenames/hashes.
inventory_items_t = "inventory_items"


def new_conn(filename):
    return sqlite3.connect(filename, detect_types=sqlite3.PARSE_DECLTYPES)

def close_conn(conn):
    conn.close()

def is_db_empty(conn):
    try:
        current_db_version(conn)
        return False
    except:
        return True

def is_db_current(conn):
    return (current_db_version(conn) == db_version)

def current_db_version(conn):
    c = conn.cursor()
    vr = c.execute('SELECT version FROM {bt}'.format(bt=backup_md_t)).fetchone()
    return vr[0]

    # Create database tables, assumes the database did not previously exist.
def initialize_database(conn):
    c = conn.cursor()
    # Table for metadata.  Just stores database version for now.
    c.execute('CREATE TABLE {bt} (version INTEGER)'.format(bt=backup_md_t))
    c.execute('INSERT INTO {bt} (version) VALUES ({v})'.format(bt=backup_md_t, v=db_version))

    c.execute('CREATE TABLE {ir} (id INTEGER PRIMARY KEY,'
              'uuid TEXT, name TEXT, path TEXT, description TEXT,'
              'type TEXT)'.format(ir=inv_roots_t))

    c.execute('CREATE TABLE {iru} (id INTEGER PRIMARY KEY,'
              'root_path INTEGER, tstamp TIMESTAMP)'.format(iru=inventory_runs_t))

    c.execute('CREATE TABLE {h} (id INTEGER PRIMARY KEY,'
              'hash TEXT)'.format(h=hashes_t))

    c.execute('CREATE TABLE {fr} (id INTEGER PRIMARY KEY,'
              'rel_path TEXT)'.format(fr=file_refs_t))

    c.execute('CREATE TABLE {ii} (id INTEGER PRIMARY KEY,'
              'inventory_run INTEGER REFERENCES {iru}(id),'
              'hash INTEGER REFERENCES {h}(id),'
              'file INTEGER REFERENCES {fr}(id),'
              'modified TIMESTAMP,'
              'filesize INTEGER)'
              .format(ii=inventory_items_t,
                      iru=inventory_runs_t,
                      h=hashes_t,
                      fr=file_refs_t))
    
    # Path data (read from config file)
    # Snapshot of a path (files, modification dates, sizes, hash values)
    # Disk data (UUID, name)
    # Hash values backed up to a disk.
    conn.commit()
    
def ready_database(conn):
    if (is_db_empty(conn)):
        initialize_database(conn)
    elif (is_db_current(conn)):
        pass
    else:
        raise Error("Database migration needed, but none have been defined")
