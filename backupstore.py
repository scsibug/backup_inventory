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

class BackupStore:
    def __init__(self, storename):
        self.conn = sqlite3.connect(storename, detect_types=sqlite3.PARSE_DECLTYPES)

    def __enter__(self):
        return self

    def __exit__(self):
        self.conn.close()

    def is_db_empty(self):
        try:
            self.current_db_version()
            return False
        except:
            return True

    def is_db_current(self):
        return (self.current_db_version() == db_version)

    def current_db_version(self):
        c = self.conn.cursor()
        vr = c.execute('SELECT version FROM {bt}'.format(bt=backup_md_t)).fetchone()
        return vr[0]

    # Create database tables, assumes the database did not previously exist.
    def initialize_database(self):
        c = self.conn.cursor()
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
    
        # Create indexes
        c.execute('CREATE UNIQUE INDEX hash_idx ON {h}(hash)'.format(h=hashes_t))
        c.execute('CREATE UNIQUE INDEX file_idx ON {fr}(rel_path)'.format(fr=file_refs_t))
        c.execute('CREATE INDEX inventory_items_hash ON {ii}(hash)'.format(ii=inventory_items_t))
        c.execute('CREATE INDEX inventory_items_file ON {ii}(file)'.format(ii=inventory_items_t))
    
        self.conn.commit()
    
    def ready_database(self):
        if (self.is_db_empty()):
            self.initialize_database()
        elif (self.is_db_current()):
            pass
        else:
            raise Error("Database migration needed, but none have been defined")
