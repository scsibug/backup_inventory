import sqlite3

db_version = 1

backup_md_table = "backup_md"

def new_conn(filename):
    return sqlite3.connect(filename)

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
    vr = c.execute('SELECT version FROM {bt}'.format(bt=backup_md_table)).fetchone()
    return vr[0]

    # Create database tables, assumes the database did not previously exist.
def initialize_database(conn):
    c = conn.cursor()
    c.execute('CREATE TABLE {bt} (version integer)'.format(bt=backup_md_table))
    c.execute('INSERT INTO {bt} (version) VALUES ({v})'.format(bt=backup_md_table, v=db_version))
    conn.commit()
    
def ready_database(conn):
    if (is_db_empty(conn)):
        initialize_database(conn)
    elif (is_db_current(conn)):
        pass
    else:
        raise Error("Database migration needed, but none have been defined")
