class BackupFile:
    """A file that needs to be backed up"""
    def __repr__(self):
        return "BackupFile(%s, %d, %d, %s)" % (self.filename, self.modified, self.size, str(self.last_backup_date))
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
