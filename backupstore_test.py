import unittest
import backupstore
from contextlib import contextmanager

@contextmanager
def backupstore_manager(storename):
    yield backupstore.BackupStore(storename)

class TestInitialize(unittest.TestCase):
#    def setUp(self):
#        self.f = ':memory:'
#        self.bs = backupstore.BackupStore(self.f)

#    def tearDown(self):
#        self.bs.close_conn()

    def run(self, result=None):
        with backupstore_manager(':memory:') as store:
            self.store = store
            super(TestInitialize, self).run(result)

    def test_create(self):
        self.store.initialize_database()

    def test_current_version(self):
        self.store.initialize_database()
        self.assertTrue(self.store.is_db_current())

    def test_ready_new_database(self):
        self.assertTrue(self.store.is_db_empty())
        self.store.ready_database()
        self.assertTrue(self.store.is_db_current())

        
if __name__ == '__main__':
    unittest.main()
