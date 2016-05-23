import unittest
import backupstore

class TestInitialize(unittest.TestCase):
    def setUp(self):
        self.f = ':memory:'
        self.conn = backupstore.new_conn(self.f)

    def tearDown(self):
        backupstore.close_conn(self.conn)

    def test_create(self):
        backupstore.initialize_database(self.conn)

    def test_current_version(self):
        backupstore.initialize_database(self.conn)
        self.assertTrue(backupstore.is_db_current)

    def test_ready_new_database(self):
        self.assertTrue(backupstore.is_db_empty(self.conn))
        backupstore.ready_database(self.conn)
        self.assertTrue(backupstore.is_db_current(self.conn))

        
if __name__ == '__main__':
    unittest.main()
