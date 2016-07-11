from webclient import WebClient
from database import Database
import unittest
import utils


client = WebClient(base_url='')


class randomMethods(unittest.TestCase):

    def setUp(self):
        self.db = Database()

    def tearDown(self):
        self.db.close()

    def test_users(self):
        record = self.db.query_one_dict("SELECT * FROM experimenters")
        first_user = record['first_name']
        self.assertEqual(first_user, "Mike")



if __name__ == '__main__':
    utils.setUpPoms()
    try:
        unittest.main(verbosity=2)
    finally:
        utils.tearDownPoms()
