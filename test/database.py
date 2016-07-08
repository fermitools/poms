import psycopg2
import psycopg2.extras
import pprint
import sys
import utils

class Database:

    def __init__(self):

        conn_string = "host=%s dbname=%s user=%s password=%s port=%s" % (utils.get_db_info())

        try:
            self.conn = psycopg2.connect( conn_string )
        except Exception as e:
            print "I am unable to connect to the database with connection string: ", conn_string
            print e
            sys.exit()

        self.cursor = self.conn.cursor()
  
        self.dict_cursor = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)


    def query_all(self, query):
        self.cursor.execute(query)
        return self.cursor.fetchall()


    def query_one(self, query):
        self.cursor.execute(query)
        return self.cursor.fetchone()


    def query_one_dict(self, query):
        self.dict_cursor.execute(query)
        return self.dict_cursor.fetchone()


    def query_all_dict(self, query):
        self.dict_cursor.execute(query)
        return  self.dict_cursor.fetchall()


    def commit(self):
        self.conn.commit()


    def rollback(self):
        self.conn.rollback()


    def close(self):
        self.cursor.close()
        self.dict_cursor.close()
        self.conn.close()


if __name__ == '__main__':
    db = Database()

    print "**query_all example**"
    records = db.query_all("SELECT * FROM experimenters")
    pprint.pprint(records)
    print "---"

    print "**query_one example**"
    record = db.query_one("SELECT * FROM experimenters")
    print record[0]
    pprint.pprint(record)
    print "---"

    print "**query_all_dict example**"
    records = db.query_all_dict("SELECT * FROM experimenters")
    for record in records:
        print record['first_name']
    print "---"

    print "**query_one_dict example**"
    record = db.query_one_dict("SELECT * FROM experimenters")
    print record['first_name']
    pprint.pprint(record)

    db.close()
