# coding=utf-8
import pymongo
import pymysql
from config import db_config

class database:
    DB_HOST = 'localhost'
    DB_PORT = '3306'
    DB_NAME = ''
    DB_USER = 'test'
    DB_PASS = 'test'
    DB_TYPE = 'mysql'
    collection = ''

    def __init__(self, config):
        if config not in db_config.keys():
            raise ValueError('Unknown database config:{0}'.format(config))
        conf = db_config[config]

        self.DB_HOST = conf['host']
        self.DB_PORT = conf['port']
        self.DB_NAME = conf['db_name']
        self.DB_USER = conf['user']
        self.DB_PASS = conf['password']
        self.DB_TYPE = conf['db_type']

    def get_db(self):

        if self.DB_TYPE == 'mysql':
            return self._mysql_connetion()

        elif self.DB_TYPE == 'mongo':
            return self._mongo_connection()
        else:
            raise ValueError('unknown database type:{0}'.format(self.DB_TYPE))

    def _mysql_connetion(self):
        return pymysql.connect(
            host=self.DB_HOST,
            port=self.DB_PORT,
            user=self.DB_USER,
            password=self.DB_PASS,
            db=self.DB_NAME,
            charset='utf8mb4')

    def _mongo_connection(self):
        link = 'mongodb://' + \
               self.DB_USER + ':' + \
               self.DB_PASS + '@' + \
               self.DB_HOST + ':' + \
               str(self.DB_PORT) + '/' + \
               self.DB_NAME + '?authMechanism=SCRAM-SHA-1'
        return pymongo.MongoClient(link)

    def query(self, query):
        if self.DB_TYPE == 'mysql':
            return self._query_mysql(query)
        elif self.DB_TYPE == 'mongo':
            return self._query_mongo(query)
        else:
            raise ValueError('unKnown database type: {0}'.format(self.DB_TYPE))

    def _query_mysql(self, query):
        with self._mysql_connetion().cursor(pymysql.cursors.DictCursor) as cur:
            cur.excute(query)
            rv = cur.fetchall()
        return rv

    def _query_mongo(self, query, args=()):
        raise pymongo.errors.OperationFailure('not support mongo query fucntion yet')

if __name__ == '__main__':
    pass
