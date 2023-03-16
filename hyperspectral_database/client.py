import gridfs
from pymongo import MongoClient

from .base import Database

__all__ = ['LightWeightedDatabaseClient']


class LightWeightedDatabaseClient(Database):
    def __init__(self,
            db_name = 'hyperspectral',
            user_id = '',
            passwd = '',
            host = '192.168.50.146',
            port = 27087,
            gridfs = True):

        super(LightWeightedDatabaseClient, self).__init__(
                 db_name = db_name,
                 user_id = user_id,
                 passwd = passwd,
                 host = host,
                 port = port)

        self._collection_list = ['data', 'spectral']
        self.fs, self.collections = self._init_gridfs_collections(self.database,
                                                                  self._collection_list)

        self.gridfs = gridfs

    def _init_gridfs_collections(self, database, name_list):
        fs = gridfs.GridFS(database)
        collections = {}
        for name in name_list:
            collections[name] = database[name]

        return fs, collections

    @property
    def gridfs(self):
        return self._gridfs

    @gridfs.setter
    def gridfs(self, gridfs):
        if not isinstance(gridfs, bool):
            raise TypeError('Argument: gridfs must be a Python boolean object.')

        self._gridfs = gridfs
        return None

    def connect(self, host, port, db_name):
        self.mongo_client = MongoClient(host = host, port = port)
        self.database = self.mongo_client[db_name]
        return None

    def __repr__(self):
        lines = self.__class__.__name__ + '(gridfs={0})'.format(self.gridfs) 
        lines += ' # Client object for sychronized function.'
        return lines

    def find(self, query, collection = 'data'):
        return self.collections[collection.lower()].find(query)

    def find_one(self, query, collection = 'data'):
        return self.collections[collection.lower()].find_one(query)

    def count_documents(self, query, collection = 'data'):
        return self.collections[collection.lower()].count_documents(query)

    def help(self):
        print('Please use hyperspectral_database.HyperspectralDatabase to connect MongoDB.')
        return None


