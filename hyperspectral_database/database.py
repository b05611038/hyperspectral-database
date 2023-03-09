import os
import abc
import json
import copy

import numpy as np
from pymongo import MongoClient, InsertOne, DeleteOne

from . import __version__
from .template import Template


__all__ = ['HyperspectralDatabase']


class Database(abc.ABC):
    def __init__(self,
            db_name,
            user_id,
            passwd,
            host,
            port):

        if not isinstance(db_name, str):
            raise TypeError('Argument: db_name must be a string.')

        if not isinstance(user_id, str):
            raise TypeError('Argument: user_id must be a string.')

        if not isinstance(passwd, str):
            raise TypeError('Argument: passwd must be a string.')

        if not isinstance(host, str):
            raise TypeError('Argument: host must be a string.')

        if not isinstance(port, int):
            raise TypeError('Argument: port must be a int.')

        self._db_name = db_name
        self._user_id = user_id
        self._passwd = passwd
        self._host = host
        self._port = port

        self.connect(self.host, self.port, self.db)
        self.__tmp = {}

    def connect(self, host, port, db_name):
        self.mongo_client = MongoClient(host = host, port = port)
        self.database = self.mongo_client[db_name]
        print('Successfully connect the mongoDB server.')
        return None

    def temp_var(self, var):
        if not isinstance(var, str):
            raise TypeError('Argument: var must be a Python string object.')

        if len(var) == 0:
            raise ValueError('Argument: var cannot be empty string.')

        stored_object = self.__tmp.get(var, None)
        return stored_object

    def add_temp_var(self, var, obj):
        if not isinstance(var, str):
            raise TypeError('Argument: var must be a Python string object.')

        if len(var) == 0:
            raise ValueError('Argument: var cannot be empty string.')

        self.__tmp[var] = obj
        return None

    def delete_temp_var(self, var):
        if not isinstance(var, str):
            raise TypeError('Argument: var must be a Python string object.')

        if self.__tmp.get(var, None) is not None:
            del self.__tmp[var]

        return None

    def __eq__(self, other):
        equal = False
        if self.port == other.port and self.db == other.db and self.user == other.user \
                and self.host == other.host:
            equal = True

        return equal

    @abc.abstractmethod
    def __repr__(self):
        raise NotImplementedError()

    @abc.abstractmethod
    def help(self):
        raise NotImplementedError()

    @abc.abstractmethod
    def find(self, collection, key_value_pair):
        return NotImplementedError()

    @abc.abstractmethod
    def find_one(self, collection, key_value_pair):
        return NotImplementedError()

    @abc.abstractmethod
    def count_documents(self, colleciton, key_value_pair):
        return NotImplementedError()

    @property
    def host(self):
        return self._host

    @host.setter
    def host(self, new_host, new_port = 27017):
        if not isinstance(new_host, str):
            raise TypeError('Argument: new_host must be a string.')

        if not isinstance(new_port, int):
            raise TypeError('Argument: new_port must be a int.')

        self._host = new_host
        self._port = new_port
        self.reinit_db()

        lprint('Successfully change database host.')

        return None

    @property
    def port(self):
        return self._port

    @property
    def db(self):
        return self._db_name

    @db.setter
    def db(self, db_name):
        if not isinstance(db_name, str):
            raise TypeError('Argument: db must be a string.')

        self._db_name = db_name
        self.reinit_db()

        lprint('Successfully change database name.')

        return None

    @property
    def user(self):
        return self._user_id

    @user.setter
    def user(self, new_id, new_passwd):
        if not isinstance(new_id, str):
            raise TypeError('Argument: new_id must be a string.')

        if not isinstance(new_passwd, str):
            raise TypeError('Argument: new_passwd must be a string.')

        self._user_id = new_id
        self._passwd = new_passwd
        self.reinit_db()

        lprint('Successfully change the user of Database client.')

        return None

    @property
    def version(self):
        return __version__

    @property
    def connected(self):
        client_connect = True
        if self.mongo_client is None:
            client_connect = False

        collection_acquire = True
        if self.database is None:
            collection_acquire = False

        _connected = False
        if client_connect and collection_acquire:
            _connected = True

        return _connected

    def close(self):
        self.mongo_client = None
        self.database = None
        self.__tmp = {}
        return None


class HyperspectralDatabase(Database):
    def __init__(self, 
        db_name = 'hyperspectral',
        user_id = '',
        passwd = '',
        host = '192.168.50.146',
        port = 27087):

        super(HyperspectralDatabase, self).__init__(
                db_name = db_name,
                 user_id = user_id,
                 passwd = passwd,
                 host = host,
                 port = port)

        self._collection_list = ['data']
        self.collections = {}
        for collection in self._collection_list:
            self.collections[collection] = self.database[collection]

    def __repr__(self):
        lines = 'HyperspectralDatabase version: {0}\n'.format(__version__)
        lines += '  User: {0}\n  Host: {1}\n  Port: {2}\n'.format(self.user, self.host, self.port)
        lines += '  Database: {0}\n    Collections:\n'.format(self.db)
        for col in self._collection_list:
            lines += '      {0}\n'.format(col)

        return lines

    def help(self):
        lines = self.__repr__()
        lines += '\nSupported methods:\n'
        for method in self.__class__.__dict__.keys():
            if method[0] != '_':
                lines += '{0}.{1}\n'.format(self.__class__.__name__, method)

        print(lines[: -1])

        return None

    def find(self, query, collection = 'data'):
        if not isinstance(collection, str):
            raise TypeError('Argument: collection must be a Python string object.')

        if collection.lower() not in self._collection_list:
            raise ValueError(collection, ' is not a valid collection selection.')

        if not isinstance(query, dict):
            raise TypeError('The argument: query only accept Python dictionary object.')

        return self.collections[collection.lower()].find(query)

    def find_one(self, query, collection = 'data'):
        if not isinstance(collection, str):
            raise TypeError('Argument: collection must be a Python string object.')

        if collection.lower() not in self._collection_list:
            raise ValueError(collection, ' is not a valid collection selection.')

        if not isinstance(query, dict):
            raise TypeError('The argument: query only accept Python dictionary object.')

        return self.collections[collection.lower()].find_one(query)

    def count_documents(self, query, collection = 'data'):
        if not isinstance(collection, str):
            raise TypeError('Argument: collection must be a Python string object.')

        if collection.lower() not in self._collection_list:
            raise ValueError(collection, ' is not a valid collection selection.')

        if not isinstance(query, dict):
            raise TypeError('The argument: query only accept Python dictionary object.')

        return self.collections[collection.lower()].count_documents(query)

    def insert_data(self, file, file_extension = '.json', collection = 'data', 
            data_args = ('datatype', 'species', 'spectral'), certain = False):
        if not isinstance(file, str):
            raise TypeError('Argument: file must be a Python string object.')

        if not isinstance(file_extension, str):
            raise TypeError('Argument: file_extension must be a Python string object.')

        if not os.path.isfile(file):
            raise OSError('No file object in the path:{0}'.format(file))

        if not file.endswith(file_extension):
            raise RuntimeError('Valid file must be a endswith {0}'.format(file_extension))

        if not isinstance(collection, str):
            raise TypeError('Argument: collection must be a Python string object.')

        if collection.lower() not in self._collection_list:
            raise ValueError(collection, ' is not a valid collection selection.')

        if not isinstance(data_args, (list, tuple)):
            raise TypeError('Argument: data_args must be a Python list/tuple object.')

        for e in data_args:
            if not isinstance(e, str):
                raise TypeError('Element in argument::data_args must be a Python string object.')

        if not isinstance(certain, bool):
            raise TypeError('Argument: certain must be a Python boolean object.')

        single_document = self._single_data_document(file, data_args, collection)
        if certain:
            requests = [InsertOne(single_document)]
            if len(requests) > 0:
                self.collections[collection].bulk_write(requests)

            print('Successfully insert file:{0} into {1}'.format(file, 
                    self.__class__.__name__))
        else:
            print('Not certain mode, no insertion in the database.')

        return None

    def batch_insert_data(self, directory, file_extension = '.json', collection = 'data', 
            data_args = ('datatype', 'species', 'spectral'), batch_size = 10000, 
            certain = False, progress = True):

        if not isinstance(directory, str):
            raise TypeError('Argument: directory must be a Python string object')

        if not os.path.isdir(directory):
            raise OSError('Path: {0} is not a directory.'.format(directory))

        if not isinstance(file_extension, str):
            raise TypeError('Argument: file_extension must be a Python string object.')

        if not isinstance(collection, str):
            raise TypeError('Argument: collection must be a Python string object.')

        if collection.lower() not in self._collection_list:
            raise ValueError(collection, ' is not a valid collection selection.')

        if not isinstance(data_args, (list, tuple)):
            raise TypeError('Argument: data_args must be a Python list/tuple object.')

        for e in data_args:
            if not isinstance(e, str):
                raise TypeError('Element in argument::data_args must be a Python string object.')

        if not isinstance(batch_size, int):
            raise TypeError('Argument: batch_size must be a Python int object.')

        if batch_size < 0:
            raise ValueError('Argument: batch_size must larger than zero.')

        if not isinstance(certain, bool):
            raise TypeError('Argument: certain must be a Python boolean object.')

        if not isinstance(progress, bool):
             raise TypeError('Argument: progress must be a Python boolean object.')

        json_files = []
        for f in os.listdir(directory):
            if f.endswith(file_extension):
                json_files.append(os.path.join(directory, f))

        file_numbers = len(json_files)
        if certain:
            requests, insert_index = [], self._get_insert_index()
            running_index, inner_batch_index = 0, 0
            for f in json_files:
                single_document = self._single_data_document(f, data_args, collection,
                        insert_index = insert_index)
                requests.append(InsertOne(single_document))
                insert_index += 1
                inner_batch_index += 1

                if progress:
                    running_index += 1
                    print('Acquring data progress: {0} / {1}'.format(running_index,
                                                                    file_numbers))

                if inner_batch_index == batch_size:
                    if len(requests) > 0:
                        self.collections[collection].bulk_write(requests)
                        print('Sucessfully insert {0} files into {1}'\
                                .format(len(requests), self.__class__.__name__))

                        requests = []

                    inner_batch_index = 0
                    print('Successfully reset file buffer.')

            if len(requests) > 0:
                self.collections[collection].bulk_write(requests)
        else:
            print('Not certain mode, no insertion in the database.')

        return None

    def _get_insert_index(self):
        insert_index = 0
        if self.count_documents({}, collection = 'data') > 0:
            cursur = self.find({}, collection = 'data')
            for doc in cursur:
                tmp_index = doc.get('insert_index', -1)
                try:
                    tmp_index = int(tmp_index)
                except ValueError:
                    tmp_index = 0

                insert_index = max(tmp_index, insert_index)

            insert_index += 1

        return insert_index

    def _single_data_document(self, json_file_path, data_args, collection, insert_index = None):
        single_data_document, content = Template(collection), {}
        with open(json_file_path, 'r') as f:
            contents = json.loads(f.read())
            f.close()

        source_filename = os.path.split(json_file_path)[-1]
        single_data_document['source_filename'] = source_filename

        for args in data_args:
            args_value = contents.get(args, None)
            if args_value is not None:
                single_data_document[args] = args_value

        if insert_index is None:
            insert_index = self._get_insert_index()

        single_data_document['insert_index'] = insert_index

        return single_data_document

    def delete_data(self, indices, collection = 'data', certain = False):
        if not isinstance(indices, (int, list, tuple)):
            raise TypeError('Argument: indices must be a Python list/tuple object')

        if isinstance(indices, int):
            indices = [indices]

        if not isinstance(collection, str):
            raise TypeError('Argument: collection must be a Python string object.')

        if collection.lower() not in self._collection_list:
            raise ValueError(collection, ' is not a valid collection selection.')

        if not isinstance(certain, bool):
            raise TypeError('Argument: certain must be a Python boolean object.')

        if certain:
            requests = []
            for index in indices:
                requests.append(DeleteOne({'insert_index': index}))

            if len(requests) > 0:
                self.collections[collection].bulk_write(requests)

            print('Successfully delete data with indices:{0}'.format(indices))
        else:
            print('Not certain mode, no deletion in the database.')

        return None

    def delete_all(self, collection = 'data', certain = False):
        if not isinstance(collection, str):
            raise TypeError('Argument: collection must be a Python string object.')

        if collection.lower() not in self._collection_list:
            raise ValueError(collection, ' is not a valid collection selection.')

        if not isinstance(certain, bool):
            raise TypeError('Argument: certain must be a Python boolean object.')

        if certain:
            requests = []
            documents = self.find({}, collection = collection)
            for doc in documents:
                object_id = doc.get('_id', None)
                if object_id is not None:
                    requests.append(DeleteOne({'_id': object_id}))

            if len(requests) > 1:
                self.collections[collection].bulk_write(requests)

            print('Successfully clear all data in {0}'.format(self.__class__.__name__))
        else:
            print('Not certain mode, no deletion in the database.')

        return None

    def get_data(self, queries, collection = 'data', 
                data_args = ('datatype', 'species', 'spectral')):
        if not isinstance(queries, (dict, list, tuple)):
            raise TypeError('Argument: queries must be a Python dict or list/tuple object.')

        if isinstance(queries, dict):
            queries = [queries]

        for query in queries:
            if not isinstance(query, dict):
                raise TypeError('Argument: query must be a Python ')

        if not isinstance(collection, str):
            raise TypeError('Argument: collection must be a Python string object.')

        if collection not in self._collection_list:
            raise ValueError(collection, ' is not a valid collection selection.')

        if not isinstance(data_args, (list, tuple)):
            raise TypeError('Argument: data_args must be a Python list/tuple object.')

        for e in data_args:
            if not isinstance(e, str):
                raise TypeError('Element in argument::data_args must be a Python string object.')

        data, counting = [], 0
        for query in queries:
            tmp_cursor = self.find(query, collection = collection)
            for doc in tmp_cursor:
                single_data = {}
                for args in data_args:
                    args_value = doc.get(args, 'unknown')
                    if args == 'spectral':
                        args_value = np.array(args_value)

                    single_data[args] = args_value

                data.append(single_data)
                counting += 1

        print('Acquiring {0} data in the {1}.'.format(counting, 
                self.__class__.__name__))

        return data

    def get_all_data(self, collection = 'data', 
            data_args = ('datatype', 'species', 'spectral')):

        return self.get_data({}, collection = collection,
                                 data_args = data_args)

    def get_data_by_indices(self, indices, collection = 'data',
                data_args = ('datatype', 'species', 'spectral')):
 
       if not isinstance(indices, (int, list, tuple)):
            raise TypeError('Argument: indices must be a Python list/tuple object')

       if isinstance(indices, int):
            indices = [indices]

       queries = []
       for index in indices:
           queries.append({'insert_index': index})

       return self.get_data(queries,
                            collection = collection,
                            data_args = data_args)

    def get_data_by_index_range(self, start, stop = None, step = None, collection = 'data',
                data_args = ('datatype', 'species', 'spectral')):

        if not isinstance(start, int):
            raise TypeError('Input argument must be a Python int object.')

        if start < 0:
            raise ValueError('Input argument must at least be zero.')

        if (stop is None) and (step is None):
            stop = copy.deepcopy(start)
            start = 0
            step = 1
        elif (stop is not None) and (step is None):
            step = 1
        elif (stop is None) and (step is not None):
            stop = copy.deepcopy(start)
            start = 0

        indices = [i for i in range(start, stop, step)]
        return self.get_data_by_indices(indices, 
                                        collection = collection,
                                        data_args = data_args)

    def get_data_by_datatypes(self, datatypes, collection = 'data',
                data_args = ('datatype', 'species', 'spectral')):

        if not isinstance(datatypes, (str, list, tuple)):
            raise TypeError('Arguemnt: datatypes must be a Python string or list/tuple object.')

        if isinstance(datatypes, str):
            datatypes = [datatypes]

        for e in datatypes:
            if not isinstance(e, str):
                raise TypeError('Element in argument:datatypes must be a Python string object.')

        queries = []
        for datatype in datatypes:
            queries.append({'datatype': datatype})

        return self.get_data(queries,
                            collection = collection,
                            data_args = data_args)

    def get_data_by_species(self, species, collection = 'data',
                data_args = ('datatype', 'species', 'spectral')):

        if not isinstance(species, (str, list, tuple)):
            raise TypeError('Arguemnt: species must be a Python string or list/tuple object.')

        if isinstance(species, str):
            species = [species]

        for s in species:
            if not isinstance(s, str):
                raise TypeError('Element in argument:species must be a Python string object.')

        queries = []
        for s in species:
            queries.append({'species': s})

        return self.get_data(queries,
                            collection = collection,
                            data_args = data_args)
    

