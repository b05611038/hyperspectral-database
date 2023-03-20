import os
import json
import copy
import warnings

import numpy as np

import gridfs
from pymongo import (UpdateOne,
                     InsertOne, 
                     DeleteOne)

from . import __version__
from .base import Database
from .utils import serialize, deserialize 
from .template import Template
from .synchronize import SynchronizedFunctionWapper 
from .pipeline import get_spectral_gridfs, get_spectral_list 


__all__ = ['HyperspectralDatabase']


class HyperspectralDatabase(Database):
    def __init__(self, 
            db_name = 'hyperspectral',
            user_id = '',
            passwd = '',
            host = '192.168.50.146',
            port = 27087,
            docs_num_per_request = 500000,
            synchronize_query_size = 50000,
            synchronize_worker = -1,
            synchronize_timeout = -1,
            gridfs = False):

        super(HyperspectralDatabase, self).__init__(
                 db_name = db_name,
                 user_id = user_id,
                 passwd = passwd,
                 host = host,
                 port = port)

        self.sync_wrapper = None
        self._collection_list = ['data', 'spectral']
        self.fs, self.collections = self._init_gridfs_collections(self.database,
                                                                  self._collection_list)

        self.docs_num_per_request = docs_num_per_request
        self.gridfs = gridfs

        self.sync_wrapper = SynchronizedFunctionWapper(self, 
                query_size = synchronize_query_size,
                timeout = synchronize_timeout)

        self.synchronize_query_size = synchronize_query_size
        self.synchronize_worker = synchronize_worker
        self.synchronize_timeout = synchronize_timeout

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

        if gridfs:
            warnings.warn('Although the Gridfs mode can access the original' + \
                    ' binary file in the {0}, it is very inefficient.'\
                    .format(self.__class__.__name__))

        self._gridfs = gridfs
        return None

    @property
    def docs_num_per_request(self):
        return self._docs_num_per_request

    @docs_num_per_request.setter
    def docs_num_per_request(self, docs_num_per_request):
        if not isinstance(docs_num_per_request, int):
            raise TypeError('Argument: docs_num_per_request must be a Python int object.')

        if docs_num_per_request <= 0:
            raise TypeError('Argument: docs_num_per_request must at least be one.')

        self._docs_num_per_request = docs_num_per_request
        return None

    @property
    def synchronize_query_size(self):
        return self._synchronize_query_size

    @synchronize_query_size.setter
    def synchronize_query_size(self, synchronize_query_size):
        if not isinstance(synchronize_query_size, int):
            raise TypeError('Argument: synchronize_query_size must be a Python int object.')

        if synchronize_query_size <= 0:
            raise ValueError('Argument: synchronize_query_size must at least be one.')

        self._synchronize_query_size = synchronize_query_size
        if self.sync_wrapper is not None:
            self.sync_wrapper.query_size = synchronize_query_size

        return None

    @property
    def synchronize_worker(self):
        return self._synchronize_worker

    @synchronize_worker.setter
    def synchronize_worker(self, synchronize_worker):
        if not isinstance(synchronize_worker, int):
            raise TypeError('Argument: synchronize_worker must be a Python int object.')

        if synchronize_worker != -1:
            if synchronize_worker < 0:
                raise ValueError('Argument: synchronize_worker must larger than zero.') 

        synchronize_worker = int(synchronize_worker)
        self._synchronize_worker = synchronize_worker
        if self.sync_wrapper is not None:
            self.sync_wrapper.num_worker = synchronize_worker

        return None

    @property
    def synchronize_timeout(self):
        return self._synchronize_timeout

    @synchronize_timeout.setter
    def synchronize_timeout(self, synchronize_timeout):
        if synchronize_timeout != -1:
            if not isinstance(synchronize_timeout, (int, float)):
                raise TypeError('Argument: synchronize_timeout must be a Python float object.')

            if synchronize_timeout <= 0:
                raise ValueError('Argument: synchronize_timeout must be a Python float object.')

            synchronize_timeout = float(synchronize_timeout)

        self._synchronize_timeout = synchronize_timeout
        if self.sync_wrapper is not None:
            self.sync_wrapper.timeout = synchronize_timeout

        return None

    def __repr__(self):
        lines = 'HyperspectralDatabase version: {0}\n'.format(__version__)
        lines += '  User: {0}\n  Host: {1}\n  Port: {2}\n'.format(self.user, self.host, self.port)
        lines += '  Gridfs mode: {0}\n'.format(self.gridfs)
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

    def lightweighted_arguments(self):
        # the attribute to initial lightweighted MongoDB client in other subprocess.
        return {'db_name': self.db,
                'user_id': self.user,
                'passwd': self._passwd,
                'host': self.host,
                'port': self.port,
                'gridfs': self.gridfs}

    def close(self):
        self.database = None
        self.sync_wrapper = None
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

        if not isinstance(query,  dict):
            raise TypeError('The argument: query only accept Python dictionary object.')

        return self.collections[collection.lower()].count_documents(query)

    def insert_data(self, file, file_extension = '.json', 
            data_collection = 'data', spectral_collection = 'spectral',
            data_args = ('datatype', 'species', 'spectral'), gridfs = True, 
            certain = False):

        if not isinstance(file, str):
            raise TypeError('Argument: file must be a Python string object.')

        if not isinstance(file_extension, str):
            raise TypeError('Argument: file_extension must be a Python string object.')

        if not os.path.isfile(file):
            raise OSError('No file object in the path:{0}'.format(file))

        if not file.endswith(file_extension):
            raise RuntimeError('Valid file must be a endswith {0}'.format(file_extension))

        if not isinstance(data_collection, str):
            raise TypeError('Argument: data_collection must be a Python string object.')

        if data_collection.lower() not in self._collection_list:
            raise ValueError(data_collection, ' is not a valid collection selection.')

        data_collection = data_collection.lower()

        if not isinstance(spectral_collection, str):
            raise TypeError('Argument: spectral_collection must be a Python string object.')

        if spectral_collection.lower() not in self._collection_list:
            raise ValueError(spectral_collection, ' is not a valid collection selection.')

        spectral_collection = spectral_collection.lower()

        if not isinstance(data_args, (list, tuple)):
            raise TypeError('Argument: data_args must be a Python list/tuple object.')

        for e in data_args:
            if not isinstance(e, str):
                raise TypeError('Element in argument::data_args must be a Python string object.')

        if not isinstance(gridfs, bool):
            raise TypeError('Argument: gridfs must be a Python boolean object.')

        if not isinstance(certain, bool):
            raise TypeError('Argument: certain must be a Python boolean object.')

        data_document, spectral_document = self._single_data_document(file, data_args, 
                data_collection, spectral_collection,
                gridfs = gridfs,
                certain = certain)

        if certain:
            if spectral_document is not None:
                self.collections[spectral_collection].bulk_write([InsertOne(spectral_document)])

            self.collections[data_collection].bulk_write([InsertOne(data_document)])
            print('Successfully insert file:{0} into {1}'.format(file, 
                    self.__class__.__name__))
        else:
            print('Not certain mode, no insertion in the database.')

        return None

    def batch_insert_data(self, directory, file_extension = '.json', 
            data_collection = 'data', spectral_collection = 'spectral',
            data_args = ('datatype', 'species', 'spectral'), batch_size = 10000, 
            gridfs = True, certain = False, progress = True):

        if not isinstance(directory, str):
            raise TypeError('Argument: directory must be a Python string object')

        if not os.path.isdir(directory):
            raise OSError('Path: {0} is not a directory.'.format(directory))

        if not isinstance(file_extension, str):
            raise TypeError('Argument: file_extension must be a Python string object.')

        if not isinstance(data_collection, str):
            raise TypeError('Argument: data_collection must be a Python string object.')

        if data_collection.lower() not in self._collection_list:
            raise ValueError(data_collection, ' is not a valid collection selection.')

        data_collection = data_collection.lower()

        if not isinstance(spectral_collection, str):
            raise TypeError('Argument: spectral_collection must be a Python string object.')

        if spectral_collection.lower() not in self._collection_list:
            raise ValueError(spectral_collection, ' is not a valid collection selection.')

        spectral_collection = spectral_collection.lower()

        if not isinstance(data_args, (list, tuple)):
            raise TypeError('Argument: data_args must be a Python list/tuple object.')

        for e in data_args:
            if not isinstance(e, str):
                raise TypeError('Element in argument::data_args must be a Python string object.')

        if not isinstance(batch_size, int):
            raise TypeError('Argument: batch_size must be a Python int object.')

        if batch_size < 0:
            raise ValueError('Argument: batch_size must larger than zero.')

        if not isinstance(gridfs, bool):
            raise TypeError('Argument: gridfs must be a Python boolean object.')

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
            data_col_requests, spectral_col_requests = [], []
            insert_index = self._get_insert_index()
            running_index, inner_batch_index = 0, 0
            for f in json_files:
                data_document, spectral_document = self._single_data_document(f, data_args, 
                        data_collection, spectral_collection,
                        insert_index = insert_index,
                        gridfs = gridfs,
                        certain = certain)

                data_col_requests.append(InsertOne(data_document))
                if spectral_col_requests is not None:
                    spectral_col_requests.append(InsertOne(spectral_document))

                insert_index += 1
                inner_batch_index += 1

                if progress:
                    running_index += 1
                    print('Acquring data progress: {0} / {1}'.format(running_index,
                                                                    file_numbers))

                if inner_batch_index == batch_size:
                    if len(spectral_col_requests) > 0:
                        self.collections[spectral_collection].bulk_write(spectral_col_requests)
                        spectral_col_requests = []

                    if len(data_col_requests) > 0:
                        self.collections[data_collection].bulk_write(data_col_requests)
                        print('Sucessfully insert {0} files into {1}'\
                                .format(len(data_col_requests), self.__class__.__name__))

                        data_col_requests = []

                    inner_batch_index = 0
                    print('Successfully reset file buffer.')

            if len(requests) > 0:
                self.collections[data_collection].bulk_write(requests)
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

    def _single_data_document(self, json_file_path, data_args, 
            data_collection, spectral_collection,
            insert_index = None, gridfs = True, certain = False):

        content = {}
        single_data_document = Template(data_collection)
        single_spectral_document = Template(spectral_collection) 
        with open(json_file_path, 'r') as f:
            contents = json.loads(f.read())
            f.close()

        source_filename = os.path.split(json_file_path)[-1]
        single_data_document['source_filename'] = source_filename

        for args in data_args:
            args_value = contents.get(args, None)
            if args_value is not None:
                if args == 'spectral':
                    if gridfs:
                        if certain:
                            args_value = serialize(np.array(args_value,
                                    dtype = np.float64))
                            args_value = self.fs.put(args_value)
                    else:
                        args_value = list(np.array(args_value, dtype = np.float64))
                        single_spectral_document['spectral'] = args_value

                single_data_document[args] = args_value

        if insert_index is None:
            insert_index = self._get_insert_index()

        single_data_document['insert_index'] = insert_index
        single_spectral_document['insert_index'] = insert_index
        if self.gridfs:
            single_spectral_document = None

        return single_data_document, single_spectral_document

    def spectral_data_reformation(self, source, target, batch_size = 10000,
            data_collection = 'data', spectral_collection = 'spectral', 
            certain = False, hint = True):

        if not isinstance(batch_size, int):
            raise TypeError('Argument: batch_size must be a Python int object.')

        if batch_size < 0:
            raise ValueError('Argument: batch_size must larger than zero.')

        if not isinstance(data_collection, str):
            raise TypeError('Argument: data_collection must be a Python string object.')

        if data_collection.lower() not in self._collection_list:
            raise ValueError(data_collection, ' is not a valid collection selection.')

        data_collection = data_collection.lower()

        if not isinstance(spectral_collection, str):
            raise TypeError('Argument: spectral_collection must be a Python string object.')

        if spectral_collection.lower() not in self._collection_list:
            raise ValueError(spectral_collection, ' is not a valid collection selection.')

        spectral_collection = spectral_collection.lower()

        if not isinstance(source, str):
            raise TypeError('Argument: source must be a Python string object.')

        if not isinstance(target, str):
            raise TypeError('Argument: target must be a Python string object.')

        if not isinstance(certain, bool):
            raise TypeError('Argument: certain must be a Python boolean object.')

        if not isinstance(hint, bool):
            raise TypeError('Argument: hint must be a Python boolean object.')

        availabel_format = ['gridfs', 'list']
        if source not in availabel_format:
            raise ValueError('Invalid selection for argument: source.')

        if target not in availabel_format:
            raise ValueError('Invalid selection for argument: target.')

        if source == target:
            raise RuntimeError('Argument: source cannot be same as argument:target.')

        original_gridfs_state = copy.deepcopy(self.gridfs)
        if certain:
            if source == 'gridfs' and target == 'list':
                self.gridfs = True

                spectral_documents = []
                doc_nums = self.count_documents({}, collection = data_collection)
                indices_in_docs = []
                docs_in_data_collection = self.find({}, collection = data_collection)
                for doc in docs_in_data_collection:
                    index_in_doc = doc.get('insert_index', None)
                    if index_in_doc is None:
                        raise RuntimeError('Cannot get args:insert_index in doc, please contact developer.')
                    else:
                        indices_in_docs.append(index_in_doc)

                splits = doc_nums // batch_size
                if (doc_nums % batch_size) != 0:
                    splits += 1

                for split_index in range(splits):
                    start_index = split_index * batch_size
                    end_index = (split_index + 1) * batch_size
                    if end_index > len(indices_in_docs):
                        end_index = len(indices_in_docs)

                    split_indices = indices_in_docs[start_index: end_index]
                    contained_spectral_data = self.get_data_by_indices(
                            split_indices, 
                            data_args = ('spectral', 'insert_index'),
                            data_collection = data_collection,
                            spectral_collection = spectral_collection)

                    requests = []
                    for doc in contained_spectral_data:
                        if not isinstance(doc, dict):
                            raise TypeError('Error datatype for document.')

                        insert_index = doc.get('insert_index', None)
                        spectral_data = doc.get('spectral', None)
                        if spectral_data is not None:
                            spectral_data = list(spectral_data)
                        else:
                            warnings.warn('spectral_data in index: {0} cannot transfrom to list.'\
                                    .format(insert_index))
                            spectral_data = 'unknown'
                        
                        if insert_index is not None:
                            spectral_document = Template(spectral_collection)
                            spectral_document['insert_index'] = insert_index
                            spectral_document['spectral'] = spectral_data
                            requests.append(InsertOne(spectral_document))

                    if len(requests) > 0:
                        self.collections[spectral_collection].bulk_write(requests)

                    if hint:
                        print('Successfully write {0} files into collection:{1}, progress: {2}/{3}'\
                                    .format(len(requests), spectral_collection, split_index + 1, splits))

            elif source == 'list' and target == 'gridfs':
                raise NotImplementedError('Please ask developer for further help.')
            else:
                raise ValueError('Method:spectral_data_reformation was not available' + \
                        ' under the setting.')

            if hint:
                print('From {0} to {1} reformation finish.'.format(source, target))

            self.gridfs = original_gridfs_state
        else:
            print('Not certain mode, no reformation process happen.')

        return None

    def _delete_gridfs_object(self, object_pointer):
        if object_pointer != 'unknown':
            self.fs.delete(object_pointer)

        return None 

    def delete_data(self, indices, data_collection = 'data', spectral_collection = 'spectral',
            certain = False):

        if not isinstance(indices, (int, list, tuple)):
            raise TypeError('Argument: indices must be a Python list/tuple object')

        if isinstance(indices, int):
            indices = [indices]

        if not isinstance(data_collection, str):
            raise TypeError('Argument: data_collection must be a Python string object.')

        if data_collection.lower() not in self._collection_list:
            raise ValueError(data_collection, ' is not a valid collection selection.')

        data_collection = data_collection.lower()

        if not isinstance(spectral_collection, str):
            raise TypeError('Argument: spectral_collection must be a Python string object.')

        if spectral_collection.lower() not in self._collection_list:
            raise ValueError(spectral_collection, ' is not a valid collection selection.')

        spectral_collection = spectral_collection.lower()

        if not isinstance(certain, bool):
            raise TypeError('Argument: certain must be a Python boolean object.')

        if certain:
            data_requests, spectral_requests, need_to_delete_pointers = [], [], []
            for index in indices:
                data_docs = self.find({'insert_index': index}, collection = data_collection)
                for doc in data_docs:
                    object_pointer = doc.get('spectral', 'unknown')

                need_to_delete_pointers.append(object_pointer)
                data_requests.append(DeleteOne({'insert_index': index}))
                spectral_requests.append(DeleteOne({'insert_index': index}))

            if len(data_requests) > 0:
                self.collections[data_collection].bulk_write(data_requests)
                for pointer in need_to_delete_pointers:
                    self._delete_gridfs_object(pointer)

            if len(spectral_requests) > 0:
                self.collections[spectral_collection].bulk_write(spectral_requests)

            print('Successfully delete data with indices:{0}'.format(indices))
        else:
            print('Not certain mode, no deletion in the database.')

        return None

    def delete_all(self, data_collection = 'data', spectral_collection = 'spectral',
            certain = False):

        if not isinstance(data_collection, str):
            raise TypeError('Argument: data_collection must be a Python string object.')

        if data_collection.lower() not in self._collection_list:
            raise ValueError(data_collection, ' is not a valid collection selection.')

        data_collection = data_collection.lower()

        if not isinstance(spectral_collection, str):
            raise TypeError('Argument: spectral_collection must be a Python string object.')

        if spectral_collection.lower() not in self._collection_list:
            raise ValueError(spectral_collection, ' is not a valid collection selection.')

        spectral_collection = spectral_collection.lower()

        if not isinstance(certain, bool):
            raise TypeError('Argument: certain must be a Python boolean object.')

        if certain:
            data_requests, spectral_requests, need_to_delete_pointers = [], [], []
            documents = self.find({}, collection = data_collection)
            for doc in documents:
                object_id = doc.get('_id', None)
                data_pointer = doc.get('spectral', 'unknown')
                if object_id is not None:
                    data_requests.append(DeleteOne({'_id': object_id}))
                    need_to_delete_pointers.append(data_pointer)

            documents = self.find({}, collection = spectral_collection)
            for doc in documents:
                object_id = doc.get('_id', None)
                spectral_requests.append(DeleteOne({'_id': object_id}))

            if len(data_requests) > 1:
                self.collections[data_collection].bulk_write(data_requests)
                for pointer in need_to_delete_pointers:
                    self._delete_gridfs_object(pointer)

            if len(spectral_requests) > 1:
                self.collections[spectral_collection].bulk_write(spectral_requests)

            print('Successfully clear all data in {0}'.format(self.__class__.__name__))
        else:
            print('Not certain mode, no deletion in the database.')

        return None

    def get_data(self, queries, data_collection = 'data', spectral_collection = 'spectral',
                data_args = ('datatype', 'species', 'spectral'), hint = True):

        if not isinstance(data_collection, str):
            raise TypeError('Argument: data_collection must be a Python string object.')

        if data_collection.lower() not in self._collection_list:
            raise ValueError(data_collection, ' is not a valid collection selection.')

        data_collection = data_collection.lower()

        if not isinstance(spectral_collection, str):
            raise TypeError('Argument: spectral_collection must be a Python string object.')

        if spectral_collection.lower() not in self._collection_list:
            raise ValueError(spectral_collection, ' is not a valid collection selection.')

        spectral_collection = spectral_collection.lower()

        if not isinstance(queries, (dict, list, tuple)):
            raise TypeError('Argument: queries must be a Python dict or list/tuple object.')

        if isinstance(queries, (list, tuple)):
            for query in queries:
                if not isinstance(query, dict):
                    raise TypeError('Argument: query must be a Python ')

            queries = {'$or': queries}

        if not isinstance(data_args, (list, tuple)):
            raise TypeError('Argument: data_args must be a Python list/tuple object.')

        for e in data_args:
            if not isinstance(e, str):
                raise TypeError('Element in argument::data_args must be a Python string object.')

        if not isinstance(hint, bool):
            raise TypeError('Argument: hint must be a Python boolean object.')

        data = []
        if not self.gridfs:
            original_data_args = copy.deepcopy(data_args)
            if ('insert_index' not in data_args) and ('spectral' in data_args):
                data_args = tuple(list(data_args) + ['insert_index'])

        counting = 0
        tmp_cursor = self.find(queries, collection = data_collection)
        for doc in tmp_cursor:
            single_data = {}
            for args in data_args:
                args_value = doc.get(args, 'unknown')
                single_data[args] = args_value

            data.append(single_data)
            counting += 1

        if counting > self.docs_num_per_request:
            raise RuntimeError('Too data to grab from {0} in the same time.' + \
                    ' Please properly split your conditions.')

        if 'spectral' in data_args:
            if self.gridfs:
                data = self.sync_wrapper(get_spectral_gridfs, 
                                         sync_args = ('docs', ),
                                         docs = data)
            else:
                data = self.sync_wrapper(get_spectral_list,
                                         sync_args = ('docs', ),
                                         docs = data,
                                         original_data_args = original_data_args,
                                         spectral_collection = spectral_collection)
        if hint:
            print('Acquiring {0} data in the {1}.'.format(len(data), 
                    self.__class__.__name__))

        return data

    def _get_docs_only_with_insert_index(self, queries, data_collection):
        docs = []
        tmp_cursor = self.find(queries, collection = data_collection)
        for doc in tmp_cursor:
            single_data = {}
            insert_index = doc.get('insert_index', None)
            if insert_index is None:
                raise RuntimeError('Cannot get insert index in document, please check source code.')

            single_data['insert_index'] = insert_index
            docs.append(single_data)

        return docs

    def _efficiently_get_data_by_proper_split(self, 
            docs_without_spectral, 
            data_collection,
            spectral_collection, 
            data_args, hint):

        indices = []
        for doc in docs_without_spectral:
            index = doc['insert_index']
            indices.append(int(index))

        splits = len(docs_without_spectral) // self.docs_num_per_request
        if (len(docs_without_spectral) % self.docs_num_per_request) != 0:
            splits += 1

        data = []
        for i in range(splits):
            start_index = int(i * self.docs_num_per_request)
            end_index = int((i + 1) * self.docs_num_per_request)
            if end_index > len(indices):
                end_index = len(indices)

            split_indices = indices[start_index: end_index]
            split_data = self.get_data_by_indices(split_indices, 
                    data_collection = data_collection,
                    spectral_collection = spectral_collection,
                    data_args = data_args,
                    hint = False)

            data += split_data

        if hint:
            print('Acquiring {0} data in the {1}.'.format(len(data),
                    self.__class__.__name__))

        return data
        
    def _properly_split_get_data(self, queries, data_collection, spectral_collection, 
            data_args, hint):

        if isinstance(queries, (list, tuple)):
            if len(queries) > 1:
                count_query = {'$or': queries}
            else:
                count_query = queries[0]

        elif isinstance(queries, dict):
            count_query = copy.deepcopy(queries)

        else:
            raise TypeError('Invalid object type for argument: queries.')

        docs_num = self.count_documents(count_query, collection = data_collection)
        if docs_num > self.docs_num_per_request:
            tmp_docs = self._get_docs_only_with_insert_index(count_query, data_collection)
            data = self._efficiently_get_data_by_proper_split(tmp_docs,
                    data_collection, spectral_collection, data_args, hint)
        else:
            data  = self.get_data(queries, data_collection = data_collection,
                                 spectral_collection = spectral_collection,
                                 data_args = data_args,
                                 hint = hint)

        return data

    def get_all_data(self, data_collection = 'data', spectral_collection = 'spectral',
            data_args = ('datatype', 'species', 'spectral'), hint = True):

        return self._properly_split_get_data({}, 
                data_collection, 
                spectral_collection, 
                data_args, hint)

    def get_data_by_indices(self, indices, 
            data_collection = 'data', spectral_collection = 'spectral', 
            data_args = ('datatype', 'species', 'spectral'), hint = True):
 
       if not isinstance(indices, (int, list, tuple)):
            raise TypeError('Argument: indices must be a Python list/tuple object')

       if isinstance(indices, int):
            indices = [indices]

       queries = []
       for index in indices:
           queries.append({'insert_index': index})

       return self._properly_split_get_data(queries,
                data_collection, 
                spectral_collection, 
                data_args, hint)

    def get_data_by_index_range(self, start, stop = None, step = None,
                data_collection = 'data', spectral_collection = 'spectral',
                data_args = ('datatype', 'species', 'spectral'), hint = True):

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
        return self.get_data_by_indices(indices, data_collection = data_collection,
                                 spectral_collection = spectral_collection,
                                 data_args = data_args,
                                 hint = hint) 

    def get_data_by_datatypes(self, datatypes, 
            data_collection = 'data', spectral_collection = 'spectral',
            data_args = ('datatype', 'species', 'spectral'), hint = True):

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

        return self._properly_split_get_data(queries,
                 data_collection,
                 spectral_collection,
                 data_args, hint)

    def get_data_by_species(self, species, 
            data_collection = 'data', spectral_collection = 'spectral',
            data_args = ('datatype', 'species', 'spectral'), hint = True):

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

        return self._properly_split_get_data(queries,
                 data_collection,
                 spectral_collection,
                 data_args, hint)

    def get_indices(self, queries, collection = 'data'):
        if not isinstance(queries, (dict, list, tuple)):
            raise TypeError('Argument: queries must be a Python dict or list/tuple object.')

        if isinstance(queries, (list, tuple)):
            for query in queries:
                if not isinstance(query, dict):
                    raise TypeError('Argument: query must be a Python ')

            queries = {'$or': queries}

        if not isinstance(collection, str):
            raise TypeError('Argument: collection must be a Python string object.')

        if collection.lower() not in self._collection_list:
            raise ValueError(collection, ' is not a valid collection selection.')

        indices = []
        tmp_cursor = self.find(queries, collection = collection)
        for doc in tmp_cursor:
            index = doc.get('insert_index', None)
            if index is not None:
                indices.append(int(index))

        return indices

    def get_all_indices(self, collection = 'data'):
        return self.get_indices({}, collection = collection)

    def get_indices_by_datatypes(self, datatypes, collection = 'data'):
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

        return self.get_indices(queries, collection = collection)

    def get_indices_by_species(self, species, collection = 'data'):
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

        return self.get_indices(queries, collection = collection)


