import copy

import numpy as np

from .utils import deserialize

__all__ = ['get_spectral_gridfs', 'get_spectral_list']

def get_spectral_gridfs(database, docs):
    data = []
    for doc in docs:
        pointer = doc.get('spectral', None)
        if pointer is not None:
            spectral_data = database.fs.get(doc.get('spectral', None)).read()
            spectral_data = deserialize(spectral_data)
        else:
            spectral_data = 'unknown'

        doc['spectral'] = spectral_data
        data.append(doc)

    return data

def get_spectral_list(database, docs, original_data_args = None, 
        spectral_collection = 'spectral'):

    data, spectral_queries, order, counting = [], [], {}, 0
    for doc in docs:
         insert_index = doc.get('insert_index', None)
         if insert_index is not None:
             spectral_queries.append({'insert_index': insert_index})

         order[insert_index] = counting
         if original_data_args is not None:
             new_doc = {}
             for args in original_data_args:
                 new_doc[args] = doc[args] 
         else:
             new_doc = None

         if new_doc is None:
             data.append(doc)
         else: 
             data.append(new_doc)

         counting += 1

    if len(spectral_queries) == 1:
        spectral_queries = spectral_queries[0]
    else:
        spectral_queries = {'$or': spectral_queries}

    spectral_documents = database.find(spectral_queries, collection = spectral_collection)
    for doc in spectral_documents:
        spectral_data = doc.get('spectral', None)
        if spectral_data is not None:
            spectral_data = np.array(spectral_data, dtype = np.float64)
        else:
            continue

        insert_index = doc.get('insert_index', None)
        if insert_index is not None:
            data[order[insert_index]]['spectral'] = spectral_data
             
    return data


