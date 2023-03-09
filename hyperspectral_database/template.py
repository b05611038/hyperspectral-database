import copy

__all__ = ['Template']


DataDocument = {
        'insert_index': 'unknown',
        'source_filename': 'unknown',
        'datatype': 'unknown',
        'species': 'unknown',
        'spectral': [],
}

def Template(collection = 'data'):
    if not isinstance(collection, str):
        raise TypeError('Argument: collection must be a Python string object.')

    collection = collection.lower()

    document = None
    if collection == 'data':
        document = copy.deepcopy(DataDocument)
    else:
        raise ValueError('{0} is not a valid selection for Template.')

    return document


