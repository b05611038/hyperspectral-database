import pickle


__all__ = ['serialize', 'deserialize']


def serialize(obj):
    if not isinstance(obj, object):
        raise TypeError('The inputted variable is not Python object.')
    
    binary_object = pickle.dumps(obj)
    return binary_object

def deserialize(binary_obj): 
    if not isinstance(binary_obj, bytes):
        raise TypeError('Input object must be a bytes object.')
    
    data = pickle.loads(binary_obj)
            
    return data


