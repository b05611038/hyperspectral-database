import abc

from pymongo import MongoClient

from . import __version__


__all__ = ['Database']


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


