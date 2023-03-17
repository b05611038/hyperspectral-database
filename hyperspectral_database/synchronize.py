import copy
import time
import platform

import multiprocessing as mp
from multiprocessing import Lock, Process

from .base import Database
from .client import LightWeightedDatabaseClient


__all__ = ['SynchronizedFunctionWapper']


def run_worker(rank, 
        lock, 
        inputs_container, 
        shared_arguments,
        queue_outputs,
        queue_task_generator,
        func, args, worker_timeout = -1.):

    database = shared_arguments.get('database', None)
    sync_database = LightWeightedDatabaseClient(**database)
    if database is None:
        raise RuntimeError('Cannot acquire attribute from HyperspectralDatabase.')

    while True:
        order_finish, kwargs = False, {}
        lock_time = time.time()
        lock.acquire()
        if queue_task_generator.empty():
            raise RuntimeError('Cannot detect order_generator object.')

        order_generator = queue_task_generator.get()
        if not order_generator.finish:
            order_index = order_generator.next()
        else:
            order_finish = True

        queue_task_generator.put(order_generator)
        lock.release()

        if order_finish:
            break
        else:
            kwargs['database'] = sync_database
            for argument in args:
                partition_mode = True
                contents = inputs_container.get(argument, None)
                if contents is None:
                    partition_mode = False
                    contents = shared_arguments.get(argument, None)

                if contents is None:
                    raise RuntimeError('Loss argument in subprocess, ' + \
                            'please ensure all arguemnt is defined.')

                if partition_mode:
                    contents = contents[order_index]

                kwargs[argument] = contents

            outputs = func(**kwargs)
            outputs = _DocumentList(outputs)
            queue_outputs.put(outputs)

    return None


class _DocumentList:
    def __init__(self, docs):
        self.docs = docs

    @property
    def docs(self):
        return self.__docs

    @docs.setter
    def docs(self, docs):
        self.__docs = []
        for doc in docs:
            if not isinstance(doc, dict):
                raise TypeError('Get invalid type for document list.')

            self.__docs.append(doc)

        return None

    def append(self, obj):
        self.__docs.append(obj)
        return None

    def __len__(self):
        return len(self.__docs)

    def __getitem__(self, idx):
        return self.__docs[idx]


class _ExecuteGenerator:
    def __init__(self, order_length, debug = False):
        self.order_length = order_length
        self.debug = debug
        self.finish = False

    def __repr__(self):
        if self.debug:
            text = self.__class__.__name__ + '(finish={0}, running_index={1})'\
                    .format(self.finish, self.__running_index)
        else:
            text = self.__class__.__name__ + '(finish={0})'.format(self.finish)

        return text

    @property
    def order_length(self):
        return self._order_length

    @order_length.setter
    def order_length(self, order_length):
        if not isinstance(order_length, int):
            raise TypeError('Argument: order_length must be a Python int object.')

        if order_length <= 0:
            raise ValueError('Argument: order_length must at least be one.')

        self._order_length = order_length
        return None

    @property
    def debug(self):
        return self._debug

    @debug.setter
    def debug(self, debug):
        if not isinstance(debug, bool):
            raise TypeError('Argument: debug must be a Python boolean object.')

        self._debug = debug
        return None

    @property
    def finish(self):
        return self.__finish

    @finish.setter
    def finish(self, finish):
        if not isinstance(finish, bool):
            raise TypeError('Argument: finish must be a Python boolean object.')

        if finish:
            raise RuntimeError('Argument: finish cannot set as True in the initial condition.')

        self.__finish = finish
        self.__running_index = 0
        return None

    def next(self):
        index_now = copy.deepcopy(self.__running_index)
        self.__running_index += 1

        if self.__running_index == self.order_length:
            self.__finish = True

        return index_now


class _OrderAllocator:
    def __init__(self, size):
        self.size = size

    def __repr__(self):
        return self.__class__.__name__ + '(size={0})'.format(self.size)

    @property
    def size(self):
        return self._size

    @size.setter
    def size(self, size):
        if not isinstance(size, int):
            raise TypeError('Argument: size must be a Python int object.')

        if size <= 0:
            raise ValueError('Argument: size must at least be one.')

        self._size = size
        return None

    def __call__(self, args, args_name = None, args_container = None):
        if args_container is None:
            args_container = {}

        if args_name is None:
            if len(list(args_container.keys())) == 0:
                args_index = 0
            else:
                args_index = max(list(args_container.keys()))
                args_index += 1

            args_name = args_index
        else:
            if not isinstance(args_name, str):
                raise TypeError('Argument: args_name must be a Python string object.')

        if args is not None:
            contents = {}
            if not isinstance(args, (tuple, list)):
                raise TypeError('Argument: queries must be a Python list/tuple object.')

            partitions = len(args) // self.size
            if partitions > 0:
                if len(args) % partitions > 0:
                    partitions += 1

            for order in range(partitions):
                if order == partitions - 1:
                    content = args[order * self.size: ]
                else:
                    content = args[order * self.size: (order + 1) * self.size]

                contents[order] = content

            args_container[args_name] = contents

        return args_container


class SynchronizedFunctionWapper:
    def __init__(self, database, query_size, num_worker = 2, 
            process_check_interval = 0.1, timeout = -1):

        if not isinstance(database, Database):
            raise TypeError('Argument: database must be a Database object.')

        if not isinstance(query_size, int):
            raise TypeError('Argument: query_size must be a Python int object.')

        if query_size <= 0:
            raise ValueError('Argument: query_size must at least be one.')

        self.database = database
        self.query_size = query_size
        self.num_worker = num_worker
        self.process_check_interval = process_check_interval
        self.timeout = timeout
        self.forbidden_keywords = ('order_generator', 'generator_lock', 'database')

        self.allocator = _OrderAllocator(query_size)

        if num_worker > 1:
            if platform.system() == 'Darwin' or platform.system() == 'Windows':
                self.mp_start_method = 'spawn'
            elif platform.system() == 'Linux':
                self.mp_start_method = 'fork'
            else:
                self.mp_start_method = None
                print('Not support multiprocessing, the get_data functions will' + \
                        ' run in single process.')
        else:
             self.mp_start_method = None

        if self.mp_start_method is not None:
            mp.set_start_method(self.mp_start_method, force = True)

    @property
    def num_worker(self):
        return self._num_worker

    @num_worker.setter
    def num_worker(self, num_worker):
        if not isinstance(num_worker, int):
            raise TypeError('Argument: num_worker must be a Python int object.')

        if num_worker == -1:
            print('Sychronized wrapper not execute. All use single process.')
        else:
            if num_worker < 0:
                raise ValueError('Argument: num_worker must at least be one.')

        self._num_worker = num_worker
        return None

    @property
    def process_check_interval(self):
        return self._process_check_interval

    @process_check_interval.setter
    def process_check_interval(self, process_check_interval):
        if not isinstance(process_check_interval, float):
            raise TypeError('Argument: process_check_interval must be a Python float object.')

        if process_check_interval < 0:
            raise ValueError('Argument: process_check_interval must be larger than zero.')

        self._process_check_interval = process_check_interval
        return None

    @property
    def timeout(self):
        return self._timeout

    @timeout.setter
    def timeout(self, timeout):
        if not isinstance(timeout, (int, float)):
            raise TypeError('Argument: timeout must be a Python float object.')

        if timeout == -1:
            self._timeout = -1.
        else:
            if timeout <= 0:
                raise ValueError('Argument: timeout (seconds) must larger than zero.')

            self._timeout = timeout

        return None

    @property
    def available(self):
        if self.mp_start_method is None:
            available = False
        else:
            available = True

        return available

    def __repr__(self):
        if self.timeout < 0:
            timeout = 'unlimited'
        else:
            timeout = self.timeout

        return self.__class__.__name__ + '(query_size={0}, num_worker={1}, timeout={2} seconds)'\
                .format(self.query_size, self.num_worker, timeout)

    def merge_process_outputs(self, outputs, queue_outputs):
        while True:
            if not queue_outputs.empty():
                docs = queue_outputs.get()
                for doc in docs:
                    outputs.append(doc)
            else:
                break

        return outputs

    def terminate_process(self, running_processes):
        for p in running_processes:
            p.terminate()
            p.close()

        running_processes = []
        return None

    def check_subprocess_finish(self, running_processes):
        is_finish = True
        for p in running_processes:
            if p.is_alive():
                is_finish = False

        return is_finish

    def __call__(self, func, sync_args = (), timeout = None, **kwargs):
        if timeout is None:
            if self.timeout > 0:
                timeout = self.timeout
        else:
            if not isinstance(timeout, (int ,float)):
                raise TypeError('Argument: timeout must be a Python float object.')

            if timeout <= 0.:
                raise ValueError('Argument: timeout must larger than zero.')

            timeout = float(timeout)

        if self.available:
            with mp.Manager() as manager:
                inputs_container = manager.dict()
                shared_arguments = manager.dict()
                if sync_args is not None:
                    for args in sync_args:
                        inputs_container = self.allocator(kwargs.get(args, None), 
                                args_name = args,
                                args_container = inputs_container)

                for argument in kwargs:
                    if argument in self.forbidden_keywords:
                        raise RuntimeError('Argument cannot be named as {0}.'.format(argument))

                    if argument not in sync_args:
                        shared_arguments[argument] = kwargs[argument]

                recorded_partition_number = 1
                for args in inputs_container:
                    partition_number_of_args = len(inputs_container[args])
                    if partition_number_of_args != 1:
                        if recorded_partition_number == 1:
                            recorded_partition_number = partition_number_of_args
                        else:
                            if partition_number_of_args != recorded_partition_number:
                                raise RuntimeError('Cannot split argument: {0} correctly'\
                                        .format(args))

                if recorded_partition_number > 1:
                    order_generator = _ExecuteGenerator(recorded_partition_number)
                    shared_arguments['database'] = self.database.lightweighted_arguments()

                    queue_outputs = manager.Queue()
                    queue_task_generator = manager.Queue()
                    queue_task_generator.put(order_generator)

                    outputs = []
                    running_processes, process_lock = [], Lock()
                    complete_warning, start_time = False, time.time()
                    for rank in range(self.num_worker):
                        func_args = list(kwargs.keys())
                        p = Process(target = run_worker,
                                args = (rank,
                                        process_lock,
                                        inputs_container,
                                        shared_arguments,
                                        queue_outputs,
                                        queue_task_generator,
                                        func,
                                        func_args))

                        p.start()
                        running_processes.append(p)

                    finish = False
                    while (not finish):
                        outputs = self.merge_process_outputs(outputs, queue_outputs)
                        if self.check_subprocess_finish(running_processes):
                            finish = True

                        time.sleep(self.process_check_interval)
                        if timeout is not None:
                            if (time.time() - start_time) > timeout:
                                print('Reach timeout limit, force stop function wrapper.')
                                complete_warning = True
                                self.terminate_process(running_processes)
                                finish = True

                    outputs = self.merge_process_outputs(outputs, queue_outputs)
                    manager.shutdown()
                else:
                    outputs = func(self.database, **kwargs)
        else:
            outputs = func(self.database, **kwargs)

        return outputs


