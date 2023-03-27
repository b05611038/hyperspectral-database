import copy
import time
import platform

import multiprocessing as mp
from multiprocessing import Lock, Process

from .base import Database
from .client import LightWeightedDatabaseClient


__all__ = ['SynchronizedFunctionWapper']


def run_worker(rank, 
        inputs_container, 
        shared_arguments,
        queue_outputs,
        func, args, worker_timeout = -1.):

    query_size = shared_arguments.get('query_size', None)
    if query_size is None:
        raise RuntimeError('Cannot acquire query_size in sync_wrapper.')

    database = shared_arguments.get('database', None)
    sync_database = LightWeightedDatabaseClient(**database)
    if database is None:
        raise RuntimeError('Cannot acquire attribute from HyperspectralDatabase.')

    partitions = None
    for argument in args:
        contents = inputs_container.get(argument, None)
        if contents is None:
            contents = shared_arguments.get(argument, None)
        else:
            length_of_contents = len(contents.get(rank, []))
            tmp_partition = length_of_contents // query_size
            if length_of_contents % query_size > 0:
                tmp_partition += 1

        if partitions is None:
            partitions = tmp_partition
        else:
            if tmp_partition != partitions:
                raise RuntimeError('Error partition assignment in the {0} sub-process'\
                        .format(rank))

    if partitions is None:
        raise RuntimeError('Cannot acquire sync argument in the sync_wrapper.')

    break_flag = False
    for split_index in range(partitions):
        split_kwargs = {'database': sync_database}
        start_index = split_index * query_size
        end_index = (split_index + 1) * query_size
        if end_index >= length_of_contents:
            end_index = length_of_contents
            break_flag = True

        for argument in args:
            contents = inputs_container.get(argument, None)
            if contents is None:
                contents = shared_arguments.get(argument, None)
            else:
                contents = contents[rank][start_index: end_index]

            if contents is None:
                    raise RuntimeError('Loss argument in subprocess, ' + \
                            'please ensure all arguemnt is defined.')

            split_kwargs[argument] = contents

        outputs = func(**split_kwargs)
        outputs = _DocumentList(outputs)
        queue_outputs.put(outputs)

        if break_flag:
            break

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


class _OrderAllocator:
    def __init__(self, num_worker):
        self.num_worker = num_worker

    def __repr__(self):
        return self.__class__.__name__ + '(size={0}, num_worker={1})'\
                .format(self.size, self.num_worker)

    @property
    def num_worker(self):
        return self._num_worker

    @num_worker.setter
    def num_worker(self, num_worker):
        if not isinstance(num_worker, int):
            raise TypeError('Argument: num_worker must be a Python int object.')

        if num_worker == -1:
            pass
        else:
            if num_worker < 0:
                raise ValueError('Argument: num_worker must at least be one.')

        self._num_worker = num_worker
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

        if self.num_worker > 1:
            if args is not None:
                contents = {}
                if not isinstance(args, (tuple, list)):
                    raise TypeError('Argument: queries must be a Python list/tuple object.')

                break_flag = False
                order_length = (len(args) // self.num_worker) + 1
                for order in range(self.num_worker):
                    start_index = order * order_length
                    end_index = (order + 1) * order_length
                    if end_index >= len(args):
                        end_index = len(args)
                        break_flag = True

                    contents[order] = args[start_index: end_index]
                    if break_flag:
                        break

                if end_index != len(args):
                    raise RuntimeError('{0} have some problem on allocate mission, please contact developer.'\
                            .format(self.__class__.__name__))

                args_container[args_name] = contents
        else:
            args_container[args_name] = args

        return args_container


class SynchronizedFunctionWapper:
    def __init__(self, database, query_size, num_worker = 8, 
            process_check_interval = 0.1, timeout = -1):

        if not isinstance(database, Database):
            raise TypeError('Argument: database must be a Database object.')

        if not isinstance(query_size, int):
            raise TypeError('Argument: query_size must be a Python int object.')

        if query_size <= 0:
            raise ValueError('Argument: query_size must at least be one.')

        self.allocator = _OrderAllocator(num_worker)

        self.database = database
        self.query_size = query_size
        self.num_worker = num_worker
        self.process_check_interval = process_check_interval
        self.timeout = timeout
        self.forbidden_keywords = ('database', )

        if platform.system() == 'Darwin' or platform.system() == 'Windows':
            self.mp_start_method = 'spawn'
        elif platform.system() == 'Linux':
            self.mp_start_method = 'fork'
        else:
            self.mp_start_method = None
            print('Not support multiprocessing, the get_data functions will' + \
                    ' run in single process.')

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
            print('Forbidden sync_wrapper. HyperspectralDatabase was operated' + \
                    ' in single process mode (recommended).')
        else:
            if num_worker < 0:
                raise ValueError('Argument: num_worker must at least be one.')

            if num_worker > 1:
                print('Synchronize ({0}-process) get_data available. This mode'.format(num_worker) + \
                        ' was not recommended in most of conditions.')
            else:
                print('Forbidden sync_wrapper. HyperspectralDatabase was operated' + \
                        ' in single process mode (recommended).')

        self._num_worker = num_worker
        if self.allocator is not None:
            self.allocator.num_worker = num_worker

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

        self._process_check_interval = float(process_check_interval)
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

    def can_exec_with_multiprocess(self):
        exec_with_multiprocess = False
        if self.num_worker > 1:
            exec_with_multiprocess = self.available

        return exec_with_multiprocess

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

            if self.can_exec_with_multiprocess():
                shared_arguments['database'] = self.database.lightweighted_arguments()
                shared_arguments['query_size'] = self.query_size

                queue_outputs = manager.Queue()

                outputs, running_processes = [], []
                complete_warning, start_time = False, time.time()
                for rank in range(self.num_worker):
                    func_args = list(kwargs.keys())
                    p = Process(target = run_worker,
                            args = (rank,
                                    inputs_container,
                                    shared_arguments,
                                    queue_outputs,
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

        return outputs


