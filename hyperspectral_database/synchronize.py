import copy
import time
import platform

import multiprocessing as mp

from .base import Database
from .client import LightWeightedDatabaseClient


__all__ = ['SynchronizedFunctionWapper']


def run_worker(rank, lock, func, args, 
        inputs_container, outputs_container, shared_container):

    database = shared_container.get('database', None)
    if database is None:
        raise RuntimeError('Cannot acquire attribute from HyperspectralDatabase.')

    sync_database = LightWeightedDatabaseClient(**database)
    order_generator = shared_container.get('order_generator', None)
    if order_generator is None:
        raise RuntimeError('Cannot detect order_generator object.')

    while True:
        order_finish = False
        lock.acquire()
        if not order_generator.finish:
            order_index = order_generator.next()
        else:
            order_finish = True

        lock.release()
        if order_finish:
            break
        else:
            print('rank: {0} | order_index: {1}'.format(rank, order_index))
            kwargs = {}
            kwargs['database'] = sync_database
            for argument in args:
                partition_mode = True
                contents = inputs_container.get(argument, None)
                if contents is None:
                    partition_mode = False
                    contents = shared_container.get(argument, None)

                if contents is None:
                    raise RuntimeError('Loss argument in subprocess, ' + \
                            'please ensure all arguemnt is defined.')

                if partition_mode:
                    contents = contents[order_index]

                kwargs[argument] = contents

            outputs = func(**kwargs)
            outputs_container[order_index] = outputs

    return None

class _ExecuteGenerator:
    def __init__(self, order_length):
        self.order_length = order_length
        self.finish = False

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

    def __repr__(self):
        return self.__class__.__name__ + '(order_length={0})'.format(self.order_length)

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
                print('Not support multiprocessing, the requests will run in single process.')
        else:
             self.mp_start_method = None

        mp.set_start_method(self.mp_start_method)
        self.reset()

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

    def all_reduce(self, complete_warning = False):
        outputs = None
        ordered_task_indices = list(self.outputs_container.keys())
        ordered_task_indices.sort()

        outputs = []
        print('ordered_task_indices: ', ordered_task_indices)
        for index in ordered_task_indices:
            outputs.append(self.outputs_container[index])
            del self.outputs_container[index]

        outputs = tuple(zip(*outputs))
        if complete_warning:
            warning.warn('The returned object might not complete, please check it carefully.')

        self.reset()

        return outputs

    def reset(self):
        if self.mp_start_method is not None:
            self.manager = mp.Manager()
            self.inputs_container = self.manager.dict()
            self.outputs_container = self.manager.dict()
            self.shared_arguments = self.manager.dict()
            self.running_processes = []
        else:
            self.manager = None
            self.inputs_container = None
            self.outputs_container = None
            self.shared_arguments = None
            self.running_processes = []

        return None

    def terminate_process(self):
        for p in self.running_processes:
            p.terminate()
            p.close()

        self.running_processes = []
        return None

    def _check_subprocess_finish(self, running_processes):
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
            if sync_args is not None:
                for args in sync_args:
                    self.inputs_container = self.allocator(kwargs.get(args, None), 
                            args_name = args,
                            args_container = self.inputs_container)

            for argument in kwargs:
                if argument in self.forbidden_keywords:
                    raise RuntimeError('Argument cannot be named as {0}.'.format(argument))

                if argument not in sync_args:
                    self.shared_arguments[argument] = kwargs[argument]

            recorded_partition_number = 1
            for args in self.inputs_container:
                partition_number_of_args = len(self.inputs_container[args])
                if partition_number_of_args != 1:
                    if recorded_partition_number == 1:
                        recorded_partition_number = partition_number_of_args
                    else:
                        if partition_number_of_args != recorded_partition_number:
                            raise RuntimeError('Cannot split argument: {0} correctly'\
                                    .format(args))

            if recorded_partition_number > 1:
                order_generator = _ExecuteGenerator(recorded_partition_number)
                self.shared_arguments['order_generator'] = order_generator
                self.shared_arguments['database'] = self.database.lightweighted_arguments()
                process_lock = mp.Lock()
                complete_warning, start_time = False, time.time()
                for rank in range(self.num_worker):
                    p = mp.Process(target = run_worker,
                        args = (rank,
                                process_lock,
                                func,
                                list(kwargs.keys()),
                                self.inputs_container,
                                self.outputs_container,
                                self.shared_arguments))

                    p.start()
                    self.running_processes.append(p)

                finish = False
                while (not finish):
                    if self._check_subprocess_finish(self.running_processes):
                        finish = True

                    time.sleep(self.process_check_interval)
                    if timeout is not None:
                        if (time.time() - start_time) > timeout:
                            print('Reach timeout limit, force stop function wrapper.')
                            complete_warning = True
                            self.terminate_process()
                            finish = True

                outputs = self.all_reduce(complete_warning)
            else:
                outputs = func(self.database, **kwargs)
        else:
            outputs = func(self.database, **kwargs)

        return outputs


