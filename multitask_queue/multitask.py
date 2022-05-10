import asyncio
import os

from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from typing import List, Dict, Optional, Callable, Any, Set, Union, Hashable
from queue import Queue

from loguru import logger

from multitask_queue.task import Task, TasksOrganizer
from multitask_queue.decorators import PLUGINS


class Multitask:
    """
    Multitask provides a simple interface to execute multiple tasks based on a
    :meth:`specific order <multitask_queue.task.TasksOrganizer>` and in specifics events
    (read events in the parameters' doc). What it does is simple, it takes a TasksOrganizer object iterate
    over it and only execute the tasks that has the same events that the Multitask instance.
    You can see this class as the level 1 of the framework.

    Note: you can modify the events of the multitask during the execution process using a task, what this mean
    is that you can create events using tasks.

    Parameters
    ----------

    multitask_id: Hashable
        Identifier of the multitask, useful for a lot of situations where the task modify it's behaviour
        based on this identifier (it could be a date or anything else that can be hashed like a str)

    events: Set[str]
        The events are one of the most important part of the framework, this allow to exclude tasks based on the
        events where they must be executed, this create a higher level workflow where you can omit complete groups
        of tasks based on that, for example, you create an event called 'Cook' and you have two tasks, one called
        'travel' which is executed on the event 'Vacation' and the other called 'fry_an_egg' that is executed on
        the event 'Cook', the idea is that the Multitask only run the 'fry_an_egg' task and omit the 'travel' one.

    tasks: Set[str]
        Useful for add manual tasks to the multitask in case that the events parameter is not sufficient.

    metadata: Optional[Dict]
        Additional descriptors for the Multitask

    reverse_order: bool = False
        Useful for the MultitaskQueue class if you are using a PriorityQueue and want to change the order
        (the multitasks are sorted based on the multitask_id
    """

    def __init__(
            self,
            multitask_id: Hashable,
            events: Set[str] = None,
            tasks: Set[str] = None,
            metadata: Optional[Dict] = None,
            reverse_order: bool = False,
    ):
        self.multitask_id = multitask_id
        self._tasks = {} if tasks is None else tasks
        self._events = {} if events is None else events
        self.metadata = metadata
        self.reverse_order = reverse_order

    def __hash__(self):
        return hash(self.multitask_id)

    def __eq__(self, other: 'Multitask'):
        return self.multitask_id == other.multitask_id

    def __lt__(self, other: 'Multitask'):
        if self.reverse_order:
            return self.multitask_id > other.multitask_id
        return self.multitask_id < other.multitask_id

    @property
    def events(self):
        return self._events

    @property
    def tasks(self):
        return self._tasks

    def add_events(self, events: Set[str]):
        """
        Add new events to the multitask
        """
        self._events.update(events)

    def add_tasks(self, tasks: Set[str]):
        """
        Add new tasks to the multitask
        """
        self._tasks.update(tasks)

    def remove_events(self, events: Set[str]):
        """
        Remove events from the multitask
        """
        self._events.remove(events)

    def remove_tasks(self, tasks: Set[str]):
        """
        Remove tasks from the multitask
        """
        self._tasks.remove(tasks)

    @staticmethod
    def _order_by_type_parallelization(tasks):
        """
        Default execution order for the parallel tasks
        """
        order = {'process': 0, 'thread': 1, 'async': 2}
        return sorted(tasks, key=lambda x: order[x.type_parallelization])

    def run(
            self,
            data: Dict[str, Any],
            tasks_organizer: TasksOrganizer,
            check_independent_finish: bool = False
    ):
        """
        Iterate over the levels of the tasks_organizer object and execute all the tasks that has the same events
        that the multitask.
        It runs the tasks of the same level following the next order:
        'pre_execution_task' -> 'independent_task' -> 'parallel_task' -> 'regular_task',
        for the 'parallel_task' the internal order is 'process' -> 'thread' -> 'async'.

        It is possible to modify the events and tasks of a multitask during the execution using a pre execution task,
        this can be done using the parameter multitask in the task.


        Parameters
        ----------

        data: Dict[str, Any]
            This data dict must contain all the necessaries parameters for the tasks that are going to be executed

        tasks_organizer: TasksOrganizer
            Classified and sorted tasks that are going to be executed in this method.

        check_independent_finish: bool = False
            Indicate if we want to wait for the end of the independent tasks before finish this method,
            True means yes.
        """
        data['multitask'] = self

        independents = []
        for classified_tasks in tasks_organizer:
            pre_execution_tasks = {
                task
                for task in classified_tasks.get('pre_execution', set())
                if (task.exec_on_events & self._events) or (task.name in self._tasks)
            }

            # run and get the results of the pre execution tasks
            for task in pre_execution_tasks:
                task.run(data)
                data.update(task.result)

            classified_tasks = {
                classification: {
                    task for task in tasks
                    if (task.exec_on_events & self._events) or (task.name in self._tasks)
                }
                for classification, tasks in classified_tasks.items()
            }

            # run the independents tasks and get the results if they were running in background
            for task in self._order_by_type_parallelization(classified_tasks.get('independent', [])):
                independents.append(task)
                data.update(task.result)
                task.run(data)

            # run the parallel and async tasks
            for task in self._order_by_type_parallelization(classified_tasks.get('parallel', [])):
                task.run(data)

            # run the regular tasks
            for task in classified_tasks.get('regular', []):
                task.run(data)
                data.update(task.result)

            # get the results of the parallel and async tasks
            for task in classified_tasks.get('parallel', []):
                data.update(task.result)

        if check_independent_finish:
            for task in independents:
                data.update(task.result)


class MultitasksOrganizer:
    """
    The behaviour of this class is identical to the one provided by the Queues, the idea of this is that
    the MultitaskQueue class can use this object to keep an execution order of the different multitasks. It's
    the equivalent of the :meth:`TasksOrganizer <multitask_queue.task.TasksOrganizer>` in the
    :meth:`Multitask <multitask_queue.multitask.Multitask>`

    Parameters
    ----------

    multitasks: Union[List[Multitask], Queue]
        This is the data structure that is going to handle the order in which the Multitasks are executed, it only
        needs to implement the put and get method.

    """

    def __init__(
            self,
            multitasks: Union[List[Multitask], Queue],
    ):
        self._multitasks = multitasks

    def put(self, multitask: Multitask):
        """
        Insert a multitask in the queue
        """
        if isinstance(self._multitasks, list):
            self._multitasks.append(multitask)
        else:
            self._multitasks.put(multitask)

    def get(self):
        """
        get the first element in the queue and pop it
        """
        if isinstance(self._multitasks, list):
            return self._multitasks.pop(0)
        return self._multitasks.get()

    def reorder(self, reverse: bool = False):
        self._multitasks.sort(reverse=reverse)

    def empty(self):
        """
        Check if the queue is empty
        """
        if isinstance(self._multitasks, list):
            return len(self._multitasks) == 0
        return self._multitasks.empty()


class MultitasksQueue:
    """
    This is the principal part of the framework (the level 2), it is fairly simple as it make heavy use of the others
    classes. What it does is that every time you instance this class it is going to scan all the tasks inside PLUGINS
    and create a :meth:`TasksOrganizer <multitask_queue.task.TasksOrganizer>` with them and once you call the run
    method of the class it is going two create two default Multitasks (explained in the run method), which use the
    previously created TasksOrganizer.

    Attributes
    ----------

    tasks_organizer: TasksOrganizer
        Sorted and classified tasks

    thread_pool: ThreadPoolExecutor

    process_pool: ProcessPoolExecutor

    async_loop: asyncio.AbstractEventLoop


    Parameters
    ----------

    max_threads: int = None
        Max number of threads for the internal thread_pool, the default is os.cpu_count()
    max_process: int = None
        Max number of process for the internal process_pool, the default is os.cpu_count()
    async_loop: asyncio.AbstractEventLoop
        Event loop of the async code, by default it takes asyncio.get_event_loop()

    Examples
    --------
    We will create three tasks, one is going to multiply a value by N the other is going to sum M to the value
    and the other will generate another multitask, we can see that if we use a value = 0 in the input
    we will get a result of 5, this is produced due that the multiplication is done first than the sum
    and that the multiplication is only applied in the preprocess and postprocess multitask and not in the
    daily multitask generated by our other task (all the settings are in the decorators):

        >>> import multitask_queue as mtq
        >>>
        >>> from typing import List
        >>>
        >>>
        >>> @mtq.regular_task(
        ...     exec_on_events=['preprocess', 'postprocess']
        ... )
        ... def mul_by_n(value: int, n: int):
        ...     return {'value': value * n}
        ...
        >>>
        >>> @mtq.parallel_task(
        ...     exec_on_events=['preprocess', 'daily', 'postprocess'],
        ...     exec_after_tasks=['mul_by_n'],
        ...     type_parallelization='thread'
        ... )
        ... def sum_by_m(value: int, m: int):
        ...     # this task is executed in another thread
        ...     return {'value': value + m}
        ...
        >>>
        >>> @mtq.pre_execution_task(
        ...     exec_on_events=['preprocess']
        ... )
        ... def multitask_generator(mt_queue: mtq.MultitasksOrganizer):
        ...     # modified by reference
        ...     mt_queue.put(mtq.Multitask(multitask_id='2020-01-01', events={'daily'}))
        ...     return {}
        ...
        >>>
        >>> mt_queue = mtq.MultitasksQueue()
        >>>
        >>> data = {'value': 0, 'n': 2, 'm': 1}
        >>>
        >>> mt_queue.run(tasks=['sum_by_m', 'mul_by_n', 'multitask_generator'], data=data)
        >>>
        >>> data
        {'value': 5, 'n': 2, 'm': 1}

    """

    def __init__(
            self,
            max_threads: int = None,
            max_process: int = None,
            async_loop: asyncio.AbstractEventLoop = None
    ):
        self.thread_pool = ThreadPoolExecutor(os.cpu_count() if max_threads is None else max_threads)
        self.process_pool = ProcessPoolExecutor(os.cpu_count() if max_process is None else max_process)
        self.async_loop = async_loop

        self.tasks_organizer = TasksOrganizer([
            Task(
                task_descriptor,
                thread_pool=self.thread_pool,
                process_pool=self.process_pool,
                async_loop=self.async_loop
            )
            for task_descriptor in PLUGINS.values()
        ])

    def run(
            self,
            data: Dict[str, Any],
            tasks: Set[str] = None,
            mt_queue: Union[List[Multitask], Queue] = None
    ):
        """
        The execution process is simple and is described below:

            1. A :meth:`multitask <multitask_queue.multitask.Multitask>` with an event 'preprocess' is created and
            executed, you should use this multitask to add new Multitasks to the queue
            (read the notes to check the parameter that you need in your task).

            2. A while loop is run until all the tasks in the multitask queue were processed (note that you can add
            an infinite number of multitasks using any task)

            3. A Multitask with event 'postprocess' is executed after the step 2, this multitask is useful to store
            the results.

        Notes
        -----

        The system automatically add the followings parameters to your data (you can use them in your tasks):
            1. user_tasks: These are the same tasks that you sent in the tasks parameters but with all the autofilled
            tasks, it's still a set.
            2. mt_queue: It's a MultitasksOrganizer object loaded with the information that you sent in the parameter
            mt_queue, useful for add new multitasks
            3. multitask: Actual multitask that is being executed, you can modify the events or other parameters
            on the tasks with this.

        Parameters
        ----------

        data: Dict[str, Any]
            This data dict must contain all the necessaries parameters for the tasks that are going to be executed
            on every multitask

        tasks: Set[str] = None
            Name of the tasks that want to be executed, useful for filters some tasks

        mt_queue: Union[List[Multitask], Queue]
            The mt_queue must be an object that implement the Queue interface (or a list) and that contain
            the Multitasks that you want to execute during the the process (you can add the multitasks in the
            preprocess Multitask)


        """
        tasks = set() if tasks is None else tasks
        data['user_tasks'] = tasks
        tasks = self.autofill_tasks(tasks, data)

        if tasks:
            tasks_organizer = self.tasks_organizer.filter_tasks({task.name for task in tasks}, copy_deep=True)
        else:
            tasks_organizer = self.tasks_organizer.copy()

        mt_queue = MultitasksOrganizer(multitasks=[] if mt_queue is None else mt_queue)
        data['mt_queue'] = mt_queue

        multitask = Multitask(
            'preprocess',
            events={'preprocess'},
        )
        multitask.run(data, tasks_organizer)

        while not mt_queue.empty():
            multitask = mt_queue.get()
            multitask.run(data, tasks_organizer)

        multitask = Multitask('postprocess', events={'postprocess'})
        multitask.run(data, tasks_organizer)

        for task in tasks_organizer.tasks.values():
            if task.type_task == 'independent':
                data.update(task.result)

        del data['multitask']
        del data['mt_queue']
        del data['user_tasks']

    def autofill_tasks(self, tasks: Set[str], data) -> Set[Task]:
        autofilled_tasks = set()
        tasks_to_check_autofill = list(tasks)

        while tasks_to_check_autofill:
            task = self.tasks_organizer[tasks_to_check_autofill[0]]
            tasks_to_check_autofill = tasks_to_check_autofill[1:]

            if task.type_task != 'autofill':
                autofilled_tasks.add(task.name)
                new_tasks = task.autofill
            else:
                task.run(data)
                new_tasks = set(task.result)

            invalid_tasks = list(t for t in new_tasks if t not in self.tasks_organizer)
            if invalid_tasks:
                raise ValueError(
                    f'The task {task.name} is autofilling '
                    f'the next tasks that are not in the DAG: {invalid_tasks}'
                )

            tasks_to_check_autofill += list(new_tasks - autofilled_tasks)

        autofilled_tasks = set(self.tasks_organizer[task] for task in autofilled_tasks)
        return autofilled_tasks
