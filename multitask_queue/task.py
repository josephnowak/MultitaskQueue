from __future__ import annotations

import asyncio
import inspect
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, Future
from functools import reduce
from typing import List, Dict, Iterable, Literal, Optional, Callable, Any, Set, Union

from pydantic import BaseModel


class TaskDescriptor(BaseModel):
    """
    Every task decorator create a TaskDescriptor object when you use it, this object allow to preserve the
    settings and inspect the parameter of the functions only once (useful to speed up the calls of the tasks).

    The meaning of all the parameters are explained in the
    :meth:`decorators doc <multitask_queue.decorators.general_task>`
    """

    name: str = None
    func: Callable
    type_task: Literal[
        'regular',
        'autofill',
        'pre_execution',
        'parallel',
        'independent'
    ]
    exec_on_events: Set[str]
    exec_after_tasks: Set[str] = set()
    exec_before_tasks: Set[str] = set()
    autofill: Set[str] = set()
    type_parallelization: Optional[Literal['thread', 'process', 'async']] = 'thread'
    parameters: List[str] = None
    default_parameters: Dict[str, Any] = None

    def __init__(
            self,
            **kwargs
    ):
        super().__init__(**kwargs)
        self.name = self.func.__name__
        signature = inspect.signature(self.func)
        self.parameters = list(signature.parameters.keys())
        self.default_parameters = {
            k: v.default
            for k, v in signature.parameters.items()
            if v.default is not inspect.Parameter.empty
        }

    def __hash__(self):
        return hash(self.name)

    def __eq__(self, other):
        return other.name == self.name


class Task:
    """
    The tasks are the smaller unit of execution of the framework (you can see it as the level 0),
    they provide a simple interface to execute a function (even in different threads or process)
    based on the settings of the task and they also provide a simple way to get the results only when necessary.

    Parameters
    ----------

    task_descriptor: TaskDescriptor
        Metadata and settings of the tasks
    thread_pool: Optional[ThreadPoolExecutor] = None
        If you have an already created thread_pool you can use it for the tasks, by default it will create
        an internal thread_pool if the type_parallelization is 'thread'
    process_pool: Optional[ProcessPoolExecutor]
        If you have an already created process_pool you can use it for the tasks, by default it will create
        an internal process_pool if the type_parallelization is 'process'
    async_loop: asyncio.AbstractEventLoop
        Only is necessary for the parallel task with type_parallelization 'async'

    """

    def __init__(
            self,
            task_descriptor: TaskDescriptor,
            thread_pool: ThreadPoolExecutor = None,
            process_pool: ProcessPoolExecutor = None,
            async_loop: asyncio.AbstractEventLoop = None
    ):
        self._task_descriptor = task_descriptor
        self._on_execution: Union[Future, asyncio.Future] = None
        self._result = dict()
        self.thread_pool = thread_pool
        self.process_pool = process_pool
        self.async_loop = async_loop

        if self.thread_pool is None and self._task_descriptor.type_parallelization == 'thread':
            self.thread_pool = ThreadPoolExecutor(1)
        if self.process_pool is None and self._task_descriptor.type_parallelization == 'process':
            self.process_pool = ProcessPoolExecutor(1)
        if self.async_loop is None and self._task_descriptor.type_parallelization == 'async':
            raise ValueError(f'The parallel async tasks needs the async_loop parameter to work')

    def __eq__(self, other):
        return isinstance(other, Task) and self.name == other.name

    def __hash__(self):
        return hash(self.name)

    def __str__(self):
        return self.name

    def __repr__(self):
        return self.name

    @property
    def task_descriptor(self):
        return self._task_descriptor

    @property
    def exec_on_events(self):
        return self.task_descriptor.exec_on_events

    @property
    def exec_before_tasks(self):
        return self.task_descriptor.exec_before_tasks

    @property
    def exec_after_tasks(self):
        return self.task_descriptor.exec_after_tasks

    @property
    def name(self):
        return self.task_descriptor.name

    @property
    def type_task(self):
        return self.task_descriptor.type_task

    @property
    def autofill(self):
        return self.task_descriptor.autofill

    @property
    def type_parallelization(self):
        return self.task_descriptor.type_parallelization

    def _get_parameters_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        return {
            **self.task_descriptor.default_parameters,
            **{k: data[k] for k in self.task_descriptor.parameters if k in data},
        }

    @property
    def result(self):
        """
        Obtain the result of your task, automatically wait for the end of the task in case that is is being
        run in another thread or process

        Returns
        -------

        A Dict with the results of your function or an empty dict if the functions was never run
        or the results were get before.
        """
        if self._on_execution is not None:
            if self.task_descriptor.type_task in ['parallel', 'independent']:
                if self.task_descriptor.type_parallelization in ['thread', 'process']:
                    self._result = self._on_execution.result()
                else:
                    self._result = self.async_loop.run_until_complete(self._on_execution)
            self._on_execution = None

        r = self._result
        self._result = dict()
        return r

    def run(self, data: Dict[str, Any]):
        """
        Execute the function sent in the settings of the task, it does not return the results

        Parameters
        ----------
        data: Dict[str, Any]
            data for the parameters of the function of the task
        """
        self._on_execution = None
        self._result = dict()

        data = self._get_parameters_data(data)
        if self.task_descriptor.type_task in ['parallel', 'independent']:
            if self.task_descriptor.type_parallelization == 'thread':
                self._on_execution = self.thread_pool.submit(self.task_descriptor.func, **data)
            elif self.task_descriptor.type_parallelization == 'process':
                self._on_execution = self.process_pool.submit(self.task_descriptor.func, **data)
            else:
                self._on_execution = self.async_loop.create_task(
                    self.task_descriptor.func(
                        **self._get_parameters_data(data)
                    )
                )
        else:
            self._result = self.task_descriptor.func(**data)


class TasksOrganizer:
    """
    All the tasks are created with the idea of being represented as a node in a Directed Acyclic Graph (DAG)
    whereby, every node should has a set of outgoing arcs, those arcs represent an execution precedence with respect
    to other tasks. Having the above abstraction in mine you can understand the purpose of this class which
    is take the DAG formed by your tasks and sort it based on a
    `topological sort <https://rosettacode.org/wiki/Topological_sort#Python.>`_ to create a simple iterable list
    which in every position of the list all the tasks are independent between them and the tasks in the position
    i must be executed before the tasks in the position i + 1.

    Parameters
    ----------

    tasks: Iterable[Task]
        List with all the tasks that want to be sorted

    """

    def __init__(self, tasks: Iterable[Task]):
        self.tasks = {task.name: task for task in tasks}
        sorted_tasks = self.sort_by_exec_order(self.tasks)
        self.max_deep = len(sorted_tasks)
        self.classified_tasks = self.classify_tasks_by_type(sorted_tasks)

    def __getitem__(self, key):
        return self.tasks[key]

    def __contains__(self, key):
        return key in self.tasks

    def filter_tasks(self, tasks_name: Set[str], copy_deep: bool = False) -> Union[None, TasksOrganizer]:
        """
        Filter tasks of the DAG, useful if there are many tasks that you don't want to use for a specific
        execution

        Parameters
        ----------

        tasks_name: Set[str]
            Tasks that must be preserved

        copy_deep: bool
            Indicate if we want to return a copy of the object or filter the tasks in-place
        """
        filtered_tasks = {
            name: task for name, task in self.tasks.items() if name in tasks_name
        }
        filtered_classified_tasks = {
            classification: [
                {
                    task for task in tasks
                    if task.name in tasks_name
                }
                for tasks in ordered_tasks
            ]
            for classification, ordered_tasks in self.classified_tasks.items()
        }
        if copy_deep:
            task_organizer = TasksOrganizer([])
            task_organizer.max_deep = self.max_deep
            task_organizer.classified_tasks = filtered_classified_tasks
            task_organizer.tasks = filtered_tasks
            return task_organizer

        self.classified_tasks = filtered_classified_tasks
        self.tasks = filtered_tasks

    def copy(self):
        task_organizer = TasksOrganizer([])
        task_organizer.max_deep = self.max_deep
        task_organizer.classified_tasks = self.classified_tasks.copy()
        task_organizer.tasks = self.tasks.copy()
        return task_organizer

    def __iter__(self) -> Iterable[Dict[str, List[Task]]]:
        """
        Returns
        -------
        Every iteration return the tasks divided by type_task for an specific level, it goes from 0 to the max_deep
        """
        iterators = {classification: iter(tasks) for classification, tasks in self.classified_tasks.items()}
        for i in range(self.max_deep):
            yield {classification: next(tasks_iter) for classification, tasks_iter in iterators.items()}

    @staticmethod
    def sort_by_exec_order(tasks: Dict[str, Task]) -> List[Set[Task]]:
        if not tasks:
            return []

        # Create the dag based on the dependencies, so the node used as Key depend of the Nodes in the values
        # It's like there is an array from every node in the values to the Key node
        dag = {u: task.exec_after_tasks.copy() for u, task in tasks.items()}

        # The decorator allow to use exec_before_tasks which means that we must execute some Task U before some other
        # tasks, so we must to put the task U as a dependency in the other tasks which depend of U
        for u, task in tasks.items():
            for v in task.exec_before_tasks:
                dag[v].add(u)

            invalid_tasks = list(item for item in task.exec_after_tasks if item not in tasks)
            if invalid_tasks:
                raise ValueError(f'The task {u} depends on the next tasks that are not in the DAG: {invalid_tasks}')

        ordered_tasks = []
        extra_items_in_deps = reduce(set.union, dag.values()) - set(dag.keys())
        dag.update({item: set() for item in extra_items_in_deps})
        # Group the tasks based in the execution order created by the dag
        while True:
            ordered = set(item for item, dependencies in dag.items() if not dependencies)
            if not ordered:
                break
            ordered_tasks.append(ordered)
            dag = {
                item: dependencies - ordered for item, dependencies in dag.items()
                if item not in ordered
            }
        if dag:
            raise ValueError(
                f'There is a cyclic dependency between the tasks, '
                f'the key is the node and the values are the dependencies: {dag}'
            )

        return [set(tasks[task_name] for task_name in group) for group in ordered_tasks]

    @staticmethod
    def classify_tasks_by_type(ordered_tasks: List[Set[Task]]) -> Dict[Any, List[Set[Task]]]:
        if not ordered_tasks:
            return {}

        # Separate the tasks based in the task_classifiers of execution
        total_types_task = set(task.type_task for group in ordered_tasks for task in group)
        total_types_ordered_tasks = {type_task: [] for type_task in total_types_task}
        for group in ordered_tasks:
            types_ordered_tasks = {type_task: set() for type_task in total_types_task}
            for task in group:
                types_ordered_tasks[task.type_task].add(task)

            for type_task, tasks in types_ordered_tasks.items():
                total_types_ordered_tasks[type_task].append(tasks)

        return total_types_ordered_tasks
