from __future__ import annotations

import asyncio
import inspect

from functools import reduce
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, Future
from typing import List, Dict, Iterable, Literal, Optional, Callable, Any, Set, Union, Hashable
from pydantic import BaseModel, validators, validate_arguments
from loguru import logger


class TaskDescriptor(BaseModel):
    """
    The decorators automatically create this TaskDescriptor object, which in the instantiation process inspect
    the function to get all parameters that it use to speed up the call of the function and of course it store
    all the information that you send in the decorators, so it has the same parameters
    """

    name: str = None
    func: Callable
    type_task: Literal[
        'regular',
        'autofill',
        'pre_execution',
        'parallel',
        'independent',
        'async',
        'async_independent'
    ]
    exec_on_events: Set[str]
    exec_after_tasks: Set[str] = set()
    exec_before_tasks: Set[str] = set()
    autofill: Set[str] = set()
    type_parallelization: Optional[Literal['thread', 'process']] = 'thread'
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
        if self.async_loop is None and self._task_descriptor.type_task in ['async', 'async_independent']:
            raise ValueError(f'The async or async_independent tasks need the async_loop parameter to work')

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

    def copy(self):
        return Task(
            task_descriptor=self.task_descriptor,
            thread_pool=self.thread_pool,
            process_pool=self.process_pool,
            async_loop=self.async_loop
        )

    def _get_parameters_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        return {
            **self.task_descriptor.default_parameters,
            **{k: data[k] for k in self.task_descriptor.parameters if k in data},
        }

    @property
    def result(self):
        if self._on_execution is not None:
            if self.task_descriptor.type_task in ['parallel', 'independent']:
                self._result = self._on_execution.result()
            else:
                self._result = self.async_loop.run_until_complete(self._on_execution)
            self._on_execution = None

        r = self._result
        self._result = dict()
        return r

    def run(self, data: Dict[str, Any]):
        self._on_execution = None
        self._result = dict()

        data = self._get_parameters_data(data)
        if self.task_descriptor.type_task in ['parallel', 'independent']:
            if self.task_descriptor.type_parallelization == 'thread':
                self._on_execution = self.thread_pool.submit(self.task_descriptor.func, **data)
            else:
                self._on_execution = self.process_pool.submit(self.task_descriptor.func, **data)

        elif self.task_descriptor.type_task in ['async', 'async_independent']:

            self._on_execution = self.async_loop.create_task(
                self.task_descriptor.func(
                    **self._get_parameters_data(data)
                )
            )
        else:
            self._result = self.task_descriptor.func(**data)


class TasksOrganizer:
    def __init__(self, tasks: List[Task]):
        self.tasks = {task.name: task for task in tasks}
        sorted_tasks = self.sort_by_exec_order(self.tasks)
        self.max_deep = len(sorted_tasks)
        self.classified_tasks = self.classify_tasks_by_type(sorted_tasks)

    def __getitem__(self, key):
        return self.tasks[key]

    def __contains__(self, key):
        return key in self.tasks

    @validate_arguments
    def filter_tasks(self, tasks_name: Set[str], copy_deep: bool = False) -> Union['TaskOrganizer', None]:
        filtered_tasks = {
            name: task.copy() if copy_deep else task
            for name, task in self.tasks.items()
            if name in tasks_name
        }
        filtered_classified_tasks = {
            classification: [
                {
                    task.copy() if copy_deep else task
                    for task in tasks
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

    def copy(self, deep: bool = True):
        task_organizer = TasksOrganizer([])
        task_organizer.max_deep = self.max_deep
        if deep:
            filtered_tasks = {name: task.copy() for name, task in self.tasks.items()}
            filtered_classified_tasks = {
                classification: [{task.copy() for task in tasks} for tasks in ordered_tasks]
                for classification, ordered_tasks in self.classified_tasks.items()
            }
            task_organizer.classified_tasks = filtered_classified_tasks
            task_organizer.tasks = filtered_tasks

        task_organizer.classified_tasks = self.classified_tasks
        task_organizer.tasks = self.tasks
        return task_organizer

    def __iter__(self) -> Iterable[Dict[str, List[Task]]]:
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
