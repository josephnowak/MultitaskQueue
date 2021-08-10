import asyncio
import os

from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, Future
from typing import List, Dict, Iterable, Literal, Optional, Callable, Any, Set, Union, Hashable
from queue import Queue

from multitask_queue.task import Task, TasksOrganizer
from multitask_queue.decorators import PLUGINS


class Multitask:

    def __init__(
            self,
            multitask_id: Hashable,
            events: Set[str] = None,
            tasks: Set[str] = None,
            metadata: Optional[Dict] = None,
            reverse_order: bool = False
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
        self._events.update(events)

    def add_tasks(self, tasks: Set[str]):
        self._tasks.update(tasks)

    def run(
            self,
            data: Dict[str, Any],
            tasks_organizer: TasksOrganizer,
            check_independent_finish: bool = False
    ):
        independents = []
        for classified_tasks in tasks_organizer:
            classified_tasks = {
                classification: {
                    task for task in tasks
                    if (task.exec_on_events & self._events) or (task.name in self._tasks)
                }
                for classification, tasks in classified_tasks.items()
            }
            # run and get the results of the pre execution tasks
            for task in classified_tasks.get('pre_execution', []):
                task.run(data)
                data.update(task.result)

            # run the independents tasks and get the results if they were running in background
            for type_task in ['independent', 'async_independent']:
                for task in classified_tasks.get(type_task, []):
                    independents.append(task)
                    result = data.update(task.result)
                    task.run(data)

            # run the parallel and async tasks
            for type_task in ['parallel', 'async']:
                for task in classified_tasks.get(type_task, []):
                    task.run(data)

            # run the regular tasks
            for task in classified_tasks.get('regular', []):
                task.run(data)
                data.update(task.result)

            # get the results of the parallel and async tasks
            for type_task in ['parallel', 'async']:
                for task in classified_tasks.get(type_task, []):
                    data.update(task.result)

        if check_independent_finish:
            for task in independents:
                data.update(task.result)


class MultitasksOrganizer:
    def __init__(
            self,
            multitasks: Union[List[Multitask], Queue],
    ):
        self._multitasks = multitasks

    def put(self, multitask: Multitask):
        if isinstance(self._multitasks, list):
            self._multitasks.append(multitask)
        else:
            self._multitasks.put(multitask)

    def get(self):
        if isinstance(self._multitasks, list):
            return self._multitasks.pop(0)
        return self._multitasks.get()

    def reorder(self, reverse: bool = False):
        self._multitasks.sort(reverse=reverse)

    def empty(self):
        if isinstance(self._multitasks, list):
            return len(self._multitasks) == 0
        return self._multitasks.empty()


class MultitasksQueue:

    def __init__(
            self,
            max_threads: int = None,
            max_process: int = None,
            async_loop: asyncio.AbstractEventLoop = None
    ):
        self.thread_pool = ThreadPoolExecutor(os.cpu_count() if max_threads is None else max_threads)
        self.process_pool = ProcessPoolExecutor(os.cpu_count() if max_process is None else max_process)
        self.async_loop = async_loop
        if self.async_loop is None:
            self.async_loop = asyncio.get_event_loop()

        self.tasks_organizer = TasksOrganizer(
            [
                Task(
                    task_descriptor,
                    thread_pool=self.thread_pool,
                    process_pool=self.process_pool,
                    async_loop=self.async_loop
                )
                for task_descriptor in PLUGINS.values()
            ]
        )

    def run(
            self,
            data: Dict[str, Any],
            tasks: Set[str] = None,
            mt_queue: List[Multitask] = None
    ):
        tasks = set() if tasks is None else tasks
        data['user_tasks'] = tasks
        tasks = self.autofill_tasks(tasks, data)
        data['user_tasks'] = tasks

        if tasks:
            tasks_organizer = self.tasks_organizer.filter_tasks({task.name for task in tasks}, copy_deep=True)
        else:
            tasks_organizer = self.tasks_organizer.copy(deep=True)

        mt_queue = MultitasksOrganizer(
            multitasks=[] if mt_queue is None else mt_queue
        )
        data['mt_queue'] = mt_queue

        multitask = Multitask(
            'preprocess',
            events={'preprocess'},
        )
        data['multitask'] = multitask
        multitask.run(data, tasks_organizer)

        while not mt_queue.empty():
            multitask = mt_queue.get()
            data['multitask'] = multitask
            multitask.run(data, tasks_organizer)

        multitask = Multitask(
            'postprocess',
            events={'postprocess'},
        )
        data['multitask'] = multitask
        multitask.run(data, tasks_organizer, check_independent_finish=True)

    def autofill_tasks(self, tasks: Set[str], data):
        autofilled_tasks = set()
        tasks_to_check_autofill = list(tasks)
        while tasks_to_check_autofill:
            task = self.tasks_organizer[tasks_to_check_autofill[0]]
            tasks_to_check_autofill = tasks_to_check_autofill[1:]
            if task.type_task != 'autofill':
                autofilled_tasks.add(task)
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
        return autofilled_tasks


