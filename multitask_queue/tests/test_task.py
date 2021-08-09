import asyncio
import time

from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from multiprocessing import Queue, Manager
from typing import List

from multitask_queue.task import Task, TaskDescriptor, TasksOrganizer


def process_append_sleep(a: Queue):
    a.put(2)
    time.sleep(0.1)
    return {}


def process_sleep_append(a: Queue):
    time.sleep(0.2)
    a.put(1)
    return {}


class TestTask:

    def test_task_descriptor(self):
        def dummy_func(a: str, b: int = 5):
            return 0

        task_descriptor = TaskDescriptor(
            func=dummy_func,
            type_task='regular',
            exec_on_events=['event_one', 'event_one', 'event_two'],
            exec_after_tasks=['func1', 'func2', 'func1'],
            exec_before_tasks=['func0', 'func-1', 'func-1'],
            autofill=['func1', 'func1', 'func0'],
            type_parallelization='thread'
        )
        assert task_descriptor.parameters == ['a', 'b']
        assert task_descriptor.default_parameters == {'b': 5}
        assert isinstance(task_descriptor.exec_on_events, set)
        assert isinstance(task_descriptor.exec_after_tasks, set)
        assert isinstance(task_descriptor.exec_before_tasks, set)
        assert isinstance(task_descriptor.autofill, set)

    def test_regular_pre_execution_task(self):
        def dummy_func(a: List, b: int):
            a.append(b)
            return {'a': a}

        for type_task in ['regular', 'pre_execution']:
            data = {'a': [], 'b': 2}
            task_dummy = Task(
                TaskDescriptor(
                    func=dummy_func,
                    type_task=type_task,
                    exec_on_events=[],
                    exec_after_tasks=[],
                    exec_before_tasks=[],
                    autofill=[],
                ),
            )
            task_dummy.run(data)
            assert data['a'] == [2]

    def test_parallel_task(self):
        def sleep_append(a: List):
            time.sleep(0.2)
            a.append(1)
            return {}

        def append_sleep(a: List):
            a.append(2)
            time.sleep(0.1)
            return {}

        thread_pool = ThreadPoolExecutor(2)
        process_pool = ProcessPoolExecutor(2)
        for type_parallelization in ['thread', 'process']:
            if type_parallelization == 'thread':
                func_sleep_append_func = sleep_append
                func_append_sleep_func = append_sleep
            else:
                func_sleep_append_func = process_sleep_append
                func_append_sleep_func = process_append_sleep

            task_sleep_append = Task(
                TaskDescriptor(
                    func=func_sleep_append_func,
                    type_task='parallel',
                    exec_on_events=[],
                    exec_after_tasks=[],
                    exec_before_tasks=[],
                    autofill=[],
                    type_parallelization=type_parallelization
                ),
                thread_pool=thread_pool,
                process_pool=process_pool
            )

            task_append_sleep = Task(
                TaskDescriptor(
                    func=func_append_sleep_func,
                    type_task='independent',
                    exec_on_events=[],
                    exec_after_tasks=[],
                    exec_before_tasks=[],
                    autofill=[],
                    type_parallelization=type_parallelization
                ),
                thread_pool=thread_pool,
                process_pool=process_pool
            )

            if type_parallelization == 'thread':
                data = {'a': list()}
            else:
                data = {'a': Manager().Queue()}
            task_sleep_append.run(data)
            task_append_sleep.run(data)

            data.update(task_sleep_append.result)
            data.update(task_append_sleep.result)

            if type_parallelization == 'thread':
                result = data['a']
            else:
                result = []
                while not data['a'].empty():
                    result.append(data['a'].get())

            assert result == [2, 1]

    def test_async_task(self):
        async def await_append(a: List):
            await asyncio.sleep(0.5)
            a.append(1)
            return {}

        async def append_await(a: List):
            a.append(2)
            await asyncio.sleep(0.3)
            return {}

        loop = asyncio.get_event_loop()
        task_await_append = Task(
            TaskDescriptor(
                func=await_append,
                type_task='async',
                exec_on_events=[],
                exec_after_tasks=[],
                exec_before_tasks=[],
                autofill=[]
            ),
            async_loop=loop
        )

        task_append_await = Task(
            TaskDescriptor(
                func=append_await,
                type_task='async_independent',
                exec_on_events=[],
                exec_after_tasks=[],
                exec_before_tasks=[],
                autofill=[]
            ),
            async_loop=loop
        )
        data = {'a': []}
        task_await_append.run(data)
        task_append_await.run(data)

        data.update(task_await_append.result)
        data.update(task_append_await.result)

        assert data['a'] == [2, 1]

    def test_task_organizer(self):
        def first_func():
            pass

        def second_func():
            pass

        def third_func():
            pass

        first_task = Task(
            TaskDescriptor(
                func=first_func,
                type_task='regular',
                exec_on_events=[],
                exec_after_tasks=['second_func'],
                exec_before_tasks=[],
                autofill=[]
            ),
        )
        second_task = Task(
            TaskDescriptor(
                func=second_func,
                type_task='regular',
                exec_on_events=[],
                exec_after_tasks=[],
                exec_before_tasks=['third_func'],
                autofill=[]
            ),
        )
        third_task = Task(
            TaskDescriptor(
                func=third_func,
                type_task='regular',
                exec_on_events=[],
                exec_after_tasks=[],
                exec_before_tasks=['first_func'],
                autofill=[]
            ),
        )
        task_organizer = TasksOrganizer([first_task, second_task, third_task])
        ordered_tasks = [{task.name for task in group} for group in task_organizer.classified_tasks['regular']]
        assert ordered_tasks == [
            {'second_func'},
            {'third_func'},
            {'first_func'},
        ]


if __name__ == "__main__":
    test = TestTask()
    # test.test_task_descriptor()
    # test.test_async_task()
    # test.test_parallel_task()
    test.test_task_organizer()
