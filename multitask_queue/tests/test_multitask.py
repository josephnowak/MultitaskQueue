from multitask_queue.multitask import MultitasksQueue, Multitask, MultitasksOrganizer
from multitask_queue.task import TasksOrganizer, Task, TaskDescriptor
from multitask_queue.decorators import regular_task, pre_execution_task, parallel_task


class TestMultitask:

    def test_multitask(self):
        def sum_to_value(value: int, a: int):
            return {'value': value + a}

        def mul_value(value: int, b: int):
            return {'value': value * b}

        def divide_value(value: int, c: int):
            return {'value': value / c}

        task_sum = Task(
            TaskDescriptor(
                func=sum_to_value,
                type_task='regular',
                exec_on_events=['modification'],
                exec_after_tasks=[],
                exec_before_tasks=[],
                autofill=[]
            )
        )
        task_mul = Task(
            TaskDescriptor(
                func=mul_value,
                type_task='regular',
                exec_on_events=['modification'],
                exec_after_tasks=['sum_to_value'],
                exec_before_tasks=[],
                autofill=['sum_to_value']
            )
        )
        task_div = Task(
            TaskDescriptor(
                func=divide_value,
                type_task='regular',
                exec_on_events=['exception'],
                exec_after_tasks=[],
                exec_before_tasks=[],
                autofill=[]
            )
        )

        tasks_organizer = TasksOrganizer([task_mul, task_sum, task_div])

        multitask = Multitask(
            multitask_id='any',
            events={'modification'}
        )
        data = {'value': 1, 'a': 1, 'b': 2, 'c': 2}
        multitask.run(data, tasks_organizer)
        assert data['value'] == 4

        multitask = Multitask(
            multitask_id='any',
            events={'exception'}
        )
        multitask.run(data, tasks_organizer)
        assert data['value'] == 2

    def test_multitasks_queue(self):
        @pre_execution_task(exec_on_events=['preprocess'])
        def enqueue_multitasks(mt_queue: MultitasksOrganizer):
            mt_queue.put(
                Multitask(
                    multitask_id=1,
                    events={'modification'}
                )
            )
            mt_queue.put(
                Multitask(
                    multitask_id=2,
                    events={'exception'}
                )
            )
            return {}

        @regular_task(exec_on_events=['modification'])
        def sum_to_value(value: int, a: int):
            return {'value': value + a}

        @regular_task(exec_on_events=['modification'], exec_after_tasks=['sum_to_value'])
        def mul_value(value: int, b: int):
            return {'value': value * b}

        @parallel_task(exec_on_events=['exception'], type_parallelization='thread')
        def divide_value(value: int, c: int):
            return {'value': value / c}

        data = {'value': 1, 'a': 1, 'b': 4, 'c': 2}
        multitasks = MultitasksQueue()
        multitasks.run(data)

        assert data['value'] == 4


if __name__ == "__main__":
    test = TestMultitask()
    # test.test_multitask()
    test.test_multitasks_queue()
