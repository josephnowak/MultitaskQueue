from multitask_queue.task import TaskDescriptor

from multitask_queue import decorators


class TestDecorators:

    def test_decorators(self):
        total_decorators = {
            'regular_task': 'regular',
            'parallel_task': 'parallel',
            'independent_task': 'independent',
            'pre_execution_task': 'pre_execution',
            'autofill_task': 'autofill'
        }
        for decorator_name, type_task in total_decorators.items():
            decorator = getattr(decorators, decorator_name)

            if decorator_name != 'autofill_task':
                parameters = dict(
                    exec_on_events=['event'],
                    exec_after_tasks=['dummy'],
                    exec_before_tasks=['dummy2'],
                    autofill=['dummy3']
                )

                if type_task in ['independent', 'parallel']:
                    parameters['type_parallelization'] = 'thread'

                @decorator(**parameters)
                def dummy_func(a: int = 5):
                    pass

                task = decorators.PLUGINS['dummy_func']
                assert task.func == dummy_func
                assert task.exec_on_events == {'event'}
                assert task.exec_after_tasks == {'dummy'}
                assert task.exec_before_tasks == {'dummy2'}
                assert task.autofill == {'dummy3'}

            else:
                @decorator
                def dummy_func(a: int = 5):
                    pass

                task = decorators.PLUGINS['dummy_func']

            assert task.type_task == type_task
            assert task.parameters == ['a']
            assert task.default_parameters == {'a': 5}

            del decorators.PLUGINS['dummy_func']


if __name__ == "__main__":
    test = TestDecorators()
    test.test_decorators()

