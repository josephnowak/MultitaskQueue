from typing import Dict, List, Union, Optional, Literal, Callable, Set, Any
from pydantic import validate_arguments, BaseModel
from timeit import default_timer as timer

from multitask_queue.task import TaskDescriptor


PLUGINS: Dict[str, TaskDescriptor] = dict()
EXECUTION_TIMES: Dict[str, List[float]] = dict()


def add_task_to_plugins(
        func: Callable,
        exec_on_events: List[str],
        type_task: str,
        type_parallelization: Literal['thread', 'async', 'process'] = None,
        exec_after_tasks: Optional[List[str]] = None,
        exec_before_tasks: Optional[List[str]] = None,
        autofill: Optional[List[str]] = None,
        debug: bool = False
):
    if func.__name__ in PLUGINS:
        raise ValueError(f'The task name must be unique and {func.__name__} is already in use')

    PLUGINS[func.__name__] = TaskDescriptor(
        func=func,
        exec_on_events=exec_on_events,
        autofill=set() if autofill is None else autofill,
        type_task=type_task,
        exec_before_tasks=set() if exec_before_tasks is None else exec_before_tasks,
        exec_after_tasks=set() if exec_after_tasks is None else exec_after_tasks,
        type_parallelization=type_parallelization
    )

    if debug:
        EXECUTION_TIMES[func.__name__] = []
        from loguru import logger

        def wrapper(*args, **kwargs):
            start = timer()
            r = func(*args, **kwargs)
            EXECUTION_TIMES[func.__name__].append(timer() - start)
            logger.info(f'{func.__name__} took {timer() - start} seconds')
            return r

        PLUGINS[func.__name__].func = wrapper


@validate_arguments
def general_task(
        exec_on_events: List[str],
        type_task: Literal[
            'regular',
            'autofill',
            'pre_execution',
            'parallel',
            'independent'
        ],
        type_parallelization: Literal['thread', 'async', 'process'] = None,
        exec_after_tasks: List[str] = None,
        exec_before_tasks: List[str] = None,
        autofill: Optional[List[str]] = None,
):
    """
    The decorators of this framework does not add any new functionality to your function, they only register
    your function as a task, to do this they create a TaskDescriptor that is automatically added to the PLUGINS
    of the framework and those PLUGINS are used by the
    :meth:`MultitasksQueue <multitask_queue.multitask.MultitasksQueue>` to know all the task.

    Parameters
    ----------
    exec_on_events: List[str]
        Events at which this Task must be executed, read Multitask events doc for more info.
    type_task: Literal['regular', 'autofill', 'pre_execution', 'parallel', 'independent']
        Indicate the way that your task is going to be executed, for more info read the docs of the other tasks
        decorators which has an specific type_task.
    exec_after_tasks: List[str]
        Indicate which task must be executed first than this, so this is equivalent to has a directed edge from
        a node A to a node B, being B the task with this decorator, so A must be run before B.
    exec_before_tasks: List[str]
        It's the equivalent to the exec_after_task but in the other direction, now B must be executed first than A
    autofill: Optional[List[str]]
        Indicate the tasks that must be automatically called every time you use this task, for example if A has
        as autofill B and C, every time you call A it will always calls B and C (the same task rule apply).
    type_parallelization: Literal['thread', 'async', 'process']
        Indicate which type of parallelization / paradigm want to be used for your parallel task
        multithreading, multiprocess or async

    Examples
    --------
    Regular task that is executed in the preprocess and postprocess events of a multitask and multiply by N a value:
        >>> @regular_task(exec_on_events=['preprocess', 'postprocess'])
        ... def mul_by_n(value: int, n: int):
        ...     return {'value': value * n}
    """
    def decorator(func) -> Callable:
        add_task_to_plugins(
            func=func,
            type_task=type_task,
            autofill=autofill,
            exec_on_events=exec_on_events,
            exec_before_tasks=exec_before_tasks,
            exec_after_tasks=exec_after_tasks,
            type_parallelization=type_parallelization
        )
        return func

    return decorator


@validate_arguments
def regular_task(
        exec_on_events: List[str],
        exec_after_tasks: List[str] = None,
        exec_before_tasks: List[str] = None,
        autofill: Optional[List[str]] = None,
):
    """
    A regular task is basically the same that a parallel task but without the overhead of parallelization,
    they are executed one after the other even in the same level so, they are synchronously executed.
    (type_task with the 'regular' in :meth:`general_task`)
    """
    def decorator(func) -> Callable:
        add_task_to_plugins(
            func=func,
            type_task='regular',
            autofill=autofill,
            exec_on_events=exec_on_events,
            exec_before_tasks=exec_before_tasks,
            exec_after_tasks=exec_after_tasks,
        )
        return func

    return decorator


@validate_arguments
def parallel_task(
        exec_on_events: List[str],
        type_parallelization: Literal['thread', 'async', 'process'],
        exec_after_tasks: List[str] = None,
        exec_before_tasks: List[str] = None,
        autofill: Optional[List[str]] = None,
):
    """
    The execution of the tasks in a DAG is by levels so, all the tasks that are in the same level can be executed at
    the same time, having that in mind we can indicate to the framework the way that we want to parallelize your task
    (multithreads, multiprocess or async).
    (type_task = 'parallel' in :meth:`general_task`)
    """
    def decorator(func) -> Callable:
        add_task_to_plugins(
            func=func,
            type_task='parallel',
            type_parallelization=type_parallelization,
            autofill=autofill,
            exec_on_events=exec_on_events,
            exec_before_tasks=exec_before_tasks,
            exec_after_tasks=exec_after_tasks,
        )
        return func

    return decorator


@validate_arguments
def independent_task(
        exec_on_events: List[str],
        type_parallelization: Literal['async', 'thread', 'process'],
        exec_after_tasks: List[str] = None,
        exec_before_tasks: List[str] = None,
        autofill: Optional[List[str]] = None,
):
    """
    This is a variant of the parallel tasks, which have the characteristic that there is no other task that depend of it
    except itself so, these tasks are run in background during all the execution of the other tasks, if more than
    one Multitask run the same independent task it is going to wait that the task finish before start the other.
    (type_task = 'independent' in :meth:`general_task`)
    """
    def decorator(func) -> Callable:
        add_task_to_plugins(
            func=func,
            type_task='independent',
            autofill=autofill,
            exec_on_events=exec_on_events,
            exec_before_tasks=exec_before_tasks,
            exec_after_tasks=exec_after_tasks,
            type_parallelization=type_parallelization
        )
        return func

    return decorator


@validate_arguments
def autofill_task(func):
    """
    This type of task is only used to call other tasks automatically based in the parameters that it receive, so
    it has the same function that the autofill parameter but with more customizable.
    (type_task = 'autofill' in :meth:`general_task`)
    """
    add_task_to_plugins(
        func=func,
        type_task='autofill',
        autofill=None,
        exec_on_events=[],
        exec_before_tasks=[],
        exec_after_tasks=[],
    )

    def wrapper(*args, **kwargs):
        return func(*args, **kwargs)

    return wrapper


@validate_arguments
def pre_execution_task(
        exec_on_events: List[str],
        exec_after_tasks: List[str] = None,
        exec_before_tasks: List[str] = None,
        autofill: Optional[List[str]] = None,
):
    """
    This task is executed before all the other type of tasks, is useful for modify the multitask events
    (type_task = 'pre_execution' in :meth:`general_task`)
    """
    def decorator(func) -> Callable:
        add_task_to_plugins(
            func=func,
            type_task='pre_execution',
            autofill=autofill,
            exec_on_events=exec_on_events,
            exec_before_tasks=exec_before_tasks,
            exec_after_tasks=exec_after_tasks,
        )
        return func

    return decorator
