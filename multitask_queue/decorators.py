import inspect

from typing import Dict, List, Union, Optional, Literal, Callable, Set, Any
from pydantic import validate_arguments, BaseModel
from timeit import default_timer as timer

from multitask_queue.task import TaskDescriptor


PLUGINS: Dict[str, TaskDescriptor] = dict()
execution_times: Dict[str, List[float]] = dict()


def add_task_to_plugins(
        func: Callable,
        exec_on_events: List[str],
        type_task: str,
        exec_after_tasks: Optional[List[str]] = None,
        exec_before_tasks: Optional[List[str]] = None,
        autofill: Optional[List[str]] = None,
        debug: bool = False
):
    if func.__name__ in PLUGINS:
        raise ValueError(f'The task name must be unique and {func.__name__} is already in use')

    if not debug:
        PLUGINS[func.__name__] = TaskDescriptor(
            func=func,
            exec_on_events=exec_on_events,
            autofill=set() if autofill is None else autofill,
            type_task=type_task,
            exec_before_tasks=set() if exec_before_tasks is None else exec_before_tasks,
            exec_after_tasks=set() if exec_after_tasks is None else exec_after_tasks,
        )
    else:
        execution_times[func.__name__] = []

        def wrapper(*args, **kwargs):
            start = timer()
            r = func(*args, **kwargs)
            execution_times[func.__name__].append(timer() - start)
            r['time_' + func.__name__] = execution_times[func.__name__]
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
            'independent',
            'async',
            'async_independent'
        ],
        exec_after_tasks: List[str] = None,
        exec_before_tasks: List[str] = None,
        autofill: Optional[List[str]] = None,
):
    """
    A task is the smaller unit of execution, every task is basically a node in a DAG so, every node consist of a set
    of dependencies (other nodes), a type of execution (the type_task option is explained in every of the
    different decorators.) and a set of events which are used to indicate to the multitask if it must execute this task

    Every task must return a dictionary, this dictionary is used to store the results and then send them to the other
    nodes, except for the autofill tasks which must return a list.

    Parameters
    ----------
    exec_on_events: List[str]
        It's used by the `Multitask` class to filter based on the attribute event
        by default there is only 'preprocess' and 'postprocess', you can add any other exec_on_events.
    type_task: Literal['regular', 'autofill', 'pre_execution', 'parallel', 'independent']
        Indicate the way that your task is going to be executed, for more info read the docs of the other tasks
        decorators
    exec_after_tasks: List[str]
        Indicate which task must be executed first than this, so this is equivalent to has a directed edge from
        a node A to a node B, being B the task with this decorator, so A must be run before B.
    exec_before_tasks: List[str]
        It's the equivalent to the exec_after_task but in the other direction, now B must be executed first than A
    autofill: Optional[List[str]]
        Indicate the task that must be automatically called every time you use this task, for example if A has
        as autofill B and C, every time you call A it will always calls B and C
    Examples
    --------
    Task that is executed in the preprocess and postprocess multitask and multiply by N a value:
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
    A regular task are all those tasks that don't need to be run in parallel or don't modify anything special like
    multitasks.
    Internally is exactly the same that the :meth:`general_task`: but using the type_task with the 'regular' option
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
        exec_after_tasks: List[str] = None,
        exec_before_tasks: List[str] = None,
        autofill: Optional[List[str]] = None,
):
    """
    A parallel task are all those tasks that must be run in parallel, so everyone of these tasks that is in the
    same execution deep that other will be executed in parallel (thread o process depending on what you select).
    (type_task = 'parallel' in :meth:`general_task`:)
    """
    def decorator(func) -> Callable:
        add_task_to_plugins(
            func=func,
            type_task='parallel',
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
        exec_after_tasks: List[str] = None,
        exec_before_tasks: List[str] = None,
        autofill: Optional[List[str]] = None,
):
    """
    This is a variant of the parallel tasks, which have the characteristic that there is no other task that depend of it
    except itself, so this tasks are run between multiple multitask in the background until it have to be run again.
    (type_task = 'independent' in :meth:`general_task`:)
    """
    def decorator(func) -> Callable:
        add_task_to_plugins(
            func=func,
            type_task='independent',
            autofill=autofill,
            exec_on_events=exec_on_events,
            exec_before_tasks=exec_before_tasks,
            exec_after_tasks=exec_after_tasks,
        )
        return func

    return decorator


@validate_arguments
def async_task(
        exec_on_events: List[str],
        exec_after_tasks: List[str] = None,
        exec_before_tasks: List[str] = None,
        autofill: Optional[List[str]] = None,
):
    """
    This is a variant of the parallel tasks, which have the characteristic that there is no other task that depend of it
    except itself, so this tasks are run between multiple multitask in the background until it have to be run again.
    (type_task = 'independent' in :meth:`general_task`:)
    """
    def decorator(func) -> Callable:
        add_task_to_plugins(
            func=func,
            type_task='async',
            autofill=autofill,
            exec_on_events=exec_on_events,
            exec_before_tasks=exec_before_tasks,
            exec_after_tasks=exec_after_tasks,
        )
        return func

    return decorator


@validate_arguments
def async_independent_task(
        exec_on_events: List[str],
        exec_after_tasks: List[str] = None,
        exec_before_tasks: List[str] = None,
        autofill: Optional[List[str]] = None,
):
    """
    This is a variant of the parallel tasks, which have the characteristic that there is no other task that depend of it
    except itself, so this tasks are run between multiple multitask in the background until it have to be run again.
    (type_task = 'independent' in :meth:`general_task`:)
    """
    def decorator(func) -> Callable:
        add_task_to_plugins(
            func=func,
            type_task='async_independent',
            autofill=autofill,
            exec_on_events=exec_on_events,
            exec_before_tasks=exec_before_tasks,
            exec_after_tasks=exec_after_tasks,
        )
        return func

    return decorator


@validate_arguments
def autofill_task(func):
    """
    This type of task is only used to call other tasks automatically based in the parameters that it receive, so
    it has the same function that the autofill parameter but with more customizable.
    (type_task = 'autofill' in :meth:`general_task`:)
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
    (type_task = 'pre_execution' in :meth:`general_task`:)
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
