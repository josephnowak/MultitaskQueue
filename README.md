# MultitaskQueue
It's a simple framework based on DAGs and Queues, it's really useful for backtesting purpose.

There are three main concepts: Task, Multitask and MultitaskQueue, the first two concepts are widly known in the world of computer science but the last one is an extention to run multiple multitasks in a first-in-first-out (FIFO) order (this behaviour can be modified, for example you can use a priority queue or a class that has put and get as methods).

The positive parts of this framework is that the tasks can be created using a simple decorator over a function and the tasks can be run in parallel (using async or threads or process)


