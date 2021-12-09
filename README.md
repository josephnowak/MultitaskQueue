# MultitaskQueue
It's a simple framework based on three composed concepts:
1. Task: A task is the smaller unit of execution or simple a node in the DAG, everyone of them has a function to execute and a set of events that indicate when to run this task.
2. Multitask: The multitask can be seen as a DAG with the ability to orquestate when and how run every task, based on events that can be modified during the execution.
3. Multitask Queue: It's an extention to run multiple multitasks in a first-in-first-out (FIFO) order (this behaviour can be modified, for example you can use a priority queue or a class that has a put and a get method)

The positive parts of this framework are the followings:
1. The tasks can be created with a simple decorator over a function.
2. The tasks can be run in parallel depending on the decorator that you use, internally it can use async code, multithreads or multiprocess.
3. The parameters of the functions are stored to speed up every call.
4. The resulting DAG from the tasks allows to optimize the parallelization of your tasks when you run them using the Multitask class.
5. The MultitaskQueue class is really easy to use, all the functions that you decorate are automatically registered to be use by this class, so you don't have to make a lot of imports and then send every task to the class to use it.
6. Enqueue new multitasks to the MultitaskQueue class is easy to do and can be done during the run process using a task so, you can add as many as you want and this allow you to recreate some workflow like the ones used in the backtesters.
