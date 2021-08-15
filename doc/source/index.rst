.. multitask_organizer documentation master file, created by
   sphinx-quickstart on Mon Jul 12 10:10:38 2021.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to Multitask Queue's documentation!
===============================================

This framework is based in four widely known concepts: Tasks, Multitasks, Queues and Directed Acyclic Graphs (DAGs).
(There are many tutorials in google about these topics in case that you don't know them).

The framework was designed to solve general problems that can be formulated using the aforementioned concepts, but
the principal focus was simplify the way that a backtest is executed (historical tests normally financial backtests),
so basically it does not only handle/organize a set of tasks, it also handle/organize a set of multitasks
where every multitask represent a big sub problem (for example it could be a date) and in every multitask
we execute some specific tasks.


.. toctree::
   :maxdepth: 2
   :caption: Contents:

   reference/decorators
   reference/task_descriptor
   reference/task
   reference/tasks_organizer
   reference/multitask
   reference/multitasks_organizer
   reference/multitasks_queue
