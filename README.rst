=====
DynaQ
=====

This project is at beginning state, many things are to do but the first step
is done, it will create object for SQLAlchemy


What is
-------

DynaQ is a system that read yaml files (structured as the system require), and
dynamically generate the orm classes for SQLAlchemy.


Why
---

SQLAlchemy knows only informations strictly related to the database structure
and doesn't store other metadata informations. In the last releases every 
object hast the "info" variable, but infos are not related in a coordinated way.

DynaQ let to store in a Database definition also semantic informations about
objects of database and informations that can be used to handle data by
applications. The use of yaml files give a clean and readable way to project 
the database and let to store meta informations and comments useful for the 
programmer.

The types are defined in abstract and hierarchical way so field can be 
categorized well and changes to the type are consistent for every fields of the
same type.

What is lost
------------

DynaQ dynamically generate SQLAlchemy classes, so it's natural a simplification
a loss of functionality and versatility. Thanks to Python power and versatility,
it's easy to add some of the not provided capabilities.

Anyway some abilities are loss:

- **Compound primary keys**: DynaQ support only single field primary keys
- **Compound custom types**: SQLAlchemy types must have only a single field.
- **Complex relations**: There are no specific logic for many to many and one to
  one relations. DynaQ supports well One to Many relation.
- **Many minor abilities**: Many functions are not yet implemented, but they are
  not structural so they may be implemented in the future.


What is gained
--------------

Some benefits of DynaQ are:

- The definition of database in yaml files is clear, readable and less error 
  prone, the types are abstract and helps the classification of fields.
- In the yaml files are stored information not strictly referred to database
  engine and let to store comments and properties useful for application 
  development.
- DynaQ will be improved with functionalities useful for application 
  development such query builder, sets of records handling, CRUD back end 
  facilities and more.


What is to do
-------------

Many things, first of all some minor improvements such as __repr__ method 
automation, better One to Many handling and so on.

Then some mayor improvements will be a query builder and CRUD UI interfaces.
But these functionalities will be added when required by other projects that
I'm thinking on now.

At the moment the yaml format is not well documented because is not complete
and may be changed and improved, please look at the "test" directory for 
samples and tests.

A user flendly DB system need an automatic restructure facility for simple
adding or deleting of fields, Alembic provide all API to do this for simple
situations.

DynaQ is a part of bigger project which will be a complete framework for the
development of business application in web environment that act as desktop 
replacement. ASAP I will publish links to the other parts of project.

Are you interested?
-------------------

I don't know how much time I can spend to this project because it depends from
other projects. But if you are interested please send me a mail we can decide
how you can participate.


Conclusion
----------

This packages is tested only on Python 2.7 environment, let me know if it works
on Python 3.* too.

At the end I will apologize for my english, i hope it's not so terrible, but I
am not english speaker.

Best regards

Claudio Driussi

