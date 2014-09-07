#!/usr/bin/env python
# -*- coding: UTF-8 -*-
#
#       setup.py
#
#       Copyright (c) 2014 
#       Author: Claudio Driussi <claudio.driussi@gmail.com>
#
from distutils.core import setup

setup(
    name='DynaQ',
    version='0.1.0',
    description='Dynamically creation of SQLAlchemy orm objects',
    long_description=open('README.rst').read(),

    url='http://github.com/storborg/funniest',
    author='Claudio Driussi',
    author_email='claudio.driussi@gmail.com',

    license='LGPL',
    classifiers=[
        'Development Status :: 3 - Alpha',
        'License :: OSI Approved :: GNU Lesser General Public License v2 or later (LGPLv2+)',
        'Programming Language :: Python :: 2.7',
        'Intended Audience :: Developers',
        'Topic :: Software Development :: Build Tools',
        'Topic :: Database',
    ],


    packages=['dynaq',],
    requires=[
        'sqlalchemy',
        'pyyaml',
    ],
    keywords='SQLAlchemy dynamic database yaml',
)
