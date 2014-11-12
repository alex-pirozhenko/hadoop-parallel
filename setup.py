#!/usr/bin/env python

from distutils.core import setup

setup(
      name='hadoop-parallel',
      version='0.0.1',
      author='Alex Pirozhenko',
      author_email='alex.pirozhenko@gmail.com',
      packages=['hadoop_parallel'],
      package_dir={'hadoop_parallel': 'src/hadoop_parallel'},
)