#!/usr/bin/env python

from distutils.core import setup
from pip.req import parse_requirements

install_reqs = parse_requirements('requirements.txt')

setup(
      name='hadoop-parallel',
      version='0.0.1',
      author='Alex Pirozhenko',
      author_email='alex.pirozhenko@gmail.com',
      packages=['hadoop_parallel'],
      package_dir={'hadoop_parallel': 'src/hadoop_parallel'},
      requires=[str(ir.req) for ir in install_reqs]
)