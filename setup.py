#!/usr/bin/env python
# coding=utf-8

from distutils.core import setup
from mapi import IS_PY2

with open('requirements.txt', 'r') as fp:
    REQUIREMENTS = fp.read().splitlines()

with open('readme.rst', 'r') as fp:
    LONG_DESCRIPTION = fp.read()

setup(
    author='Jessy Williams',
    author_email='jessy@jessywilliams.com',
    description=(
        'An API for media database APIs which allows you to search for metadata'
        'using a simple, common interface'
    ),
    license='MIT',
    long_description=LONG_DESCRIPTION,
    name='mapi',
    packages=['mapi'],
    install_requires=REQUIREMENTS,
    url='https://github.com/jkwill87/mapi',
    version='3.0.1'
)
