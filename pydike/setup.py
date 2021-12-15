#!/usr/bin/env python

from distutils.core import setup

setup(name='pydike',
      version='0.1',
      description='NDP Python Package',
      url='https://github.com/peterpuhov-github/caerus-dikeHDFS',
      author='caerus developers',
      author_email='caerus-developers@tbd.com',
      license='Apache-2.0',
      packages=['pydike.client', 'pydike.core', 'pydike.server'],
     )
