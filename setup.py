#!/usr/bin/python

"""twimp
=====

Another python RTMP implementation: this one is supposed to match
the twisted design principles closer than other implementations.
"""

import os
import sys

from distutils.core import setup


sys.path.insert(0, os.path.dirname(__file__))
from twimp import __version__
sys.path.pop(0)


setup(name='twimp',
      version=__version__,
      description='Twisted RTMP implementation',
      long_description=__doc__,
      platforms=['any'],
      license='Apache License v2.0',
      author='Arek Korbik',
      author_email='arkadini@gmail.com',
      maintainer='Arek Korbik',
      maintainer_email='arkadini@gmail.com',
      url='http://github.com/arkadini/twimp',
      packages=['twimp', 'twimp.crypto', 'twimp.server', 'twimp.scripts',
                'twimp.auth'])
