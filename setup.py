#!/usr/bin/python

"""twimp
=====

Another python RTMP implementation: this one is supposed to match
the twisted design principles closer than other implementations.
"""

from distutils.core import setup

setup(name='twimp',
      version='0.1',
      description='Twisted RTMP implementation',
      long_description=__doc__,
      platforms=['any'],
      license='Apache License v2.0',
      author='Arek Korbik',
      author_email='arkadini@gmail.com',
      maintainer='Arek Korbik',
      maintainer_email='arkadini@gmail.com',
      url='http://github.com/arkadini/twimp',
      packages=['twimp', 'twimp.crypto', 'twimp.server', 'twimp.scripts'])
