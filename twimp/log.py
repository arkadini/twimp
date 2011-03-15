#   Copyright (c) 2010 Arek Korbik
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.


import logging


LOG_CATEGORY = 'twimp'
LOG_FORMAT = ('%(asctime)s  %(process)5d  '
              '%(levelname)-7s %(name)-20s %(message)s '
              '[%(pathname)s:%(lineno)d]')
TWISTED_CATEGORY = '%s.twisted' % LOG_CATEGORY

_logger = None


def _ensure_main_logger():
    global _logger

    if not _logger:
        log = logging.getLogger(LOG_CATEGORY)
        log.setLevel(logging.NOTSET)

        handler = logging.StreamHandler()
        handler.setLevel(logging.NOTSET)

        formatter = logging.Formatter(LOG_FORMAT)
        handler.setFormatter(formatter)

        log.addHandler(handler)

        _logger = log


def get_logger(subname=None):
    _ensure_main_logger()

    if subname:
        logger = logging.getLogger('%s.%s' % (LOG_CATEGORY, subname))
    else:
        logger = logging.getLogger(LOG_CATEGORY)

    return logger


labels_to_levels = {
    'all': 1,
    'debug': logging.DEBUG,
    'info': logging.INFO,
    'warning': logging.WARNING,
    'error': logging.ERROR,
    'critical': logging.CRITICAL,
    'none': logging.NOTSET,
    }

def _parse_level(s):
    cat = None
    if ':' in s:
        cat, slevel = s.split(':', 1)
    else:
        slevel = s

    if slevel in labels_to_levels:
        level = labels_to_levels[slevel.strip().lower()]
    else:
        try:
            level = max(0, min(6, 6 - int(slevel))) * 10
        except:
            raise RuntimeError('cannot parse "%s" as "[CATEGORY:]LEVEL"' %
                               slevel)
    return cat, level

def set_levels(levels_string):
    levels = levels_string.split(',')
    for s in levels:
        cat, level = _parse_level(s)
        logger = get_logger(cat)
        logger.setLevel(level)


def set_levels_from_env(varname='RTMP_DEBUG'):
    import os
    levels = os.environ.get(varname)
    if levels:
        set_levels(levels)


def hook_twisted(levels=None, redirect_stdout=0):
    _ensure_main_logger()
    if levels:
        set_levels(levels)

    from twisted.python import log
    plo = log.PythonLoggingObserver(TWISTED_CATEGORY)
    log.startLoggingWithObserver(plo.emit, setStdout=redirect_stdout)
