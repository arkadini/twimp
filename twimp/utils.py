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


from collections import deque
import logging
from operator import itemgetter

from twisted.internet import protocol
from twisted.python import failure

from twimp import vecbuf

LOG_CATEGORY = 'utils'
import twimp.log
log = twimp.log.get_logger(LOG_CATEGORY)


def ms_time(t):
    "Convert time t from (real) seconds to (int) milliseconds."

    return int(t * 1000)

def ms_time_wrapped(t):
    """Convert time t from (real) seconds to (int) milliseconds and
    wrap it to [0; 0xffffffff] range."""

    return ms_time(t) % 0x100000000


class GeneratorWrapperProtocol(protocol.Protocol):
    def __init__(self, proto=None):
        self._buf = vecbuf.VecBuf()
        self._toread = 0

        self._handler_changed = False

        self._handler = proto
        if proto:
            self._toread = self._handler.send(None)

    def init_handler(self, proto=None, do_init=True):
        if proto:
            if self._handler:
                # need to indicate we should retry on StopIteration...
                self._handler_changed = True
            self._handler = proto
        if do_init:
            self._toread = self._handler.send(None)

    def connectionLost(self, reason=protocol.connectionDone):
        # clean up
        self._buf.read(len(self._buf))
        self._handler = None
        self._toread = None

    def dataReceived(self, data):
        self._buf.write(data)

        while 1: # need to be able to retry immediately when handlers change
            try:
                while len(self._buf) >= self._toread:
                    self._toread = self._handler.send(self._buf)
                break           # no data, no retry
            except StopIteration:
                if self._handler_changed:
                    self._handler_changed = False
                else:
                    self._toread = None
                    self._handler = None
                    break
            except:
                # undocumented, but real; this way we can signal
                # protocol errors directly, and by not passing/raising
                # directly we avoid a stack trace log, with the same
                # effect - connectionLost
                f = failure.Failure()
                log.info(f.value, exc_info=log.isEnabledFor(logging.DEBUG))
                return f


class FrameSorter(object):
    def __init__(self, callback, keys):
        self._max = len(keys)
        assert self._max > 1, 'need at least two sources'
        self._queues = dict((k, [None, deque(), k]) for k in keys)
        self._active = 0

        self._callback = callback

        self._key_getter = itemgetter(0)
        self._grpos_getter = itemgetter(0)

    def add(self, key, grpos, data):
        sq = self._queues[key]
        if not sq[1]:
            self._active += 1
            sq[0] = grpos
        sq[1].append((grpos, data))
        self._check_send()

    def _check_send(self):
        if self._active < self._max:
            return

        while self._active >= self._max:
            first_q = min(self._queues.itervalues(), key=self._key_getter)
            key = first_q[2]
            second_q = min((q for (k, q) in self._queues.iteritems()
                            if k != key), key=self._key_getter)

            switch_grpos = second_q[0]
            frame_queue = first_q[1]
            while frame_queue:
                if frame_queue[0][0] > switch_grpos:
                    break
                (grpos, frame) = frame_queue.popleft()
                self._callback(grpos, key, frame)

            if not frame_queue:
                self._active -= 1
            else:
                first_q[0] = frame_queue[0][0]

    def flush(self):
        sorted_frames = sorted(((grpos, key, frame)
                                for (min_grpos, frame_queue, key)
                                    in self._queues.values()
                                for (grpos, frame) in frame_queue),
                               key=self._grpos_getter)
        self._active = 0
        self._queues = dict((k, [None, deque(), k]) for k in self._queues)

        for f in sorted_frames:
            self._callback(*f)
