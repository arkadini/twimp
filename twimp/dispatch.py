#   Copyright (c) 2011  Arek Korbik
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

from twisted.internet import defer

from twimp import amf0
from twimp import chunks
from twimp.error import CallResultError, CallAbortedException
from twimp.proto import EventDispatchProtocol, EventDispatchFactory
from twimp.utils import ms_time


LOG_CATEGORY = 'dispatch'
import twimp.log
log = twimp.log.get_logger(LOG_CATEGORY)


class CallDispatchProtocol(EventDispatchProtocol):
    def __init__(self):
        EventDispatchProtocol.__init__(self)

    def session_time(self):
        return time.time() - self.session_init_time

    def unknownCommandType(self, cmd, ts, ms_id, args):
        trans_id = args[0]

        handler_m = getattr(self, 'remote_%s' % (cmd,), None)

        if handler_m is None:
            d = defer.maybeDeferred(self.unknownRemoteCall, cmd, ts, ms_id,
                                    args[1:])
        else:
            d = defer.maybeDeferred(handler_m, ts, ms_id, *args[1:])

        if trans_id:
            d.addCallback(self._remote_handler_cb, ms_id, trans_id)

        d.addErrback(self._remote_abort_handler_eb)
        d.addErrback(self._remote_handler_eb, ms_id, trans_id)

    def _remote_abort_handler_eb(self, failure):
        failure.trap(CallAbortedException)
        # log failure but do nothing more
        log.debug('remote call aborted: %s', failure.value)

    def _remote_handler_cb(self, result, ms_id, trans_id):
        # log.debug('remote call result: %r', result)
        if not isinstance(result, (tuple, list)):
            result = (result,)

        body = self.encode_amf('_result', trans_id, *result)

        ts = ms_time(self.session_time())
        self.muxer.sendMessage(ts, chunks.MSG_COMMAND, ms_id, body)

    def _remote_handler_eb(self, failure, ms_id, trans_id):
        if log.isEnabledFor(logging.DEBUG):
            log.info('remote call failure: %s', failure.value,
                     exc_info=(failure.type, failure.value,
                               failure.getTracebackObject()))
        else:
            log.info('remote call failure: %s', failure.value)

        fatal = False
        if failure.check(CallResultError):
            body = self.encode_amf('_error', trans_id,
                                   *failure.value.get_error_args())
            fatal = failure.value.is_fatal
        else:
            err = amf0.Object(code='NetStream.Failed', level='error',
                              description=repr(failure.value))
            body = self.encode_amf('_error', trans_id, None, err)

        ts = ms_time(self.session_time())
        self.muxer.sendMessage(ts, chunks.MSG_COMMAND, ms_id, body)

        if fatal:
            self.transport.loseConnection()

    def unknownRemoteCall(self, cmd, ts, ms_id, args):
        # seems that we're just supposed to silently ignore the request
        log.warning('unknown method called: %s, args: %r', cmd, args)
        raise CallAbortedException('unknown command %r' % (cmd,))
