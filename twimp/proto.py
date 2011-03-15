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
import struct

from twisted.internet import protocol
from twisted.internet import reactor, defer
from twisted.internet.protocol import Factory
from twisted.python import failure

from twimp import amf0
from twimp import chunks
from twimp import const
from twimp.chunks import Demuxer, Muxer
from twimp.handshake import Handshaker
from twimp.primitives import _s_ulong_b as _s_ulong, _s_double_ulong_b
from twimp.utils import GeneratorWrapperProtocol
from twimp.vecbuf import semiflatten, VecBuf

from twimp.helpers import vb

LOG_CATEGORY = 'proto'
import twimp.log
log = twimp.log.get_logger(LOG_CATEGORY)


# defer.Deferred.debug = 1


# twisted.internet.abstract.FileDescriptor does ''.join() on sequences
# passed to writeSequence(), and that doesn't allow buffer objects -
# so a quick workaround, for now...
def _fix_writeSequence(obj):
    orig_writeSequence = obj.writeSequence
    def writeSequence(seq):
        return orig_writeSequence(semiflatten(seq))
    obj.writeSequence = writeSequence
    return orig_writeSequence


class BaseProtocol(GeneratorWrapperProtocol):
    handshaker_class = Handshaker
    demuxer_class = Demuxer
    muxer_class = Muxer
    is_client = False

    def __init__(self):
        GeneratorWrapperProtocol.__init__(self)

        self.bytes_read = 0
        self.session_init_time = None

        # handshaker gets instantiated on connection
        self._hs = None

        # demuxer and muxer get instantiated on successful handshake
        self._demuxer = None
        self.muxer = None

    def buildHandshaker(self, protocol, init_time, is_client):
        return self.handshaker_class(protocol, init_time, is_client=is_client)

    def buildDemuxer(self, protocol):
        return self.demuxer_class(protocol)

    def buildMuxer(self, transport):
        return self.muxer_class(transport)

    def _init_handshaker(self):
        # note: this may need a connected transport
        self._hs = self.buildHandshaker(self, self.factory.init_time,
                                        self.is_client)
        self.init_handler(self._hs.gen_handler())

    def _init_muxers(self):
        self._demuxer = self.buildDemuxer(self)
        self.muxer = self.buildMuxer(self.transport)
        self.init_handler(self._demuxer.gen_handler())

    def connectionMade(self):
        self.orig_writeSequence = _fix_writeSequence(self.transport)
        # start handshake "mode"
        self._init_handshaker()

    def connectionLost(self, reason=protocol.connectionDone):
        del self.transport.writeSequence
        del self.orig_writeSequence
        GeneratorWrapperProtocol.connectionLost(self, reason)

    def handshakeSucceeded(self, init_ts, hs_delay):
        # switch to chunks "mode"
        self.session_init_time = self.factory.init_time + init_ts
        self._init_muxers()

    def handshakeFailed(self):
        # this is called _directly before_ connectionLost()
        pass

    def messageReceived(self, header, body):
        # print header, _ellip(body.read(len(body)).encode('hex'))
        pass


class BaseFactory(Factory):
    protocol = BaseProtocol

    def __init__(self, init_time):
        self.init_time = init_time


_s_uctrl_single = struct.Struct('>HL')

class UserControlDispatchDemuxer(Demuxer):
    def __init__(self, protocol):
        super(UserControlDispatchDemuxer, self).__init__(protocol)

        # { type => (verify_size, cnv_size, cnv_func, handler_func) }
        self.user_ctrl_handlers = None
        self.build_user_control_dispatch()

    def build_user_control_dispatch(self):
        self.user_ctrl_handlers = {
            const.UCTRL_STREAM_BEGIN:
                (_s_ulong.size, _s_ulong.size, _s_ulong.unpack,
                 self.doUserControlStreamBegin),
            const.UCTRL_STREAM_EOF:
                (_s_ulong.size, _s_ulong.size, _s_ulong.unpack,
                 self.doUserControlStreamEOF),
            const.UCTRL_STREAM_DRY:
                (_s_ulong.size, _s_ulong.size, _s_ulong.unpack,
                 self.doUserControlStreamDry),
            const.UCTRL_BUFFER_LENGTH:
                (_s_double_ulong_b.size, _s_double_ulong_b.size,
                 _s_double_ulong_b.unpack, self.doUserControlBufferLength),
            const.UCTRL_STREAM_RECORDED:
                (_s_ulong.size, _s_ulong.size, _s_ulong.unpack,
                 self.doUserControlStreamRecorded),
            const.UCTRL_PING:
                (_s_ulong.size, _s_ulong.size, _s_ulong.unpack,
                 self.doUserControlPing),
            const.UCTRL_PONG:
                (_s_ulong.size, _s_ulong.size, _s_ulong.unpack,
                 self.doUserControlPong),
            }

    def doUserControlMessage(self, header, evt_type, body):
        handler = self.user_ctrl_handlers.get(evt_type)
        if not handler:
            self.doUserControlUnknownType(self, header, evt_type, body)
            return

        verify_size, cnv_size, cnv_func, handler_func = handler
        if len(body) != verify_size:
            raise ProtocolContractError(('expected user ctrl msg of size: %d, '
                                         'got: %d') % (verify_size,
                                                       len(body)))

        args = (header,) + cnv_func(body.read(cnv_size))
        handler_func(*args)

    # handling pings automatically
    def doUserControlPing(self, header, peer_time):
        sm = self.protocol.muxer.sendMessage
        sm(0, chunks.PROTO_USER_CONTROL, 0,
           vb(_s_uctrl_single.pack(const.UCTRL_PONG, peer_time)))

    # ... implementation of the rest left to the user
    def doUserControlStreamBegin(self, header, stream_id):
        pass

    def doUserControlStreamEOF(self, header, stream_id):
        pass

    def doUserControlStreamDry(self, header, stream_id):
        pass

    def doUserControlStreamRecorded(self, header, stream_id):
        pass

    def doUserControlBufferLength(self, header, stream_id, length):
        pass

    def doUserControlPong(self, header, echo_time):
        pass

    def doUserControlUnknownType(self, header, evt_type, body):
        pass


class CancellableCallQueue(object):
    def __init__(self, reactor=reactor):
        self.reactor = reactor
        self.pending = {}
        self._next_key = 0

    def callLater(self, delay, f, *args, **kw):
        call_key, self._next_key = self._next_key, self._next_key + 1
        clid = self.reactor.callLater(delay, self._call_wrapper, call_key, f,
                                      args, kw)
        self.pending[call_key] = clid
        return call_key, clid

    def cancel(self, (call_key, clid)):
        self.pending.pop(call_key, None)
        return clid.cancel()

    def _call_wrapper(self, call_key, f, args, kw):
        del self.pending[call_key]
        f(*args, **kw)

    def cancel_all(self):
        remaining = self.pending.copy()
        self.pending.clear()

        for key, clid  in remaining.iteritems():
            if clid.active():
                clid.cancel()


class DispatchProtocol(BaseProtocol):
    demuxer_class = UserControlDispatchDemuxer

    def __init__(self):
        BaseProtocol.__init__(self)

        self.msg_dispatch = None

        self.bytes_read = 0 # all bytes received minus headers (?)
        self._next_ack = 0
        self.window_size = 2500000

        self.set_next_ack()

        self.build_message_dispatch()

    def set_window_size(self, new_size):
        if new_size != self.window_size:
            old_size, self.window_size = self.window_size, new_size
            self.set_next_ack(old_size)
            self.check_send_ack(self)

    def set_next_ack(self, old_window_size=0):
        # the actual "algorithm" is here - Adobe's software seems to send
        # acks after _half_ the window size...
        old_ack_inc = int(old_window_size / 2)
        ack_inc = int(self.window_size / 2)
        self._next_ack += ack_inc - old_ack_inc

    def check_send_ack(self):
        if self._next_ack < self.bytes_read:
            self.set_next_ack()
            self.muxer.sendMessage(0, chunks.PROTO_ACK, 0,
                                   vb(_s_ulong.pack(self.bytes_read)))

    def bytes_received(self, count):
        self.bytes_read += count
        self.check_send_ack()

    def build_message_dispatch(self):
        # for now we only support AMF0

        def amf_args(header, body):
            return header.abs_time, header.ms_id, amf0.decode(body)
        def data_args(header, body):
            return header.type, header.abs_time, header.ms_id, body

        # { type => (handler, make_args) }
        self.msg_dispatch = {
            const.RTMP_AUDIO: (self.doData, data_args),
            const.RTMP_VIDEO: (self.doData, data_args),
            const.RTMP_DATA: (self.doMeta, amf_args),
            const.RTMP_COMMAND: (self.doCommand, amf_args),
            }

    def messageReceived(self, header, body):
        entry = self.msg_dispatch.get(header.type, None)
        if not entry:
            self.unknownMessageType(header, body)
            return

        handler, make_args = entry

        args = make_args(header, body)
        handler(*args)

    def unknownMessageType(self, header, body):
        pass

    def doCommand(self, ts, ms_id, args):
        pass

    def doMeta(self, ts, ms_id, args):
        pass

    def doData(self, type_, ts, ms_id, body):
        pass

    def connectionLost(self, reason=protocol.connectionDone):
        self._cc_queue.cancel_all()
        BaseProtocol.connectionLost(self, reason)


class DispatchFactory(BaseFactory):
    protocol = DispatchProtocol


class DeferredTracker(object):
    init_trans_id = 1

    def __init__(self):
        self._pending = {}
        self._next_trans_id = {}

    def next_trans(self, key):
        trans_id = self._next_trans_id.setdefault(key, self.init_trans_id)
        r, trans_id = trans_id, trans_id + 1
        self._next_trans_id[key] = trans_id
        return r

    def push_deferred(self, key, trans_id, d):
        if key not in self._pending:
            self._pending[key] = {}
        key_queue = self._pending[key]
        key_queue[trans_id] = d

    def pop_deferred(self, key, trans_id):
        d = None
        key_queue = self._pending.get(key, None)
        if key_queue:
            d = key_queue.pop(trans_id, None)
        return d

    def iter_all(self):
        return ((key, d)
                for (key, key_queue) in self._pending.iteritems()
                for d in key_queue.itervalues())

    def clear(self):
        self._pending = {}

    def reset(self):
        self._next_trans_id = {}


class CommandResultError(RuntimeError):
    pass

class CommandDispatchProtocol(DispatchProtocol):

    def __init__(self):
        DispatchProtocol.__init__(self)

        self._cc_queue = CancellableCallQueue()
        self._call_tracker = DeferredTracker()

    def doCommand(self, ts, ms_id, args):
        cmd = args[0]

        handler_m = getattr(self, 'command_%s' % (cmd,), None)

        if handler_m is None:
            self._cc_queue.callLater(0, self.unknownCommandType, cmd, ts,
                                     ms_id, args[1:])
        else:
            self._cc_queue.callLater(0, self._handler_wrapper, handler_m,
                                     ts, ms_id, args[1:])

    def _handler_wrapper(self, handler, ts, ms_id, args):
        # wrap in try/except...?
        handler(ts, ms_id, *args)

    def command__result(self, ts, ms_id, trans_id, *args):
        d = self._call_tracker.pop_deferred(ms_id, trans_id)

        if d:
            d.callback(args)
        else:
            self.unexpectedCallResult(ts, ms_id, trans_id, args)

    def command__error(self, ts, ms_id, trans_id, *args):
        d = self._call_tracker.pop_deferred(ms_id, trans_id)

        if d:
            d.errback(failure.Failure(CommandResultError(*args)))
        else:
            self.unexpectedCallError(ts, ms_id, trans_id, args)

    def unknownCommandType(self, cmd, ts, msid, args):
        raise NotImplementedError('unknown command %r%r' % (cmd,
                                                            (ts, msid, args)))

    def unexpectedCallResult(self, ts, ms_id, trans_id, args):
        log.warning('unexpected _result: at %r, stream %r, trans %r, args: %r',
                    ts, ms_id, trans_id, args)

    def unexpectedCallError(self, ts, ms_id, trans_id, args):
        log.warning('unexpected _error: at %r, stream %r, trans %r, args: %r',
                    ts, ms_id, trans_id, args)


    def connectionLost(self, reason=protocol.connectionDone):
        self._cc_queue.cancel_all()
        pending_calls = list(self._call_tracker.iter_all())
        self._call_tracker.clear()
        for ms_id, d in pending_calls:
            d.errback(reason)
        DispatchProtocol.connectionLost(self, reason)

    def encode_amf(self, *args):
        # for now only supporting AMF0
        return amf0.encode(*args)

    def _send_command(self, ts, ms_id, body, track_id):
        # track_id > 0 -> will return a tracking deferred
        d = None

        if track_id:
            d = defer.Deferred()
            self._call_tracker.push_deferred(ms_id, track_id, d)

        self.muxer.sendMessage(0, chunks.MSG_COMMAND, ms_id, body)
        return d

    def _sendRemote(self, ms_id, cmd, args, kwargs, track):
        # ignoring kwargs for now...
        trans_id = 0
        if track:
            trans_id = self._call_tracker.next_trans(ms_id)
        encoded_args = self.encode_amf(cmd, trans_id, *args)

        # hardcoding 0 time, does not seem to matter much...
        return self._send_command(0, ms_id, encoded_args, trans_id)

    def callRemote(self, ms_id, cmd, *args, **kw):
        return self._sendRemote(ms_id, cmd, args, kw, True)

    def signalRemote(self, ms_id, cmd, *args, **kw):
        # similar to callRemote, except we don't expect any results
        return self._sendRemote(ms_id, cmd, args, kw, False)


class CommandDispatchFactory(DispatchFactory):
    protocol = CommandDispatchProtocol


class ProtocolContractError(ValueError):
    pass

class UnexpectedStatusError(ValueError):
    pass

class EventDispatchProtocol(CommandDispatchProtocol):
    def __init__(self):
        CommandDispatchProtocol.__init__(self)

        # { ms_id => deque([(code, Deferred), ...]) }
        self._event_callbacks = {}

    def _add_status_deferred(self, ms_id, code, d):
        q = self._event_callbacks.get(ms_id, None)
        if q is None:
            q = self._event_callbacks[ms_id] = deque()
        q.append((code, d))

    def _pop_status_deferred(self, ms_id):
        code, d = None, None
        q = self._event_callbacks.get(ms_id, None)
        if q:
            code, d = q.popleft()
        return code, d

    def _pop_status_all(self):
        ret = ((code, d)
               for q in self._event_callbacks.itervalues()
               for (code, d) in q)
        self._event_callbacks = {}
        return ret

    def waitStatus(self, ms_id, code):
        d = defer.Deferred()
        self._add_status_deferred(ms_id, code, d)
        return d

    def command_onStatus(self, ts, ms_id, _trans_id, _none, info):
        # trans_id not used, and _none seems to always be None...
        code, d = self._pop_status_deferred(ms_id)

        if not d:
            self.unhandledOnStatus(ts, ms_id, info)
        else:
            try:
                evt_code = info.code
            except AttributeError, e:
                d.errback(ProtocolContractError(e))
            else:
                if code is None or evt_code == code:
                    d.callback(info)
                else:
                    d.errback(UnexpectedStatusError(info))

    def unhandledOnStatus(self, ts, ms_id, info):
        log.warning('unhandled onStatus: at %r, stream %r, info: %r',
                    ts, ms_id, info)

    def connectionLost(self, reason=protocol.connectionDone):
        for _code, d in self._pop_status_all():
            d.errback(reason)

        CommandDispatchProtocol.connectionLost(self, reason)


class EventDispatchFactory(CommandDispatchFactory):
    protocol = EventDispatchProtocol
