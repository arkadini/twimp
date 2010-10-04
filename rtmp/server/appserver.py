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
import time
import struct

from twisted.internet import defer
from twisted.internet import protocol

from rtmp import amf0
from rtmp import chunks
from rtmp import const
from rtmp.crypto.handshake import CryptoHandshaker
from rtmp.primitives import _s_ulong_b as _s_ulong
from rtmp.proto import UserControlDispatchDemuxer
from rtmp.proto import EventDispatchProtocol, EventDispatchFactory
from rtmp.server import errors
from rtmp.utils import ms_time

from rtmp.helpers import vb

LOG_CATEGORY = 'appsrv'
import rtmp.log
log = rtmp.log.get_logger(LOG_CATEGORY)


# user control message with a single long field
_s_uc_single = struct.Struct('>HL')


class NetStream(object):
    def __init__(self, protocol, stream_id):
        self.protocol = protocol
        self.id = stream_id

        self.buffer_length = 100
        self.protocol.route_buffer_messages(self.id, self._set_buffer_length)

    def _set_buffer_length(self, ts, length):
        self.buffer_length = length

    def close(self):
        self.unset_listeners()
        self.protocol.route_buffer_messages(self.id, None)

    def set_listeners(self, data_callback=None, meta_callback=None,
                      mute_callback=None):
        p = self.protocol
        p.route_data_messages(self.id, data_callback)
        p.route_meta_messages(self.id, meta_callback)
        p.route_mute_messages(self.id, mute_callback)

    def unset_listeners(self):
        p = self.protocol
        for method in (p.route_data_messages, p.route_meta_messages,
                       p.route_mute_messages):
            method(self.id, None)

    def send(self, ts, type_, data):
        return self.protocol.muxer.sendMessage(ts, type_, self.id, data)

    def asend(self, ts, type_, *args):
        # AMF-encode args and send using our stream_id
        return self.protocol.muxer.sendMessage(ts, type_, self.id,
                                               self.protocol.encode_amf(*args))

    def call(self, cmd, *args, **kwargs):
        return self.protocol.callRemote(self.id, cmd, *args, **kwargs)

    def signal(self, cmd, *args, **kwargs):
        return self.protocol.signalRemote(self.id, cmd, *args, **kwargs)

    def ctrlStreamBegin(self):
        sm = self.protocol.muxer.sendMessage
        return sm(0, chunks.PROTO_USER_CONTROL, 0,
                  vb(_s_uc_single.pack(const.UCTRL_STREAM_BEGIN, self.id)))

    def ctrlStreamRecorded(self):
        sm = self.protocol.muxer.sendMessage
        return sm(0, chunks.PROTO_USER_CONTROL, 0,
                  vb(_s_uc_single.pack(const.UCTRL_STREAM_RECORDED, self.id)))

    def set_chunk_size(self, new_size):
        sm = self.protocol.muxer.sendMessage
        sm(0, chunks.PROTO_SET_CHUNK_SIZE, 0, vb(_s_ulong.pack(new_size)))
        self.protocol.muxer.set_chunk_size(new_size)


class NetStreamManager(object):
    def __init__(self):
        self._streams = {}
        self._next_id = 1

    def make_stream(self, protocol):
        s_id, self._next_id = self._next_id, self._next_id + 1

        s = NetStream(protocol, s_id)
        self._streams[s_id] = s
        return s

    def del_stream(self, ms_id):
        self._streams.pop(ms_id, None)

    def get_stream(self, ms_id):
        return self._streams.get(ms_id, None)

    def get_streams(self):
        return self._streams.values()


class DelegateSelectedUserControlDemuxer(UserControlDispatchDemuxer):
    def doUserControlBufferLength(self, header, stream_id, length):
        self.protocol.demuxerUCBufferLength(header.abs_time, header.ms_id,
                                            stream_id, length)


class CallResultError(ValueError):
    """Call resulted in an error"""

    level = 'error'
    code = 'NetStream.Failed'   # looks like the most generic error type code

    def __init__(self, *args, **kwargs):
        ValueError.__init__(self, *args)
        self.is_fatal = kwargs.get('fatal', False)

    def get_error_args(self):
        dsc = self.__doc__
        if self.args:
            dsc = '%s: %s' % (dsc, self.args[0])
        return (None, amf0.Object(level=self.level, code=self.code,
                                  description=dsc))


class CallAbortedException(Exception):
    """Call aborted"""


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

CDP = CallDispatchProtocol


class ConnectFailedError(CallResultError):
    """Connection attempt failed"""
    code = 'NetConnection.Connect.Failed'

class InvalidAppError(CallResultError):
    """The specified app is invalid"""
    code = 'NetConnection.Connect.InvalidApp'

class PlayFailed(CallResultError):
    """Failure while attempting to play"""
    code = 'NetStream.Play.Failed'

class PlayNotFound(CallResultError):
    """Requested stream not found"""
    code = 'NetStream.Play.StreamNotFound'


def check_connected(method):
    def wrapped(self, *args, **kwargs):
        if not self._connected:
            self._fail('not connected')
            return

        return method(self, *args, **kwargs)
    wrapped.__name__ == method.__name__
    return wrapped

def check_connected_remote(method):
    def wrapped(self, *args, **kwargs):
        if not self._connected:
            self._fail('not connected')
            raise CallAbortedException('not connected')

        return method(self, *args, **kwargs)
    wrapped.__name__ = method.__name__
    return wrapped

def _harden_connect(method):
    def wrapped(self, *args, **kwargs):
        try:
            return method(self, *args, **kwargs)
        except (CallResultError, CallAbortedException):
            raise
        except Exception, e:
            raise ConnectFailedError(repr(e), fatal=True)
    wrapped.__name__ = method.__name__
    return wrapped


rtmp_to_internal_types = {
    const.RTMP_AUDIO: chunks.MSG_AUDIO,
    const.RTMP_VIDEO: chunks.MSG_VIDEO,
    }

class AppDispatchServerProtocol(CDP):
    handshaker_class = CryptoHandshaker
    demuxer_class = DelegateSelectedUserControlDemuxer

    def __init__(self):
        CDP.__init__(self)
        self._app = None
        self._nsmgr = NetStreamManager()

        self._connected = False

        self._data_routes = {}
        self._meta_routes = {}
        self._mute_routes = {}
        self._buffer_routes = {}

    def handshakeSucceeded(self, init_ts, hs_delay):
        CDP.handshakeSucceeded(self, init_ts, hs_delay)
        self.transport.setTcpNoDelay(1)

    def connectionLost(self, reason=protocol.connectionDone):
        if self._app:           # check self._connected ?
            self._app.connectionLost(reason)
            self._app = None    # ?

        for ns in self._nsmgr.get_streams():
            ns.close()

        CDP.connectionLost(self, reason)

    def _fail(self, msg=None):
        if msg is not None:
            log.info('ADSP._fail: %s', msg)
        self.transport.loseConnection()

    @check_connected_remote
    def unknownRemoteCall(self, cmd, ts, ms_id, args):
        handler_m = getattr(self._app, 'remote_%s' % (cmd,), None)

        # commands callable on stream 0 should be handled in the protocol
        if ms_id == 0 or handler_m is None:
            return CDP.unknownRemoteCall(self, cmd, ts, ms_id, args)

        ns = self._nsmgr.get_stream(ms_id)
        if not ns:
            raise CallResultError('invalid stream %r' % (ms_id,))

        return handler_m(ts, ns, *args)

    @_harden_connect
    def remote_connect(self, ts, ms_id, cmd_obj, *opts):
        app_path = cmd_obj.app
        if app_path is None:
            raise InvalidAppError('no app path given', fatal=True)

        d = None
        app_factory_args = self.factory.get_app_factory(app_path)
        if app_factory_args:
            app_factory, path_args, path_kwargs = app_factory_args
            self._app = app_factory(self, self.factory.server)
            d = defer.maybeDeferred(self._app.connect, cmd_obj, opts,
                                    *path_args, **path_kwargs)
            d.addCallbacks(self._connect_succeeded, self._connect_failed)
        else:
            raise InvalidAppError('app not found', fatal=True)

        return d

    def _connect_succeeded(self, result):
        self._connected = True

        sm = self.muxer.sendMessage
        sm(0, chunks.PROTO_WINDOW_SIZE, 0, vb('002625a0'.decode('hex')))
        sm(0, chunks.PROTO_SET_BANDWIDTH, 0, vb('002625a002'.decode('hex')))
        sm(0, chunks.PROTO_USER_CONTROL, 0, vb('000000000000'.decode('hex')))

        return result

    def _connect_failed(self, failure):
        if not failure.check(CallResultError, CallAbortedException):
            raise ConnectFailedError(repr(failure.value), fatal=True)
        return failure

    @check_connected_remote
    def remote_createStream(self, ts, ms_id, _none):
        sm = self.muxer.sendMessage
        s = self._nsmgr.make_stream(self)
        log.info('created message stream: %r', s.id)

        return None, s.id

    @check_connected_remote
    def remote_play(self, ts, ms_id, *args):
        return self._call_play(ms_id, args)

    @check_connected_remote
    def remote_publish(self, ts, ms_id, *args):
        return self._call_publish(ms_id, args)


    def _call_play(self, ms_id, args):
        ns = self._nsmgr.get_stream(ms_id)
        if not ns:
            raise PlayFailed('invalid stream %r' % (ms_id,))

        def translate_not_found_eb(failure):
            # should this be onStatus or _error? :/
            failure.trap(errors.NotFoundError)
            raise PlayNotFound(str(failure.value))

        def play_cb(result):
            log.debug('play handler done')
            return result

        d = defer.maybeDeferred(self._app.play, ns, *args[1:])
        d.addCallbacks(play_cb, translate_not_found_eb)
        return d

    def _call_publish(self, ms_id, args):
        ns = self._nsmgr.get_stream(ms_id)
        if not ns:
            raise CallResultError('invalid stream %r' % (ms_id,))

        d = defer.maybeDeferred(self._app.publish, ns, *args[1:])
        return d

    def _set_route_messages(self, ms_id, callback, table):
        if callback:
            table[ms_id] = callback
        else:
            table.pop(ms_id, None)

    def route_data_messages(self, ms_id, callback):
        self._set_route_messages(ms_id, callback, self._data_routes)

    def route_meta_messages(self, ms_id, callback):
        self._set_route_messages(ms_id, callback, self._meta_routes)

    def route_mute_messages(self, ms_id, callback):
        self._set_route_messages(ms_id, callback, self._mute_routes)

    def route_buffer_messages(self, ms_id, callback):
        self._set_route_messages(ms_id, callback, self._buffer_routes)

    def doMeta(self, ts, ms_id, args):
        handler = self._meta_routes.get(ms_id)

        if handler:
            handler(ts, args)
        elif not self._connected:
            self._fail('not connected')
            return

    def doData(self, type_, ts, ms_id, body):
        msg_type = rtmp_to_internal_types[type_]
        handler = self._data_routes.get(ms_id)

        if handler:
            handler(ts, msg_type, body)
        elif not self._connected:
            self._fail('not connected')
            return

    @check_connected_remote
    def remote_receiveAudio(self, ts, ms_id, _none, do_receive):
        handler = self._mute_routes.get(ms_id)

        if handler:
            handler(ts, chunks.MSG_AUDIO, do_receive)

    @check_connected_remote
    def remote_receiveVideo(self, ts, ms_id, _none, do_receive):
        handler = self._mute_routes.get(ms_id)

        if handler:
            handler(ts, chunks.MSG_VIDEO, do_receive)

    @check_connected
    def demuxerUCBufferLength(self, ts, ms_id, stream_id, length):
        handler = self._buffer_routes.get(stream_id)

        if handler:
            handler(ts, length)


class AppDispatchServerFactory(EventDispatchFactory):
    protocol = AppDispatchServerProtocol
    server = None

    def __init__(self, init_time, server):
        EventDispatchFactory.__init__(self, init_time)

        self.server = server

    def get_app_factory(self, app_path):
        """Subclass and override me to return app factories according
        to the given app_path.
        """


class URLDispatchingServerFactory(AppDispatchServerFactory):
    def __init__(self, init_time, server, urls):
        AppDispatchServerFactory.__init__(self, init_time, server)

        self.urls = urls

    def get_app_factory(self, app_path):
        for pattern, app_factory in self.urls:
            m = pattern.match(app_path)
            if m:
                # django's way: args xor kwargs, not both
                kwargs = m.groupdict()
                if kwargs:
                    args = ()
                else:
                    args = m.groups()
                return app_factory, args, kwargs

        return None

def make_urls(*urls):
    import re

    return [(re.compile(pattern), factory) for (pattern, factory) in urls]
