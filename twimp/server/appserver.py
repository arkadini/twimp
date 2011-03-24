#   Copyright (c) 2010, 2011  Arek Korbik
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


import struct

from twisted.internet import defer
from twisted.internet import protocol

from twimp import chunks
from twimp import const
from twimp.crypto.handshake import CryptoHandshaker
from twimp.dispatch import  EventDispatchFactory, CallDispatchProtocol
from twimp.error import CallResultError, CallAbortedException
from twimp.error import ConnectFailedError, InvalidAppError
from twimp.error import PlayFailed, PlayNotFound
from twimp.primitives import _s_ulong_b as _s_ulong
from twimp.proto import UserControlDispatchDemuxer
from twimp.server import errors

from twimp.helpers import vb

LOG_CATEGORY = 'appsrv'
import twimp.log
log = twimp.log.get_logger(LOG_CATEGORY)


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


def check_connected(method):
    def wrapped(self, *args, **kwargs):
        if not self._connected:
            self._fail('not connected')
            return

        return method(self, *args, **kwargs)
    wrapped.__name__ = method.__name__
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

CDP = CallDispatchProtocol

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
