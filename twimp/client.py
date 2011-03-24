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


import time

from twisted.internet import protocol
from twisted.internet.protocol import ClientFactory
from twisted.internet import reactor

from twimp import amf0
from twimp import chunks
from twimp.primitives import _s_ulong_b as _s_ulong
from twimp.dispatch import EventDispatchProtocol, EventDispatchFactory
from twimp.urls import parse_rtmp_url

from twimp.helpers import vb, ignore_disconnect_eb

LOG_CATEGORY = 'client'
import twimp.log
log = twimp.log.get_logger(LOG_CATEGORY)


CLIENT_VERSION = 'TwiMMF/1.0 (compatible; FMSc/1.0)'


class BaseClientProtocol(EventDispatchProtocol):
    is_client = True


class BaseClientFactory(EventDispatchFactory, ClientFactory):
    protocol = BaseClientProtocol



# connection failures:
# before tcp connected:
#   -> factory.clientConnectionFailed()
# after tcp connected, before handshake completed:
#   -> factory.clientConnectionLost(HandshakeFailedError())
# after handshake completed, before server's "ok connected":
#   -> app.connectionFailed()
#   -> factory.clientConnectionLost()
# after fully connected:
#   -> app.connectionLost()
#   -> factory.clientConnectionLost()


class SimpleAppClientProtocol(BaseClientProtocol):
    def __init__(self):
        BaseClientProtocol.__init__(self)

        self._app = None

    def handshakeSucceeded(self, init_ts, hs_delay):
        BaseClientProtocol.handshakeSucceeded(self, init_ts, hs_delay)
        self._init_connect()

    def make_connect_params(self, **kw):
        app_params = self._app.get_connect_params()
        factory_params = self.factory.get_connect_params()

        params = amf0.Object(app_params)

        for k, v in factory_params.items():
            setattr(params, k, v)

        for k, v in kw.items():
            setattr(params, k, v)

        return params

    def _init_connect(self):
        # why change chunk size? :/
        sm = self.muxer.sendMessage
        sm(0, chunks.PROTO_SET_CHUNK_SIZE, 0, vb('00000400'.decode('hex')))
        self.muxer.set_chunk_size(0x0400)

        app_path = self.factory.app_path
        app_url = self.factory.url

        self._app = self.factory.make_app(self)

        def _connected(info):
            self._app.makeConnection(info)

        def _connect_failed(failure):
            # this errback might have been fired because of connection
            # failure, which we handle elsewhere...
            if self._app:
                self._app.failConnection(failure)

        params = self.make_connect_params(app=app_path,
                                          # flashVer='LNX 10,0,22,87',
                                          flashVer=CLIENT_VERSION,
                                          tcUrl=app_url,
                                          objectEncoding=0)
        d = self.callRemote(0, 'connect', params, {})
        # TODO: wrap actual server responses to failures where appropriate
        d.addCallbacks(_connected, _connect_failed)

    def connectionLost(self, reason=protocol.connectionDone):
        if self._app:
            if self._app.connected:
                self._app.breakConnection(reason)
            else:
                self._app.failConnection(reason)
            self._app = None

        BaseClientProtocol.connectionLost(self, reason)

class SimpleAppClientFactory(BaseClientFactory):
    protocol = SimpleAppClientProtocol

    def __init__(self, url, connect_params,
                 app_factory, *app_args, **app_kwargs):
        BaseClientFactory.__init__(self, time.time())

        self.url = None
        self.app_path = None

        if connect_params is None:
            connect_params = {}
        self._connect_params = connect_params

        self._app_factory = app_factory
        self._app_args = app_args
        self._app_kwargs = app_kwargs

        self._parse_url(url)

    def _parse_url(self, url):
        scheme, host, port, path = parse_rtmp_url(url)
        self.url = url
        self.app_path = path

    def get_connect_params(self):
        return self._connect_params.copy()

    def make_app(self, protocol):
        return self._app_factory(protocol, *self._app_args, **self._app_kwargs)


class InvalidStreamState(ValueError):
    pass

(STREAM_STATE_PUBLISHING, STREAM_STATE_PLAYING) = range(1, 3)

class ClientStream(object):
    def __init__(self, protocol, ms_id):
        self.protocol = protocol
        self.id = ms_id

        self._source = None
        self._sink = None

        self._state = None
        self.closed = False

    def publish(self, name, source, mode='live', chunk_size=None):
        if self._state is not None:
            raise InvalidStreamState('requested publish in state %d' %
                                     self._state)
        self._state = STREAM_STATE_PUBLISHING

        # mode ignored for now...
        mode = 'live'
        d = self.protocol.waitStatus(self.id, 'NetStream.Publish.Start')
        self.protocol.signalRemote(self.id, 'publish', None, name, mode)

        d.addCallbacks(self._start_writing, self._publish_failed,
                       callbackArgs=(source, chunk_size))
        return d

    def _publish_failed(self, failure):
        self._state = None
        log.info('publish failed: %r', failure)
        return failure

    def _start_writing(self, _info, source, chunk_size):
        if chunk_size:
            self.set_chunk_size(chunk_size)

        self._source = source
        source.connect(self)
        source.start()

    def set_chunk_size(self, new_size):
        sm = self.protocol.muxer.sendMessage
        sm(0, chunks.PROTO_SET_CHUNK_SIZE, 0, vb(_s_ulong.pack(new_size)))
        self.protocol.muxer.set_chunk_size(new_size)

    def play(self, receiver):
        raise NotImplementedError()

    def _failed_unpublishing(self, failure):
        log.info('failed unpublishing - Huh?!')
        return failure

    def stop_publishing(self):
        if self._state != STREAM_STATE_PUBLISHING:
            raise InvalidStreamState('requested stop publish in state %d' %
                                     self._state)
        self._source.stop()
        self._source.disconnect()
        self._source = None

        self._state = None

        d = self.protocol.waitStatus(self.id, 'NetStream.Unpublish.Success')
        self.protocol.signalRemote(self.id, 'closeStream', None)

        d.addErrback(ignore_disconnect_eb)
        d.addErrback(self._failed_unpublishing)
        return d

    def _force_cleanup(self):
        if self._source:
            self._source.stop()
            self._source.disconnect()
            self._source = None
        if self._sink:
            self._sink.stop()
            self._sink.disconnect()
            self._sink = None
        self._state = None

    def close(self, force=False):
        if force:
            self._force_cleanup()

        if self._state is not None:
            raise InvalidStreamState('cannot close in state %d' %
                                     self._state)
        self.protocol = None
        self.closed = True

    def write_meta(self, ts, data):
        self.protocol.muxer.sendMessage(ts, chunks.MSG_DATA, self.id, data)

    def write_audio(self, ts, data):
        self.protocol.muxer.sendMessage(ts, chunks.MSG_AUDIO, self.id, data)

    def write_video(self, ts, data):
        self.protocol.muxer.sendMessage(ts, chunks.MSG_VIDEO, self.id, data)


class BaseClientApp(object):
    def __init__(self, protocol):
        self.protocol = protocol
        self.connected = False

    def makeConnection(self, info):
        self.connected = True
        self.connectionMade(info)

    def breakConnection(self, reason):
        self.connected = False
        self.connectionLost(reason)

    def failConnection(self, reason):
        # this should only be called on instantiated but not connected apps
        # self.connected = False
        self.connectionFailed(reason)


    ##
    # utility methods

    def disconnect(self):
        self.protocol.transport.loseConnection()

    def createStream(self, stream_factory=ClientStream):
        # keep track of created streams so we can close them later?...
        def _stream_created((_none, ms_id)):
            return stream_factory(self.protocol, int(ms_id))

        d = self.protocol.callRemote(0, 'createStream', None)
        d.addCallback(_stream_created)

        return d

    def closeStream(self, stream, force=False):
        if not stream.closed:
            stream.close(force)
        self.protocol.signalRemote(0, 'deleteStream', None, stream.id)
        # lose track of the stream if tracking...?


    ##
    # public, overridable app interface

    def connectionMade(self, info):
        pass

    def get_connect_params(self):
        return {}

    def connectionLost(self, reason):
        pass

    def connectionFailed(self, reason):
        pass


def connect_client_factory(url, factoryFactory, *args, **kwargs):
    connect_params = dict(fpad=False) # no proxy, direct tcp/rtmp connection
    scheme, host, port, app = parse_rtmp_url(url)

    factory = factoryFactory(url, connect_params, *args, **kwargs)

    reactor.connectTCP(host, port, factory)
    return factory


def simple_connect(url, *args, **kwargs):
    return connect_client_factory(url, SimpleAppClientFactory,
                                  *args, **kwargs)
