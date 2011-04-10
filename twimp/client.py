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


import logging
import time

from twisted.internet import protocol
from twisted.internet.protocol import ClientFactory
from twisted.internet import reactor, defer
from twisted.python.failure import Failure

from twimp import amf0
from twimp import chunks
from twimp.error import UnexpectedStatusError, CommandResultError
from twimp.error import ClientConnectError
from twimp.primitives import _s_ulong_b as _s_ulong
from twimp.dispatch import CallDispatchProtocol, CallDispatchFactory
from twimp.urls import parse_rtmp_url

from twimp.helpers import vb, ignore_disconnect_eb

LOG_CATEGORY = 'client'
import twimp.log
log = twimp.log.get_logger(LOG_CATEGORY)


CLIENT_VERSION = 'TwiMMF/1.0 (compatible; FMSc/1.0)'


class BaseClientProtocol(CallDispatchProtocol):
    is_client = True


class BaseClientFactory(CallDispatchFactory, ClientFactory):
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

    def _connect_call_failed(self, failure):
        # this errback might have been fired because of connection
        # failure, which we handle elsewhere...
        log.debug('_connect_call_failed: %r', failure.value)
        if self._app:
            self._app.failConnection(failure)
            self._app = None
        return failure

    def _failed_disconnect(self, failure):
        log.info("Failed 'connecting': %r", failure.value)
        self.transport.loseConnection()

    def _init_connect(self):
        # why change chunk size? :/
        sm = self.muxer.sendMessage
        sm(0, chunks.PROTO_SET_CHUNK_SIZE, 0, vb('00000400'.decode('hex')))
        self.muxer.set_chunk_size(0x0400)

        app_path = self.factory.get_connect_app_path()
        app_url = self.factory.get_connect_url()

        self._app = self.factory.make_app(self)

        def _connected(info):
            log.debug('_connected: %r', info)
            self._app.makeConnection(info)

        def _translate_failure(failure):
            failure.trap(CommandResultError)
            # for the moment we know we tried to connect, hence ConnectError
            # TODO: add a more fine-grained translation(?)
            return Failure(ClientConnectError(*failure.value.args))

        params = self.make_connect_params(app=app_path,
                                          # flashVer='LNX 10,0,22,87',
                                          flashVer=CLIENT_VERSION,
                                          tcUrl=app_url,
                                          objectEncoding=0)
        log.debug('invoking connect(%r)', params)
        d = self.callRemote(0, 'connect', params, {})
        d.addErrback(_translate_failure)
        d.addCallbacks(_connected, self._connect_call_failed)
        d.addErrback(self._failed_disconnect)

    def connectionLost(self, reason=protocol.connectionDone):
        if self._app:
            if self._app.connected:
                self._app.breakConnection(reason)
            else:
                self._app.failConnection(reason)
            self._app = None

        BaseClientProtocol.connectionLost(self, reason)

    ##
    # implementation of some common methods servers might want to call
    #

    def command_close(self, ts, ms_id, trans_id, *args):
        log.info('Closing connection on server request.')
        self.transport.loseConnection()

    def remote_onBWCheck(self, ts, ms_id, _none, probe):
        log.debug('Bandwidth check probe (size: %r)', len(probe))
        return 0

    def command_onBWDone(self, ts, ms_id, trans_id, _none,
                         bw, _ignore1=None, _ignore2=None, latency=None):
        log.info('Bandwidth check done (estimated bandwidth: %r, latency: %r)',
                 bw, latency)

    ##
    # FCPublish/FCUnpublish and complementary onFCPublish/onFCUnpublish
    # helpers and handlers
    #

    def call_FCPublish(self, name, ok_code='NetStream.Publish.Start'):
        d = self._events.wait('onFCPublish', ok_code)
        self.signalRemote(0, 'FCPublish', None, name)
        return d

    def command_onFCPublish(self, ts, ms_id, _trans_id, _none, info):
        def miss_handler():
            log.warning('unhandled onFCPublish: at %r, stream %r, info: %r',
                        ts, ms_id, info)
        self._events.dispatch('onFCPublish', info, miss_h=miss_handler)

    def call_FCUnpublish(self, name, ok_code='NetStream.Unpublish.Success'):
        d = self._events.wait('onFCUnpublish', ok_code)
        self.signalRemote(0, 'FCUnpublish', None, name)
        return d

    def command_onFCUnpublish(self, ts, ms_id, _trans_id, _none, info):
        def miss_handler():
            log.warning('unhandled onFCUnpublish: at %r, stream %r, info: %r',
                        ts, ms_id, info)
        self._events.dispatch('onFCUnpublish', info, miss_h=miss_handler)


class SimpleAppClientFactory(BaseClientFactory):
    protocol = SimpleAppClientProtocol

    def __init__(self, url, connect_params,
                 app_factory, *app_args, **app_kwargs):
        BaseClientFactory.__init__(self, time.time())

        self._url = None
        self._app_path = None

        if connect_params is None:
            connect_params = {}
        self._connect_params = connect_params

        self._app_factory = app_factory
        self._app_args = app_args
        self._app_kwargs = app_kwargs

        self._parse_url(url)

    def _parse_url(self, url):
        scheme, host, port, path = parse_rtmp_url(url)
        self._url = url
        self._app_path = path

    def get_connect_params(self):
        return self._connect_params.copy()

    def get_connect_app_path(self):
        return self._app_path

    def get_connect_url(self):
        return self._url

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
        self._fcpublished = None
        self.closed = False

    def publish(self, name, source, mode='live', chunk_size=None,
                fcpublish='no'):
        if self._state is not None:
            raise InvalidStreamState('requested publish in state %r' %
                                     (self._state,))
        self._state = STREAM_STATE_PUBLISHING

        def do_publish(_result):
            # mode ignored for now...
            mode = 'live'
            d = self.protocol.waitStatus(self.id, 'NetStream.Publish.Start')
            self.protocol.signalRemote(self.id, 'publish', None, name, mode)

            d.addCallbacks(self._start_writing, self._publish_failed,
                           callbackArgs=(source, chunk_size))
            return d

        if fcpublish != 'no':
            self._fcpublished = None
            d = self.protocol.call_FCPublish(name)
            d.addCallback(self._fcpublish_ok, name)
            if fcpublish == 'force':
                d.addErrback(self._fcpublish_retry, name)

            d.addErrback(self._fcpublish_failed)
        else:
            d = defer.succeed(None)

        d.addCallback(do_publish)

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

    def _fcpublish_failed(self, failure):
        self._state = None
        log.info('FCPublish failed: %r', failure.value,
                 exc_info=log.isEnabledFor(logging.DEBUG))
        return failure

    def _fcpublish_retry(self, failure, name):
        log.debug('FCPublish failed, %r', failure.value)
        if failure.check(UnexpectedStatusError):
            if failure.value.args[0].code == 'NetStream.Publish.BadName':
                log.debug('releasing stream and re-trying FCPublish...')
                self.protocol.signalRemote(0, 'releaseStream', None, name)
                d = self.protocol.call_FCPublish(name)
                # note: not re-trying on subsequent failures;
                # assuming _fcpublish_failed is attached after us:
                d.addCallback(self._fcpublish_ok, name)
                return d

        return failure

    def _fcpublish_ok(self, result, name):
        self._fcpublished = name
        log.debug('FCPublish succeeded: %r', result)

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
            raise InvalidStreamState('requested stop publish in state %r' %
                                     (self._state,))

        def log_failure(failure):
            # log the failure and consume it
            log.info(failure.value, exc_info=log.isEnabledFor(logging.DEBUG))
            return None

        def do_stop(_result):
            self._source.stop()
            self._source.disconnect()
            self._source = None

            self._state = None

        def do_unpublish(_result):
            d = self.protocol.waitStatus(self.id, 'NetStream.Unpublish.Success')
            self.protocol.signalRemote(self.id, 'closeStream', None)

            d.addErrback(ignore_disconnect_eb)
            d.addErrback(self._failed_unpublishing)
            return d

        if self._fcpublished is not None:
            d = self.protocol.call_FCUnpublish(self._fcpublished)
            self._fcpublished = None
            d.addCallbacks(self._fcunpublish_ok, self._fcunpublish_failed)
            d.addErrback(log_failure) # log and forget any errors
        else:
            d = defer.succeed(None)

        d.addCallback(do_stop)
        d.addCallback(do_unpublish)

        return d

    def _fcunpublish_ok(self, result):
        # nothing to do
        return result

    def _fcunpublish_failed(self, failure):
        # undocumented possibility - just return the failure so it can
        # be later logged and ignored
        return failure

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
            raise InvalidStreamState('cannot close in state %r' %
                                     (self._state,))
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
