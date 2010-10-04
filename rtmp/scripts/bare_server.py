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


import time

from twisted.internet import protocol
from twisted.internet.protocol import Protocol, Factory
# from twisted.internet import epollreactor
# epollreactor.install()
from twisted.internet import reactor

from rtmp import chunks
from rtmp.chunks import Demuxer, Muxer
from rtmp.handshake import Handshaker
from rtmp.utils import GeneratorWrapperProtocol
from rtmp.vecbuf import semiflatten, VecBuf


def _fix_writeSequence(obj):
    orig_writeSequence = obj.writeSequence
    def writeSequence(seq):
        return orig_writeSequence(semiflatten(seq))
    obj.writeSequence = writeSequence
    return orig_writeSequence

def _ellip(s, maxlen=130):
    import math
    def _d10(n):
        return int(math.log10(n)) + 1
    l = len(s)
    if l > maxlen:
        ellipsis = ' ...(%d)... '
        to_hide = l - maxlen + len(ellipsis) - 2
        digits_to_hide = _d10(to_hide)
        digits_to_hide = _d10(to_hide + digits_to_hide)
        ellipsis = ellipsis % (to_hide + digits_to_hide)
        return s[:(l - to_hide - len(ellipsis) - 4)] + ellipsis + s[-4:]

class VerboseDemuxer(Demuxer):
    def controlMessageReceived(self, header, body):
        print header, _ellip(body.peek(len(body)).encode('hex'))
        Demuxer.controlMessageReceived(self, header, body)


class RTMPBaseServerProtocol(GeneratorWrapperProtocol):
    def __init__(self, *a, **kw):
        GeneratorWrapperProtocol.__init__(self, *a, **kw)

        now = time.time()
        self._hs = Handshaker(self, now)

        # muxer and demuxer get instantiated on successful handshake
        self._demux = None
        self._mux = None

        self.bytes_read = 0

        # start in handshake "mode"
        self.init_handler(self._hs.gen_handler())

    def _init_chunks(self):
        self._demux = VerboseDemuxer(self)
        self._mux = Muxer(self.transport)

    def connectionMade(self):
        self.orig_writeSequence = _fix_writeSequence(self.transport)

    def connectionLost(self, reason=protocol.connectionDone):
        print 'Bye, bye...', reason
        del self.transport.writeSequence
        del self.orig_writeSequence
        GeneratorWrapperProtocol.connectionLost(self, reason)

    def handshakeSucceeded(self, init_ts, hs_delay):
        print 'hands shaken:', init_ts, hs_delay

        # switch to chunks "mode"
        self._init_chunks()
        self.init_handler(self._demux.gen_handler())

    def handshakeFailed(self):
        print 'no hands, no shake :('

    def messageReceived(self, header, body):
        print header, _ellip(body.read(len(body)).encode('hex'))


from flvlib import astypes
from cStringIO import StringIO
def decode_amf(vecbuf):
    end = len(vecbuf)
    s = StringIO(vecbuf.read(end))
    args = []

    while s.tell() < end:
        try:
            args.append(astypes.get_script_data_value(s))
        except Exception, e:
            print e
            break
    return args

def encode_amf(*args):
    # s = StringIO()
    s = VecBuf()
    for a in args:
        s.write(astypes.make_script_data_value(a))
    return s
    # return s.getvalue()

from rtmp.primitives import _s_ulong_b
class DispatchProtocol(RTMPBaseServerProtocol):
    def __init__(self, *a, **kw):
        RTMPBaseServerProtocol.__init__(self, *a, **kw)
        self.msg_handlers = {
            0x14: 'command',
            0x12: 'data',
            # 0x13: SO,
            0x08: 'audio',
            0x09: 'video',
            # 0x16: aggregate,
            }

        self.bytes_read = 0
        self._next_ack = 2500000

    def messageReceived(self, header, body):
        msg_type = header.type
        # def decode_amf(data):
        #     return data

        self.bytes_read += len(body)
        if self._next_ack < self.bytes_read:
            self._next_ack += 2500000
            print '[ack: %08x]' % self.bytes_read
            self._mux.sendMessage(0, chunks.PROTO_ACK, 0,
                                  v(_s_ulong_b.pack(self.bytes_read)))
        if msg_type == 0x08:
            self.doAudio(header.abs_time, header.ms_id, body)
        elif msg_type == 0x09:
            self.doVideo(header.abs_time, header.ms_id, body)
        elif msg_type == 0x14:
            self.doCommand(header.abs_time, header.ms_id, decode_amf(body))
        elif msg_type == 0x12:
            self.doData(header.abs_time, header.ms_id, decode_amf(body))
        else:
            self.unknownMessageType(header, body)

    def unknownMessageType(self, header, body):
        print ('do not know what to do:', header,
               _ellip(body.read(len(body)).encode('hex')))

    def doCommand(self, ts, msid, args):
        print 'doCommand', ts, msid, ':'
        print [(x[:] if isinstance(x, buffer) else x)
               for x in args]

    def doData(self, ts, msid, args):
        print 'doData', ts, msid, [(x[:] if isinstance(x, buffer) else x)
                                   for x in args]

    def doAudio(self, ts, msid, body):
        print 'doAudio', ts, msid, len(body)

    def doVideo(self, ts, msid, body):
        print 'doVideo', ts, msid, len(body)

class CallDispatchProtocol(DispatchProtocol):
    def doCommand(self, ts, msid, args):
        cmd = args[0]
        if cmd == 'connect':
            self.call_connect(ts, msid, *args[1:])
        elif cmd == 'createStream':
            self.call_createStream(ts, msid, *args[1:])
        elif cmd == 'call':
            self.call_call(ts, msid, *args[1:])
        else:
            self.unknownCommandType(cmd, ts, msid, args[1:])

    def call_connect(self, ts, msid, trans_id, cmd_obj, *opts):
        print 'connect:', ts, msid, trans_id, cmd_obj, opts

    def call_call(self, ts, msid, trans_id, cmd_obj, *opts):
        print 'call:', ts, msid, trans_id, cmd_obj, opts

    def call_createStream(self, ts, msid, trans_id, cmd_obj, *opts):
        print 'createStream:', ts, msid, trans_id, cmd_obj, opts

    def unknownCommandType(self, cmd, ts, msid, args):
        raise NotImplementedError('unknown command %r%r' % (cmd,
                                                            (ts, msid, args)))


class BareServer(CallDispatchProtocol):
    pass


class RTMPApp(object):
    pass

ERROR, OK = -1, 0

class SimplePublishPlayApp(RTMPApp):
    def connect(self, request, req_opts):
        print 'SimplePublishPlayApp.connect(%r, %r)' % (request, req_opts)
        print 'Yay!!!'

    # all "call"s are ignored (w/ MethodNotFound ?)

    def play(self, net_stream, stream_name, start=-2, duration=-1,
             reset=True):
        assert start == -2, 'only live streams'

        # ignoring duration and reset for now...

        s = self.server.find_stream(stream_name)

        if not s:
            # supposedly FMS supports pre-subscribing to non-existing
            # streams...?
            print 'stream %r not found' % (stream_name,)
            return ERROR        # FIXME: proper error

        c = net_stream.get_controller()

        if c:
            c.reset()
            c.play(s)
        else:
            c = Player(s)
            # c.connect(net_stream)
            net_stream.set_controller(c)

        return OK

    def publish(self, net_stream, stream_name, publish_type):
        publish_type = 'live'
        if publish_type != 'live':
            return ERROR  # this app is only for live streams, figure
                          # out the right error to return

        if net_stream.get_controller():
            return ERROR        # shouldn't happen (?)

        # normalize stream_name somehow? :/

        # let's assume we can "cancel" a live stream being
        # captured/relayed, so if there's an active stream called
        # stream_name already, we cancel it first...
        s = self.server.find_stream(stream_name)
        if s:
            s.cancel()          # ...? :/

        s = self.server.make_stream(stream_name) # ...

        r = Recorder(s)

        # r.connect(net_stream)
        net_stream.set_controller(r)

        return OK

    # not supported a.t.m.:
    # def play2(self, ...):
    #     pass

    # (net) stream -> Player/Recorder -> (media) stream(s)

    # to route to particular streams:
    #  * seek
    #  * pause
    #  * receiveAudio
    #  * receiveVideo
    #  * deleteStream

import re
urls = [
    (re.compile(r''), SimplePublishPlayApp)
]

def v(s):
    return VecBuf([s])

class NetStream(object):
    def __init__(self, stream_id, protocol):
        self.id = stream_id
        self.protocol = protocol
        self._ctrl = None

    def get_controller(self):
        return self._ctrl

    def set_controller(self, ctrl):
        if ctrl:
            ctrl.on_controller_set(self)
            self.protocol.route_data_messages(self.id, self.on_data)
        else:
            if self._ctrl:
                self._ctrl.on_controller_set(None)
            self.protocol.route_data_messages(self.id, None)
        self._ctrl = ctrl

    def on_data(self, ts, type_, data):
        self._ctrl.on_data(self, ts, type_, data)

class NetStreamManager(object):
    def __init__(self):
        self._streams = {}
        self._next_id = 1

    def make_stream(self, protocol):
        s_id = self._next_id
        self._next_id += 1
        s = NetStream(s_id, protocol)
        self._streams[s_id] = s
        return s

    def get_stream(self, stream_id):
        return self._streams.get(stream_id, None)

    def get_streams(self):
        return self._streams.values()

class MediaStream(object):
    def __init__(self, stream_name):
        self.name = stream_name
        self._data = {}
        self._all_data = []
        self._listeners = set()

    def cancel(self):
        pass

    def add_data(self, ts, type_, data):
        self._data.setdefault(type_, []).append((ts, data))
        self._all_data.append((ts, type_, data))

        self.on_data_added(ts, type_, data)

    def add_listener(self, listener):
        self._listeners.add(listener)

    def del_listener(self, listener):
        self._listeners.discard(listener)

    def on_data_added(self, ts, type_, data):
        for l in self._listeners:
            l(self, ts, type_, data)

class Controller(object):
    def __init__(self, mstream):
        self._mstream = mstream

    def on_controller_set(self, nstream):
        # self._nstream = nstream
        # if nstream:
        pass

def clone(vb):
    return VecBuf(vb.peek_seq(len(vb)))

class Player(Controller):
    def __init__(self, mstream):
        Controller.__init__(self, mstream)
        self._nstream = None

    def on_controller_set(self, nstream):
        if nstream:
            self._mstream.add_listener(self.on_data_added)
        else:
            self._mstream.del_listener(self.on_data_added)
        self._nstream = nstream

    def on_data_added(self, mstream, ts, type_, data):
        print '--> data:', ts, type_, len(data), self._nstream.protocol
        self._nstream.protocol._mux.sendMessage(ts, type_, self._nstream.id,
                                                clone(data))

class Recorder(Controller):
    def on_data(self, nstream, ts, type_, data):
        self._mstream.add_data(ts, type_, data)

class DummyServer(object):
    def __init__(self):
        self._streams = {}

    def find_stream(self, stream_name):
        return self._streams.get(stream_name)

    def make_stream(self, stream_name):
        s = MediaStream(stream_name)
        self._streams[stream_name] = s
        return s
_the_server = DummyServer()

class URLDispatchingServer(CallDispatchProtocol):
    def __init__(self):
        CallDispatchProtocol.__init__(self)
        self._urls = urls
        self._app = None
        self._nstream_mgr = NetStreamManager()
        self._routes = {}

    def call_connect(self, ts, msid, trans_id, cmd_obj, *opts):
        app_path = cmd_obj.app
        if app_path is None:
            self.transport.loseConnection()
            return

        for p, h in self._urls:
            if p.match(app_path):
                self._app = h()
                self._app.server = _the_server
                conn_ret = self._app.connect(cmd_obj, {})
                self._connect_done(trans_id, cmd_obj)
                return

        self._fail('no app found for %r' % (app_path,))

    def connectionLost(self, reason=protocol.connectionDone):
        for ns in self._nstream_mgr.get_streams():
            ns.set_controller(None)

    def _fail(self, msg=None):
        if msg is not None:
            print msg
        self.transport.loseConnection()

    def _connect_done(self, trans_id, cmd_obj):
        sm = self._mux.sendMessage
        sm(0, chunks.PROTO_WINDOW_SIZE, 0, v('002625a0'.decode('hex')))
        sm(0, chunks.PROTO_SET_BANDWIDTH, 0, v('002625a002'.decode('hex')))
        sm(0, chunks.PROTO_USER_CONTROL, 0, v('000000000000'.decode('hex')))
        sm(0, chunks.MSG_COMMAND, 0,
           encode_amf('_result', trans_id, {'capabilities': 31.0},
                      {'code': 'NetConnection.Connect.Success',
                       'description': 'Connection succeeded.',
                       'level': 'status',
                       'objectEncoding': 0.0}))
        sm(0, chunks.PROTO_SET_CHUNK_SIZE, 0, v('00001000'.decode('hex')))
        self._mux.set_chunk_size(0x1000)


    def call_createStream(self, ts, msid, trans_id, cmd_obj, *opts):
        if not self._app:
            self._fail('not connected')
            return

        sm = self._mux.sendMessage
        s = self._nstream_mgr.make_stream(self)
        print 'createStream =', s.id

        sm(0, chunks.MSG_COMMAND, 0,
           encode_amf('_result', trans_id, None, s.id))

    def unknownCommandType(self, cmd, ts, msid, args):
        if not self._app:
            self._fail('not connected')
            return

        if cmd == 'play':
            self._call_play(msid, args)
        elif cmd == 'publish':
            self._call_publish(msid, args)
        else:
            sm = self._mux.sendMessage
            trans_id = args[0]
            sm(0, chunks.MSG_COMMAND, 0,
               encode_amf('_error', trans_id, None,
                          {'code': 'NetStream.Failed',
                           'description': 'unknown command type %r.' % cmd,
                           'level': 'error'}))
            print '[unknownCommandType %s%r]' % (cmd, tuple(args))
            # CallDispatchProtocol.unknownCommandType(self, cmd, ts, msid, args)

    def _call_play(self, msid, args):
        ns = self._nstream_mgr.get_stream(msid)
        if ns is None:
            self._fail('not existing stream %d' % (msid,))
            return

        print 'trying to play:', args[2:]
        err = self._app.play(ns, *args[2:])
        if err:
            self._fail('no play :(')
            return

        print 'hmm, okay, let us play...'

    def _call_publish(self, msid, args):
        ns = self._nstream_mgr.get_stream(msid)
        if ns is None:
            self._fail('not existing stream %d' % (msid,))
            return

        print 'trying to publish:', args[2:]
        err = self._app.publish(ns, *args[2:])
        if err:
            self._fail('no publish :(')
            return

        print 'hmm, okay, let us publish...'

    def route_data_messages(self, ms_id, callback):
        if callback:
            self._routes[ms_id] = callback
        else:
            self._routes.pop(ms_id, None)

    def doData(self, ts, ms_id, args):
        CallDispatchProtocol.doData(self, ts, ms_id, args)
        h = self._routes.get(ms_id)
        if h:
            h(ts, chunks.MSG_DATA, encode_amf(args))

    def doAudio(self, ts, ms_id, body):
        CallDispatchProtocol.doAudio(self, ts, ms_id, body)
        h = self._routes.get(ms_id)
        if h:
            h(ts, chunks.MSG_AUDIO, body)

    def doVideo(self, ts, ms_id, body):
        CallDispatchProtocol.doVideo(self, ts, ms_id, body)
        h = self._routes.get(ms_id)
        if h:
            h(ts, chunks.MSG_VIDEO, body)


def main():
    factory = Factory()
    factory.protocol = URLDispatchingServer

    reactor.listenTCP(1935, factory)
    reactor.run()


if __name__ == '__main__':
    main()
