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


from twisted.internet import defer, protocol, error, reactor
from twisted.trial import unittest

from rtmp.amf0 import encode as encode_amf, decode as decode_amf, Object
from rtmp import chunks
from rtmp import const
# from rtmp.chunks import Header, absolutize
# from rtmp.chunks import Muxer, Demuxer
# from rtmp.utils import GeneratorWrapperProtocol
# from rtmp.vecbuf import VecBuf, flatten


from rtmp import proto
from rtmp.proto import DispatchProtocol, CommandDispatchProtocol
from rtmp.proto import EventDispatchProtocol
from rtmp.helpers import vb, vb_clone

from helpers import StringTransport


class TestDemuxerMixin(object):
    """Allow injecting complete ready messages as if they were demuxed
    by the demuxer.
    """
    def inject(self, cs_id, time, type_, ms_id, body):
        h = chunks.Header(cs_id, time, len(body), type_, ms_id)
        if cs_id == 2 and 0 < type_ < 8 and ms_id == 0:
            self.controlMessageReceived(h, body)
        else:
            self.protocol.messageReceived(h, body)

    def sinject(self, cs_id, time, type_, ms_id, data):
        body = vb(data)
        self.inject(cs_id, time, type_, ms_id, body)

def mixin(base_class, mixin_class, name=None):
    if not name:
        name = 'Mixed' + base_class.__name__
    return type(name, (mixin_class, base_class), {})


class TestMuxer(object):
    def __init__(self, transport):
        self.transport = transport
        self.chunk_size = chunks.DEFAULT_CHUNK_SIZE
        self.messages = []

    def set_chunk_size(self, new_chunk_size):
        self.chunk_size = new_chunk_size

    def sendMessage(self, time, type_, ms_id, body, absolute=False):
        self.messages.append((time, type_, ms_id, body.read(len(body)),
                              absolute))

class TestHandshaker(object):
    def __init__(self, protocol, epoch_base, is_client=False):
        self.protocol = protocol

    def gen_handler(self):
        s = yield 1
        s.read(1)
        self.protocol.handshakeSucceeded(0, 0)


class TestFactory(protocol.Factory):
    init_time = 0


def unvb(vb):
    return vb.read(len(vb))[:]


class _ProtocolTestBase(unittest.TestCase):
    def setUp(self):
        self.messages = []

    def clear(self):
        self.messages[:] = []


    def make_protocol_class(self):
        raise NotImplementedError()

    def make_factory(self):
        return TestFactory()

    def make_protocol(self):
        f = self.make_factory()
        f.protocol = self.make_protocol_class()

        p = f.buildProtocol(None)

        return p

    def connect_protocol(self, protocol):
        t = StringTransport()
        protocol.makeConnection(t)

        # ... and let's skip the handshake
        protocol.dataReceived('.')

        return t

    def build_proto(self):
        p = self.make_protocol()
        t = self.connect_protocol(p)
        return p, t, p._demuxer, p.muxer


class TestDispatchProtocol(_ProtocolTestBase):
    def make_protocol_class(self):
        tself = self
        class TDispatchProtocol(DispatchProtocol):
            demuxer_class = mixin(DispatchProtocol.demuxer_class,
                                  TestDemuxerMixin)
            muxer_class = TestMuxer
            handshaker_class = TestHandshaker

            def doCommand(self, ts, ms_id, args):
                tself.messages.append(('command', ts, ms_id, args))

            def doMeta(self, ts, ms_id, args):
                tself.messages.append(('meta', ts, ms_id, args))

            def doData(self, type_, ts, ms_id, body):
                tself.messages.append(('data', ts, ms_id, type_, unvb(body)))

            def unknownMessageType(self, header, body):
                header_args = (header.cs_id, header.abs_time, header.size,
                               header.type, header.ms_id)
                tself.messages.append(('unknown', header_args, unvb(body)))

        return TDispatchProtocol

    def test_dispatch_data(self):
        p, t, d, m = self.build_proto()

        d.sinject(3, 0, const.RTMP_AUDIO, 1, 'laalala')
        d.sinject(4, 0, const.RTMP_VIDEO, 1, 'watchme')
        self.assertEquals(self.messages, [('data', 0, 1, const.RTMP_AUDIO,
                                           'laalala'),
                                          ('data', 0, 1, const.RTMP_VIDEO,
                                           'watchme')])

    def test_dispatch_meta(self):
        p, t, d, m = self.build_proto()

        d.inject(3, 0, const.RTMP_DATA, 1, encode_amf('onStatus', None))
        self.assertEquals(self.messages, [('meta', 0, 1, ['onStatus', None])])

    def test_dispatch_command(self):
        p, t, d, m = self.build_proto()

        d.inject(3, 0, const.RTMP_COMMAND, 1, encode_amf('onStatus', 0, None))
        self.assertEquals(self.messages, [('command', 0, 1,
                                           ['onStatus', 0.0, None])])

    def test_dispatch_unknown(self):
        p, t, d, m = self.build_proto()

        d.sinject(3, 0, 99, 1, '....')
        self.assertEquals(self.messages,
                          [('unknown', (3, 0, 4, 99, 1), '....')])


class ArtificialError(RuntimeError):
    pass

class TestCommandDispatchProtocol(_ProtocolTestBase):
    timeout = 10

    # note: CDP tests ideally should be performed with a controllable
    # reactor, but the CDP code would need to be adapted, so for now
    # just relying on ordered scheduling of callLater() calls...

    def make_protocol_class(self):
        tself = self
        CDP = CommandDispatchProtocol
        class TCommandDispatchProtocol(CDP):
            demuxer_class = mixin(CDP.demuxer_class, TestDemuxerMixin)
            muxer_class = TestMuxer
            handshaker_class = TestHandshaker

            def unknownMessageType(self, header, body):
                bclone = vb_clone(body)
                tself.messages.append(('unk_msg', header, unvb(bclone)))
                CDP.unknownMessageType(self, header, body)

            def unknownCommandType(self, cmd, ts, ms_id, args):
                tself.messages.append(('unk_cmd', cmd, ts, ms_id, args))

            def unexpectedCallResult(self, ts, ms_id, trans_id, args):
                tself.messages.append(('_result', ts, ms_id, trans_id, args))

            def unexpectedCallError(self, ts, ms_id, trans_id, args):
                tself.messages.append(('_error', ts, ms_id, trans_id, args))

            # some custom command handlers
            def command_foobar(self, ts, ms_id, trans_id, arg1, arg2):
                tself.messages.append(('foobar', ts, ms_id, trans_id,
                                       arg1, arg2))

            def command_breakdown(self, ts, ms_id, trans_id, *args):
                raise ArtificialError('breakdown on request')

        return TCommandDispatchProtocol

    def test_command_simple(self):
        p, t, dmx, mux = self.build_proto()

        dmx.inject(3, 0, const.RTMP_COMMAND, 1,
                   encode_amf('foobar', 0, 'foo', 'bar'))

        d = defer.Deferred()
        d.addCallback(lambda _: self.assertEquals(self.messages,
                                                  [('foobar', 0, 1, 0.0,
                                                    'foo', 'bar')]))

        reactor.callLater(0, d.callback, None)
        return d

    def test_command_simple_after_exception(self):
        p, t, dmx, mux = self.build_proto()

        dmx.inject(3, 0, const.RTMP_COMMAND, 1,
                   encode_amf('breakdown', 0, 'burn', 'baby', 'burn'))
        dmx.inject(3, 0, const.RTMP_COMMAND, 1,
                   encode_amf('foobar', 0, 'foo', 'bar'))

        d = defer.Deferred()
        d.addCallback(lambda _: self.assertEquals(self.messages,
                                                  [('foobar', 0, 1, 0.0,
                                                    'foo', 'bar')]))

        def check_clean_logs(result):
            # we know the breakdown command handler raises an
            # exception, so let's clean it so the test can pass, but
            # verify it was actually logged, and only once, while
            # cleaning, too
            self.assertEquals(len(self.flushLoggedErrors(ArtificialError)),
                              1)
            return result
        d.addBoth(check_clean_logs)

        reactor.callLater(0, d.callback, None)
        return d

    def test_command_unknown(self):
        p, t, dmx, mux = self.build_proto()

        dmx.inject(3, 0, const.RTMP_COMMAND, 1,
                   encode_amf('method_not_there_for_sure', 0, 'a', 'rrrr'))

        d = defer.Deferred()
        def verify_unknown(result):
            self.assertEquals(self.messages,
                              [('unk_cmd', 'method_not_there_for_sure',
                                0, 1, [0.0, 'a', 'rrrr'])])
            return result
        d.addCallback(verify_unknown)

        reactor.callLater(0, d.callback, None)
        return d

    def test_unexpectedCallResult(self):
        p, t, dmx, mux = self.build_proto()

        dmx.inject(3, 0, const.RTMP_COMMAND, 1,
                   encode_amf('_result', 2, None, 'Surprise!'))

        d = defer.Deferred()
        def verify_unexpected(result):
            self.assertEquals(self.messages,
                              [('_result', 0, 1, 2.0, (None, 'Surprise!'))])
            return result
        d.addCallback(verify_unexpected)

        reactor.callLater(0, d.callback, None)
        return d

    def test_unexpectedCallError(self):
        p, t, dmx, mux = self.build_proto()

        dmx.inject(3, 0, const.RTMP_COMMAND, 1,
                   encode_amf('_error', 2, None, 'Surprise!'))

        d = defer.Deferred()
        def verify_unexpected(result):
            self.assertEquals(self.messages,
                              [('_error', 0, 1, 2.0, (None, 'Surprise!'))])
            return result
        d.addCallback(verify_unexpected)

        reactor.callLater(0, d.callback, None)
        return d

    def test_callRemote_result(self):
        p, t, dmx, mux = self.build_proto()

        d = p.callRemote(1, 'echo', 'sing it back')

        self.assertEquals(len(mux.messages), 1)
        msg = mux.messages[0]
        self.assertEquals(msg[0:3] + msg[4:],
                          (0, chunks.MSG_COMMAND, 1, False))

        # can't rely on transaction number, so we skip it
        decoded = decode_amf(vb(msg[3]))
        self.assertEquals((decoded[0], decoded[2]), ('echo', 'sing it back'))

        dmx.inject(3, 0, const.RTMP_COMMAND, 1,
                   encode_amf('_result', decoded[1], None, decoded[2]))

        d.addCallback(self.assertEquals, (None, 'sing it back'))

        return d

    def test_callRemote_error(self):
        p, t, dmx, mux = self.build_proto()

        d = p.callRemote(1, 'echo', 'sing it back')

        self.assertEquals(len(mux.messages), 1)
        msg = mux.messages[0]
        self.assertEquals(msg[0:3] + msg[4:],
                          (0, chunks.MSG_COMMAND, 1, False))

        # can't rely on transaction number, so we skip it
        decoded = decode_amf(vb(msg[3]))
        self.assertEquals((decoded[0], decoded[2]), ('echo', 'sing it back'))

        err_info = Object(code='Failed', level='error', desc='no echo method')
        dmx.inject(3, 0, const.RTMP_COMMAND, 1,
                   encode_amf('_error', decoded[1], None, err_info))

        self.assertFailure(d, proto.CommandResultError)
        d.addCallback(lambda r: self.assertEquals(r.args, (None, err_info)))

        return d

    def test_callRemote_disconnect(self):
        p, t, dmx, mux = self.build_proto()

        d = p.callRemote(1, 'echo', 'sing it back')

        self.assertEquals(len(mux.messages), 1)
        msg = mux.messages[0]
        self.assertEquals(msg[0:3] + msg[4:],
                          (0, chunks.MSG_COMMAND, 1, False))

        # can't rely on transaction number, so we skip it
        decoded = decode_amf(vb(msg[3]))
        self.assertEquals((decoded[0], decoded[2]), ('echo', 'sing it back'))

        p.connectionLost()

        self.assertFailure(d, error.ConnectionDone)
        return d


info1 = Object(code='Foo.Code', level='status', desc='description1')
info2 = Object(code='Bar.Code', level='status', desc='description2')

class TestEventDispatchProtocol(_ProtocolTestBase):
    timeout = 10

    def make_protocol_class(self):
        tself = self
        EDP = EventDispatchProtocol
        class TEventDispatchProtocol(EDP):
            demuxer_class = mixin(EDP.demuxer_class, TestDemuxerMixin)
            muxer_class = TestMuxer
            handshaker_class = TestHandshaker

            def unhandledOnStatus(self, ts, ms_id, info):
                tself.messages.append(('unhandled', ts, ms_id, info))

        return TEventDispatchProtocol

    def test_waitStatus(self):
        p, t, dmx, mux = self.build_proto()

        # wait for event with specific code
        d = p.waitStatus(1, info1.code)
        d.addCallback(self.assertEquals, info1)

        # then wait for any event on message stream 1
        d.addCallback(lambda _: p.waitStatus(1, None))
        d.addCallback(self.assertEquals, info2)

        dmx.inject(3, 0, const.RTMP_COMMAND, 1,
                   encode_amf('onStatus', 0, None, info1))
        dmx.inject(3, 0, const.RTMP_COMMAND, 1,
                   encode_amf('onStatus', 0, None, info2))

        return d

    def test_waitStatus_mixed(self):
        p, t, dmx, mux = self.build_proto()

        # wait for event with specific code, on message stream 2
        d1 = p.waitStatus(2, info1.code)
        d1.addCallback(self.assertEquals, info1)

        # also wait for any event on message stream 1
        d2 = p.waitStatus(1, None)
        d2.addCallback(self.assertEquals, info2)

        dmx.inject(3, 0, const.RTMP_COMMAND, 1,
                   encode_amf('onStatus', 0, None, info2))
        dmx.inject(3, 0, const.RTMP_COMMAND, 2,
                   encode_amf('onStatus', 0, None, info1))

        return defer.DeferredList([d1, d2], fireOnOneErrback=1)

    def test_waitStatus_unexpected(self):
        p, t, dmx, mux = self.build_proto()

        code_other = 'Baz.Code'

        # wait for event with specific code
        d = p.waitStatus(1, info1.code)
        d.addCallback(self.assertEquals, info1)

        # wait for another even, with some other specific code
        d.addCallback(lambda _: p.waitStatus(1, code_other))
        self.assertFailure(d, proto.UnexpectedStatusError)

        dmx.inject(3, 0, const.RTMP_COMMAND, 1,
                   encode_amf('onStatus', 0, None, info1))
        dmx.inject(3, 0, const.RTMP_COMMAND, 1,
                   encode_amf('onStatus', 0, None, info2))

        return d

    def test_waitStatus_unhandled(self):
        p, t, dmx, mux = self.build_proto()

        # wait for event with specific code, on message stream 1
        d = p.waitStatus(1, info2.code)
        d.addCallback(self.assertEquals, info2)
        unhandled = ('unhandled', 0, 2, info1)
        d.addCallback(lambda _: self.assertEquals(self.messages, [unhandled]))

        # first event sent on ms_id 2
        dmx.inject(3, 0, const.RTMP_COMMAND, 2,
                   encode_amf('onStatus', 0, None, info1))
        # ... and the next one, on ms_id 1
        dmx.inject(3, 0, const.RTMP_COMMAND, 1,
                   encode_amf('onStatus', 0, None, info2))

        return d

    def test_waitStatus_no_code(self):
        p, t, dmx, mux = self.build_proto()

        # the following info object lacks 'code' field
        wrong_info = Object(level='status', desc='description2')

        # wait for any event
        d = p.waitStatus(1, None)
        self.assertFailure(d, proto.ProtocolContractError)

        dmx.inject(3, 0, const.RTMP_COMMAND, 1,
                   encode_amf('onStatus', 0, None, wrong_info))

        return d

    def test_waitStatus_disconnected(self):
        p, t, dmx, mux = self.build_proto()

        # wait for any event
        d = p.waitStatus(1, None)
        self.assertFailure(d, error.ConnectionDone)

        p.connectionLost()

        return d
