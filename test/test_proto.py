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


from twisted.internet import protocol
from twisted.trial import unittest

from twimp.amf0 import encode as encode_amf
from twimp import chunks
from twimp import const
# from twimp.chunks import Header, absolutize
# from twimp.chunks import Muxer, Demuxer
# from twimp.utils import GeneratorWrapperProtocol
# from twimp.vecbuf import VecBuf, flatten


from twimp.proto import DispatchProtocol
from twimp.helpers import vb

from test.helpers import StringTransport, unvb


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
