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


import itertools as it

from twisted.trial import unittest

from twimp import chunks
from twimp.chunks import Header, absolutize
from twimp.chunks import Muxer, Demuxer
from twimp.utils import GeneratorWrapperProtocol
from twimp.vecbuf import VecBuf, flatten

from helpers import StringTransport


class TestHeader(unittest.TestCase):
    def assertAttrs(self, obj, **kw):
        for k, v in kw.items():
            self.assertEquals(getattr(obj, k), v,
                              '<obj>.%s (%r) != %r' % (k, getattr(obj, k), v))

    def test_basic(self):
        "Just simple header construction."

        h1 = Header(1, 234, 567, 8, 9)
        self.assertAttrs(h1, cs_id=1, time=234, size=567, type=8, ms_id=9,
                         absolute=True, abs_time=234, real_time=234,
                         real_size=567, real_type=8, real_ms_id=9)

        h2 = Header(1, 234, 567, 8, None)
        self.assertAttrs(h2, cs_id=1, time=234, size=567, type=8, ms_id=None,
                         absolute=False, abs_time=None, real_time=234,
                         real_size=567, real_type=8, real_ms_id=None)

        h3 = Header(1, 234, None, None, None)
        self.assertAttrs(h3, cs_id=1, time=234, size=None, type=None,
                         ms_id=None, absolute=False, abs_time=None,
                         real_time=234, real_size=None, real_type=None,
                         real_ms_id=None)

        h4 = Header(1, None, None, None, None)
        self.assertAttrs(h4, cs_id=1, time=None, size=None, type=None,
                         ms_id=None, absolute=False, abs_time=None,
                         real_time=None, real_size=None, real_type=None,
                         real_ms_id=None)


    def test_absolutize(self):
        "Absolutize relative to an absolute header."

        base = Header(1, 234, 567, 8, 9)
        ah1 = absolutize(base, base)
        self.assertAttrs(ah1, cs_id=1, time=234, size=567, type=8, ms_id=9,
                         absolute=True, abs_time=234, real_time=234,
                         real_size=567, real_type=8, real_ms_id=9)

        h2 = Header(1, 234, 567, 8, None)
        ah2 = absolutize(h2, base)
        self.assertAttrs(ah2, cs_id=1, time=234, size=567, type=8, ms_id=9,
                         absolute=False, abs_time=468, real_time=234,
                         real_size=567, real_type=8, real_ms_id=None)


        h3 = Header(1, 234, None, None, None)
        ah3 = absolutize(h3, base)
        self.assertAttrs(ah3, cs_id=1, time=234, size=567, type=8, ms_id=9,
                         absolute=False, abs_time=468, real_time=234,
                         real_size=None, real_type=None, real_ms_id=None)

        h4 = Header(1, None, None, None, None)
        ah4 = absolutize(h4, base)
        self.assertAttrs(ah4, cs_id=1, time=234, size=567, type=8, ms_id=9,
                         absolute=False, abs_time=468, real_time=None,
                         real_size=None, real_type=None, real_ms_id=None)

    def test_absolutize_with_zero(self):
        "Absolutize relative to an absolute header, using 0 values."

        base = Header(1, 234, 567, 8, 9)
        self.assertAttrs(base, cs_id=1, time=234, size=567, type=8, ms_id=9,
                         absolute=True, abs_time=234, real_time=234,
                         real_size=567, real_type=8, real_ms_id=9)

        h1 = Header(0, 0, 0, 0, 0)
        ah1 = absolutize(h1, base)
        self.assertAttrs(ah1, cs_id=0, time=0, size=0, type=0, ms_id=0,
                         absolute=True, abs_time=0, real_time=0,
                         real_size=0, real_type=0, real_ms_id=0)

        h2 = Header(0, 0, 0, 0, None)
        ah2 = absolutize(h2, base)
        self.assertAttrs(ah2, cs_id=0, time=0, size=0, type=0, ms_id=9,
                         absolute=False, abs_time=234, real_time=0,
                         real_size=0, real_type=0, real_ms_id=None)


        h3 = Header(0, 0, None, None, None)
        ah3 = absolutize(h3, base)
        self.assertAttrs(ah3, cs_id=0, time=0, size=567, type=8, ms_id=9,
                         absolute=False, abs_time=234, real_time=0,
                         real_size=None, real_type=None, real_ms_id=None)

        h4 = Header(0, None, None, None, None)
        ah4 = absolutize(h4, base)
        self.assertAttrs(ah4, cs_id=0, time=234, size=567, type=8, ms_id=9,
                         absolute=False, abs_time=468, real_time=None,
                         real_size=None, real_type=None, real_ms_id=None)

    def test_absolutize_relative(self):
        "Absolutize relative to a relative (absolutized) header."

        base = Header(1, 234, 567, 8, 9)
        rel1 = Header(1, 10, 567, 8, None)
        absd1 = absolutize(rel1, base)
        self.assertAttrs(absd1, cs_id=1, time=10, size=567, type=8, ms_id=9,
                         absolute=False, abs_time=244, real_time=10,
                         real_size=567, real_type=8, real_ms_id=None)

        h1 = Header(1, 20, 567, 8, None)
        a1h1 = absolutize(h1, absd1)
        self.assertAttrs(a1h1, cs_id=1, time=20, size=567, type=8, ms_id=9,
                         absolute=False, abs_time=264, real_time=20,
                         real_size=567, real_type=8, real_ms_id=None)

        h2 = Header(1, 20, None, None, None)
        a1h2 = absolutize(h2, absd1)
        self.assertAttrs(a1h2, cs_id=1, time=20, size=567, type=8, ms_id=9,
                         absolute=False, abs_time=264, real_time=20,
                         real_size=None, real_type=None, real_ms_id=None)

        h3 = Header(1, None, None, None, None)
        a1h3 = absolutize(h3, absd1)
        self.assertAttrs(a1h3, cs_id=1, time=10, size=567, type=8, ms_id=9,
                         absolute=False, abs_time=254, real_time=None,
                         real_size=None, real_type=None, real_ms_id=None)

        rel2 = Header(1, 10, None, None, None)
        absd2 = absolutize(rel2, base)
        self.assertAttrs(absd2, cs_id=1, time=10, size=567, type=8, ms_id=9,
                         absolute=False, abs_time=244, real_time=10,
                         real_size=None, real_type=None, real_ms_id=None)

        a2h1 = absolutize(h1, absd2)
        self.assertAttrs(a2h1, cs_id=1, time=20, size=567, type=8, ms_id=9,
                         absolute=False, abs_time=264, real_time=20,
                         real_size=567, real_type=8, real_ms_id=None)

        a2h2 = absolutize(h2, absd2)
        self.assertAttrs(a2h2, cs_id=1, time=20, size=567, type=8, ms_id=9,
                         absolute=False, abs_time=264, real_time=20,
                         real_size=None, real_type=None, real_ms_id=None)

        a2h3 = absolutize(h3, absd2)
        self.assertAttrs(a2h3, cs_id=1, time=10, size=567, type=8, ms_id=9,
                         absolute=False, abs_time=254, real_time=None,
                         real_size=None, real_type=None, real_ms_id=None)

        rel3 = Header(1, None, None, None, None)
        absd3 = absolutize(rel3, base)
        self.assertAttrs(absd3, cs_id=1, time=234, size=567, type=8, ms_id=9,
                         absolute=False, abs_time=468, real_time=None,
                         real_size=None, real_type=None, real_ms_id=None)

        a3h1 = absolutize(h1, absd3)
        self.assertAttrs(a3h1, cs_id=1, time=20, size=567, type=8, ms_id=9,
                         absolute=False, abs_time=488, real_time=20,
                         real_size=567, real_type=8, real_ms_id=None)

        a3h2 = absolutize(h2, absd3)
        self.assertAttrs(a3h2, cs_id=1, time=20, size=567, type=8, ms_id=9,
                         absolute=False, abs_time=488, real_time=20,
                         real_size=None, real_type=None, real_ms_id=None)

        a3h3 = absolutize(h3, absd3)
        self.assertAttrs(a3h3, cs_id=1, time=234, size=567, type=8, ms_id=9,
                         absolute=False, abs_time=702, real_time=None,
                         real_size=None, real_type=None, real_ms_id=None)


class TestDemuxerProtocol(GeneratorWrapperProtocol):
    def __init__(self, *a, **kw):
        GeneratorWrapperProtocol.__init__(self, *a, **kw)
        self.messages = []

    def messageReceived(self, header, body):
        # print header, body.peek(len(body)).encode('hex')
        self.messages.append((header, body))

    def pop_messages(self):
        msgs, self.messages[:] = self.messages[:], []
        return msgs

class MessagePassingDemuxer(Demuxer):
    def controlMessageReceived(self, header, body):
        # also pass all protocol control messages to self.protocol as
        # normal messages
        body_clone = VecBuf(body.peek_seq(len(body)))
        Demuxer.controlMessageReceived(self, header, body)
        self.protocol.messageReceived(header, body_clone)


def p(*s):
    return ''.join(s).decode('hex')

data = [
    # first, just normal beginning of connection...
    p('020000000000040500000000', '002625a0'),     # window_size(2500000)
    p('020000000000050600000000', '002625a002'),   # set_bw(2500000, 2)
    p('020000000000060400000000', '000000000000'), # user_ctrl(0x0, 0x0)
    p('020000000000040100000000', '00001000'),     # set_chunk_size(0x1000)
    p('050000000000a0148fbd0000', '2e' * 0xa0),    # call(...)
    p('020000000000060400000000', '00040000bd8f'), # user_ctrl(0x4, 0xbd8f)
    p('020000000000060400000000', '00000000bd8f'), # user_ctrl(0x0, 0xbd8f)

    # slightly compressed header on repeated call() on chunk stream 5
    p('450000000000a814',         '2e' * 0xa8),    # call(...)

    # some examples of data/audio/video messages
    p('06000000000000088fbd0000'),                 # audio()
    p('07000000000002098fbd0000', '0000'),         # video()
    ### 10 chunks, 10 messages
    p('07000000000005098fbd0000', '00' * 5),       # video()
    p('4600000000000408',         '00000000'),     # audio()
    # more compressed header
    p('86000000',                 '00000000'),     # audio()
    p('020000000000040100000000', '00000020'),     # set_chunk_size(0x20)

    # ok, time for chunked messages
    p('4600000000004308',         '01' * 0x20),    # audio() (chunk 1/3)
    p('c6',                       '02' * 0x20),    # audio() (chunk 2/3)
    p('c6',                       '030303'),       # audio() (chunk 3/3)

    # completely compressed header on first chunk
    p('c6',                       '04' * 0x20),    # audio() (chunk 1/3)
    p('c6',                       '05' * 0x20),    # audio() (chunk 2/3)
    p('c6',                       '060606'),       # audio() (chunk 3/3)
    ### 20 chunks, 16 messages

    # interleaved chunk streams
    p('c6',                       '07' * 0x20),    # audio() (chunk 1/3)
    p('c6',                       '08' * 0x20),    # audio() (chunk 2/3)
    p('4700000000000109',         '00'),           # video()
    p('c6',                       '090909'),       # audio() (chunk 3/3)

    # change of chunk size in between message chunks
    p('4600000000004308',         '0a' * 0x20),    # audio() (chunk 1/3)
    p('020000000000040100000000', '00000030'),     # set_chunk_size(0x30)
    p('c6',                       '0b' * 0x23),    # audio() (chunk 2+3/3)

    # timestamped messages:
    p('0600001e000058088fbd0000', '0c' * 0x30),    # audio() (chunk 1/2)
    p('c6',                       '0d' * 0x28),    # audio() (chunk 2/2)

    # ... with compressed header, and different time offset
    p('86000014',                 '0e' * 0x30),    # audio() (chunk 1/2)
    ### 30 chunks, 21 messages
    p('c6',                       '0f' * 0x28),    # audio() (chunk 2/2)

    # ... with completely compressed header, repeated offset
    p('c6',                       '10' * 0x30),    # audio() (chunk 1/2)
    p('c6',                       '11' * 0x28),    # audio() (chunk 2/2)

    # absolute, timestamp > 0x00ffffff
    p('07ffffff0000000890bd000001000000', ''),     # audio() @ 0x01000000 ms

    # relative, time offset > 0x00ffffff
    p('46ffffff0000010801000000', '00'),           # audio() @ 0x01000046 ms
    # and even more relative, and even bigger offset
    p('86ffffff01000001',         '00'),           # audio() @ 0x02000047 ms

    # relative, time offset > 0x00ffffff, chunked
    p('46ffffff0000310801000002', '00' * 0x30),    # (1/2) @ 0x03000049 ms
    p('c601000002',               '01'),           # (2/2) @ 0x03000049 ms

    # completely compressed, time offset > 0x00ffffff, chunked
    p('c601000003',               '02' * 0x30),    # (1/2) @ 0x04000052 ms
    p('c601000003',               '03'),           # (2/2) @ 0x04000052 ms
    ### 40 chunks, 28 messages

    # this one's to make sure we receive all the previous ones ^^
    p('07ffffff0000000890bd000001000001', ''),     # audio() @ 0x01000001 ms
    ### 41 chunks, 29 messages
]

# the following list should be in sync with the data list above (with
# None - where a complete message is not expected in response to
# feeding the corresponding data element)

messages = [
    ((2, 0, 0, 4, 5, 0), '\x00\x26\x25\xa0'),
    ((2, 0, 0, 5, 6, 0), '\x00\x26\x25\xa0\x02'),
    ((2, 0, 0, 6, 4, 0), '\x00\x00\x00\x00\x00\x00'),
    ((2, 0, 0, 4, 1, 0), '\x00\x00\x10\x00'),
    ((5, 0, 0, 160, 20, 48527), '.' * 160),
    ((2, 0, 0, 6, 4, 0), '\x00\x04\x00\x00\xbd\x8f'),
    ((2, 0, 0, 6, 4, 0), '\x00\x00\x00\x00\xbd\x8f'),
    ((5, 0, 0, 168, 20, 48527), '.' * 168),
    ((6, 0, 0, 0, 8, 48527), ''),
    ((7, 0, 0, 2, 9, 48527), '\x00\x00'),
    ### 10

    ((7, 0, 0, 5, 9, 48527), '\x00\x00\x00\x00\x00'),
    ((6, 0, 0, 4, 8, 48527), '\x00\x00\x00\x00'),
    ((6, 0, 0, 4, 8, 48527), '\x00\x00\x00\x00'),
    ((2, 0, 0, 4, 1, 0), '\x00\x00\x00\x20'),
    None,
    None,
    ((6, 0, 0, 67, 8, 48527), ('\x01' * 32) + ('\x02' * 32) + ('\x03' * 3)),
    None,
    None,
    ((6, 0, 0, 67, 8, 48527), ('\x04' * 32) + ('\x05' * 32) + ('\x06' * 3)),
    ### 20

    None,
    None,
    ((7, 0, 0, 1, 9, 48527), '\x00'),
    ((6, 0, 0, 67, 8, 48527), ('\x07' * 32) + ('\x08' * 32) + ('\x09' * 3)),
    None,
    ((2, 0, 0, 4, 1, 0), '\x00\x00\x00\x30'),
    ((6, 0, 0, 67, 8, 48527), ('\x0a' * 32) + ('\x0b' * 35)),
    None,
    ((6, 30, 30, 88, 8, 48527), ('\x0c' * 48) + ('\x0d' * 40)),
    None,
    ### 30

    ((6, 50, 20, 88, 8, 48527), ('\x0e' * 48) + ('\x0f' * 40)),
    None,
    ((6, 70, 20, 88, 8, 48527), ('\x10' * 48) + ('\x11' * 40)),
    ((7, 16777216, 16777216, 0, 8, 48528), ''),
    ((6, 16777286, 16777216, 1, 8, 48527), '\x00'),
    ((6, 33554503, 16777217, 1, 8, 48527), '\x00'),
    None,
    ((6, 50331721, 16777218, 49, 8, 48527), ('\x00' * 48) + ('\x01' * 1)),
    None,
    ((6, 67108940, 16777219, 49, 8, 48527), ('\x02' * 48) + ('\x03' * 1)),
    ### 40

    ((7, 16777217, 16777217, 0, 8, 48528), ''),
]


def chopped(data, piece_size_stream):
    i, l = 0, len(data)
    sizes = iter(piece_size_stream)
    while i < l:
        s = sizes.next()
        yield data[i:i+s]
        i += s

class TestDemuxer(unittest.TestCase):
    def assertMatching(self, (h, body), (m_h, m_body), msg=''):
        self.assertEquals((h.cs_id, h.abs_time, h.time, h.size, h.type,
                           h.ms_id), m_h, msg)
        self.assertEquals(body.read(len(body))[:], m_body, msg)

    def test_data_per_chunk(self):
        t = StringTransport()
        p = TestDemuxerProtocol()

        dmx = MessagePassingDemuxer(p)

        p.init_handler(dmx.gen_handler())

        p.makeConnection(t)

        for i, (d, m) in enumerate(zip(data, messages)):
            p.dataReceived(d)
            rcvd = p.pop_messages()
            if m:
                fail_msg = '(not matching index: %d)' % i
                self.assertEquals(len(rcvd), 1, fail_msg)
                self.assertMatching(rcvd[0], m, fail_msg)

    def _test_data_chopped(self, sizes):
        t = StringTransport()
        p = TestDemuxerProtocol()

        dmx = MessagePassingDemuxer(p)

        p.init_handler(dmx.gen_handler())

        p.makeConnection(t)

        # filter out Nones
        expected = [m for m in messages if m]

        for d in chopped(''.join(data), sizes):
            p.dataReceived(d)

        received = p.pop_messages()

        self.assertEquals(len(received), len(expected))
        for i, (r, e) in enumerate(zip(received, expected)):
            self.assertMatching(r, e, '(not matching index: %d)' % i)

    def test_data_all_at_once(self):
        sizes = it.repeat(2**31) # should be enough to feed all in one call
        self._test_data_chopped(sizes)

    def test_data_all_by_1(self):
        sizes = it.repeat(1)    # one by one...
        self._test_data_chopped(sizes)

    def test_data_all_by_4(self):
        sizes = it.repeat(4)
        self._test_data_chopped(sizes)

    def test_data_all_by_7(self):
        sizes = it.repeat(7)
        self._test_data_chopped(sizes)

    def test_data_all_by_975312468(self):
        sizes = it.cycle([9, 7, 5, 3, 1, 2, 4, 6, 8])
        self._test_data_chopped(sizes)


class TestChunker(unittest.TestCase):
    def assertChunkerMatches(self, chunker, test_chunks):
        chunker_chunks = [(h, flatten(body)) for (h, body) in chunker]
        self.assertEquals(chunker_chunks, test_chunks)

    def assertChunkMatches(self, (h, body), test_chunk):
        self.assertEquals((h, flatten(body)), test_chunk)

    def test_default_chunk_size(self):
        c = chunks.Chunker()
        chunk_size = chunks.DEFAULT_CHUNK_SIZE

        self.assertChunkerMatches(c(3, '\x00\x00\x00',
                                    VecBuf(['.' * chunk_size])),
                                  [('\x00\x00\x00', '.' * chunk_size)])

        self.assertChunkerMatches(c(3, '\x00\x00\x00',
                                    VecBuf(['.' * (chunk_size + 5)])),
                                  [('\x00\x00\x00',  '.' * chunk_size),
                                   ('\xc3',  '.' * 5)])

    def test_changing_chunk_size(self):
        c = chunks.Chunker()

        c.set_chunk_size(5)
        self.assertChunkerMatches(c(3, '\x00\x00\x00',
                                    VecBuf(['.' * 16])),
                                  [('\x00\x00\x00',  '.....'),
                                   ('\xc3', '.....'),
                                   ('\xc3', '.....'),
                                   ('\xc3', '.')])

        c1 = c(3, '\x00\x00\x00', VecBuf(['.' * 16]))
        c.set_chunk_size(1)
        self.assertChunkMatches(c1.next(), ('\x00\x00\x00', '.'))
        c.set_chunk_size(4)
        self.assertChunkMatches(c1.next(), ('\xc3', '....'))
        c.set_chunk_size(7)
        self.assertChunkMatches(c1.next(), ('\xc3', '.......'))
        c.set_chunk_size(128)
        self.assertChunkMatches(c1.next(), ('\xc3', '....'))

        self.assertRaises(StopIteration, c1.next)

    def test_multiple_chunk_streams(self):
        c = chunks.Chunker()

        c1 = c(3, '\x00\x00\x00', VecBuf(['.' * 16]))
        c2 = c(4, '\x01\x01\x01', VecBuf([',' * 16]))

        i = 1
        cs1, cs2 = [], []
        while 1:
            c.set_chunk_size(i)
            try:
                (h1, b1), (h2, b2) = c1.next(), c2.next()
            except StopIteration:
                break
            cs1.append(h1 + flatten(b1))
            cs2.append(h2 + flatten(b2))
            i += 1

        self.assertEquals(cs1,
                          ['\x00\x00\x00' + '.',
                           '\xc3' + '..',
                           '\xc3' + '...',
                           '\xc3' + '....',
                           '\xc3' + '.....',
                           '\xc3' + '.'])
        self.assertEquals(cs2,
                          ['\x01\x01\x01' + ',',
                           '\xc4' + ',,',
                           '\xc4' + ',,,',
                           '\xc4' + ',,,,',
                           '\xc4' + ',,,,,',
                           '\xc4' + ','])

        self.assertRaises(StopIteration, c1.next)
        self.assertRaises(StopIteration, c2.next)


class TestMuxerDemuxer(unittest.TestCase):
    def test_combined(self):

        d_in = {}
        d_out = {}

        type_reverse = { 1: chunks.PROTO_SET_CHUNK_SIZE,
                         4: chunks.PROTO_USER_CONTROL,
                         5: chunks.PROTO_WINDOW_SIZE,
                         6: chunks.PROTO_SET_BANDWIDTH,
                         8: chunks.MSG_AUDIO,
                         9: chunks.MSG_VIDEO,
                         20: chunks.MSG_COMMAND }

        # for now, make sure we're using SimpleChunkProducer:
        class LocalTestMuxer(Muxer):
            chunk_producer_class = chunks.SimpleChunkProducer

        # mux messages first
        tm = StringTransport()
        mux = LocalTestMuxer(tm)

        for msg in messages:
            if not msg:
                continue
            head, body = msg
            msg_type = type_reverse[head[4]]
            mux.sendMessage(head[1], msg_type, head[5], VecBuf([body]))
            if msg_type == chunks.PROTO_SET_CHUNK_SIZE:
                mux.set_chunk_size(int(body.encode('hex'), 16))
            d_in.setdefault(head[4], []).append((head[1], head[4], head[5],
                                                 body))

        # ... now try to demux the resulting binary data
        td = StringTransport()
        p = TestDemuxerProtocol()

        dmx = MessagePassingDemuxer(p)

        p.init_handler(dmx.gen_handler())

        p.makeConnection(td)

        p.dataReceived(tm.value())

        demuxed = p.pop_messages()

        # we can't rely on the received messages to be in the exact
        # same order as the sent ones...

        for head, body in demuxed:
            d_out.setdefault(head.type, []).append((head.abs_time,
                                                    head.type,
                                                    head.ms_id,
                                                    body.read(len(body))))

        self.assertEquals(d_in, d_out)

    def test_many_headers(self):
        # OK, this test is a bit brittle, as we assume the following
        # properties of the encoder/muxer:
        # * it will allocate new chunk stream ids as long as we
        #   provide it with unseen combinations of type and message
        #   stream id
        # * the allocated ids will be consecutive numbers
        # * given the exactly same args to sendMessage the second call
        #   will produce a bitstream of a message with completely
        #   compressed header
        #
        # At the moment that seems the only reasonable way to force
        # muxer to use higher chunk stream ids, with different levels
        # of header compression, using only the public API...

        # for now, make sure we're using SimpleChunkProducer:
        class LocalTestMuxer(Muxer):
            chunk_producer_class = chunks.SimpleChunkProducer

        # first, generate and mux a whole bunch of messages
        tm = StringTransport()
        mux = LocalTestMuxer(tm)

        for i in range(256 + 64 + 1):
            mux.sendMessage(0, chunks.MSG_DATA, i, VecBuf(['']))
            mux.sendMessage(0, chunks.MSG_DATA, i, VecBuf(['']))

        # ... now demux the resulting binary data
        td = StringTransport()
        p = TestDemuxerProtocol()

        dmx = MessagePassingDemuxer(p)

        p.init_handler(dmx.gen_handler())

        p.makeConnection(td)

        p.dataReceived(tm.value())

        demuxed = p.pop_messages()

        # ... and check it maches the result we expect based on the
        # above assumptions
        first_csid = demuxed[0][0].cs_id
        for i in range(256 + 64 + 1):
            h, body = demuxed[2 * i]
            self.assertEquals((h.cs_id, h.real_time, h.real_size, h.real_type,
                               h.real_ms_id),
                              (i + first_csid, 0, 0, 18, i))
            self.assertEquals((h.cs_id, h.time, h.size, h.type, h.ms_id),
                              (i + first_csid, 0, 0, 18, i))
            self.assertEquals(len(body), 0)

            h, body = demuxed[2 * i + 1]
            self.assertEquals((h.cs_id, h.real_time, h.real_size, h.real_type,
                               h.real_ms_id),
                              (i + first_csid, None, None, None, None))
            self.assertEquals((h.cs_id, h.time, h.size, h.type, h.ms_id),
                              (i + first_csid, 0, 0, 18, i))
            self.assertEquals(len(body), 0)
