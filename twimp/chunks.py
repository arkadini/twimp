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


from primitives import _s_time_size_type, _s_time, _s_set_bw
from primitives import _s_ulong_l, _s_ulong_b, _s_uchar, _s_ushort, _s_ushort_l
import vecbuf


class Header(object):
    def __init__(self, cs_id, time, size, type, ms_id, **kw):
        self.cs_id = cs_id
        self.time = time
        self.size = size
        self.type = type
        self.ms_id = ms_id

        self.real_time = kw.get('real_time', time)
        self.real_size = kw.get('real_size', size)
        self.real_type = kw.get('real_type', type)
        self.real_ms_id = kw.get('real_ms_id', ms_id)

        self.absolute = (self.real_ms_id is not None)
        self.base_time = kw.get('base_time', time if self.absolute else None)
        self.abs_time = (time if self.absolute else
                         None if (self.base_time is None or
                                  self.time is None) else
                         self.base_time + self.time)

    def __repr__(self):
        def _repr_maybe(v, provided):
            return ('%s' if provided else '(%s)') % (v,)
        vmiss = zip([self.time, self.size, self.type, self.ms_id],
                    [self.real_time is None, self.real_size is None,
                     self.real_type is None, self.real_ms_id is None])
        atime = None
        if self.abs_time is not None:
            # atime = '%.03f' % (self.abs_time / 1000.0,)
            atime = '%07d' % self.abs_time
        return '<%s %s @ %s :: %s>' % (self.__class__.__name__, self.cs_id,
                                       atime,
                                       ', '.join([_repr_maybe(v, not miss)
                                                  for (v, miss) in vmiss]))

def absolutize(h, base):
    abs_h = Header(h.cs_id,
                   base.time if h.time is None else h.time,
                   base.size if h.size is None else h.size,
                   base.type if h.type is None else h.type,
                   base.ms_id if h.ms_id is None else h.ms_id,
                   base_time=base.abs_time, real_time=h.time,
                   real_size=h.size, real_type=h.type, real_ms_id=h.ms_id)
    return abs_h


class ChunkStreamParseError(ValueError):
    pass

class ChunkStreamValueError(ValueError):
    pass


_sizes_1 = [1, 2, 0]
_sizes_2 = [11, 7, 3, 0]


def read_header_head(s):
    # v = _s_uchar.unpack(s.read(1))
    v = ord(s.read(1)[0])
    return v >> 6, v & 0x3f

class Demuxer(object):
    chunk_size = 128

    def __init__(self, protocol):
        self.chstr_map = {}
        self.msg_map = {}
        self.protocol = protocol

        # handlers: {type => (verify_size, cnv_size, cnv_func, pass_rest,
        #                     handler_func)}
        # TODO: should some of those be handled directly by a higer layer?...
        self.ctrl_handlers = {
            0x1: (_s_ulong_b.size, _s_ulong_b.size, _s_ulong_b.unpack, False,
                  self.doSetChunkSize),
            0x2: (_s_ulong_b.size, _s_ulong_b.size, _s_ulong_b.unpack, False,
                  self.doAbortMessage),
            0x3: (_s_ulong_b.size, _s_ulong_b.size, _s_ulong_b.unpack, False,
                  self.doACK),
            0x4: (None,            _s_ushort.size,  _s_ushort.unpack,  True,
                  self.doUserControlMessage),
            0x5: (_s_ulong_b.size, _s_ulong_b.size, _s_ulong_b.unpack, False,
                  self.doWindowSize),
            0x6: (_s_set_bw.size,  _s_set_bw.size,  _s_set_bw.unpack,  False,
                  self.doSetBandwidth),
            }

    def doSetChunkSize(self, header, new_size):
        if not new_size > 0:
            raise ChunkStreamValueError('set chunk size: '
                                        'need positive chunk size')
        self.chunk_size = new_size

    def doAbortMessage(self, header, cs_id):
        chunks_acc = self.chstr_map.pop(cs_id, None)
        if chunks_acc:
            self.msg_map[cs_id] = chunks_acc[0]

    def doACK(self, header, seq_num):
        pass

    def doUserControlMessage(self, header, evt_type, body):
        pass

    def doWindowSize(self, header, window_size):
        pass

    def doSetBandwidth(self, header, window_size, limit_type):
        pass

    def controlMessageUnknown(self, header, body):
        # this should only be invoked on undocumented control message 7
        raise NotImplementedError('TODO!')

    def controlMessageReceived(self, header, body):
        handler = self.ctrl_handlers.get(header.type)
        if not handler:
            self.controlMessageUnknown(self, header, body)
            return

        verify_size, cnv_size, cnv_func, pass_rest, handler_func = handler
        if verify_size is not None:
            if len(body) != verify_size:
                raise ChunkStreamParseError(('expected msg of size: %d, '
                                             'got: %d') % (verify_size,
                                                           len(body)))

        args = (header,) + cnv_func(body.read(cnv_size))
        if pass_rest:
            args += (body,)

        handler_func(*args)

    def gen_handler(self):
        s = ''

        while 1:
            if len(s) < 1:
                s = yield 1     # need 1 byte to read "basic header"

            htype, csid = read_header_head(s)

            csid_sel = min(csid, 2)
            head_size = _sizes_1[csid_sel] + _sizes_2[htype]

            if len(s) < head_size:
                s = yield head_size # bytes to read the rest of the header

            if csid == 0:
                csid = _s_uchar.unpack(s.read(1))[0] + 64
            elif csid == 1:
                csid = _s_ushort_l.unpack(s.read(2))[0] + 64

            m_time, m_size, m_type, m_msid = None, None, None, None

            if htype == 3:
                pass
            else:
                if htype == 2:
                    _time_1, m_time = _s_time.unpack(s.read(3))
                    m_time += _time_1 << 8
                else:
                    (_time_1, m_time, _size_1,
                     m_size, m_type) = _s_time_size_type.unpack(s.read(7))
                    m_time += _time_1 << 8
                    m_size += _size_1 << 8
                    if htype == 0:
                        m_msid, = _s_ulong_l.unpack(s.read(4))
                if m_time == 0x00ffffff:
                    if len(s) < 4:
                        s = yield 4 # 4 bytes of "extended timestamp"
                    m_time, = _s_ulong_b.unpack(s.read(4))

            h = Header(csid, m_time, m_size, m_type, m_msid)
            accbody = None

            # fill header using message and chunk stream caches...

            if h.cs_id in self.chstr_map:
                _h, accbody, to_read = self.chstr_map[h.cs_id]
                # TODO: check/warn header consistency (with the cached
                # first one)
                h = _h
            elif not h.absolute and h.cs_id in self.msg_map:
                h_base = self.msg_map[h.cs_id]
                h = absolutize(h, h_base)
                to_read = h.size
            else:
                # TODO: check/warn header is absolute here
                to_read = h.size

            need_bytes = min(to_read, self.chunk_size)
            if need_bytes > 0:
                if len(s) < need_bytes:
                    s = yield need_bytes

                if accbody is None:
                    accbody = s.read_seq(need_bytes)
                else:
                    accbody += s.read_seq(need_bytes)
            else:
                if accbody is None:
                    accbody = []

            if to_read > self.chunk_size:
                self.chstr_map[h.cs_id] = (h, accbody,
                                           to_read - self.chunk_size)
            else:
                self.chstr_map.pop(h.cs_id, None)
                self.msg_map[h.cs_id] = h

                if h.cs_id == 2 and 0 < h.type < 8 and h.ms_id == 0:
                    self.controlMessageReceived(h, vecbuf.VecBuf(accbody))
                else:
                    self.protocol.messageReceived(h, vecbuf.VecBuf(accbody))


def _encode_basic_header(h_type, cs_id):
    if cs_id > 0x013f:          # 255 + 64
        return _s_ext_csid.pack((h_type << 6) | 1, cd_id - 64)
    elif cs_id > 0x40:          # 64
        return _s_double_uchar.pack(h_type << 6, cs_id - 64)

    return _s_uchar.pack((h_type << 6) | cs_id)

def encode_full_header(cs_id, time, size, msg_type, ms_id):
    """Serialize an absolute (type 0) chunk header."""

    h1 = _encode_basic_header(0, cs_id)

    write_time = time
    if time >= 0xffffff:
        write_time = 0xffffff
        h4 = _s_ulong_b.pack(time)
    else:
        write_time = time
        h4 = ''

    h2 = _s_time_size_type.pack((write_time >> 8) & 0xffff, write_time & 0xff,
                                (size >> 8) & 0xffff, size & 0xff,
                                msg_type)

    h3 = _s_ulong_l.pack(ms_id)

    return h1 + h2 + h3 + h4

def encode_comp_header(h_type, cs_id, time, size, msg_type, ms_id):
    """Serialize any type of chunk header."""

    if h_type == 3:
        # just the "basic header"
        return _encode_basic_header(h_type, cs_id)
    elif h_type == 0:
        # delegate
        return encode_full_header(cs_id, time, size, msg_type, ms_id)
    else:
        # check time field overflow
        write_time = time
        if time >= 0xffffff:
            write_time = 0xffffff
            h4 = _s_ulong_b.pack(time)
        else:
            write_time = time
            h4 = ''

        if h_type == 2:
            return (_encode_basic_header(h_type, cs_id) +
                    _s_time.pack((write_time >> 8) & 0xffff,
                                 write_time & 0xff) +
                    h4)

        return (_encode_basic_header(h_type, cs_id) +
                _s_time_size_type.pack((write_time >> 8) & 0xffff,
                                       write_time & 0xff,
                                       (size >> 8) & 0xffff, size & 0xff,
                                       msg_type) +
                h4)


DEFAULT_CHUNK_SIZE = 128

class Chunker(object):
    """A class to break messages into chunks."""

    chunk_size = DEFAULT_CHUNK_SIZE

    def set_chunk_size(self, new_chunk_size):
        self.chunk_size = new_chunk_size

    def __call__(self, cs_id, first_header, body):
        """Make a generator yielding body in appropriately-sized chunks.

        @type first_header: str

        @type body: VecBuf

        @return: generator yielding 2-tuples of string and sequence of
                 strings or buffers; each tuple representing header
                 and body of a single chunk
        """
        to_send = len(body)

        if to_send <= self.chunk_size:
            yield first_header, body.read_seq(to_send)
        else:
            to_send -= self.chunk_size
            yield first_header, body.read_seq(self.chunk_size)
            header = _encode_basic_header(3, cs_id)

            while to_send:
                chunk = min(to_send, self.chunk_size)
                to_send -= chunk
                yield header, body.read_seq(chunk)


class SimpleChunkProducer(object):
    """A chunk producer, for use with the Muxer class, that does no
    internal buffering and writes chunks as soon as a Chunker is
    queued.
    """

    def __init__(self, transport):
        self.transport = transport
        # not registering ourselves as a streaming producer, since we
        # don't implement that interface properly...

    def queue_chunker(self, priority, chunker):
        # write all chunks immediately (effectively ignores priority)
        for chunk_head, chunk_body in chunker:
            self.transport.write(chunk_head)
            self.transport.writeSequence(chunk_body)

    def sync(self, priority):
        # a noop in this simple implementation
        pass

    # IPushProducer interface
    def pauseProducing(self):
        pass

    def resumeProducing(self):
        pass

    def stopProducing(self):
        pass


(AMF_v0, AMF_v3) = range(2)

(PROTO_SET_CHUNK_SIZE, PROTO_ABORT_MESSAGE, PROTO_ACK, PROTO_USER_CONTROL,
 PROTO_WINDOW_SIZE, PROTO_SET_BANDWIDTH, MSG_COMMAND, MSG_DATA, MSG_SO,
 MSG_AUDIO, MSG_VIDEO, MSG_AGGREGATE) = range(12)

msg_type_dispatch = [(0x01, 0x01),
                     (0x02, 0x02),
                     (0x03, 0x03),
                     (0x04, 0x04),
                     (0x05, 0x05),
                     (0x06, 0x06),
                     (0x14, 0x11),
                     (0x12, 0x0f),
                     (0x13, 0x10),
                     (0x08, 0x08),
                     (0x09, 0x09),
                     (0x16, 0x16)]

class Muxer(object):
    chunker_class = Chunker
    chunk_producer_class = SimpleChunkProducer

    AMF_ver = AMF_v0

    def __init__(self, transport):
        # (dynamic) explicitly reserved chunk stream ids:
        # { (ms_id, msg_type) => cs_id }
        self._reserved_csids = {}

        # dynamically allocated, purgeable, chunk stream id cache:
        # { (ms_id, msg_type) => cs_id }
        self._adhoc_csids = {}
        # note: purging needs to be done carefully, since reusing
        # cs_id for a chunk stream with a higher priority might
        # corrupt chunk order if our "consumer" has buffered chunks
        # and is actually prioritizing the chunks

        # used/sent header cache:
        # { cs_id => (abs_time, time_diff, size, msg_type, ms_id) }
        self._cached = {}

        self._chunker = self.chunker_class()
        self.producer = self.chunk_producer_class(transport)

        self._precompute_dispatch(self.AMF_ver)

    def _precompute_dispatch(self, amf_ver):
        self._type_dispatch = [elt[amf_ver] for elt in msg_type_dispatch]

    def _make_adhoc_csid(self, mt):
        # this is a simple, not an optimal implementation...
        cs_id_r = 2
        cs_id_t = 2

        if self._reserved_csids:
            cs_id_r = max(self._reserved_csids.values())
        if self._adhoc_csids:
            cs_id_t = max(self._adhoc_csids.values())

        cs_id = max(cs_id_r, cs_id_t) + 1
        self._adhoc_csids[mt] = cs_id
        return cs_id

    def set_chunk_size(self, new_chunk_size):
        """Should be called immediately after sending/queueing
        PROTO_SET_CHUNK_SIZE message.
        """
        # force complete chunking of any messages queued in the
        # producer with priority at least equal to priority of
        # PROTO_SET_CHUNK_SIZE (0)
        self.producer.sync(0)

        # now it should be safe to change the chunk size
        self._chunker.set_chunk_size(new_chunk_size)

    def sendMessage(self, time, type_, ms_id, body, absolute=False):
        """Build and send binary representation of message.

        @param time: absolute time of the message, in milliseconds, in
                     [0, 0xffffffff] range
        @type time: int

        @param type_: type of message, one from PROTO_* and MSG_* constants

        @param ms_id: message stream id, should be 0 for PROTO_* messages

        @param body: the payload of the message
        @type body:  VecBuf
        """
        size = len(body)

        # first: priority based on (abstracted) message type
        priority = 0x10
        if type_ == MSG_VIDEO:
            priority += 0x10

        # second: get the actual message type, depending on the
        # negotiated AMF version
        msg_type = self._type_dispatch[type_]

        # third, figure out cs_id
        cs_id = None
        if msg_type < 0x08:
            # protocol message: use reserved chunk stream 2
            cs_id = 2
            # ... make sure the stream id is 0
            ms_id = 0
            # ... increase the priority
            priority -= 0x10
            # ... and don't compress the header at all
            absolute = True
        else:
            mt = (ms_id, msg_type)
            # note, valid chunk stream id cannot be 0
            cs_id = (self._reserved_csids.get(mt) or
                     self._adhoc_csids.get(mt) or
                     self._make_adhoc_csid(mt))

        if absolute or cs_id not in self._cached:
            self._cached[cs_id] = (time, time, size, msg_type, ms_id)
            raw_header = encode_full_header(cs_id, time, size, msg_type, ms_id)
        else:
            (h_abs_time, h_time, h_size, h_msg_type,
             h_ms_id) = self._cached[cs_id]

            if ms_id == h_ms_id:
                # header is relative, compress time
                new_time = time - h_abs_time # use just the diff

                if new_time < 0:
                    # could happen after a seek...? :/ - don't compress
                    new_time = time
                    h_type = 0
                else:
                    if msg_type == h_msg_type and size == h_size:
                        if new_time == h_time:
                            h_type = 3
                        else:
                            h_type = 2
                    else:
                        h_type = 1
            else:
                # absolute header here, use absolute time
                new_time = time
                h_type = 0

            self._cached[cs_id] = (time, new_time, size, msg_type, ms_id)
            raw_header = encode_comp_header(h_type, cs_id, new_time, size,
                                            msg_type, ms_id)

        self.producer.queue_chunker(priority,
                                    self._chunker(cs_id, raw_header, body))
