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


import math

from twisted.internet import error

from twimp.vecbuf import VecBuf


def ignore_disconnect_eb(failure):
    failure.trap(error.ConnectionLost, error.ConnectionDone)
    # just do nothing.

def ellip(s, maxlen=130):
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
    else:
        return s

def vb(s):
    # TODO: add this functionality to the vecbuf/VecBuf itself
    return VecBuf([s])

def vb_read(vb):
    # TODO: add this functionality to the vecbuf/VecBuf itself
    return vb.read(len(vb))

def vb_clone(vb):
    # TODO: add this functionality to the vecbuf/VecBuf itself
    return VecBuf(vb.peek_seq(len(vb)))


##
# note: the following mixins shold be used carefully, or not at all
#

class VerboseDemuxerMixin(object):
    def _print_header(self, header):
        print '%-54r  ' % (header,),

    def controlMessageReceived(self, header, body):
        self._print_header(header)
        print ellip(body.peek(len(body)).encode('hex'))
        super(VerboseDemuxerMixin, self).controlMessageReceived(header, body)


class PrintMsgDemuxerMixin(object):
    def _print_header(self, header):
        print '%-54r  ' % (header,),

    def doSetChunkSize(self, header, new_size):
        super(PrintMsgDemuxerMixin, self).doSetChunkSize(header, new_size)
        self._print_header(header)
        print '(chunk size: %d)' % new_size

    def doAbortMessage(self, header, cs_id):
        super(PrintMsgDemuxerMixin, self).doAbortMessage(header, cs_id)
        self._print_header(header)
        print '(abort: %d)' % cs_id

    def doACK(self, header, seq_num):
        super(PrintMsgDemuxerMixin, self).doACK(header, seq_num)
        self._print_header(header)
        print '(ack: %d)' % seq_num

    def doWindowSize(self, header, window_size):
        super(PrintMsgDemuxerMixin, self).doWindowSize(header, window_size)
        self._print_header(header)
        print '(window: %d)' % window_size

    def doSetBandwidth(self, header, window_size, limit_type):
        super(PrintMsgDemuxerMixin, self).doSetBandwidth(header, window_size,
                                                         limit_type)
        self._print_header(header)
        print '(peer bw: %d %d)' % (window_size, limit_type)


    ### user control events:

    def doUserControlStreamBegin(self, header, stream_id):
        p = super(PrintMsgDemuxerMixin, self)
        p.doUserControlStreamBegin(header, stream_id)
        self._print_header(header)
        print '(ctrl stream begin: %d)' % stream_id

    def doUserControlStreamEOF(self, header, stream_id):
        p = super(PrintMsgDemuxerMixin, self)
        p.doUserControlStreamEOF(header, stream_id)
        self._print_header(header)
        print '(ctrl stream EOF: %d)' % stream_id

    def doUserControlStreamDry(self, header, stream_id):
        p = super(PrintMsgDemuxerMixin, self)
        p.doUserControlStreamDry(header, stream_id)
        self._print_header(header)
        print '(ctrl stream dry: %d)' % stream_id

    def doUserControlStreamRecorded(self, header, stream_id):
        p = super(PrintMsgDemuxerMixin, self)
        p.doUserControlStreamRecorded(header, stream_id)
        self._print_header(header)
        print '(ctrl stream recorded: %d)' % stream_id

    def doUserControlBufferLength(self, header, stream_id, length):
        p = super(PrintMsgDemuxerMixin, self)
        p.doUserControlBufferLength(header, stream_id, length)
        self._print_header(header)
        print '(ctrl buffer length: %d %d)' % (stream_id, length)

    def doUserControlPing(self, header, peer_time):
        p = super(PrintMsgDemuxerMixin, self)
        p.doUserControlPing(header, peer_time)
        self._print_header(header)
        print '(ctrl ping: %d)' % peer_time

    def doUserControlPong(self, header, echo_time):
        p = super(PrintMsgDemuxerMixin, self)
        p.doUserControlPong(header, echo_time)
        self._print_header(header)
        print '(ctrl pong: %d)' % echo_time

    def doUserControlUnknownType(self, header, evt_type, body):
        p = super(PrintMsgDemuxerMixin, self)
        _body = body.peek(len(body))
        self._print_header(header)
        print '(ctrl ?? (%d))' % evt_type, _body[:].encode('hex')
        p.doUserControlUnknownType(header, evt_type, body)


user_ctrl_types = {
    0: 'stream begin',
    1: 'stream EOF',
    2: 'stream dry',
    3: 'buffer len',
    4: 'is recorded',
    6: 'ping',
    7: 'pong',
    }

class PrintMsgPureDemuxerMixin(PrintMsgDemuxerMixin):
    """A mixin for pure Demuxer that doesn't dispatch user control events.
    """
    def _print_header(self, header):
        print '%-54r  ' % (header,),

    def doUserControlMessage(self, header, evt_type, body):
        _body = body.peek(len(body))
        super(PrintMsgDemuxerMixin, self).doUserControlMessage(header,
                                                               evt_type, body)
        m = user_ctrl_types.get(evt_type, '??')
        self._print_header(header)
        print '(ctrl: (%d) %s)' % (evt_type, m), _body[:].encode('hex')
