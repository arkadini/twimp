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


import struct

from twimp.chunks import Demuxer
from twimp.helpers import PrintMsgDemuxerMixin
from twimp.proto import DispatchProtocol


_v_codec_ids = {1: 'JPEG', 2: 'H.263', 3: 'Screen Video', 4: 'VP6',
                5: 'VP6 Alpha', 6: 'Screen Video 2', 7: 'H.264'}
_v_frame_types = {1: 'keyframe', 2: 'interframe', 3: 'disposable interfame',
                  4: 'generated keyframe', 5: 'info frame'}
_h264_types = {0: 'sequence header', 1: 'NAL unit', 2: 'end of sequence'}

_a_codec_ids = {0: 'PCM', 1: 'ADPCM', 2: 'MP3', 3: 'PCM(<)',
                4: 'Nellymoser 16k', 5: 'Nellymoser 8k', 6: 'Nellymoser',
                7: 'A-law', 8: 'u-law', 10: 'AAC', 11: 'Speex', 14: 'MP3 8k',
                15: 'Device audio'}

class PrintMsgProtocol(DispatchProtocol):
    def messageReceived(self, header, body):
        print '%-54r  ' % (header,),
        ret = DispatchProtocol.messageReceived(self, header, body)
        return ret

    def unknownMessageType(self, header, body):
        print body.peek(len(body)).encode('hex')

    def doCommand(self, ts, ms_id, args):
        print args

    def doMeta(self, ts, ms_id, args):
        print args

    def doData(self, type_, ts, ms_id, body):
        if type_ == 0x09:
            ft_codec, h264_type = struct.unpack('BB', body.peek(2))
            frame_type, codec_id = ft_codec >> 4, ft_codec & 0x0f
            print _v_codec_ids.get(codec_id, '(unknown: %02x)' % codec_id), \
                _v_frame_types.get(frame_type, '(unknown: %02x)' % frame_type),

            if codec_id == 7 and frame_type != 5:
                print _h264_types.get(h264_type, '(unknown: %02x)' % h264_type),
                cts1, cts = struct.unpack_from('>bH', body.peek(5), 2)
                print (cts | (cts1 << 16),), '\t',
            print body.peek(len(body)).encode('hex')
        elif type_ == 0x08:
            codec_id = struct.unpack('B', body.peek(1))[0] >> 4
            print _a_codec_ids.get(codec_id, '(unknown: %02x)' % codec_id), \
                '\t',
            print body.peek(len(body)).encode('hex')
        else:
            print type_, body.peek(len(body)).encode('hex')




def main(f, block=4096):
    # skip the handshakes for now
    f.read(1 + 1536 + 1536)

    class _Demuxer(PrintMsgDemuxerMixin, Demuxer):
        pass

    p = PrintMsgProtocol()
    p.init_handler(_Demuxer(p).gen_handler())

    while True:
        data = f.read(block)
        if not data:
            break
        ret = p.dataReceived(data)
        if ret:
            raise ret


if __name__ == '__main__':
    import sys

    main(file(sys.argv[1]))
