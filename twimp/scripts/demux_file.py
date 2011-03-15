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


from cStringIO import StringIO
import struct

from twimp.chunks import Demuxer, Header
from twimp.utils import GeneratorWrapperProtocol


# temprarily here...
def write_head(s, h):
    csid = h.cs_id
    nmiss = len([x for x in [h.real_time, h.real_type, h.real_ms_id]
                 if x is None])
    htype = nmiss << 6

    if csid > 0x013f:            # 255 + 64
        s.write(struct.pack('>BH', htype | 1, csid - 64))
    elif csid > 0x40:            # 64 :P
        s.write(struct.pack('>BB', htype | 0, csid - 64))
    else:
        s.write(struct.pack('B', htype | csid))

    if nmiss == 2:
        s.write(struct.pack('>HB',
                            (h.real_time >> 8) & 0xffff,
                            h.real_time & 0xff))
    elif nmiss < 2:
        write_time = h.real_time
        if nmiss == 0 and write_time >= 0xffffff:
            write_time = 0xffffff
        s.write(struct.pack('>HBHBB',
                            (write_time >> 8) & 0xffff,
                            write_time & 0xff,
                            (h.real_size >> 8) & 0xffff,
                            h.real_size & 0xff, h.real_type))
        if nmiss == 0:
            s.write(struct.pack('<L', h.real_ms_id))
            if h.real_time > 0xffffff:
                s.write(struct.pack('>L', h.real_time))


class PrintMsgProtocol(GeneratorWrapperProtocol):
    def messageReceived(self, header, body):
        print header, body.peek(len(body)).encode('hex')

class PrintMsgDemuxer(Demuxer):
    def controlMessageReceived(self, header, body):
        print header, body.peek(len(body)).encode('hex')
        Demuxer.controlMessageReceived(self, header, body)

# more verbose printers, unused
class PrintMsgVerboseProtocol(GeneratorWrapperProtocol):
    def messageReceived(self, header, body):
        s = StringIO()
        write_head(s, header)
        print (header, s.getvalue().encode('hex'),
               body.peek(len(body)).encode('hex'))

class PrintMsgVerboseDemuxer(Demuxer):
    def controlMessageReceived(self, header, body):
        s = StringIO()
        write_head(s, header)
        print (header, s.getvalue().encode('hex'),
               body.peek(len(body)).encode('hex'))
        Demuxer.controlMessageReceived(self, header, body)


def main(f, block=4096):
    # skip the handshakes for now
    f.read(1 + 1536 + 1536)

    p = PrintMsgProtocol()
    p.init_handler(PrintMsgDemuxer(p).gen_handler())

    while True:
        data = f.read(block)
        if not data:
            break
        p.dataReceived(data)


if __name__ == '__main__':
    import sys

    # fix header name, to make output diffable against older tools...
    Header.__name__ = 'MHeader'

    main(file(sys.argv[1]))
