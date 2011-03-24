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


import random
import struct
import time

from twisted.internet.error import ConnectError

from primitives import _s_uchar
from primitives import _s_ulong_b as _s_ulong
from utils import ms_time


_s_ts_simpver = struct.Struct('>LL')

def generate_random_bytes(count):
    n = random.getrandbits(count * 8)
    fmt = '%%0%dx' % (count * 2)
    return (fmt % n).decode('hex')


class HandshakeFailedError(ConnectError):
    """Handshake failed"""


class Handshaker(object):
    """The handshake protocol.

    Interaction should go something like this:
      * cl: ()->v+CLHS sent->(v+SVHS rcvd)->SVHS sent->(CLHS rcvd) OK!
      * sv: ()->(v rcvd)->v+SVHS sent->(CLHS rcvd)->CLHS sent->(SVHS rcvd) OK!

    where:
      * v    - version packet
      * SVHS - server handshake packet
      * CLHS - client handshake packet
    """

    packet_bytes = 1536
    protocol_version = 3

    def __init__(self, protocol, epoch_base, is_client=False):
        self.epoch = epoch_base
        self.version_packet = _s_uchar.pack(self.protocol_version)
        self.protocol = protocol
        self.is_client = is_client

    def epoch_time(self):
        return time.time() - self.epoch


    def defaultHandshakeFailed(self, msg=''):
        self.protocol.handshakeFailed()
        # raising exception should cause closing the connection eventually
        raise HandshakeFailedError(string=msg)


    def version_supported(self, version):
        return version == self.protocol_version

    def generate_request(self, context=None):
        return (_s_ts_simpver.pack(ms_time(self.epoch_time()), 0) +
                generate_random_bytes(self.packet_bytes - 8))

    def generate_response(self, request):
        return (buffer(request, 0, 4) +
                (buffer(_s_ulong.pack(ms_time(self.epoch_time()))) +
                 buffer(request, 8)))

    def verify_response(self, request, response):
        return (buffer(request, 0, 4) == buffer(response, 0, 4) and
                buffer(request, 8) == buffer(response, 8))

    def gen_handler(self):
        p = self.protocol

        if self.is_client:
            ts_init = time.time()
            hs_mine = self.generate_request()
            p.transport.writeSequence((self.version_packet, hs_mine))

        # wait for the "version packet"
        s = yield 1
        version_other, = _s_uchar.unpack(s.read(1))

        if not self.version_supported(version_other):
            self.defaultHandshakeFailed('protocol version mismatch')
            return

        # now, the primary handshake packet
        s = yield self.packet_bytes

        hs_other = s.read(self.packet_bytes)

        if not self.is_client:
            ts_init = time.time()
            hs_mine = self.generate_request(context=hs_other)
            p.transport.writeSequence((self.version_packet, hs_mine))

        p.transport.write(self.generate_response(hs_other))

        # and now, the handshake response
        s = yield self.packet_bytes

        hs_response = buffer(s.read(self.packet_bytes))
        ts_received_back = time.time()

        if not self.verify_response(hs_mine, hs_response):
            self.defaultHandshakeFailed('response verification failed')
            return

        p.handshakeSucceeded(ts_init - self.epoch, ts_received_back - ts_init)
