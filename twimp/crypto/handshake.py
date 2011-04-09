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


from hashlib import sha256
import hmac
import random
import struct

from twimp.handshake import Handshaker
from twimp.primitives import _s_ulong_b as _s_ulong

from twimp.utils import ms_time

LOG_CATEGORY = 'crypto.hs'
import twimp.log
log = twimp.log.get_logger(LOG_CATEGORY)


def _hmac(key, msg, digestmod=sha256):
    return hmac.new(key, msg, digestmod).digest()


# most constants defined in this module were caught floating around
# the net...
_shared_key_suffix = ('f0eec24a8068bee82e00d0d1029e7e57'
                      '6eec5d2d29806fab93b8e636cfeb31ae').decode('hex')

_fms_key = 'Genuine Adobe Flash Media Server 001'
_full_fms_key = _fms_key + _shared_key_suffix

_fp_key = 'Genuine Adobe Flash Player 001'
_full_fp_key = _fp_key + _shared_key_suffix


def _make_offset_extractor(pos, count, mod, shift):
    return (lambda data: sum(map(ord, data[pos:pos+count])) % mod + shift)

(OFFSET_SCHEME_1, OFFSET_SCHEME_2) = range(2)

schemes = [
    (_make_offset_extractor(8, 4, 728, 12),),
    (_make_offset_extractor(772, 4, 728, 776),),
    ]

# list of offset schemes, sorted by client's min version in descending order
schemes_by_client_ver = [
    ((10,0,32,0), OFFSET_SCHEME_2, (10,0,32,2)),
    ((9,0,115,0), OFFSET_SCHEME_1, (9,0,115,0)),
    ]

def find_client_offset_scheme(version):
    for min_scheme_version, scheme, _cli_version in schemes_by_client_ver:
        if version >= min_scheme_version:
            return scheme
    return None


DEFAULT_SERVER_COMPAT_VERSION = (3, 0, 1, 1)
DEFAULT_CLIENT_COMPAT_VERSION = schemes_by_client_ver[0][2] # the latest one
NO_VERSION = (0, 0, 0, 0)


_s_ts_ver = struct.Struct('>LBBBB')

def generate_random_bytes(count):
    n = random.getrandbits(count * 8)
    fmt = '%%0%dx' % (count * 2)
    return (fmt % n).decode('hex')


class CryptoHandshaker(Handshaker):
    server_compat_version = DEFAULT_SERVER_COMPAT_VERSION
    client_compat_version = DEFAULT_CLIENT_COMPAT_VERSION

    compat_version = None

    strict = True

    def __init__(self, protocol, epoch_base, is_client=False):
        Handshaker.__init__(self, protocol, epoch_base, is_client=is_client)

        self._digest_offset_extractor = None
        if self.is_client:
            self.compat_version = self.client_compat_version

            # find a scheme matching our client version
            scheme = find_client_offset_scheme(self.compat_version)
            if scheme is not None:
                self._digest_offset_extractor = schemes[scheme][0]
            else:
                # otherwise we'll do non-crypto handshake
                self.compat_version = NO_VERSION
        else:
            self.compat_version = self.server_compat_version


    # the following four methods are verbose, *sigh*
    def select_own_key_short(self):
        if self.is_client:
            return _fp_key
        return _fms_key

    def select_own_key(self):
        if self.is_client:
            return _full_fp_key
        else:
            return _full_fms_key

    def select_other_key_short(self):
        if self.is_client:
            return _fms_key
        return _fp_key

    def select_other_key(self):
        if self.is_client:
            return _full_fms_key
        else:
            return _full_fp_key


    def _check_client_scheme(self, scheme, data):
        digest_offset_extractor, = schemes[scheme]

        # get digest offset
        offset = digest_offset_extractor(data)

        # calculate digest
        key = self.select_other_key_short() # assuming we're the server
        digest = _hmac(key, data[:offset] + data[offset+32:])

        return buffer(data, offset, 32) == buffer(digest)


    def discover_client_scheme(self, context):
        ts, v0, v1, v2, v3 = _s_ts_ver.unpack_from(context, 0)
        client_ver = (v0, v1, v2, v3)

        if client_ver == NO_VERSION:
            log.info("no client version - won't use crypto handshake")
            return None

        log.debug('client version: %s', client_ver)

        # non-zero version - let's find out the scheme
        scheme = find_client_offset_scheme(client_ver)
        log.debug('scheme from ver: %s', scheme)
        if scheme is not None:
            if self._check_client_scheme(scheme, context):
                log.debug('verified client scheme: %s', scheme)
                self._digest_offset_extractor = schemes[scheme][0]
                return scheme

        log.debug('trying all known schemes...')
        # the exact version<->scheme match didn't help - try all the
        # other known schemes
        for i in xrange(len(schemes)):
            if i == scheme:
                # we've checked that one already
                continue

            if self._check_client_scheme(i, context):
                log.debug('verified client scheme: %s', i)
                self._digest_offset_extractor = schemes[i][0]
                return i

        log.info("couldn't figure the client scheme out")
        if not self.strict:
            # Why so strict? Let's just use the lowest scheme we know
            log.debug('selecting scheme anyway: %s', OFFSET_SCHEME_1)
            return OFFSET_SCHEME_1

        return None

    def generate_base_request(self):
        return (_s_ts_ver.pack(ms_time(self.epoch_time()),
                               *self.compat_version) +
                generate_random_bytes(self.packet_bytes - 8))

    def generate_request(self, context=None):
        base_request = self.generate_base_request()

        if self.is_client:
            if not self._digest_offset_extractor:
                return base_request
        else:
            scheme = self.discover_client_scheme(context)
            if scheme is None:
                return base_request

        # truncate, to leave space for digest
        request = buffer(base_request, 0, self.packet_bytes - 32)

        # get digest offset
        offset = self._digest_offset_extractor(request)

        # calculate digest
        key = self.select_own_key_short()
        digest = _hmac(key, request)

        # insert digest at the offset and return the whole packet
        return request[:offset] + digest + request[offset:]

    def generate_simple_response(self, request):
        # echo the request, except for bytes 4-7, where we put our timestamp
        return (buffer(request, 0, 4) +
                (buffer(_s_ulong.pack(ms_time(self.epoch_time()))) +
                 buffer(request, 8)))

    def generate_response(self, request):
        if not self._digest_offset_extractor:
            return self.generate_simple_response(request)

        # extract the digest embedded in request
        offset = self._digest_offset_extractor(request)
        req_digest = buffer(request, offset, 32)

        # calculate a digest of the request digest
        key = self.select_own_key()
        digest_key = _hmac(key, req_digest)

        # prepare response
        response = generate_random_bytes(self.packet_bytes - 32)

        # calculate response digest and append it to the response
        digest = _hmac(digest_key, response)
        return response + digest

    def verify_simple_response(self, request, response):
        return (buffer(request, 0, 4) == buffer(response, 0, 4) and
                buffer(request, 8) == buffer(response, 8))

    def verify_response(self, request, response):
        if not self._digest_offset_extractor:
            return self.verify_simple_response(request, response)

        # extract the digest embedded in request
        offset = self._digest_offset_extractor(request)
        req_digest = buffer(request, offset, 32)

        # calculate what would have to be the digest of the request
        # digest calculated by the peer
        key = self.select_other_key()
        digest_key = _hmac(key, req_digest)

        # calculate our version of the response digest
        digest = _hmac(digest_key, buffer(response, 0, self.packet_bytes - 32))

        # print 'verify_response (%s)' % (('relaxed', 'strict')[self.strict])
        # print 'other:', response[-32:].encode('hex')
        # print ' mine:', digest.encode('hex')
        # print '   ==:', response[-32:] == digest

        if not self.strict:
            # in relaxed mode, just say it's all good
            return True

        # the response and our digests should match
        return buffer(response, len(response) - 32) == buffer(digest)
