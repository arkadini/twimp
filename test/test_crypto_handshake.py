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
import time

from twisted.python import failure
from twisted.trial import unittest

from rtmp.crypto import handshake as chandshake
from rtmp.crypto.handshake import CryptoHandshaker
from rtmp.handshake import HandshakeFailedError
from rtmp.utils import GeneratorWrapperProtocol

from helpers import StringTransport


class TestHandshakeProtocol(GeneratorWrapperProtocol):
    def __init__(self, *a, **kw):
        GeneratorWrapperProtocol.__init__(self, *a, **kw)
        self.handshake_status = None

    def handshakeSucceeded(self, init_ts, hs_delay):
        self.handshake_status = 'ok'

    def handshakeFailed(self):
        self.handshake_status = 'fail'


class TestHandshaker(unittest.TestCase):
    protocol_version = 3
    handshake_length = 1536

    def test_complete_exchange_with_cli_ver_9_0_115_0(self):
        class GeneratedHandshaker(CryptoHandshaker):
            client_compat_version = (9,0,115,0)
        return self._test_complete_exchange(GeneratedHandshaker)

    def test_complete_exchange_with_cli_ver_10_0_32_2(self):
        class GeneratedHandshaker(CryptoHandshaker):
            client_compat_version = (10,0,32,2)
        return self._test_complete_exchange(GeneratedHandshaker)

    def test_complete_exchange_with_cli_ver_0(self):
        class GeneratedHandshaker(CryptoHandshaker):
            client_compat_version = (0, 0, 0, 0)
        return self._test_complete_exchange(GeneratedHandshaker)

    def test_complete_exchange_with_cli_ver_too_low(self):
        # here a class imitating a client whose version is lower than
        # the earlies one supporting crypted handshakes, but still
        # claiming some version and performing a valid crypto
        # handshake (with the newest offset scheme)
        class GeneratedHandshaker(CryptoHandshaker):
            def __init__(self, *a, **kw):
                CryptoHandshaker.__init__(self, *a, **kw)
                self.compat_version = (1, 1, 1, 1)
                self._digest_offset_extractor = chandshake.schemes[-1][0]

        return self._test_complete_exchange(GeneratedHandshaker)

    def test_complete_exchange_with_cli_ver_too_high(self):
        # here a class imitating a client whose version is higher than
        # all that we know support crypted handshakes, but performing
        # a valid crypto handshake (just with the oldest offset scheme)
        class GeneratedHandshaker(CryptoHandshaker):
            def __init__(self, *a, **kw):
                CryptoHandshaker.__init__(self, *a, **kw)
                self.compat_version = (255, 0, 3, 2)
                self._digest_offset_extractor = chandshake.schemes[0][0]

        return self._test_complete_exchange(GeneratedHandshaker)

    def test_complete_exchange_with_cli_unknown_scheme(self):
        # here a class imitating a client with an offset scheme that
        # we don't know about
        class GeneratedHandshaker(CryptoHandshaker):
            def __init__(self, *a, **kw):
                CryptoHandshaker.__init__(self, *a, **kw)
                self.compat_version = (255, 254, 253, 252)
                x = chandshake._make_offset_extractor(22, 6, 728, 20)
                self._digest_offset_extractor = x

        return self._test_complete_exchange(GeneratedHandshaker,
                                            client_failure=True,
                                            server_failure=True)

    def test_complete_exchange_with_cli_unknown_key_1(self):
        # here a class imitating a client who generates digests with an
        # unknown key

        _fp_key = 'Haxored Adobe Flash Player 777'
        _full_fp_key = _fp_key + chandshake._shared_key_suffix

        class GeneratedHandshaker(CryptoHandshaker):
            def select_own_key_short(self):
                if self.is_client:
                    return _fp_key
                return CryptoHandshaker.select_own_key_short(self)

            def select_own_key(self):
                if self.is_client:
                    return _full_fp_key
                return CryptoHandshaker.select_own_key(self)

        return self._test_complete_exchange(GeneratedHandshaker,
                                            client_failure=True,
                                            server_failure=True)

    def test_complete_exchange_with_cli_unknown_key_2(self):
        # here a class imitating a client who generates digests with a
        # key that has unknown shared part

        _full_fp_key = chandshake._fp_key + (' ' * 32)

        class GeneratedHandshaker(CryptoHandshaker):
            def select_own_key(self):
                if self.is_client:
                    return _full_fp_key
                return CryptoHandshaker.select_own_key(self)

        return self._test_complete_exchange(GeneratedHandshaker,
                                            server_failure=True)


    def _test_complete_exchange(self, client_handshaker_class,
                                client_failure=False, server_failure=False):
        tcli = StringTransport()
        tsrv = StringTransport()

        p_cli = TestHandshakeProtocol()
        p_srv = TestHandshakeProtocol()

        now = time.time()
        hs_cli = client_handshaker_class(p_cli, now - 0.001, is_client=True)
        hs_srv = CryptoHandshaker(p_srv, now - 0.042)


        p_srv.init_handler(hs_srv.gen_handler())
        p_cli.init_handler(hs_cli.gen_handler(), do_init=False)

        p_srv.makeConnection(tsrv)
        self.assertEquals(tsrv.value(), '')

        p_cli.makeConnection(tcli)
        self.assertEquals(tcli.value(), '')

        p_cli.init_handler()

        # self.assertEquals(len(tcli.value()), 1 + 1536)
        err = p_srv.dataReceived(tcli.value())
        tcli.clear()

        # self.assertEquals(len(tsrv.value()), 1 + 1536 + 1536)
        self.assertIdentical(err, None)
        self.assertEquals(p_cli.handshake_status, None)

        err = p_cli.dataReceived(tsrv.value() + '\x00')
        tsrv.clear()

        if client_failure:
            self.assertIsInstance(err, failure.Failure)
            self.assertIsInstance(err.value, HandshakeFailedError)
            self.assertFalse(tcli.disconnecting, 'should not be disconnecting')
            self.assertEquals(p_cli.handshake_status, 'fail')
            self.assertEquals(p_srv.handshake_status, None)
        else:
            self.assertIdentical(err, None)
            self.assertFalse(tcli.disconnecting, 'disconnecting')
            self.assertEquals(p_cli.handshake_status, 'ok')
            self.assertEquals(p_srv.handshake_status, None)


        err = p_srv.dataReceived(tcli.value() + '\x00')
        tcli.clear()

        if server_failure:
            self.assertIsInstance(err, failure.Failure)
            self.assertIsInstance(err.value, HandshakeFailedError)
            self.assertFalse(tsrv.disconnecting, 'should not be disconnecting')
            self.assertEquals(p_srv.handshake_status, 'fail')
        else:
            self.assertIdentical(err, None)
            self.assertFalse(tsrv.disconnecting, 'disconnecting')
            self.assertEquals(p_srv.handshake_status, 'ok')


    def test_server_side_ok(self):
        t = StringTransport()

        p = TestHandshakeProtocol()

        now = time.time()
        hs = CryptoHandshaker(p, now)

        p.init_handler(hs.gen_handler())

        p.makeConnection(t)
        # server shouldn't respond before spoken to
        self.assertEquals(t.value(), '')

        ver_pkt = struct.pack('B', self.protocol_version)

        err = p.dataReceived(ver_pkt)

        self.assertIdentical(err, None)

        # server may wait until it receives handshake packet before
        # responding, so not testing here

        hs_rand_data = '.' * (self.handshake_length - 8)
        timestamp = 42
        hs_pkt = struct.pack('>LL', timestamp, 0) + hs_rand_data

        err = p.dataReceived(hs_pkt)

        self.assertIdentical(err, None)
        # ... and here the server might send all the packets already,
        # and our implementation should do so, so that's what we test
        self.assert_(len(t.value()) == 1 + self.handshake_length * 2,
                     'Response too short (%d)' % len(t.value()))

        self.assertEquals(struct.unpack_from('B', t.value(), 0)[0],
                          self.protocol_version)

        srv_hs_pkt = t.value()[1:1+self.handshake_length]
        hs_pkt_echo = t.value()[1+self.handshake_length:
                                    1+self.handshake_length*2]

        self.assertEquals(struct.unpack_from('>L', hs_pkt_echo, 0)[0],
                          timestamp)
        self.assertEquals(hs_pkt_echo[8:], hs_rand_data)

        # server response ok, let's finish the server side, and
        # pretend client starts sending more data

        err = p.dataReceived(srv_hs_pkt + '\x00')

        self.assertIdentical(err, None)
        self.assertFalse(t.disconnecting, 'disconnecting')
        self.assertEquals(p.handshake_status, 'ok')

    def test_server_side_invalid_1(self):
        t = StringTransport()

        p = TestHandshakeProtocol()

        now = time.time()
        hs = CryptoHandshaker(p, now)

        p.init_handler(hs.gen_handler())

        p.makeConnection(t)
        self.assertEquals(t.value(), '')

        # sending version different from the server's version
        ver_pkt = struct.pack('B', self.protocol_version + 1)

        err = p.dataReceived(ver_pkt)

        self.assertIsInstance(err, failure.Failure)
        self.assertIsInstance(err.value, HandshakeFailedError)
        self.assertFalse(t.disconnecting, 'should not be disconnecting')
        self.assertEquals(p.handshake_status, 'fail')

    def test_server_side_invalid_2(self):
        t = StringTransport()

        p = TestHandshakeProtocol()

        now = time.time()
        hs = CryptoHandshaker(p, now)

        p.init_handler(hs.gen_handler())

        p.makeConnection(t)
        self.assertEquals(t.value(), '')

        ver_pkt = struct.pack('B', self.protocol_version)
        invalid_hs_data = (('abcdefg' * int(self.handshake_length * 2 / 7)) +
                           'abcdefg'[:int(self.handshake_length * 2 % 7)])

        # version is ok
        err = p.dataReceived(ver_pkt)
        self.assertIdentical(err, None)
        # ... but the rest is just (the right amount of) rubbish
        err = p.dataReceived(invalid_hs_data)

        self.assertIsInstance(err, failure.Failure)
        self.assertIsInstance(err.value, HandshakeFailedError)
        self.assertFalse(t.disconnecting, 'should not be disconnecting')
        self.assertEquals(p.handshake_status, 'fail')

    # more tests, especially client side, would be nice but
    # duplicating crypto code here seems to make little sense
