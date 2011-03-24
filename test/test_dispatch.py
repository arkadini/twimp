#   Copyright (c) 2011  Arek Korbik
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


from twisted.internet import defer, reactor

from twimp.amf0 import encode as encode_amf, Object
from twimp import chunks
from twimp import const
from twimp.dispatch import CallDispatchProtocol
from twimp.error import CallAbortedException

from test.test_proto import _ProtocolTestBase, mixin
from test.test_proto import TestDemuxerMixin, TestMuxer, TestHandshaker

from test.common import ArtificialRemoteError
from test.helpers import muxer_messages


class TestCallDispatchProtocol(_ProtocolTestBase):
    def make_protocol_class(self):
        tself = self
        CDP = CallDispatchProtocol
        class TCallDispatchProtocol(CDP):
            demuxer_class = mixin(CDP.demuxer_class, TestDemuxerMixin)
            muxer_class = TestMuxer
            handshaker_class = TestHandshaker

            # override, so the timestamps in results are predictable
            def session_time(self):
                return 0

            def unknownRemoteCall(self, cmd, ts, ms_id, args):
                tself.messages.append(('unknown', cmd, ts, ms_id, args))
                CDP.unknownRemoteCall(self, cmd, ts, ms_id, args)

            def command_baz(self, ts, ms_id, trans_id, arg1, arg2):
                tself.messages.append(('c_baz', ts, ms_id, trans_id,
                                       arg1, arg2))

            def remote_baz(self, ts, ms_id, arg1, arg2):
                # this method will never be called, command_baz will
                # be dispatched to always before
                tself.messages.append(('r_baz', ts, ms_id, arg1, arg2))

            def remote_foo(self, ts, ms_id, arg1, arg2):
                tself.messages.append(('foo', ts, ms_id, arg1, arg2))
                return 'f', 'o', 'o'

            def remote_bar(self, ts, ms_id, *args):
                tself.messages.append(('bar', ts, ms_id, args))
                # implicit None returned

            def remote_breakdown1(self, ts, ms_id, *args):
                tself.messages.append(('!1', ts, ms_id, args))
                raise ArtificialRemoteError('brokendo')

            def remote_breakdown2(self, ts, ms_id, *args):
                tself.messages.append(('!2', ts, ms_id, args))
                raise RuntimeError('brokendo')

            def remote_breakdown3(self, ts, ms_id, *args):
                tself.messages.append(('!3', ts, ms_id, args))
                raise ArtificialRemoteError('broken, bye', fatal=True)

            def remote_breakdown4(self, ts, ms_id, *args):
                tself.messages.append(('!4', ts, ms_id, args))
                raise CallAbortedException("don't send anything")

        return TCallDispatchProtocol

    def test_call_unknown(self):
        p, t, dmx, mux = self.build_proto()

        # not expecting a return value:
        dmx.inject(3, 0, const.RTMP_COMMAND, 1,
                   encode_amf('method_not_there_for_sure', 0, 'foo'))

        # expecting a return value:
        dmx.inject(3, 0, const.RTMP_COMMAND, 1,
                   encode_amf('method_not_there_for_sure', 1, 'bar'))

        def check_messages(_result):
            self.assertEquals(self.messages,
                              [('unknown', 'method_not_there_for_sure',
                                0, 1, ['foo']),
                               ('unknown', 'method_not_there_for_sure',
                                0, 1, ['bar'])])

        def verify_sent_messages(_result):
            msgs = muxer_messages(mux)
            self.assertEquals(msgs, [])

        d = defer.Deferred()
        d.addCallback(check_messages)
        d.addCallback(verify_sent_messages)
        d.addCallback(lambda _: self.assertFalse(t.disconnecting))

        reactor.callLater(0, d.callback, None)
        return d

    def test_call_comand(self):
        p, t, dmx, mux = self.build_proto()

        dmx.inject(3, 0, const.RTMP_COMMAND, 1,
                   encode_amf('baz', 0, 'a', 'b'))

        def check_messages(_result):
            self.assertEquals(self.messages,
                              [('c_baz', 0, 1, 0.0, 'a', 'b')])

        def verify_sent_messages(_result):
            self.assertEquals(mux.messages, [])

        d = defer.Deferred()
        d.addCallback(check_messages)
        d.addCallback(verify_sent_messages)

        reactor.callLater(0, d.callback, None)
        return d

    def test_call_with_return(self):
        p, t, dmx, mux = self.build_proto()

        dmx.inject(3, 0, const.RTMP_COMMAND, 1,
                   encode_amf('foo', 1, 'a', 'b'))

        def check_messages(_result):
            self.assertEquals(self.messages,
                              [('foo', 0, 1, 'a', 'b')])

        def verify_sent_messages(_result):
            msgs = muxer_messages(mux)
            self.assertEquals(msgs, [(0, 6, 1,
                                      ['_result', 1.0, 'f', 'o', 'o'],
                                      False)])

        d = defer.Deferred()
        d.addCallback(check_messages)
        d.addCallback(verify_sent_messages)

        reactor.callLater(0, d.callback, None)
        return d

    def test_call_without_return(self):
        p, t, dmx, mux = self.build_proto()

        dmx.inject(3, 0, const.RTMP_COMMAND, 1,
                   encode_amf('foo', 0, 'a', 'b'))

        def check_messages(_result):
            self.assertEquals(self.messages,
                              [('foo', 0, 1, 'a', 'b')])

        def verify_sent_messages(_result):
            self.assertEquals(mux.messages, [])

        d = defer.Deferred()
        d.addCallback(check_messages)
        d.addCallback(verify_sent_messages)

        reactor.callLater(0, d.callback, None)
        return d

    def test_call_with_implicit_none_return(self):
        p, t, dmx, mux = self.build_proto()

        dmx.inject(3, 0, const.RTMP_COMMAND, 1,
                   encode_amf('bar', 1))

        def check_messages(_result):
            self.assertEquals(self.messages,
                              [('bar', 0, 1, ())])

        def verify_sent_messages(_result):
            msgs = muxer_messages(mux)
            self.assertEquals(msgs, [(0, 6, 1, ['_result', 1.0, None], False)])

        d = defer.Deferred()
        d.addCallback(check_messages)
        d.addCallback(verify_sent_messages)

        reactor.callLater(0, d.callback, None)
        return d

    def _test_call_raising_error_1(self, trans_id):
        p, t, dmx, mux = self.build_proto()

        dmx.inject(3, 0, const.RTMP_COMMAND, 1,
                   encode_amf('breakdown1', trans_id))

        def check_messages(_result):
            self.assertEquals(self.messages,
                              [('!1', 0, 1, ())])

        def verify_sent_messages(_result):
            msgs = muxer_messages(mux)
            error_tuple = ('Artificially.Failed', 'error')

            self.assertEquals(len(msgs), 1)
            msg = msgs[0]

            self.assertEquals(msg[0:3] + msg[4:],
                              (0, chunks.MSG_COMMAND, 1, False))
            args = msg[3]
            self.assertEquals(len(args), 4)
            self.assertEquals(args[:3], ['_error', trans_id, None])
            self.assertIsInstance(args[3], Object)
            self.assertEquals((args[3].code, args[3].level),
                              error_tuple)

        d = defer.Deferred()
        d.addCallback(check_messages)
        d.addCallback(verify_sent_messages)
        d.addCallback(lambda _: self.assertFalse(t.disconnecting))

        reactor.callLater(0, d.callback, None)
        return d

    def test_call_raising_error_1_unexpected(self):
        # using trans_id = 0, so the result of the call is not expected
        return self._test_call_raising_error_1(0)

    def test_call_raising_error_1_expected(self):
        # using trans_id = 1 - the result of the call is expected
        return self._test_call_raising_error_1(1)

    def test_call_raising_error_2(self):
        p, t, dmx, mux = self.build_proto()

        dmx.inject(3, 0, const.RTMP_COMMAND, 1,
                   encode_amf('breakdown2', 0))

        def check_messages(_result):
            self.assertEquals(self.messages,
                              [('!2', 0, 1, ())])

        def verify_sent_messages(_result):
            msgs = muxer_messages(mux)
            error_tuple = ('NetStream.Failed', 'error')

            self.assertEquals(len(msgs), 1)
            msg = msgs[0]

            self.assertEquals(msg[0:3] + msg[4:],
                              (0, chunks.MSG_COMMAND, 1, False))
            args = msg[3]
            self.assertEquals(len(args), 4)
            self.assertEquals(args[:3], ['_error', 0.0, None])
            self.assertIsInstance(args[3], Object)
            self.assertEquals((args[3].code, args[3].level),
                              error_tuple)

        d = defer.Deferred()
        d.addCallback(check_messages)
        d.addCallback(verify_sent_messages)
        d.addCallback(lambda _: self.assertFalse(t.disconnecting))


        reactor.callLater(0, d.callback, None)
        return d

    def test_call_raising_error_fatal(self):
        p, t, dmx, mux = self.build_proto()

        dmx.inject(3, 0, const.RTMP_COMMAND, 1,
                   encode_amf('breakdown3', 0))

        def check_messages(_result):
            self.assertEquals(self.messages,
                              [('!3', 0, 1, ())])

        def verify_sent_messages(_result):
            msgs = muxer_messages(mux)
            error_tuple = ('Artificially.Failed', 'error')

            self.assertEquals(len(msgs), 1)
            msg = msgs[0]

            self.assertEquals(msg[0:3] + msg[4:],
                              (0, chunks.MSG_COMMAND, 1, False))
            args = msg[3]
            self.assertEquals(len(args), 4)
            self.assertEquals(args[:3], ['_error', 0.0, None])
            self.assertIsInstance(args[3], Object)
            self.assertEquals((args[3].code, args[3].level),
                              error_tuple)

        d = defer.Deferred()
        d.addCallback(check_messages)
        d.addCallback(verify_sent_messages)
        d.addCallback(lambda _: self.assertTrue(t.disconnecting))

        reactor.callLater(0, d.callback, None)
        return d

    def test_call_aborted(self):
        p, t, dmx, mux = self.build_proto()

        dmx.inject(3, 0, const.RTMP_COMMAND, 1,
                   encode_amf('breakdown4', 1))

        def check_messages(_result):
            self.assertEquals(self.messages,
                              [('!4', 0, 1, ())])

        def verify_sent_messages(_result):
            self.assertEquals(mux.messages, [])

        d = defer.Deferred()
        d.addCallback(check_messages)
        d.addCallback(verify_sent_messages)
        d.addCallback(lambda _: self.assertFalse(t.disconnecting))

        reactor.callLater(0, d.callback, None)
        return d
