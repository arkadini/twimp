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


from twisted.internet import defer, error, reactor

from twimp.amf0 import encode as encode_amf, decode as decode_amf, Object
from twimp import chunks
from twimp import const
from twimp.dispatch import CommandDispatchProtocol
from twimp.dispatch import EventDispatchProtocol
from twimp.dispatch import CallDispatchProtocol
from twimp.error import UnexpectedStatusError, ProtocolContractError
from twimp.error import CommandResultError
from twimp.error import CallAbortedException

from twimp.helpers import vb, vb_clone

from test.test_proto import _ProtocolTestBase, mixin
from test.test_proto import TestDemuxerMixin, TestMuxer, TestHandshaker

from test.common import ArtificialError, ArtificialRemoteError
from test.helpers import muxer_messages, unvb


class TestCommandDispatchProtocol(_ProtocolTestBase):
    timeout = 10

    # note: CDP tests ideally should be performed with a controllable
    # reactor, but the CDP code would need to be adapted, so for now
    # just relying on ordered scheduling of callLater() calls...

    def make_protocol_class(self):
        tself = self
        CDP = CommandDispatchProtocol
        class TCommandDispatchProtocol(CDP):
            demuxer_class = mixin(CDP.demuxer_class, TestDemuxerMixin)
            muxer_class = TestMuxer
            handshaker_class = TestHandshaker

            def unknownMessageType(self, header, body):
                bclone = vb_clone(body)
                tself.messages.append(('unk_msg', header, unvb(bclone)))
                CDP.unknownMessageType(self, header, body)

            def unknownCommandType(self, cmd, ts, ms_id, args):
                tself.messages.append(('unk_cmd', cmd, ts, ms_id, args))

            def unexpectedCallResult(self, ts, ms_id, trans_id, args):
                tself.messages.append(('_result', ts, ms_id, trans_id, args))

            def unexpectedCallError(self, ts, ms_id, trans_id, args):
                tself.messages.append(('_error', ts, ms_id, trans_id, args))

            # some custom command handlers
            def command_foobar(self, ts, ms_id, trans_id, arg1, arg2):
                tself.messages.append(('foobar', ts, ms_id, trans_id,
                                       arg1, arg2))

            def command_breakdown(self, ts, ms_id, trans_id, *args):
                raise ArtificialError('breakdown on request')

        return TCommandDispatchProtocol

    def test_command_simple(self):
        p, t, dmx, mux = self.build_proto()

        dmx.inject(3, 0, const.RTMP_COMMAND, 1,
                   encode_amf('foobar', 0, 'foo', 'bar'))

        d = defer.Deferred()
        d.addCallback(lambda _: self.assertEquals(self.messages,
                                                  [('foobar', 0, 1, 0.0,
                                                    'foo', 'bar')]))

        reactor.callLater(0, d.callback, None)
        return d

    def test_command_simple_after_exception(self):
        p, t, dmx, mux = self.build_proto()

        dmx.inject(3, 0, const.RTMP_COMMAND, 1,
                   encode_amf('breakdown', 0, 'burn', 'baby', 'burn'))
        dmx.inject(3, 0, const.RTMP_COMMAND, 1,
                   encode_amf('foobar', 0, 'foo', 'bar'))

        d = defer.Deferred()
        d.addCallback(lambda _: self.assertEquals(self.messages,
                                                  [('foobar', 0, 1, 0.0,
                                                    'foo', 'bar')]))

        def check_clean_logs(result):
            # we know the breakdown command handler raises an
            # exception, so let's clean it so the test can pass, but
            # verify it was actually logged, and only once, while
            # cleaning, too
            self.assertEquals(len(self.flushLoggedErrors(ArtificialError)),
                              1)
            return result
        d.addBoth(check_clean_logs)

        reactor.callLater(0, d.callback, None)
        return d

    def test_command_unknown(self):
        p, t, dmx, mux = self.build_proto()

        dmx.inject(3, 0, const.RTMP_COMMAND, 1,
                   encode_amf('method_not_there_for_sure', 0, 'a', 'rrrr'))

        d = defer.Deferred()
        def verify_unknown(result):
            self.assertEquals(self.messages,
                              [('unk_cmd', 'method_not_there_for_sure',
                                0, 1, [0.0, 'a', 'rrrr'])])
            return result
        d.addCallback(verify_unknown)

        reactor.callLater(0, d.callback, None)
        return d

    def test_unexpectedCallResult(self):
        p, t, dmx, mux = self.build_proto()

        dmx.inject(3, 0, const.RTMP_COMMAND, 1,
                   encode_amf('_result', 2, None, 'Surprise!'))

        d = defer.Deferred()
        def verify_unexpected(result):
            self.assertEquals(self.messages,
                              [('_result', 0, 1, 2.0, (None, 'Surprise!'))])
            return result
        d.addCallback(verify_unexpected)

        reactor.callLater(0, d.callback, None)
        return d

    def test_unexpectedCallError(self):
        p, t, dmx, mux = self.build_proto()

        dmx.inject(3, 0, const.RTMP_COMMAND, 1,
                   encode_amf('_error', 2, None, 'Surprise!'))

        d = defer.Deferred()
        def verify_unexpected(result):
            self.assertEquals(self.messages,
                              [('_error', 0, 1, 2.0, (None, 'Surprise!'))])
            return result
        d.addCallback(verify_unexpected)

        reactor.callLater(0, d.callback, None)
        return d

    def test_callRemote_result(self):
        p, t, dmx, mux = self.build_proto()

        d = p.callRemote(1, 'echo', 'sing it back')

        self.assertEquals(len(mux.messages), 1)
        msg = mux.messages[0]
        self.assertEquals(msg[0:3] + msg[4:],
                          (0, chunks.MSG_COMMAND, 1, False))

        # can't rely on transaction number, so we skip it
        decoded = decode_amf(vb(msg[3]))
        self.assertEquals((decoded[0], decoded[2]), ('echo', 'sing it back'))

        dmx.inject(3, 0, const.RTMP_COMMAND, 1,
                   encode_amf('_result', decoded[1], None, decoded[2]))

        d.addCallback(self.assertEquals, (None, 'sing it back'))

        return d

    def test_callRemote_error(self):
        p, t, dmx, mux = self.build_proto()

        d = p.callRemote(1, 'echo', 'sing it back')

        self.assertEquals(len(mux.messages), 1)
        msg = mux.messages[0]
        self.assertEquals(msg[0:3] + msg[4:],
                          (0, chunks.MSG_COMMAND, 1, False))

        # can't rely on transaction number, so we skip it
        decoded = decode_amf(vb(msg[3]))
        self.assertEquals((decoded[0], decoded[2]), ('echo', 'sing it back'))

        err_info = Object(code='Failed', level='error', desc='no echo method')
        dmx.inject(3, 0, const.RTMP_COMMAND, 1,
                   encode_amf('_error', decoded[1], None, err_info))

        self.assertFailure(d, CommandResultError)
        d.addCallback(lambda r: self.assertEquals(r.args, (None, err_info)))

        return d

    def test_callRemote_disconnect(self):
        p, t, dmx, mux = self.build_proto()

        d = p.callRemote(1, 'echo', 'sing it back')

        self.assertEquals(len(mux.messages), 1)
        msg = mux.messages[0]
        self.assertEquals(msg[0:3] + msg[4:],
                          (0, chunks.MSG_COMMAND, 1, False))

        # can't rely on transaction number, so we skip it
        decoded = decode_amf(vb(msg[3]))
        self.assertEquals((decoded[0], decoded[2]), ('echo', 'sing it back'))

        p.connectionLost()

        self.assertFailure(d, error.ConnectionDone)
        return d


info1 = Object(code='Foo.Code', level='status', desc='description1')
info2 = Object(code='Bar.Code', level='status', desc='description2')

class TestEventDispatchProtocol(_ProtocolTestBase):
    timeout = 10

    def make_protocol_class(self):
        tself = self
        EDP = EventDispatchProtocol
        class TEventDispatchProtocol(EDP):
            demuxer_class = mixin(EDP.demuxer_class, TestDemuxerMixin)
            muxer_class = TestMuxer
            handshaker_class = TestHandshaker

            def unhandledOnStatus(self, ts, ms_id, info):
                tself.messages.append(('unhandled', ts, ms_id, info))

        return TEventDispatchProtocol

    def test_waitStatus(self):
        p, t, dmx, mux = self.build_proto()

        # wait for event with specific code
        d = p.waitStatus(1, info1.code)
        d.addCallback(self.assertEquals, info1)

        # then wait for any event on message stream 1
        d.addCallback(lambda _: p.waitStatus(1, None))
        d.addCallback(self.assertEquals, info2)

        dmx.inject(3, 0, const.RTMP_COMMAND, 1,
                   encode_amf('onStatus', 0, None, info1))
        dmx.inject(3, 0, const.RTMP_COMMAND, 1,
                   encode_amf('onStatus', 0, None, info2))

        return d

    def test_waitStatus_mixed(self):
        p, t, dmx, mux = self.build_proto()

        # wait for event with specific code, on message stream 2
        d1 = p.waitStatus(2, info1.code)
        d1.addCallback(self.assertEquals, info1)

        # also wait for any event on message stream 1
        d2 = p.waitStatus(1, None)
        d2.addCallback(self.assertEquals, info2)

        dmx.inject(3, 0, const.RTMP_COMMAND, 1,
                   encode_amf('onStatus', 0, None, info2))
        dmx.inject(3, 0, const.RTMP_COMMAND, 2,
                   encode_amf('onStatus', 0, None, info1))

        return defer.DeferredList([d1, d2], fireOnOneErrback=1)

    def test_waitStatus_unexpected(self):
        p, t, dmx, mux = self.build_proto()

        code_other = 'Baz.Code'

        # wait for event with specific code
        d = p.waitStatus(1, info1.code)
        d.addCallback(self.assertEquals, info1)

        # wait for another even, with some other specific code
        d.addCallback(lambda _: p.waitStatus(1, code_other))
        self.assertFailure(d, UnexpectedStatusError)

        dmx.inject(3, 0, const.RTMP_COMMAND, 1,
                   encode_amf('onStatus', 0, None, info1))
        dmx.inject(3, 0, const.RTMP_COMMAND, 1,
                   encode_amf('onStatus', 0, None, info2))

        return d

    def test_waitStatus_unhandled(self):
        p, t, dmx, mux = self.build_proto()

        # wait for event with specific code, on message stream 1
        d = p.waitStatus(1, info2.code)
        d.addCallback(self.assertEquals, info2)
        unhandled = ('unhandled', 0, 2, info1)
        d.addCallback(lambda _: self.assertEquals(self.messages, [unhandled]))

        # first event sent on ms_id 2
        dmx.inject(3, 0, const.RTMP_COMMAND, 2,
                   encode_amf('onStatus', 0, None, info1))
        # ... and the next one, on ms_id 1
        dmx.inject(3, 0, const.RTMP_COMMAND, 1,
                   encode_amf('onStatus', 0, None, info2))

        return d

    def test_waitStatus_no_code(self):
        p, t, dmx, mux = self.build_proto()

        # the following info object lacks 'code' field
        wrong_info = Object(level='status', desc='description2')

        # wait for any event
        d = p.waitStatus(1, None)
        self.assertFailure(d, ProtocolContractError)

        dmx.inject(3, 0, const.RTMP_COMMAND, 1,
                   encode_amf('onStatus', 0, None, wrong_info))

        return d

    def test_waitStatus_disconnected(self):
        p, t, dmx, mux = self.build_proto()

        # wait for any event
        d = p.waitStatus(1, None)
        self.assertFailure(d, error.ConnectionDone)

        p.connectionLost()

        return d


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
