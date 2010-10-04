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


import time

from twisted.internet import defer, reactor

from rtmp.amf0 import encode as encode_amf, decode as decode_amf, Object
from rtmp import chunks
from rtmp import const
from rtmp.server import appserver
from rtmp.server.appserver import CallDispatchProtocol, CallResultError
from rtmp.server.appserver import AppDispatchServerProtocol
from rtmp.server.appserver import AppDispatchServerFactory
from rtmp.server.appserver import CallResultError, CallAbortedException

from rtmp.helpers import vb

from test.test_proto import _ProtocolTestBase, mixin
from test.test_proto import TestDemuxerMixin, TestMuxer, TestHandshaker


class ArtificialRemoteError(CallResultError):
    """Completely artificial test error"""
    code = 'Artificially.Failed'

def muxer_messages(mux):
    return [(m[0], m[1], m[2],
             (decode_amf(vb(m[3]))
              if m[1] in (chunks.MSG_COMMAND, chunks.MSG_DATA)
              else m[3]),
             m[4])
            for m in mux.messages]

def wait(result=None, delay=0):
    d = defer.Deferred()
    reactor.callLater(delay, d.callback, result)
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


class TestApp(object):
    def __init__(self, protocol, server):
        self.protocol = protocol
        self.server = server

        self.connected = False
        self.disconnected = False

    def connect(self, request, req_opts):
        self.connected = True
        return (True, True)

    def play(self, net_stream, stream_name, start=-2, duration=-1,
             reset=True):
        pass

    def publish(self, net_stream, stream_name, publish_type):
        pass

    def connectionLost(self, reason):
        self.disconnected = True

    def remote_echo(self, ts, net_stream, arg):
        net_stream.signal('onEcho', arg)
        return arg

    def remote_raising(self, ts, net_stream, arg):
        raise ArtificialRemoteError('hereyouare')

    def remote_error(self, ts, net_stream, arg):
        raise Exception('oops')


class TestAppFailures(TestApp):
    def connect(self, request, req_opts):
        # programmer's error:
        raise Exception('foo')


class TAppDispatchServerFactory(AppDispatchServerFactory):
    def get_app_factory(self, app_path):
        if app_path == 'fail':
            return TestAppFailures, (), {}
        elif app_path != 'invalid':
            return TestApp, (), {}
        return None

class TestAppDispatchServerProtocol(_ProtocolTestBase):
    def make_factory(self):
        f = TAppDispatchServerFactory(0, None)
        return f

    def assertErrorMatches(self, decoded_error, match):
        msg = decoded_error
        self.assertEquals(msg[0:3] + msg[4:], match[0:3] + match[4:])

        error_args = msg[3]
        self.assertEquals(len(error_args), 4)
        self.assertEquals(error_args[:3], match[3][:3])
        self.assertIsInstance(error_args[3], Object)
        self.assertEquals((error_args[3].code, error_args[3].level),
                          match[3][3])

    def assertErrorsMatch(self, decoded_errors, matches):
        self.assertEquals(len(decoded_errors), len(matches))
        for e, m in zip(decoded_errors, matches):
            self.assertErrorMatches(e, m)

    def make_protocol_class(self):
        tself = self
        ADSP = AppDispatchServerProtocol
        class TAppDispatchServerProtocol(ADSP):
            demuxer_class = mixin(ADSP.demuxer_class, TestDemuxerMixin)
            muxer_class = TestMuxer
            handshaker_class = TestHandshaker

            # make message timestamps predictable
            def session_time(self):
                return 0

            def unknownRemoteCall(self, cmd, ts, ms_id, args):
                tself.messages.append(('unknown', cmd, ts, ms_id, args))
                return ADSP.unknownRemoteCall(self, cmd, ts, ms_id, args)

            def connectionLost(self, reason=None):
                print 'connectionLost:', reason
                return ADSP.connectionLost(self, reason)

        return TAppDispatchServerProtocol

    def test_call_not_connected_createStream(self):
        p, t, dmx, mux = self.build_proto()

        # calling something that's definitely supported
        dmx.inject(3, 0, const.RTMP_COMMAND, 0,
                   encode_amf('createStream', 1, None))

        d = defer.Deferred()

        def verify_sent_messages(_result):
            msgs = muxer_messages(mux)
            self.assertEquals(msgs, [])

        d.addCallback(verify_sent_messages)
        d.addCallback(lambda _: self.assertTrue(t.disconnecting))

        reactor.callLater(0, d.callback, None)
        return d

    def test_call_not_connected_unknown(self):
        p, t, dmx, mux = self.build_proto()

        # calling something undefined
        dmx.inject(3, 0, const.RTMP_COMMAND, 0,
                   encode_amf('sure_not_there', 1, None))

        d = defer.Deferred()
        d.addCallback(lambda _: self.assertEquals(len(mux.messages), 0))
        d.addCallback(lambda _: self.assertTrue(t.disconnecting))

        reactor.callLater(0, d.callback, None)
        return d

    def test_connect_fail_bad_args(self):
        p, t, dmx, mux = self.build_proto()

        dmx.inject(3, 0, const.RTMP_COMMAND, 0,
                   encode_amf('connect', 1))

        d = defer.Deferred()

        def verify_sent_messages(_result):
            self.assertErrorsMatch(muxer_messages(mux),
                                   [(0, chunks.MSG_COMMAND, 0,
                                     ['_error', 1.0, None,
                                      ('NetConnection.Connect.Failed',
                                       'error')],
                                     False)])

        d.addCallback(verify_sent_messages)
        d.addCallback(lambda _: self.assertTrue(t.disconnecting))

        reactor.callLater(0, d.callback, None)
        return d

    def test_connect_fail_incomplete_args(self):
        p, t, dmx, mux = self.build_proto()

        dmx.inject(3, 0, const.RTMP_COMMAND, 0,
                   encode_amf('connect', 1, Object(app=None)))

        d = defer.Deferred()

        def verify_sent_messages(_result):
            self.assertErrorsMatch(muxer_messages(mux),
                                   [(0, chunks.MSG_COMMAND, 0,
                                     ['_error', 1.0, None,
                                      ('NetConnection.Connect.InvalidApp',
                                       'error')],
                                     False)])

        d.addCallback(verify_sent_messages)
        d.addCallback(lambda _: self.assertTrue(t.disconnecting))

        reactor.callLater(0, d.callback, None)
        return d

    def test_connect_fail_no_app(self):
        p, t, dmx, mux = self.build_proto()

        dmx.inject(3, 0, const.RTMP_COMMAND, 0,
                   encode_amf('connect', 1, Object(app='invalid')))

        d = defer.Deferred()

        def verify_sent_messages(_result):
            self.assertErrorsMatch(muxer_messages(mux),
                                   [(0, chunks.MSG_COMMAND, 0,
                                     ['_error', 1.0, None,
                                      ('NetConnection.Connect.InvalidApp',
                                       'error')],
                                     False)])

        d.addCallback(verify_sent_messages)
        d.addCallback(lambda _: self.assertTrue(t.disconnecting))

        reactor.callLater(0, d.callback, None)
        return d

    def test_connect_fail_programmer_error(self):
        p, t, dmx, mux = self.build_proto()

        dmx.inject(3, 0, const.RTMP_COMMAND, 0,
                   encode_amf('connect', 1, Object(app='fail')))

        d = defer.Deferred()

        def verify_sent_messages(_result):
            self.assertErrorsMatch(muxer_messages(mux),
                                   [(0, chunks.MSG_COMMAND, 0,
                                     ['_error', 1.0, None,
                                      ('NetConnection.Connect.Failed',
                                       'error')],
                                     False)])

        d.addCallback(verify_sent_messages)
        d.addCallback(lambda _: self.assertTrue(t.disconnecting))

        reactor.callLater(0, d.callback, None)
        return d

    def test_connect_ok(self):
        p, t, dmx, mux = self.build_proto()

        dmx.inject(3, 0, const.RTMP_COMMAND, 0,
                   encode_amf('connect', 1, Object(app='foobar')))

        d = defer.Deferred()

        def verify_sent_messages(_result):
            msgs = muxer_messages(mux)
            self.assertEquals(len(msgs), 4)
            self.assertEquals(msgs[0][1:3], (chunks.PROTO_WINDOW_SIZE, 0))
            self.assertEquals(msgs[1][1:3], (chunks.PROTO_SET_BANDWIDTH, 0))
            self.assertEquals(msgs[2][1:4], (chunks.PROTO_USER_CONTROL, 0,
                                             '000000000000'.decode('hex')))
            self.assertEquals(msgs[3][1:4], (chunks.MSG_COMMAND, 0,
                                             ['_result', 1.0, True, True]))

        d.addCallback(verify_sent_messages)
        d.addCallback(lambda _: self.assertFalse(t.disconnecting))

        reactor.callLater(0, d.callback, None)
        return d

    def make_connection(self):
        p, t, dmx, mux = self.build_proto()

        dmx.inject(3, 0, const.RTMP_COMMAND, 0,
                   encode_amf('connect', 1, Object(app='foobar')))

        d = defer.Deferred()

        def clear_sent_messages(_result):
            mux.messages[:] = []

        d.addCallback(clear_sent_messages)
        d.addCallback(lambda _: self.assertFalse(t.disconnecting))

        reactor.callLater(0, d.callback, None)
        return p, t, dmx, mux, d

    def test_createStream(self):
        p, t, dmx, mux, d = self.make_connection()

        d.addCallback(lambda _: dmx.inject(3, 0, const.RTMP_COMMAND, 0,
                                           encode_amf('createStream', 2,
                                                      None)))
        d.addCallback(wait)

        def verify_sent_messages(_result):
            msgs = muxer_messages(mux)
            self.assertEquals(msgs, [(0, 6, 0, ['_result', 2.0, None, 1.0],
                                      False)])

        d.addCallback(verify_sent_messages)
        d.addCallback(lambda _: self.assertFalse(t.disconnecting))

        return d

    def test_createStream_wrong_args(self):
        p, t, dmx, mux, d = self.make_connection()

        d.addCallback(lambda _: dmx.inject(3, 0, const.RTMP_COMMAND, 0,
                                           encode_amf('createStream', 2)))
        d.addCallback(wait)

        def verify_sent_messages(_result):
            self.assertErrorsMatch(muxer_messages(mux),
                                   [(0, chunks.MSG_COMMAND, 0,
                                     ['_error', 2.0, None,
                                      ('NetStream.Failed', 'error')],
                                     False)])

        d.addCallback(verify_sent_messages)
        d.addCallback(lambda _: self.assertFalse(t.disconnecting))

        return d

    def test_call_unknown(self):
        p, t, dmx, mux, d = self.make_connection()

        # calling something undefined
        dmx.inject(3, 0, const.RTMP_COMMAND, 0,
                   encode_amf('sure_not_there', 2, None))
        d.addCallback(wait)

        def verify_sent_messages(_result):
            self.assertEquals(mux.messages, [])

        d.addCallback(verify_sent_messages)
        d.addCallback(lambda _: self.assertFalse(t.disconnecting))

        return d

    def test_appcall_not_connected(self):
        p, t, dmx, mux = self.build_proto()

        d = defer.Deferred()

        d.addCallback(lambda _: dmx.inject(3, 0, const.RTMP_COMMAND, 1,
                                           encode_amf('echo', 1, 'abc')))
        d.addCallback(wait)

        def verify_sent_messages(_result):
            self.assertEquals(mux.messages, [])

        d.addCallback(verify_sent_messages)
        d.addCallback(lambda _: self.assertTrue(t.disconnecting))

        reactor.callLater(0, d.callback, None)
        return d

    def test_appcall_stream_0(self):
        p, t, dmx, mux, d = self.make_connection()

        # call defined custom 'echo', on msg stream 0
        dmx.inject(3, 0, const.RTMP_COMMAND, 0,
                   encode_amf('echo', 2, 'abc'))
        d.addCallback(wait)

        def verify_sent_messages(_result):
            self.assertEquals(mux.messages, [])

        def verify_protocol_calls(_result):
            self.assertEquals(self.messages,
                              [('unknown', 'echo', 0, 0, ['abc'])])

        d.addCallback(verify_sent_messages)
        d.addCallback(verify_protocol_calls)
        d.addCallback(lambda _: self.assertFalse(t.disconnecting))

        return d

    def test_appcall_no_stream(self):
        p, t, dmx, mux, d = self.make_connection()

        # call defined custom 'echo', on msg stream 1, (which wasn't created)
        dmx.inject(3, 0, const.RTMP_COMMAND, 1,
                   encode_amf('echo', 2, 'abc'))
        d.addCallback(wait)

        def verify_sent_messages(_result):
            self.assertErrorsMatch(muxer_messages(mux),
                                   [(0, chunks.MSG_COMMAND, 1,
                                     ['_error', 2.0, None,
                                      ('NetStream.Failed', 'error')],
                                     False)])

        def verify_protocol_calls(_result):
            self.assertEquals(self.messages,
                              [('unknown', 'echo', 0, 1, ['abc'])])

        d.addCallback(verify_sent_messages)
        d.addCallback(verify_protocol_calls)
        d.addCallback(lambda _: self.assertFalse(t.disconnecting))

        return d

    def test_appcall(self):
        p, t, dmx, mux, d = self.make_connection()

        d.addCallback(lambda _: dmx.inject(3, 0, const.RTMP_COMMAND, 0,
                                           encode_amf('createStream', 2,
                                                      None)))
        d.addCallback(wait)

        # assuming server gives us stream 1; call echo on that stream
        d.addCallback(lambda _: dmx.inject(3, 0, const.RTMP_COMMAND, 1,
                                           encode_amf('echo', 1, 'abc')))
        d.addCallback(wait)

        def verify_sent_messages(_result):
            msgs = muxer_messages(mux)
            self.assertEquals(msgs,
                              [(0, 6, 0, ['_result', 2.0, None, 1.0], False),
                               (0, 6, 1, ['onEcho', 0.0, 'abc'], False),
                               (0, 6, 1, ['_result', 1.0, 'abc'], False)])

        def verify_protocol_calls(_result):
            self.assertEquals(self.messages,
                              [('unknown', 'echo', 0, 1, ['abc'])])

        d.addCallback(verify_sent_messages)
        d.addCallback(verify_protocol_calls)
        d.addCallback(lambda _: self.assertFalse(t.disconnecting))

        return d

    def test_appcall_raising(self):
        p, t, dmx, mux, d = self.make_connection()

        d.addCallback(lambda _: dmx.inject(3, 0, const.RTMP_COMMAND, 0,
                                           encode_amf('createStream', 2,
                                                      None)))
        d.addCallback(wait)

        d.addCallback(lambda _: dmx.inject(3, 0, const.RTMP_COMMAND, 1,
                                           encode_amf('raising', 1, 'abc')))
        d.addCallback(wait)

        def verify_sent_messages(_result):
            msgs = muxer_messages(mux)
            self.assertEquals(msgs[0:1],
                              [(0, 6, 0, ['_result', 2.0, None, 1.0], False)])
            self.assertErrorsMatch(msgs[1:],
                                   [(0, chunks.MSG_COMMAND, 1,
                                     ['_error', 1.0, None,
                                      ('Artificially.Failed', 'error')],
                                     False)])

        def verify_protocol_calls(_result):
            self.assertEquals(self.messages,
                              [('unknown', 'raising', 0, 1, ['abc'])])

        d.addCallback(verify_sent_messages)
        d.addCallback(verify_protocol_calls)
        d.addCallback(lambda _: self.assertFalse(t.disconnecting))

        return d

    def test_appcall_error(self):
        p, t, dmx, mux, d = self.make_connection()

        d.addCallback(lambda _: dmx.inject(3, 0, const.RTMP_COMMAND, 0,
                                           encode_amf('createStream', 2,
                                                      None)))
        d.addCallback(wait)

        d.addCallback(lambda _: dmx.inject(3, 0, const.RTMP_COMMAND, 1,
                                           encode_amf('error', 1, 'abc')))
        d.addCallback(wait)

        def verify_sent_messages(_result):
            msgs = muxer_messages(mux)
            self.assertEquals(msgs[0:1],
                              [(0, 6, 0, ['_result', 2.0, None, 1.0], False)])
            self.assertErrorsMatch(msgs[1:],
                                   [(0, chunks.MSG_COMMAND, 1,
                                     ['_error', 1.0, None,
                                      ('NetStream.Failed', 'error')],
                                     False)])

        def verify_protocol_calls(_result):
            self.assertEquals(self.messages,
                              [('unknown', 'error', 0, 1, ['abc'])])

        d.addCallback(verify_sent_messages)
        d.addCallback(verify_protocol_calls)
        d.addCallback(lambda _: self.assertFalse(t.disconnecting))

        return d
