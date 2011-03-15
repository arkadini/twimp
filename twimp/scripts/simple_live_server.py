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


from twisted.internet import reactor, error

from twimp.amf0 import Object
from twimp.server import appserver
from twimp.server.appserver import URLDispatchingServerFactory, make_urls
from twimp.server.controllers import RTMPPlayer, RTMPRecorder
from twimp.server import inmemory

LOG_CATEGORY = 'livesrv'
import twimp.log
log = twimp.log.get_logger(LOG_CATEGORY)


# SERVER_VERSION_STRING = 'FMS/3,0,1,1'
# SERVER_VERSION = (3, 0, 1, 1)
SERVER_VERSION_STRING = 'TwiMMFs/1,0,0,0'
SERVER_VERSION = (1, 0, 0, 0)


class SimplePublishPlayApp(object):
    # implements(IServerApp)

    sg = None
    ctrl = None
    ns = None

    def __init__(self, protocol, server):
        self.protocol = protocol
        self.server = server

    def connect(self, request, req_opts):
        log.info('connect(%r, %r)', request, req_opts)

        # note: order of setting attributes is important
        server_info = (Object(fmsVer=SERVER_VERSION_STRING)
                       .s(capabilities=31)
                       .s(mode=1))

        status_info = (Object(level='status')
                       .s(code='NetConnection.Connect.Success')
                       .s(description='Connection succeeded.')
                       .s(objectEncoding=0)
                       .s(data={'version': '%d,%d,%d,%d' % SERVER_VERSION}))

        return server_info, status_info

    # all "call"s are ignored (w/ MethodNotFound ?)

    def play(self, net_stream, stream_name, start=-2, duration=-1,
             reset=True):
        assert start == -2, 'only live streams'

        # ignoring duration and reset for now...

        log.info('opening stream %r', stream_name)

        def got_streamgroup(streamgroup):
            self.sg = streamgroup
            log.debug('starting playing %r', streamgroup)

            self.ctrl = c = RTMPPlayer(streamgroup)
            c.connect(net_stream)

            log.debug('calling c.start()...')
            d = c.start()
            def _dbg(r, msg):
                log.debug(msg)
                return r
            d.addCallback(_dbg, 'c.start()... done.')

            return d

        d = self.server.open(stream_name, namespace=self.ns)
        d.addCallback(got_streamgroup)
        return d

    def publish(self, net_stream, stream_name, publish_type):
        publish_type = 'live'
        if publish_type != 'live':
            # this app is only for live streams
            raise CallResultError('only live streams for now')

        def got_streamgroup(streamgroup):
            self.sg = streamgroup

            self.ctrl = r = RTMPRecorder(streamgroup)
            r.connect(net_stream)

            d = r.start()
            return d

        # normalize stream_name better? :/
        # this just throws away url-type args:
        args = ''
        name_args = stream_name.split('?', 1)
        stream_name = name_args[0]
        if len(name_args) > 1:
            args = name_args[1]

        log.info('requested publishing of: %r with args %r', stream_name, args)
        d = self.server.open(stream_name, mode='l', namespace=self.ns)

        # AppDispatchServerProtocol should handle reporting failures
        # in server.open() for us... (strange idea? :/ )
        d.addCallback(got_streamgroup)
        return d

    def connectionLost(self, reason):
        log.info('app connection lost: %r (%r)', reason, self.sg)

        if self.ctrl:
            self.ctrl.stop()
            self.ctrl.disconnect()
            self.ctrl = None

        if self.sg:
            sg, self.sg = self.sg, None
            self.server.close(sg)


# _very_ simple example of how url/url-params can be used for authentication
from hashlib import md5
class SimpleParametrizedPublishPlayApp(SimplePublishPlayApp):
    salt = 'salt:'

    def connect(self, request, opts, ns, token):
        if md5(self.salt + ns).hexdigest() == token.lower():
            self.ns = ns
            return SimplePublishPlayApp.connect(self, request, opts)
        raise appserver.InvalidAppError('app not found', fatal=True)


# note: the order should be from the most specific to most generic
urls = make_urls(
    # the example of using groups in the path pattern
    (r'^live/(?P<ns>\w+)/(?P<token>\w+)/?$', SimpleParametrizedPublishPlayApp),
    # for testing: an app available through any app path
    (r'', SimplePublishPlayApp),
    )


def run(main_port=1935, *other_ports):
    import time
    # enabling a couple of namespaces, for the use of the parametrized app
    namespaces = [None, '1', '2']
    factory = URLDispatchingServerFactory(time.time(),
                                          inmemory.IMServer(namespaces),
                                          urls)

    listening_any = False
    for port in (main_port,) + other_ports:
        try:
            reactor.listenTCP(port, factory)
            listening_any = True
        except error.CannotListenError, e:
            log.warn(e)
        except Exception, e:
            log.exception(e)

    if listening_any:
        reactor.run()


def main(argv):
    import optparse

    usage = '%prog [options] [PORT ...]'
    epilog = ('If not specified PORT defaults to 1935. '
              'Log levels: none/0, critical/1, error/2, warning/3, info/4,'
              ' debug/5.')
    parser = optparse.OptionParser(usage=usage, epilog=epilog)

    parser.add_option('-d', '--debug', action='store', dest='debug',
                      help=('comma separated list of "[CATEGORY:]LEVEL" log'
                            ' level specifiers'))
    parser.add_option('-v', '--verbose', action='store_const', const='info',
                      dest='debug',
                      help='equvalent to "-d info"')

    options, args = parser.parse_args(argv)

    twimp.log.set_levels_from_env()
    if options.debug:
        twimp.log.set_levels(options.debug)
    twimp.log.hook_twisted()

    run(*map(int, args))

if __name__ == '__main__':
    import sys

    main(sys.argv[1:])
