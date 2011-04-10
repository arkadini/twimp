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


"""
A simple protocol+factory pair of classes implementing support for
(challenge - response) authentication schemes requiring multiple
re-connections and passing information through "description" field of
the connect rejection result (as used by FMS, Red5, ...).
"""


from urllib import urlencode

from twisted.internet import reactor
from twisted.python.failure import Failure

from twimp.auth.helpers import check_get_rejected_desc
from twimp.client import SimpleAppClientProtocol, SimpleAppClientFactory
from twimp.urls import parse_rtmp_url, unparse_rtmp_url, parse_normalize_app


LOG_CATEGORY = 'auth.cli'
import twimp.log
log = twimp.log.get_logger(LOG_CATEGORY)


class SimpleAuthAppClientProtocol(SimpleAppClientProtocol):
    def _connect_call_failed(self, failure):
        log.debug('_connect_call_failed: %r', failure.value)

        if not self.factory._do_auth:  # ... :/
            return SimpleAppClientProtocol._connect_call_failed(self, failure)

        log.debug('_connect_call_failed(2): %r', failure.value)

        desc = check_get_rejected_desc(failure)
        try:
            args = self.factory.auth.get_auth_args(self.factory.cred, desc)
        except:
            return SimpleAppClientProtocol._connect_call_failed(self, Failure())

        if args is not None:
            self.factory.set_app_auth_args(args)
            self.factory._auth_reconnect = True  # umm... bleehh! :/

        return failure


class SimpleAuthAppClientFactory(SimpleAppClientFactory):
    protocol = SimpleAuthAppClientProtocol

    def __init__(self, url, auth, cred, connect_params,
                 app_factory, *app_args, **app_kwargs):
        self.auth = auth
        self.cred = cred
        self._do_auth = bool(auth and cred)
        self._auth_reconnect = False

        SimpleAppClientFactory.__init__(self, url, connect_params, app_factory,
                                        *app_args, **app_kwargs)
        if self._do_auth:
            self.set_app_auth_args(self.auth.get_auth_args(self.cred, None))

    def _parse_url(self, url):
        scheme, host, port, path = parse_rtmp_url(url)
        app, napp, args = parse_normalize_app(path)

        # self._url here is just teh base of the url
        self._url = unparse_rtmp_url((scheme, host, port, None))

        if self.cred:
            self.cred.uri = '/' + napp

        self._app_path = app
        self._app_path_args = args
        self._app_path_auth_args = []

    def get_connect_app_path(self):
        if self._app_path_args or self._app_path_auth_args:
            return '%s?%s' % (self._app_path,
                              urlencode(self._app_path_args +
                                        self._app_path_auth_args))
        else:
            return self._app_path

    def get_connect_url(self):
        path = self.get_connect_app_path()
        if path:
            return '%s/%s' % (self._url, path)
        return self._url        # unlikely...

    def set_app_auth_args(self, args):
        self._app_path_auth_args = args.items()

    def clientConnectionLost(self, connector, reason):
        log.debug('clientConnectionLost(%r, %r) [%r]', connector, reason,
                  self._auth_reconnect)
        if self._auth_reconnect:
            self._auth_reconnect = False
            connector.connect()
        else:
            if self._do_auth:
                # update the auth args in case we'll want to re-connect...
                self.set_app_auth_args(self.auth.get_auth_args(self.cred, None))
            SimpleAppClientFactory.clientConnectionLost(self, connector,
                                                        reason)


def connect_auth_client_factory(url, auth, cred,
                                factoryFactory, *args, **kwargs):
    connect_params = dict(fpad=False) # no proxy, direct tcp/rtmp connection
    scheme, host, port, path = parse_rtmp_url(url)
    factory = factoryFactory(url, auth, cred, connect_params, *args, **kwargs)

    reactor.connectTCP(host, port, factory)
    return factory
