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
Implementation of selected functionality of the HTTP Digest
Authentication (RFC2617), slightly adapted to RTMP case.
"""


from twisted.cred.error import Unauthorized, UnauthorizedLogin
from twisted.python.hashlib import md5
from twisted.python.randbytes import secureRandom

from twimp.auth.base import Credentials, Authenticator


DA_FIXED_REALM = 'live'
DA_FIXED_METHOD = 'publish'
DA_FIXED_QOP = 'auth'

def _HA1(username, realm, password):
    a = md5('%s:%s:%s' % (username, realm, password))
    return a.hexdigest()

def _HA2(method, uri):
    a = md5('%s:%s' % (method, uri))
    return a.hexdigest()

def _response(HA1, nonce, nc, cnonce, qop, HA2):
    a = md5('%s:%s:%s:%s:%s:%s' % (HA1, nonce, nc, cnonce, qop, HA2))
    return a.hexdigest()

def _gen_cnonce():
    return secureRandom(4).encode('hex')

def _form_nc(nc):
    return '%08x' % nc


class DigestAuthCredentials(Credentials):
    realm = DA_FIXED_REALM
    method = DA_FIXED_METHOD
    qop = DA_FIXED_QOP

    def __init__(self, username, HA1, password=None):
        self.invalid = False

        self.username = username
        if not HA1:
            if not password:
                raise ValueError('No password nor precalculated HA1 given.')
            HA1 = _HA1(self.username, self.realm, password)
        self.HA1 = HA1

        self.uri = None

        self.nonce = None
        self.nc = None

    def update(self, nonce):
        if nonce != self.nonce:
            self.nonce = nonce
            self.nc = 0

    def inc(self):
        self.nc += 1
        return self.nc


class DigestAuthenticator(Authenticator):
    def _response(self, cred, cnonce):
        return _response(cred.HA1,
                         cred.nonce, _form_nc(cred.inc()), cnonce, cred.qop,
                         _HA2(cred.method, cred.uri))

    def _gen_response(self, args, cred, nonce):
        cnonce = _gen_cnonce()

        cred.update(nonce)
        response = self._response(cred, cnonce)

        args.update(user=cred.username, nonce=nonce, cnonce=cnonce,
                    nc=_form_nc(cred.nc), response=response)
        return args


    def initial_args(self, args, cred, _challenge):
        if cred.invalid:
            raise Unauthorized('Credentials invalidated: %r' % (cred,))

        if cred.nonce:
            return self._gen_response(args, cred, cred.nonce)

        # no auth on first connect whatsoever
        return {}

    def code_403(self, args, cred, _code_str, _challenge):
        args.update(user=cred.username)
        return args

    def reason_needauth(self, args, cred, opts, _challenge):
        nonce = opts['nonce']   # can't do without
        return self._gen_response(args, cred, nonce)

    def reason_authfail(self, args, cred, opts, challenge):
        cred.invalid = True
        raise UnauthorizedLogin('authentication failed: %s' %
                                (opts.get('detail', '(no details)'),))
