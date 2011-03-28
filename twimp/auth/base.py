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
A sketch of a client-side authentication API, in the current state
largely driven by a rather specific needs.
"""


from urlparse import parse_qsl

from twisted.cred.error import Unauthorized, UnauthorizedLogin

from twimp.auth.error import AuthProtocolContractError


def parse_url_args(argstr):
    args = {}
    if argstr:
        args.update(parse_qsl(argstr.lstrip('?')))
    return args

def parse_description_string(desc, authmod=None):
    base, opts = None, None

    for rpart in desc.split(':'):
        part = rpart.strip(' []')
        if 'authmod=' in part:
            kvs = dict(v.strip().split('=', 1) for v in part.split(';'))
            if not authmod or kvs.get('authmod') == authmod:
                base = kvs
        elif part.startswith('?'):
            opts = parse_url_args(part)

    return base, opts


class Credentials(object):
    invalid = True


class Authenticator(object):
    authmod = None

    def __init__(self, authmod=None):
        self.authmod = authmod

    def unhandled_code(self, code_str, cred, challenge):
        raise AuthProtocolContractError('unknown code in challenge: %r',
                                        (challenge,))

    def unhandled_reason(self, reason, cred, challenge):
        raise AuthProtocolContractError('unknown reason in challenge: %r',
                                        (challenge,))

    def initial_args(self, args, cred, challenge):
        if cred.invalid:
            raise Unauthorized('Credentials invalidated: %r' % (cred,))
        return args

    def get_auth_args(self, cred, challenge):
        """Generate authentication arguments using given credentials
        and challenge.

        @type cred: DigestAuthCredentials

        @type challenge: None or str

        @rtype: dict
        """

        base, opts = None, None
        args = {}

        if challenge:
            base, opts = parse_description_string(challenge,
                                                  authmod=self.authmod)

        if not base:
            if not challenge:
                # assuming initial connect - args based on possible cred state
                return self.initial_args(args, cred, challenge)

            raise UnauthorizedLogin('could not authenticate: %s' %
                                    (challenge,))
        else:
            if self.authmod:
                args.update(authmod=self.authmod)

            code_str = base.get('code')
            if code_str:
                code = code_str.split(None, 1)[0]
                handler_m = getattr(self, 'code_%s' % code, None)
                if handler_m is None:
                    return self.unhandled_code(code_str, cred, challenge)
                else:
                    return handler_m(args, cred, code_str, challenge)
            elif opts and opts.has_key('reason'):
                reason = opts['reason']
                handler_m = getattr(self, 'reason_%s' % (reason,))

                if handler_m is None:
                    return self.unhandled_reason(reason, cred, challenge)
                else:
                    return handler_m(args, cred, opts, challenge)
            else:
                raise AuthProtocolContractError("Don't know what to do: %r" %
                                                (challenge,))
