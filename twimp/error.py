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


from twimp import amf0


class ProtocolContractError(ValueError):
    pass


class UnexpectedStatusError(ValueError):
    pass


class CommandResultError(RuntimeError):
    pass



class CallResultError(ValueError):
    """Call resulted in an error"""

    level = 'error'
    code = 'NetStream.Failed'   # looks like the most generic error type code

    def __init__(self, *args, **kwargs):
        ValueError.__init__(self, *args)
        self.is_fatal = kwargs.get('fatal', False)

    def get_error_args(self):
        dsc = self.__doc__
        if self.args:
            dsc = '%s: %s' % (dsc, self.args[0])
        return (None, amf0.Object(level=self.level, code=self.code,
                                  description=dsc))


class CallAbortedException(Exception):
    """Call aborted"""


class ConnectFailedError(CallResultError):
    """Connection attempt failed"""
    code = 'NetConnection.Connect.Failed'


class InvalidAppError(CallResultError):
    """The specified app is invalid"""
    code = 'NetConnection.Connect.InvalidApp'


class PlayFailed(CallResultError):
    """Failure while attempting to play"""
    code = 'NetStream.Play.Failed'


class PlayNotFound(CallResultError):
    """Requested stream not found"""
    code = 'NetStream.Play.StreamNotFound'
