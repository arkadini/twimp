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


from twisted.test import proto_helpers

from twimp.amf0 import decode as decode_amf
from twimp import chunks

from twimp.helpers import vb



class StringTransport(proto_helpers.StringTransport):
    def writeSequence(self, iovec):
        """A writeSequence() implementation able to handle buffer
        instances in iovec."""

        for elt in iovec:
            self.write(elt)


def muxer_messages(mux):
    return [(m[0], m[1], m[2],
             (decode_amf(vb(m[3]))
              if m[1] in (chunks.MSG_COMMAND, chunks.MSG_DATA)
              else m[3]),
             m[4])
            for m in mux.messages]


def unvb(vb):
    return vb.read(len(vb))[:]
