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


from zope.interface import Interface, Attribute


# TODO: actually provide documentation!
# TODO: let's have interfaces for everything! (O_o)

class IClientStream(Interface):
    def publish(name, source, mode='live', chunk_size=None):
        pass

    def set_chunk_size(size):
        pass

    def play(self, sink):
        pass

    def stop_publishing():
        pass

    def stop_playing():
        pass

    def close(force=False):
        pass

    def write_meta(ts, data):
        pass

    def write_audio(ts, data):
        pass

    def write_video(ts, data):
        pass


class IMediaSource(Interface):
    def connect(stream):
        pass

    def disconnect():
        pass

    def start():
        pass

    def stop():
        pass


class IClientApp(Interface):
    ##
    # event notification, "entry points" for custom apps

    def connectionMade(info):
        pass

    def get_connect_params():
        pass

    def connectionLost(reason):
        pass

    def connectionFailed(reason):
        pass

    ##
    # utilities

    def disconnect():
        pass

    def createStream(stream_factory):
        pass

    def closeStream(stream, force=False):
        pass

    ##
    # methods forming connection with protocol

    def makeConnection(info):
        pass

    def breakConnection(reason):
        pass

    def failConnection(reason):
        pass


class IBaseFactory(Interface):
    init_time = Attribute(
        'Time reference point for all connections made by this factory. '
        'A float, in seconds, as in time.time().')


class IAppClientFactory(IBaseFactory):
    def get_connect_params():
        pass

    def make_app(protocol):
        pass
