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


from zope.interface import Interface

class IStreamServer(Interface):
    def open(name, mode='r', namespace=None):
        """
        @param mode: 'r', 'l', 'w' or 'a'

        @rtype: twisted.internet.defer.Deferred(IStreamGroup)
        """

    def close(streamgroup):
        """
        @rtype: twisted.internet.defer.Deferred
        """

    # def create():
    #     # just an alias for open with specific flags...?
    #     pass

    def delete(streamgroup):
        """
        @rtype: twisted.internet.defer.Deferred
        """


class IStream(Interface):
    # * meta
    # * params
    # * headers
    # * data frames (grpos, flags, frame body):
    #   - duration
    #   - number of frames (?)
    #   - grpos of the first frame (?)
    #   - grpos of the last frame (?)
    # * indices (?)
    #
    # all methods async, unless specified otherwise

    def params():
        """
        @rtype: twisted.internet.defer.Deferred(dict)
        """

    def set_params(params):
        """
        @type params: dict

        @rtype: twisted.internet.defer.Deferred
        """

    def meta():
        """
        @rtype: twisted.internet.defer.Deferred(dict)
        """

    def set_meta(meta):
        """
        @type meta: dict

        @rtype: twisted.internet.defer.Deferred
        """


    def read_headers(callback):
        """
        @type callback: (grpos, flags, data) -> None
        """

    def write_headers(data, grpos=0, flags=0):
        """
        @rtype: twisted.internet.defer.Deferred
        """


    def seek(offset, whence=0, frames=None):
        """
        @param offset: offset in grpos range

        @param whence: as in 'man 2 lseek'

        @param frames: offset in number of frames (offset must be None)
        """

    def pseek(offset, whence=0, frames=None, flag_mask=0):
        """
        @param flag_mask: asbolute value represents the flag mask of
                          the frame types to seek to, the sign
                          represents the direction (positive is to the
                          right / later)
        """

    def read(callback, grpos_range, frames=None):
        # grpos, flags, data
        """
        @type callback: (grpos, flags, data) -> None

        @rtype: twisted.internet.defer.Deferred
        """

    def write(grpos, flags, data):
        # grpos, flags, data
        #
        # only appending
        """
        @rtype: twisted.internet.defer.Deferred
        """

    def trim(grpos_range, frames=None, flag_mask=0):
        """
        @rtype: twisted.internet.defer.Deferred
        """


    # getting "live updates"
    def subscribe(callback, preroll_grpos_range=0, preroll_frames=0,
                  preroll_from_frame=None, flag_mask=0):
        """
        @return: an opaque subscription object, that can be passed to
                 unsubscribe() to unsubscribe
        @rtype: twisted.internet.defer.Deferred(object)
        """

    def unsubscribe(subscription):
        """
        @rtype: twisted.internet.defer.Deferred
        """

    def find_frame_backward(self, grpos_range, frames=None, flag_mask=0):
        """
        @rtype: twisted.internet.defer.Deferred
        """

    def frame_to_grpos(self, frame):
        """
        @rtype: twisted.internet.defer.Deferred
        """


class ILiveStream(IStream):
    def set_buffering(grpos_range=0, frames=0, flag_mask=0):
        """
        @rtype: twisted.internet.defer.Deferred
        """


class IStreamGroup(Interface):
    # * meta
    # * streams
    #
    # (convenience) methods for working with groups of streams
    # * getting / finding streams
    # * (properly interleaved) reading
    # * seeking
    # * subscribing with (properly interleaved) preroll
    # all methods async, unless specified otherwise

    def meta():
        """
        @rtype: twisted.internet.defer.Deferred(dict)
        """

    def set_meta(meta):
        """
        @type meta: dict

        @rtype: twisted.internet.defer.Deferred
        """

    def streams():
        """
        @rtype: twisted.internet.defer.Deferred
        """

    def streams_by_params(template):
        """
        @type template: dict

        @rtype: twisted.internet.defer.Deferred
        """

    def add_stream(stream):
        """
        @type stream: IStream

        @rtype: twisted.internet.defer.Deferred
        """

    def make_stream():
        """
        @rtype: twisted.internet.defer.Deferred(IStream)
        """

    def seek(offset, whence=0):
        """
        @param offset: offset in grpos range

        @param whence: as in 'man 2 lseek'

        @rtype: twisted.internet.defer.Deferred
        """

    def read_to(callback, grpos, cb_args_map=None):
        """
        @param callback: (grpos, flags, data, *cb_args) -> None

        @param grpos: an absolute grpos, common for all streams in
                      this group, up to which (not including) data
                      will be read, properly interleaved

        @param cb_args_map: None or a dict mapping *all* group streams
                            to tuples of args - those will be
                            included, as *cb_args, in the args of
                            callback invocations, with cb_args matching the
                            streams

        @return: tuple (task, Deferred), where task can be used to
                 cancel the reading
        """

    def subscribe(callback, preroll_grpos_range=0, preroll_from_frames=None,
                  cb_args_map=None):
        """
        @param callback: (grpos, flags, data, *cb_args) -> None

        @param grpos: an absolute grpos, common for all streams in
                      this group, up to which (not including) data
                      will be read, properly interleaved

        @param cb_args_map: None or a dict mapping *all* group streams
                            to tuples of args - those will be
                            included, as *cb_args, in the args of
                            callback invocations, with cb_args matching the
                            streams

        @rtype: twisted.internet.defer.Deferred(subscription)
        """
