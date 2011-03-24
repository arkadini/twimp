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


from collections import deque

from twisted.internet import defer

from twimp.amf0 import Object
from twimp import chunks
from twimp.primitives import _s_uchar, _s_double_uchar

from twimp.helpers import vb_clone, vb

LOG_CATEGORY = 'ctrls'
import twimp.log
log = twimp.log.get_logger(LOG_CATEGORY)


# frame flags
FF_KEYFRAME = 1
FF_INTERFRAME = 2

# params' stream type
TYPE_VIDEO = 'video/x-flv-tag-video'
TYPE_AUDIO = 'audio/x-flv-tag-audio'


class Controller(object):
    def __init__(self, streamgroup):
        self._sg = streamgroup
        self._nstream = None

    def connect(self, nstream):
        self._nstream = nstream

    def disconnect(self):
        self._nstream = None

    def start(self):
        pass

    def stop(self):
        pass


class BufferingWriter(object):
    def __init__(self, nstream, track_types,
                 rewrite_ts=False, use_info_marks=False):
        self.nstream = nstream
        self.bufs = dict((t, deque()) for t in track_types)

        self.rewrite = rewrite_ts
        self.mark = use_info_marks

        self.prerolling = True

        self.base_gp = 0

        if len(track_types) == 1:
            if chunks.MSG_VIDEO in track_types:
                # force info markers, otherwise flash will buffer _a lot_
                # before deciding there's not gonna be any audio
                self.mark = True
            else:
                # no video - no markers
                self.mark = False

        # those two seem to go together... *shrug*
        if self.mark:
            self.rewrite = True

    def write(self, type, gp, flags, data):
        if self.prerolling:
            b = self.bufs[type]
            b.append((gp, data))
        elif self.rewrite:
            self._send_rewrite(gp, type, data)
        else:
            self._send(gp, type, data)

    def preroll_done(self):
        if self.rewrite:
            gps = [b[-1][0] for b in self.bufs.values() if b]
            if gps:
                self.base_gp = max(gps)

        log.debug('preroll_done: buf lens: %r, rewr: %r (%r), mark: %r',
                  [(t, len(b)) for (t, b) in self.bufs.items()], self.rewrite,
                  self.base_gp, self.mark)

        for type, mark in ((chunks.MSG_VIDEO, self.mark),
                           (chunks.MSG_AUDIO, False)):
            frames = self.bufs.get(type)
            if mark:
                # TODO: use correct codec id for the info markers,
                #       not always "7"
                self.nstream.send(0, t, vb('\x57\x00'))
                if frames:
                    if self.rewrite:
                        self._send_many_zero_gp(type, frames)
                    else:
                        self._send_many(type, frames)
                    frames.clear()
                self.nstream.send(0, t, vb('\x57\x01'))
            elif frames:
                if self.rewrite:
                    self._send_many_zero_gp(type, frames)
                else:
                    self._send_many(type, frames)
                frames.clear()

        self.prerolling = False

        log.debug('preroll_done: done.')

    # "protected" write helpers
    def _send(self, gp, type, data):
        self.nstream.send(gp, type, data)

    def _send_rewrite(self, gp, type, data):
        self.nstream.send(gp - self.base_gp, type, data)

    def _send_many(self, type, frames):
        for gp, data in frames:
            self.nstream.send(gp, type, data)

    def _send_many_zero_gp(self, type, frames):
        for gp, data in frames:
            self.nstream.send(0, type, data)


class DefaultBurstPolicy(object):
    def __init__(self, max_grpos_range=3000, h264_frames=64):
        self.grpos_range = max_grpos_range
        self.h264_frames = h264_frames

    def __call__(self, meta, track_types, nstream, done_cb=None):
        # returns: ([(grpos range, frames, flag mask), ...], writer)

        rewrite = False
        use_marks = False
        writer = BufferingWriter

        if meta and meta.get('videocodecid') == 'avc1':
            # check there's more than just audio (don't just trust the meta)?
            params = {chunks.MSG_VIDEO: (0, self.h264_frames, 0),
                      chunks.MSG_AUDIO: (0, 0, 0)}
        else:
            gp_range = min(nstream.buffer_length, self.grpos_range)
            params = {chunks.MSG_VIDEO: (gp_range, 0, 0),
                      chunks.MSG_AUDIO: (gp_range, 0, 0)}

        return (map(params.get, track_types),
                writer(nstream, track_types,
                       rewrite_ts=rewrite, use_info_marks=use_marks))


class RTMPPlayer(Controller):
    burst_policy = DefaultBurstPolicy()

    def __init__(self, streamgroup):
        Controller.__init__(self, streamgroup)
        self._nstream = None
        self._subscription = None
        self._tracks = []

        self._send_audio = True
        self._send_video = True

        self._writer = None

        self._stream_meta = None

    def _scan_tracks(self):
        def got_streams(streams, type_):
            if streams:
                t = streams[0] # selecting *some* of the matching tracks
                self._tracks.append((t, type_))

        d1 = self._sg.streams_by_params({'type': TYPE_AUDIO})
        d1.addCallback(got_streams, chunks.MSG_AUDIO)

        d2 = self._sg.streams_by_params({'type': TYPE_VIDEO})
        d2.addCallback(got_streams, chunks.MSG_VIDEO)

        return defer.DeferredList([d1, d2],
                                  fireOnOneErrback=1, consumeErrors=1)

    def _add_headers_cb(self, _result):
        d = defer.succeed(None)

        def debug_cb(result, msg):
            log.debug(msg)
            return result

        def header_callback_maker(type_):
            def header_callback(ts, flags, data):
                return self.on_header_data_added(type_, ts, flags, data)
            return header_callback

        def do_read_headers(_result, s, type_):
            return s.read_headers(header_callback_maker(type_))

        log.debug('[adding headers]')
        for s, type_ in self._tracks:
            d.addCallback(do_read_headers, s, type_)

        d.addCallback(debug_cb, '[headers done..]')
        return d

    def _send_meta_cb(self, _result):
        def got_meta(meta):
            if meta:
                self._stream_meta = meta
                self._nstream.asend(0, chunks.MSG_DATA, 'onStatus',
                                    Object(code='NetStream.Data.Start'))
                log.debug('sending meta: %r', meta)
                self._nstream.asend(0, chunks.MSG_DATA, 'onMetaData',
                                    Object(meta))

        d = self._sg.meta()
        d.addCallback(got_meta)
        return d

    def _subscribe_cb(self, _result):
        d = defer.succeed(None)
        self._subscription = []

        def subscr_callback_maker(writer, type_):
            # TODO: handle mute statuses
            def data_callback(gp, flags, data):
                return writer.write(type_, gp, flags, vb_clone(data))
            return data_callback

        def do_subscribe(_result, s, type_, params):
            log.debug('(%d: subscribing: %r, %r)',  type_, s, params)
            return s.subscribe(subscr_callback_maker(self._writer, type_),
                               preroll_grpos_range=params[0],
                               preroll_frames=params[1], flag_mask=params[2])

        def subscribed(subscription, s):
            self._subscription.append((s, subscription))
            log.debug('(done subscribing: %r)', s)

        def all_subscribed(_result):
            self._writer.preroll_done()

        types = [t for (s, t) in self._tracks]
        subs_params, writer = self.burst_policy(self._stream_meta, types,
                                                self._nstream)
        self._writer = writer


        for (s, type_), params in zip(self._tracks, subs_params):
            d.addCallback(do_subscribe, s, type_, params)
            d.addCallback(subscribed, s)

        d.addCallback(all_subscribed)
        return d

    def _send_status_cb(self, _result):
        self._nstream.signal('onStatus', None,
                             Object(code='NetStream.Play.Reset',
                                    level='status',
                                    description='reset'))

        self._nstream.ctrlStreamBegin()
        # TODO: depending on self._sg being live streamgroup, call the:
        # self._nstream.ctrlStreamRecorded()

        self._nstream.signal('onStatus', None,
                             Object(code='NetStream.Play.Start',
                                    level='status',
                                    description='started'))

        # increase the chunk size, we're gonna send some data...
        self._nstream.set_chunk_size(4096)

    def start(self):
        self._nstream.set_listeners(mute_callback=self.on_mute_message)

        d = self._scan_tracks()

        d.addCallback(self._send_status_cb)

        d.addCallback(self._send_meta_cb)
        d.addCallback(self._add_headers_cb)

        d.addCallback(self._subscribe_cb)
        return d

    def stop(self):
        self._nstream.unset_listeners()

        d = defer.succeed(None)

        if self._subscription:
            def do_unsubscribe(_result, s, subscr):
                return s.unsubscribe(subscr)
            for s, subscr in self._subscription:
                d.addCallback(do_unsubscribe, s, subscr)
            self._subscription = None
            return d

        return d

    def on_header_data_added(self, type_, ts, flags, data):
        log.debug('=> %s, %s, %s, %s, %s',
                  ts, type_, flags, self._nstream.id, len(data))
        self._nstream.send(0, type_, vb_clone(data))

    def on_mute_message(self, ts, type_, do_send):
        log.info('on_mute_message: %s, %s, %s', ts, type_, do_send)

        if type_ == chunks.MSG_AUDIO:
            self._send_audio = do_send
        else:
            self._send_video = do_send


class DefaultCachePolicy(object):
    def __init__(self, grpos_range=3000, h264_frames=64):
        # *_params: { type => (grpos range, frames, flag mask) }
        self.default_params = {chunks.MSG_VIDEO: (grpos_range, None, 0),
                               chunks.MSG_AUDIO: (grpos_range, None, 0)}
        # see note on caching in the RTMPPlayer._scan_tracks()
        self.h264_params = {chunks.MSG_VIDEO: (None, h264_frames, 0),
                            chunks.MSG_AUDIO: (None, None, 0)}

    def __call__(self, meta, track_types):
        params = self.default_params
        if meta and meta.get('videocodecid') == 'avc1':
            # h264-tuned settings
            params = self.h264_params
        return map(params.get, track_types)


class RTMPRecorder(Controller):
    cache_policy = DefaultCachePolicy()
    frame_types = {chunks.MSG_VIDEO: TYPE_VIDEO,
                   chunks.MSG_AUDIO: TYPE_AUDIO}

    def __init__(self, streamgroup):
        Controller.__init__(self, streamgroup)
        self._tracks = {}

        self._stream_meta = None
        self._audio_headers = 0

    def start(self):
        self._nstream.set_listeners(data_callback=self.on_data,
                                    meta_callback=self.on_meta)

        self._nstream.signal('onStatus', None,
                             Object(code='NetStream.Publish.Start',
                                    level='status',
                                    description='published'))

        return defer.succeed(None)

    def stop(self):
        self._nstream.unset_listeners()
        self._stream_meta = None

        return defer.succeed(None)

    def on_meta(self, ts, args):
        meta = None
        if (len(args) > 2 and args[0] == '@setDataFrame' and
            args[1] == 'onMetaData'):
            meta = args[2]
        elif len(args) > 1 and args[0] == 'onMetaData':
            meta = args[1]

        if meta is not None:
            log.debug('storing meta: %r', meta)
            self._stream_meta = dict(meta)

            d = self._sg.set_meta(self._stream_meta)
            # TODO: have an errback here, for a really distributed
            #       server implementation


    def _make_stream(self, ts, type_):
        sg_type = self.frame_types.get(type_, None)
        if not sg_type:
            return None

        def set_caching(_result):
            types = self._tracks.keys()
            cache_params = self.cache_policy(self._stream_meta, types)
            dl = [self._tracks[type].set_buffering(grpos_range=p[0],
                                                   frames=p[1], flag_mask=p[2])
                  for type, p in zip(types, cache_params)]
            d = defer.DeferredList(dl, fireOnOneErrback=1, consumeErrors=1)
            return d

        def stream_made(stream):
            self._tracks[type_] = stream
            d = stream.set_params({'type': sg_type})
            d.addCallback(set_caching)
            return d

        d = self._sg.make_stream()
        d.addCallback(stream_made)

        return d

    def on_data(self, ts, type_, data):
        stream = self._tracks.get(type_, None)

        if not stream:
            d = self._make_stream(ts, type_)
            if not d:
                return
            # FIXME: there should really be a queue between nstream and sg
            # instead, for the moment, we'll just re-try the on_data()
            d.addCallback(lambda _: self.on_data(ts, type_, data))
            return

        flags = 0
        if type_ == chunks.MSG_VIDEO:
            bytes = len(data)
            frame_type, codec_id, h264_type = None, None, None

            if bytes > 1:
                ft_codec, h264_type = _s_double_uchar.unpack(data.peek(2))
                frame_type, codec_id = ft_codec >> 4, ft_codec & 0x0f
            elif bytes > 0:
                ft_codec, = _s_uchar.unpack(data.peek(1))
                frame_type, codec_id = ft_codec >> 4, ft_codec & 0x0f

            if frame_type == 1 and codec_id == 7 and h264_type == 0:
                d = stream.write_headers(data)
                return
            elif frame_type is not None:
                if frame_type == 1:
                    flags = FF_KEYFRAME
                else:           # FIXME: this is not necessarily correct
                    flags = FF_INTERFRAME
        elif type_ == chunks.MSG_AUDIO:
            bytes = len(data)
            codec_id, aac_type = None, None

            if bytes > 1:
                ft_codec, aac_type = _s_double_uchar.unpack(data.peek(2))
                codec_id = ft_codec >> 4
            elif bytes > 0:
                ft_codec, = _s_uchar.unpack(data.peek(1))
                codec_id = ft_codec >> 4

            if codec_id == 10 and aac_type == 0:
                self._audio_headers += 1
                d = stream.write_headers(data)
                return
            elif codec_id != 10 and self._audio_headers == 0:
                # Flash, doesn't use real headers for those formats,
                # but let's mark that there is an audio track with an
                # empty packet (Flash doesn't seem to mind those)
                # early on
                self._audio_headers += 1
                d = stream.write_headers(vb(data.peek(1)))

            if codec_id is not None:
                flags = FF_KEYFRAME # audio usually is all keyframes
        else:
            log.error('Unsupported data type: %r', type_)
            return

        d = stream.write(ts, flags, data)
        # TODO: have an errback here, (and after all stream.write*()
        #       calls above) for a really distributed server
        #       implementation
