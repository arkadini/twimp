#   Copyright (c) 2010 Arek Korbik, Alessandro Decina
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


import struct

from twisted.internet import glib2reactor
glib2reactor.install()
from twisted.internet import reactor

# not importing gst here, we want to parse command line options ourselves
# import gst

from twimp import amf0
from twimp.client import BaseClientApp, SimpleAppClientFactory
from twimp.client import connect_client_factory
from twimp.primitives import _s_uchar
from twimp.vecbuf import VecBuf

from twimp.helpers import ellip

LOG_CATEGORY = 'livepubcli'
import twimp.log
log = twimp.log.get_logger(LOG_CATEGORY)

try:
    from fractions import Fraction
except ImportError:
    class Fraction(object):
        def __init__(self, string):
            if '/' in string:
                n, d = string.split('/', 1)
                try:
                    num = int(n)
                    denom = int(d)
                except ValueError:
                    raise ValueError('Invalid rational value: %r' % (string,))
            else:
                import decimal
                try:
                    d = decimal.Decimal(string).normalize()
                except decimal.DecimalException:
                    raise ValueError('Invalid rational value: %r' % (string,))
                exp = - d.as_tuple()[2]
                if exp > 0:
                    denom = 10 ** exp
                    num = int(d * denom)
                else:
                    num = int(d)
                    denom = 1
            self.numerator = num
            self.denominator = denom

        def __repr__(self):
            return '%s("%d/%d")' % (self.__class__.__name__,
                                    self.numerator, self.denominator)

        def __str__(self):
            return '%d/%d' % (self.numerator, self.denominator)


_s_double_uchar = struct.Struct('>BB')
_s_h264video = struct.Struct('>BBbH') # flags+codec, frame type,
                                      # signed 24-bit cts value split
                                      # into 1+2 bytes

class Codec(object):
    description = None

    def createBin(self, **attributes):
        codecBin = self.createBinReal(**attributes)
        self.addProbes(codecBin)

        return codecBin

    def createBinReal(self, **attributes):
        description = self.description % attributes
        return gst.parse_bin_from_description(description,
                ghost_unconnected_pads=True)

    def addProbes(self, codecBin):
        for padName in ("sink", "src"):
            pad = codecBin.get_pad(padName)
            pad.connect("notify::caps", self.padNotifyCaps)

    def padNotifyCaps(self, pad, pspec):
        log.debug("pad %s negotiated caps %s", pad, pad.get_negotiated_caps())

    def getHeadersFromCaps(self, caps):
        return None


class VideoCodec(Codec):
    codecType = "video"


class AudioCodec(Codec):
    codecType = "audio"


class H263VideoCodec(VideoCodec):
    codecId = 2
    name = 'h263'
    description = ("ffenc_flv bitrate=%(bitrate)d "
            "max-key-interval=%(key_interval)d")

    def createBinReal(self, **attributes):
        attributes['bitrate'] *= 1000
        return super(H263VideoCodec, self).createBinReal(**attributes)


class SorensonVideoCodec(H263VideoCodec):
    name = 'sorenson'


class H264VideoCodec(VideoCodec):
    codecId = 7
    name = 'h264'
    description = ("x264enc bitrate=%(bitrate)g bframes=0 b-adapt=false "
            "me=2 subme=6 cabac=false key-int-max=%(key_interval)d")

    def getHeadersFromCaps(self, caps):
        return [buffer(caps[0]['codec_data'])]


class MP3AudioCodec(AudioCodec):
    codecId = 2
    name = 'mp3'
    description = "lame bitrate=%(bitrate)d"


class AACAudioCodec(AudioCodec):
    codecId = 10
    name = 'aac'
    description = "faac bitrate=%(bitrate)d"

    def createBinReal(self, **attributes):
        attributes['bitrate'] *= 1000
        return super(AACAudioCodec, self).createBinReal(**attributes)

    def getHeadersFromCaps(self, caps):
        return [buffer(caps[0]['codec_data'])]

class SpeexAudioCodec(AudioCodec):
    codecId = 11
    name = 'speex'
    description = 'speexenc bitrate=%(bitrate)d '

    def createBinReal(self, **attributes):
        attributes['bitrate'] *= 1000
        return super(SpeexAudioCodec, self).createBinReal(**attributes)

    def getHeadersFromCaps(self, caps):
        # flash doesn't want speex headers
        # return map(buffer, caps[0]['streamheader'])
        return None

class NoCodec(object):
    name = 'none'

def codecMap(*codecs):
    return dict((codec.name, codec()) for codec in codecs)

video_codecs = codecMap(H263VideoCodec, SorensonVideoCodec,
        H264VideoCodec, NoCodec)
audio_codecs = codecMap(MP3AudioCodec, AACAudioCodec, SpeexAudioCodec, NoCodec)

class NewGstSource(object):
    def __init__(self, audiobitrate, videobitrate, audiorate,
            channels, framerate, width, height, keyrate, audio_codec=None,
            video_codec=None, audiosrc='pulsesrc', videosrc='v4l2src',
            audio_opts='', video_opts='', videosink='xvimagesink', view=False,
            view_window=None):

        # default args translate to:
        # aac: 64kbit, 44.1kHz, 16-bit, mono
        # h264: 400kbit: 320 x 240 @ 15fps, 5 secs key interval
        if audio_codec is None:
            audio_codec = AACAudioCodec()
        elif isinstance(audio_codec, NoCodec):
            audio_codec = None

        if video_codec is None:
            video_codec = H264VideoCodec()
        elif isinstance(video_codec, NoCodec):
            video_codec = None

        if audiorate is None:
            if audio_codec is not None and audio_codec.codecId == 11:
                audiorate = 32000
            else:
                audiorate = 44100

        self.audio_codec = audio_codec
        self.audio_bitrate = audiobitrate
        self.audio_rate = audiorate
        self.channels = channels
        self.audio_opts = audio_opts

        self.video_codec = video_codec
        self.video_bitrate = videobitrate
        self.framerate = framerate
        self.width = width
        self.height = height
        self.key_rate = keyrate
        self.key_interval = int(round(keyrate * framerate.numerator /
                                      float(framerate.denominator)))
        self.video_opts = video_opts

        self.videosink = videosink
        self.view = view
        self.view_window = view_window

        self.audiosrc = audiosrc
        self.videosrc = videosrc

        self.pipeline = None
        self._stream = None

        self.asink = None
        self.audio_caps = None
        self.vsink = None
        self.video_caps = None
        self.video_avc_profile = None
        self.video_avc_level = None

        self._ah1 = 0
        self._header_audio = None

        self.complex_audio_header = (self.audio_codec is not None and
                self.audio_codec.codecId == 10)

    def _make_pipeline_audio(self):
        source = gst.element_factory_make(self.audiosrc)
        queue = gst.element_factory_make("queue")
        audiorate = gst.element_factory_make("audiorate")
        audioconvert = gst.element_factory_make("audioconvert")
        capsfilter = gst.element_factory_make("capsfilter")
        tee = gst.element_factory_make("tee")
        encoder = self.audio_codec.createBin(bitrate=self.audio_bitrate)
        self.asink = sink = gst.element_factory_make("appsink")
        sink.props.async = True

        # set output rate and channels
        filter_caps = gst.Caps()
        audio_formats = ("audio/x-raw-int", "audio/x-raw-float")
        for audio_format in audio_formats:
            structure = gst.Structure(audio_format)
            structure["rate"] = self.audio_rate
            structure["channels"] = self.channels
            filter_caps.append_structure(structure)
        capsfilter.props.caps = filter_caps

        audioBin = gst.Bin()
        audioBin.add(source, queue, audiorate, audioconvert, capsfilter,
                tee, encoder, sink)

        gst.element_link_many(source, queue, audiorate, audioconvert,
                capsfilter, tee, encoder, sink)

        return audioBin

    def _make_pipeline_video(self):
        videosrc = gst.element_factory_make(self.videosrc)
        queue = gst.element_factory_make("queue")
        videorate = gst.element_factory_make("videorate")
        ffmpegcolorspace = gst.element_factory_make("ffmpegcolorspace")
        videoscale = gst.element_factory_make("videoscale")
        capsfilter = gst.element_factory_make("capsfilter")
        tee = gst.element_factory_make("tee")
        encoder = self.video_codec.createBin(bitrate=self.video_bitrate,
                key_interval=self.key_interval)
        self.vsink = sink = gst.element_factory_make("appsink")
        sink.props.async = True

        # set video resolution/framerate
        filter_caps = gst.Caps()
        video_formats = ("video/x-raw-yuv", "video/x-raw-rgb")
        for video_format in video_formats:
            structure = gst.Structure(video_format)
            structure["width"] = self.width
            structure["height"] = self.height
            structure["framerate"] = gst.Fraction(self.framerate.numerator,
                    self.framerate.denominator)
            filter_caps.append_structure(structure)
        capsfilter.props.caps = filter_caps

        videoBin = gst.Bin()
        videoBin.add(videosrc, queue, videorate, ffmpegcolorspace, videoscale,
                capsfilter, tee, encoder, sink)
        gst.element_link_many(videosrc, queue, videorate, ffmpegcolorspace,
                videoscale, capsfilter, tee, encoder, sink)

        if self.view:
            queue1 = gst.element_factory_make("queue")
            ffmpegcolorspace1 = gst.element_factory_make("ffmpegcolorspace")
            videoscale1 = gst.element_factory_make("videoscale")
            videosink = gst.element_factory_make(self.videosink)
            videoBin.add(queue1, ffmpegcolorspace1, videoscale1, videosink)
            gst.element_link_many(tee, queue1, ffmpegcolorspace1, videoscale1,
                    videosink)

        return videoBin

    def make_pipeline(self):
        if not (self.audio_codec or self.video_codec):
            raise RuntimeError('no audio nor video codecs specified')

        self.pipeline = gst.Pipeline()
        if self.audio_codec:
            self.pipeline.add(self._make_pipeline_audio())
        if self.video_codec:
            self.pipeline.add(self._make_pipeline_video())

    def connect(self, stream):
        self._stream = stream
        # ...???

    def disconnect(self):
        self._stream = None

    def write_rtmp_meta_headers(self, ts):
        meta = dict(duration=0.0)

        if self.vsink:
            meta.update(width=self.width, height=self.height,
                        framerate=(float(self.framerate.numerator) /
                                   self.framerate.denominator),
                        videodatarate=self.video_bitrate,
                        videokeyframe_frequency=self.key_rate)
            if self.video_codec.codecId == 7:
                meta.update(videocodecid='avc1',
                            avcprofile=self.video_avc_profile,
                            avclevel=self.video_avc_level)

        if self.asink:
            meta.update(audiosamplerate=self.audio_rate,
                        audiodatarate=self.audio_bitrate,
                        audiochannels=self.channels)
            if self.audio_codec.codecId == 2:
                meta.update(audiocodecid='.mp3')
            elif self.audio_codec.codecId == 10:
                meta.update(audiocodecid='mp4a')

        wm = self._stream.write_meta
        wm(ts, amf0.encode('onStatus',
                           amf0.Object(code='NetStream.Data.Start')))
        wm(ts, amf0.encode('onMetaData', meta))
        wm(ts, amf0.encode('@setDataFrame', 'onMetaData', meta))

    def prepare_audio(self):
        log.info('prepare_audio, caps: "%s"', self.audio_caps)
        if self.audio_codec.codecId in (10, 11): # aac and speex are always
                                         # marked 44kHz, stereo
            rate = 44100
            channels = 2
        else:
            rate = self.audio_rate
            channels = self.channels

        h1 = self.audio_codec.codecId << 4
        h1 |= {44100: 3, 22050: 2, 11025: 1}.get(rate, 3) << 2
        h1 |= 1 << 1             # always 16-bit size
        h1 |= int(channels == 2)

        self._ah1 = h1

        if self.complex_audio_header:
            self._header_audio = _s_double_uchar.pack(h1, 1) # h1 | "keyframe"
        else:
            self._header_audio = _s_uchar.pack(h1)

    def prepare_video(self):
        log.info('prepare_video, caps: "%s"', self.video_caps)
        if self.video_codec.codecId == 7:
            codec_data = self.video_caps[0]['codec_data']
            self.video_avc_profile = ord(codec_data[1])
            self.video_avc_level = ord(codec_data[3])

            self._make_rtmp_video = self._make_rtmp_video_complex
        else:
            self._make_rtmp_video = self._make_rtmp_video_simple

    def write_rtmp_audio_headers(self, ts, codec_data):
        h = _s_double_uchar.pack(self._ah1, 0)
        for buf in codec_data:
            self._stream.write_audio(ts, VecBuf([h, buf]))

    def write_rtmp_video_headers(self, ts, codec_data):
        h = _s_h264video.pack(0x10 | self.video_codec.codecId, 0, 0, 0)
        for buf in codec_data:
            self._stream.write_video(ts, VecBuf([h, buf]))

    def make_rtmp_audio(self, gst_buf):
        return VecBuf([self._header_audio, buffer(gst_buf)])

    def _audio_data_cb(self, element):
        buf = element.emit('pull-buffer')
        ts = int(buf.timestamp / 1000000.0)
        ts = ts % 0x100000000   # api clients are supposed to handle
                                # timestamp wraps (4-bytes only)
        log.debug('[A] %5d [%d] %s', int(ts), int(buf.duration / 1000000),
                  ellip(str(buf).encode('hex'), maxlen=102))

        frame = self.make_rtmp_audio(buf)
        reactor.callFromThread(self._stream.write_audio, ts, frame)

    def _make_rtmp_video_simple(self, gst_buf):
        flags = 0x10
        if gst_buf.flags & gst.BUFFER_FLAG_DELTA_UNIT:
            flags = 0x20
        return VecBuf([_s_uchar.pack(flags | self.video_codec.codecId),
                       buffer(gst_buf)])

    def _make_rtmp_video_complex(self, gst_buf):
        flags = 0x10
        if gst_buf.flags & gst.BUFFER_FLAG_DELTA_UNIT:
            flags = 0x20
        return VecBuf([_s_h264video.pack(flags | self.video_codec.codecId, 1, 0, 0),
                       buffer(gst_buf)])

    def make_rtmp_video(self, gst_buf):
        return self._make_rtmp_video(gst_buf)

    def _video_data_cb(self, element):
        buf = element.emit('pull-buffer')
        ts = int(buf.timestamp / 1000000.0)
        ts = ts % 0x100000000
        log.debug('[V] %5d [%d] %s', int(ts), int(buf.duration / 1000000),
                  ellip(str(buf).encode('hex'), maxlen=102))

        frame = self.make_rtmp_video(buf)
        reactor.callFromThread(self._stream.write_video, ts, frame)

    def _got_caps_cb(self, pad, args):
        cid, is_video  = self._caps_pending.pop(pad)
        pad.disconnect(cid)

        caps = pad.get_negotiated_caps()
        if is_video:
            self.video_caps = caps
        else:
            self.audio_caps = caps

        if not self._caps_pending:
            self._have_caps()

    def _have_caps(self):
        self._attach_sink_callbacks()

        if self.vsink:
            self.prepare_video()

        if self.asink:
            self.prepare_audio()

        self._send_headers()

    def _send_headers(self):
        reactor.callFromThread(self.write_rtmp_meta_headers, 0)

        # first send headers for gormats that need headers
        if self.vsink:
            headers = self.video_codec.getHeadersFromCaps(self.video_caps)
            if headers is not None:
                reactor.callFromThread(self.write_rtmp_video_headers, 0,
                        headers)
        if self.asink:
            headers = self.audio_codec.getHeadersFromCaps(self.audio_caps)
            if headers is not None:
                reactor.callFromThread(self.write_rtmp_audio_headers, 0,
                                       headers)

    def _build_pipeline(self):
        self.make_pipeline()
        log.info('pipeline: %r', self.pipeline)

        self._caps_pending = {}
        if self.audio_codec:
            pad = self.asink.get_static_pad('sink')
            cid = pad.connect('notify::caps', self._got_caps_cb)
            self._caps_pending[pad] = (cid, 0)

        if self.video_codec:
            pad = self.vsink.get_static_pad('sink')
            cid = pad.connect('notify::caps', self._got_caps_cb)
            self._caps_pending[pad] = (cid, 1)

    def _attach_sink_callbacks(self):
        if self.asink:
            self.asink.connect('new-buffer', self._audio_data_cb)
            self.asink.props.emit_signals = True

        if self.vsink:
            self.vsink.connect('new-buffer', self._video_data_cb)
            self.vsink.props.emit_signals = True

    def _bus_message_eos_cb(self, bus, message):
        log.info("eos, quitting")
        reactor.stop()

    def _bus_message_error_cb(self, bus, message):
        gerror, debug = message.parse_error()
        log.error("%s -- %s", gerror.message, debug)
        reactor.stop()

    def _bus_sync_message_element_cb(self, bus, message):
        structure = message.structure
        if structure.get_name() == 'prepare-xwindow-id':
            if self.view_window is None:
                # let the sink create a window
                return

            sink = message.src
            sink.set_xwindow_id(self.view_window)

    def start(self):
        if self.pipeline is None:
            self._build_pipeline()

        bus = self.pipeline.get_bus()
        bus.add_signal_watch()
        bus.connect('message::eos', self._bus_message_eos_cb)
        bus.connect('message::error', self._bus_message_error_cb)
        bus.enable_sync_message_emission()
        bus.connect('sync-message::element', self._bus_sync_message_element_cb)

        self.pipeline.set_state(gst.STATE_PLAYING)

        msg = bus.timed_pop_filtered(20 * gst.SECOND,
                                     gst.MESSAGE_ERROR |
                                     gst.MESSAGE_ASYNC_DONE)
        if msg is None:
            log.error("timeout: pipeline failed to preroll")
            reactor.stop()

    def stop(self):
        if self.pipeline:
            self.pipeline.set_state(gst.STATE_NULL)


class SimplePublishingApp(BaseClientApp):
    def __init__(self, protocol, publish_source, publish_name='livestream'):
        BaseClientApp.__init__(self, protocol)
        self._stream = None
        self.publish_name = publish_name
        self.publish_source = publish_source

    def get_connect_params(self):
        return dict(
            # videoCodecs=252,
            # audioCodecs=3191,
            # videoFunction=1,
            # capabilities=15,
            )

    def connectionMade(self, info):
        log.info('Yay, connected! (%r)', info)
        # assert server (info) supports necessary codecs?
        d = self.createStream()
        d.addCallbacks(self._start_streaming, self._disconnect)

    def _start_streaming(self, stream):
        self._stream = stream
        stream.publish(self.publish_name, self.publish_source)

    def _disconnect(self, failure):
        self.disconnect()

    def connectionLost(self, reason):
        log.info('app disconnected: %s', reason.getErrorMessage())
        if self._stream:
            self.closeStream(self._stream, force=True)
            self._stream = None

        self.publish_source = None

    def connectionFailed(self, reason):
        log.error("app couldn't connect: %s", reason.getErrorMessage())


class OneTimeAppFactory(SimpleAppClientFactory):
    def clientConnectionFailed(self, _connector, reason):
        log.error('connecting to %r failed: %s', self.url,
                  reason.getErrorMessage())
        reactor.stop()

    def clientConnectionLost(self, _connector, reason):
        log.info('lost connection: %s', reason.getErrorMessage())
        if reactor.running:
            reactor.stop()


def run(url, stream_name='livestream', audio_codec=None, video_codec=None,
        audio_bitrate=64, video_bitrate=400, samplerate=44100,
        framerate=Fraction('15/1'), channels=1,
        width=320, height=240, keyframe_interval=5.0, view=False,
        view_window=None):
    src = NewGstSource(audio_bitrate, video_bitrate,
                       samplerate, channels, framerate, width, height,
                       keyframe_interval, audio_codec=audio_codec,
                       video_codec=video_codec, view=view,
                       view_window=view_window)
    f = connect_client_factory(url, OneTimeAppFactory,
                               SimplePublishingApp,
                               src,
                               stream_name)
    reactor.run()


def main(argv):
    import optparse

    def _check_fraction(option, opt, value):
        try:
            return Fraction(value)
        except ValueError:
            raise optparse.OptionValueError('option %s: invalid rational value:'
                                            ' %r' % (opt, value))

    optparse.Option.TYPES += ('Fraction',)
    optparse.Option.TYPE_CHECKER['Fraction'] = _check_fraction

    default_streamname = 'livestream'

    usage = '%prog [options] URL [STREAM-NAME]'
    epilog = ('URL should be of the form: rtmp://host[:port]/app, '
              'STREAM-NAME defaults to "%s".' % (default_streamname,))
    parser = optparse.OptionParser(usage=usage, epilog=epilog)

    audio_choices = sorted(audio_codecs.keys())
    video_choices = sorted(video_codecs.keys())

    parser.add_option('-a', '--audio-codec', action='store', dest='acodec',
                      default='aac',
                      choices=audio_choices,
                      help=('one of: %s; (default: %%default)' %
                            ', '.join(audio_choices)))
    parser.add_option('-v', '--video-codec', action='store', dest='vcodec',
                      default='h264',
                      choices=video_choices,
                      help=('one of: %s; (default: %%default)' %
                            ', '.join(video_choices)))
    parser.add_option('-r', '--audio-bitrate', action='store', dest='arate',
                      type='float',
                      help='audio bitrate in kbit/s (default: %default)',
                      default=64.0)
    parser.add_option('-R', '--video-bitrate', action='store', dest='vrate',
                      type='float',
                      help='video bitrate in kbit/s (default: %default)',
                      default=400.0)
    parser.add_option('-s', '--sample-rate', action='store', dest='srate',
                      type='int',
                      help=('audio sample rate in Hz (default: speex: 32000, '
                            'other codecs: 44100)'))
    parser.add_option('-f', '--frame-rate', action='store', dest='frate',
                      type='Fraction',
                      help='video frame rate in frames/sec (default: %default)',
                      default='15/1')
    parser.add_option('-c', '--channels', action='store', dest='channels',
                      type='int',
                      help='number of audio channels (default: %default)',
                      default=1)
    parser.add_option('-W', '--width', action='store', dest='width',
                      type='int',
                      help='video width (default: %default)',
                      default=320)
    parser.add_option('-H', '--height', action='store', dest='height',
                      type='int',
                      help='video height (default: %default)',
                      default=240)
    parser.add_option('-k', '--keyframe-interval', action='store',
                      dest='keyint', type='float',
                      help='keyframe interval in seconds (default: %default)',
                      default=5.0)
    parser.add_option('-d', '--debug', action='store', dest='debug',
                      help=('comma separated list of "[CATEGORY:]LEVEL" log'
                            ' level specifiers'),
                      metavar='LEVELS')
    parser.add_option('-w', '--view', action='store_true', dest='view',
                      help='view what is being published',
                      default=False)

    options, args = parser.parse_args(argv)

    if len(args) < 2:
        parser.error('No server URL specified.')
    url = args[1]

    stream_name = default_streamname
    if len(args) > 2:
        stream_name = args[2]

    audio_codec = audio_codecs.get(options.acodec, None)
    video_codec = video_codecs.get(options.vcodec, None)

    if video_codec is None and audio_codec is None:
        parser.error("Can't disable both audio and video.")

    twimp.log.set_levels_from_env()
    if options.debug:
        twimp.log.set_levels(options.debug)
    twimp.log.hook_twisted()

    run(url, stream_name, audio_codec=audio_codec, video_codec=video_codec,
        audio_bitrate=options.arate, video_bitrate=options.vrate,
        samplerate=options.srate, framerate=options.frate,
        channels=options.channels, width=options.width, height=options.height,
        keyframe_interval=options.keyint, view=options.view)


if __name__ == '__main__':
    import sys
    args, sys.argv[:] = sys.argv[:], sys.argv[0:1]
    import gst
    main(args)
