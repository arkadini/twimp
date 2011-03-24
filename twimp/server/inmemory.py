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


from zope.interface import implements

from twisted.internet import defer

# try:
#     import tasks
#     HAVE_TASKS = 1
# except ImportError:
#     tasks = None
#     HAVE_TASKS = 0

from twimp.server.interfaces import IStream, ILiveStream, IStreamGroup
from twimp.server.interfaces import IStreamServer

from twimp.server.errors import InvalidFrameNumber, StreamNotFoundError
from twimp.server.errors import NamespaceNotFoundError, StreamExistsError


class IMServerStream(object):
    def __init__(self):
        self.meta = {}
        self.params = {}
        self.headers = []
        self.data = []

        self.data_offset = 0

        self.data_listeners = set()

        self.state = None            # ???


class IMServerStreamGroup(object):
    def __init__(self, name=None, namespace=None):
        self.meta = {}
        self.streams = []

        self.name = name
        self.namespace = namespace


class IMStream(object):
    implements(IStream)

    def __init__(self, server_stream):
        self._s = server_stream
        self._pos = 0
        self._grpos = 0


    # 'protected' helpers for easier subclassing
    def notify_write_listeners(self, grpos, flags, data):
        for c in self._s.data_listeners:
            try:
                c(grpos, flags, data)
            except:
                defer.fail()    # this should end up in logs somewhere... :/


    ##
    # IStreamGroup interface implementation

    def params(self):
        return defer.succeed(self._s.params.copy())

    def set_params(self, params):
        self._s.params = params.copy()
        return defer.succeed(None)

    def meta(self):
        return defer.succeed(self._s.meta.copy())

    def set_meta(self, meta):
        self._s.meta = meta.copy()
        return defer.succeed(None)


    def read_headers(self, callback):
        # [the immediate direct approach]:
        # for f in self._s.headers:
        #     callback(*f)
        #
        # return defer.succeed(None) # number of frames?

        # [the cancellable task approach]:
        # class SequenceIterTask(tasks.CompTask):
        #     def __init__(self, seq, callback):
        #         tasks.CompTask.__init__(self)
        #         self._iter = iter(seq)
        #         self._elt_callback = callback
        #
        #     def _call_read_callback(self, elt):
        #         self._elt_callback(*elt)
        #
        #     def do(self, input):
        #         try:
        #             elt = self._iter.next()
        #         except:
        #             return
        #
        #         return tasks.GenericTask(self._call_read_callback)(elt)
        #
        # t = SequenceIterTask(self._s.headers, callback)
        # return t, t.run()

        # [the immediate direct + non-cancellable task compromise approach]:
        for f in self._s.headers:
            callback(*f)

        # if HAVE_TASKS:
        #     t = tasks.CompTask()
        #     return t, t.run()

        return None, defer.succeed(None)

    def write_headers(self, data, grpos=0, flags=0):
        self._s.headers.append((grpos, flags, data))
        return defer.succeed(None)

    def seek(self, offset, whence=0, frames=None):
        raise NotImplementedError('FIXME!!!')

    def pseek(self, offset, whence=0, frames=None, flag_mask=0):
        raise NotImplementedError('FIXME!!!')

    def read(self, callback, grpos_range, frames=None):
        if grpos_range:
            # here we only handle the case of data shrinking from the
            # left / earlier side...
            end_grpos = self._grpos + grpos_range
            pos = self._pos
            grpos = self._grpos
            while 1:
                idx = pos - self._s.data_offset
                if idx < 0:
                    pos -= idx
                    idx = 0
                try:
                    f = self._s.data[idx]
                except IndexError:
                    # we're behind the last frame -> no read
                    break
                grpos = f[0]
                if grpos >= end_grpos:
                    grpos = end_grpos
                    break
                callback(*f)
                pos += 1
            self._pos = pos
            self._grpos = grpos
        elif frames:
            pos = self._pos
            grpos = self._grpos
            while 1:
                idx = pos - self._s.data_offset
                if idx < 0:
                    pos -= idx
                    frames += idx
                if frames < 1:
                    break
                try:
                    f = self._s.data[idx]
                except IndexError:
                    # we're behind the last frame -> no more read
                    break
                grpos = f[0]
                callback(*f)
                pos += 1
                frames -= 1
            self._pos = pos
            self._grpos = grpos

        # if HAVE_TASKS:
        #     t = tasks.CompTask()
        #     return t, t.run()

        return None, defer.succeed(None)


    def write(self, grpos, flags, data):
        self._s.data.append((grpos, flags, data))
        self.notify_write_listeners(grpos, flags, data)
        return defer.succeed(None)

    def _scan_from_end(self, grpos_range, frames=None, flag_mask=0):
        if not self._s.data:
            return None

        pos = len(self._s.data) - 1

        if grpos_range > 0:
            grpos = self._s.data[pos][0]
            target_grpos = grpos - grpos_range
            while pos > 0:
                pos -= 1
                f = self._s.data[pos]
                if f[0] < target_grpos:
                    pos += 1
                    break
        elif frames > 0:
            pos = max(0, len(self._s.data) - frames)

        if flag_mask < 0:
            mask = - flag_mask
            fpos = pos
            while fpos >= 0:
                f = self._s.data[fpos]
                if f[1] & mask:
                    break
                fpos -= 1
            if fpos >= 0:
                pos = fpos
        elif flag_mask > 0:
            mask = flag_mask
            fpos = pos
            end_pos = len(self._s.data) - 1
            while fpos <= end_pos:
                f = self._s.data[fpos]
                if f[1] & mask:
                    break
                fpos += 1
            if fpos <= end_pos:
                pos = fpos

        return self._s.data_offset + pos

    def trim(self, grpos_range, frames=None, flag_mask=0):
        raw_pos = self._scan_from_end(grpos_range, frames=frames,
                                      flag_mask=flag_mask)

        if raw_pos is not None:
            pos = raw_pos - self._s.data_offset
            if pos > 0:
                self._s.data_offset += pos
                self._s.data[:pos] = []

        return defer.succeed(None)

    def subscribe(self, callback, preroll_grpos_range=0, preroll_frames=0,
                  preroll_from_frame=None, flag_mask=0):
        pos = None

        if preroll_grpos_range > 0 or preroll_frames > 0:
            raw_pos = self._scan_from_end(preroll_grpos_range,
                                          frames=preroll_frames,
                                          flag_mask=flag_mask)
            if raw_pos is not None:
                pos = raw_pos - self._s.data_offset
        elif preroll_from_frame is not None:
            pos = preroll_from_frame - self._s.data_offset
            if not (0 <= pos < len(self._s.data)):
                e = InvalidFrameNumber('frame %r' % (preroll_from_frame))
                return defer.fail(e)

        if pos is not None:
            for f in self._s.data[pos:]:
                callback(*f)

        self._s.data_listeners.add(callback)
        return defer.succeed(callback)

    def unsubscribe(self, subscription):
        self._s.data_listeners.remove(subscription)
        return defer.succeed(None)


    def find_frame_backward(self, grpos_range, frames=None, flag_mask=0):
        pos = self._scan_from_end(grpos_range, frames=frames,
                                  flag_mask=flag_mask)

        return defer.succeed(pos)


    def frame_to_grpos(self, frame):
        data_len = len(self._s.data)
        if frame < 0:
            frame = self._s.data_offset + data_len + frame

        raw_frame = frame - self._s.data_offset

        if 0 <= raw_frame < data_len:
            return defer.succeed(self._s.data[raw_frame][0])

        return defer.fail(InvalidFrameNumber('frame %r' % (frame,)))


class IMLiveStream(IMStream):
    implements(ILiveStream)

    def __init__(self, server_stream):
        IMStream.__init__(self, server_stream)

        self._buffer_grpos = 0
        self._buffer_frames = 0
        self._buffer_flagmask = 0

        self._grpos_last = None
        self._grpos_first = None
        self._index = None

        if self._s.data:
            self._grpos_first = self._s.data[0][0]
            self._grpos_last = self._s.data[-1][0]

        self._set_buffering(grpos_range=0, frames=0, flag_mask=0)

    def write(self, grpos, flags, data):
        self._write_selected(grpos, flags, data)
        self.notify_write_listeners(grpos, flags, data)
        return defer.succeed(None)

    def set_buffering(self, grpos_range=0, frames=0, flag_mask=0):
        self._set_buffering(grpos_range=grpos_range, frames=frames,
                            flag_mask=flag_mask)
        return defer.succeed(None)

    def _set_buffering(self, grpos_range=0, frames=0, flag_mask=0):
        if grpos_range > 0:
            self._buffer_grpos = grpos_range
            self._buffer_frames = 0
            if flag_mask != 0:
                self._write_selected = self._write_buffering_with_index
                self._cut_selected = self._cut_grpos_flagmask
                self._buffer_flagmask = abs(flag_mask)
                self._init_index()
            else:
                self._write_selected = self._write_buffering_no_index
                self._cut_selected = self._cut_grpos
                self._buffer_flagmask = 0
                self._index = None
        elif frames > 0:
            self._buffer_frames = frames
            self._buffer_grpos = 0
            if flag_mask != 0:
                self._write_selected = self._write_buffering_with_index
                self._cut_selected = self._cut_frames_flagmask
                self._buffer_flagmask = abs(flag_mask)
                self._init_index()
            else:
                self._write_selected = self._write_buffering_no_index
                self._cut_selected = self._cut_frames
                self._buffer_flagmask = 0
                self._index = None
        else:
            self._buffer_grpos = 0
            self._buffer_frames = 0
            self._buffer_flagmask = 0
            self._write_selected = self._write_no_buffering

    def _init_index(self):
        index = []
        if self._s.data:
            offset = self._s.data_offset
            for i, (grpos, flags, data) in enumerate(self._s.data):
                if self._buffer_flagmask & flags:
                    index.append((i + offset, grpos))
        self._index = index

    def _cut_grpos(self):
        d = self._s.data
        target_grpos = self._grpos_last - self._buffer_grpos
        pos, grpos, l = 0, self._grpos_first, len(d)
        while grpos < target_grpos and pos < l:
            pos += 1
            grpos = d[pos][0]
        if pos > 0:
            self._s.data_offset, d[:pos] = self._s.data_offset + pos, []
            self._grpos_first = d[0][0]

    def _cut_frames(self):
        d = self._s.data
        l = len(d)
        pos = l - self._buffer_frames
        if pos > 0:
            self._s.data_offset, d[:pos] = self._s.data_offset + pos, []
            self._grpos_first = d[0][0]

    def _cut_grpos_flagmask(self):
        d = self._s.data
        target_grpos = self._grpos_last - self._buffer_grpos
        i_pos, i_len = 0, len(self._index)
        while i_pos < i_len:
            abs_pos, grpos = self._index[i_pos]
            if grpos > target_grpos:
                break
            i_pos += 1
        if i_pos > 0:
            i_pos -= 1
            offset = self._s.data_offset
            pos, self._index[:i_pos] = self._index[i_pos][0] - offset, []
            self._s.data_offset, d[:pos] = offset + pos, []
            self._grpos_first = d[0][0]

    def _cut_frames_flagmask(self):
        offset = self._s.data_offset
        target_pos = len(self._s.data) - self._buffer_frames
        if target_pos < 1:
            return
        i_pos, i_len = 0, len(self._index)
        target_pos += offset
        while i_pos < i_len:
            abs_pos, grpos = self._index[i_pos]
            if abs_pos > target_pos:
                break
            i_pos += 1
        if i_pos > 0:
            i_pos -= 1
            pos, self._index[:i_pos] = self._index[i_pos][0] - offset, []
            d = self._s.data
            self._s.data_offset, d[:pos] = offset + pos, []
            self._grpos_first = d[0][0]

    def _write_no_buffering(self, grpos, flags, data):
        if self._s.data:
            self._s.data[0] = (grpos, flags, data)
        else:
            self._s.data.append((grpos, flags, data))

        self._s.data_offset += 1
        self._grpos_first = self._grpos_last = grpos

    def _write_buffering_no_index(self, grpos, flags, data):
        self._s.data.append((grpos, flags, data))
        if self._grpos_first is None:
            self._grpos_first = self._s.data[0][0]
        self._grpos_last = grpos
        self._cut_selected()

    def _update_index(self, frame, grpos, flags):
        if flags & self._buffer_flagmask:
            self._index.append((frame, grpos))

    def _write_buffering_with_index(self, grpos, flags, data):
        self._s.data.append((grpos, flags, data))
        if self._grpos_first is None:
            self._grpos_first = self._s.data[0][0]
        self._grpos_last = grpos
        self._update_index(len(self._s.data) + self._s.data_offset - 1,
                           grpos, flags)
        self._cut_selected()


class IMStreamGroup(object):
    implements(IStreamGroup)

    def __init__(self, server_streamgroup):
        self._g = server_streamgroup
        self._streams = self.build_streams(self._g.streams)


    # 'protected' helpers for easier subclassing
    def build_stream(self, server_stream):
        return IMStream(server_stream)

    def build_streams(self, server_streams):
        return [self.build_stream(s) for s in server_streams]


    ##
    # IStreamGroup interface implementation

    def meta(self):
        return defer.succeed(self._g.meta.copy())

    def set_meta(self, meta):
        self._g.meta = meta.copy()
        return defer.succeed(None)

    def streams(self):
        return defer.succeed(self._streams[:])

    def streams_by_params(self, template):
        def got_params(results):
            t = frozenset(template.iteritems())
            return [s for (s, params) in zip(self._streams,
                                             [r[1] for r in results])
                    if t <= frozenset(params.iteritems())]

        dl = defer.DeferredList([s.params() for s in self._streams],
                                fireOnOneErrback=1, consumeErrors=1)
        dl.addCallback(got_params)
        return dl

    def add_stream(self, stream):
        raise NotImplementedError('FIXME!!!')

    def make_stream(self):
        ss = IMServerStream()
        self._g.streams.append(ss)

        s = self.build_stream(ss)
        self._streams.append(s)

        return defer.succeed(s)

    def seek(self, offset, whence=0):
        raise NotImplementedError('FIXME!!!')

    def read_to(self, callback, grpos, cb_args_map=None):
        raise NotImplementedError('FIXME!!!')

    def subscribe(self, callback, preroll_grpos_range=0,
                  preroll_from_frames=None, cb_args_map=None):

        pos_frames = None
        if not preroll_grpos_range and preroll_from_frames is not None:
            pos_frames = [(s, preroll_from_frames[s]) for s in self._streams]

        def extra_args_cb_maker(callback, s):
            if s in cb_args_map:
                args = cb_args_map[s]
                if not isinstance(args, (tuple, list)):
                    args = (args,)
            else:
                args = ()

            def cb_wrapper(time, flags, data):
                callback(time, flags, data, *args)

            return cb_wrapper

        def subscriptions_made(results, streams):
            return [(s, r[1]) for (s, r) in zip(streams, results)]

        if cb_args_map is None:
            cb_args_map = {}

        ds = []

        if pos_frames is None:
            for s in self._streams:
                cb = extra_args_cb_maker(callback, s)
                d = s.subscribe(cb, preroll_grpos_range=preroll_grpos_range)
                ds.append(d)
        else:
            for s, from_frame in pos_frames:
                cb = extra_args_cb_maker(callback, s)
                d = s.subscribe(cb, preroll_from_frame=from_frame)
                ds.append(d)

        dl = defer.DeferredList(ds, fireOnOneErrback=1, consumeErrors=1)
        dl.addCallback(subscriptions_made, self._streams)
        return dl

    def unsubscribe(self, subscription):
        dl = defer.DeferredList([stream.unsubscribe(sub)
                                 for (stream, sub) in subscription],
                                fireOnOneErrback=1, consumeErrors=1)

        dl.addCallback(lambda results: None)
        return dl


class IMLiveStreamGroup(IMStreamGroup):
    implements(IStreamGroup)

    def build_stream(self, server_stream):
        return IMLiveStream(server_stream)


class IMServer(object):
    implements(IStreamServer)

    def __init__(self, namespaces=None):
        # _store: { namespace => { name => stream_group } }
        if namespaces:
            self._store = dict((ns, {}) for ns in namespaces)
        else:
            self._store = {None: {}}

    def open(self, name, mode='r', namespace=None):
        if mode not in ('r', 'l'):
            raise NotImplementedError('TBD later!')

        if mode == 'l':
            return defer.maybeDeferred(self._open_live, namespace, name)
        elif mode == 'r':
            return defer.maybeDeferred(self._open_readable, namespace, name)

    def _open_readable(self, namespace, name):
        ns = self._store.get(namespace, None)
        if ns is None:
            raise NamespaceNotFoundError('Unknown namespace %r' % namespace)

        server_sg = ns.get(name, None)
        if server_sg is None:
            raise StreamNotFoundError('Unknown stream %r' % name)

        sg = IMStreamGroup(server_sg)
        return defer.succeed(sg)

    def _open_live(self, namespace, name):
        ns = self._store.get(namespace, None)
        if ns is None:
            raise NamespaceNotFoundError('Unknown namespace %r' % namespace)

        server_sg = ns.get(name, None)
        if server_sg is not None:
            raise StreamExistsError('Stream already exists: %r' % name)

        server_sg = IMServerStreamGroup(name, namespace)
        ns[name] = server_sg

        sg = IMLiveStreamGroup(server_sg)

        return defer.succeed(sg)

    def close(self, streamgroup):
        if isinstance(streamgroup, IMLiveStreamGroup):
            server_sg = streamgroup._g # cheating a bit...
            ns = self._store[server_sg.namespace]
            del ns[server_sg.name]

        return defer.succeed(None)

    def delete(self, streamgroup):
        raise NotImplementedError('TBD later!')
