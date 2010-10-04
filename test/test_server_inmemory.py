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


from twisted.internet import defer
from twisted.trial import unittest

from rtmp.server import interfaces, inmemory
from rtmp.vecbuf import VecBuf, flatten

# from helpers import StringTransport


class TestIMStream(unittest.TestCase):
    def setUp(self):
        ss = inmemory.IMServerStream()
        ss.meta = dict(comment='Just a comment', foo='bar')
        ss.params = dict(type='unknown/x-uknown', format='????')
        ss.headers = [(0, 0, 'abcd')]
        ss.data = [(0, 1, '.1'), (5, 2, '.'), (10, 2, '.'), (15, 2, '.'),
                   (20, 1, '.2'), (25, 2, '.'), (30, 2, '.'), (35, 2, '.'),
                   (40, 1, '!!')]
        self.s = inmemory.IMStream(ss)

        ess = inmemory.IMServerStream()
        self.es = inmemory.IMStream(ess)

    def test_params_get(self):
        d = self.s.params()
        d.addCallback(self.assertEquals,
                      {'type': 'unknown/x-uknown', 'format': '????'})
        return d

    def test_params_set(self):
        d = self.es.params()
        d.addCallback(self.assertEquals, {})

        def set_params(_result):
            return self.es.set_params(dict(type='test/x-test'))
        d.addCallback(set_params)

        d.addCallback(lambda _: self.es.params())
        d.addCallback(self.assertEquals,
                      {'type': 'test/x-test'})
        return d

    def test_params_reset(self):
        d = self.s.params()
        d.addCallback(self.assertEquals,
                      {'type': 'unknown/x-uknown', 'format': '????'})

        def set_params(_result):
            return self.s.set_params(dict(type='test/x-test'))
        d.addCallback(set_params)

        d.addCallback(lambda _: self.s.params())
        d.addCallback(self.assertEquals,
                      {'type': 'test/x-test'})
        return d

    def test_meta_get(self):
        d = self.s.meta()
        d.addCallback(self.assertEquals,
                      {'foo': 'bar', 'comment': 'Just a comment'})
        return d

    def test_meta_set(self):
        d = self.es.meta()
        d.addCallback(self.assertEquals, {})

        def set_meta(_result):
            return self.es.set_meta(dict(type='test/x-test'))
        d.addCallback(set_meta)

        d.addCallback(lambda _: self.es.meta())
        d.addCallback(self.assertEquals,
                      {'type': 'test/x-test'})
        return d

    def test_meta_reset(self):
        d = self.s.meta()
        d.addCallback(self.assertEquals,
                      {'foo': 'bar', 'comment': 'Just a comment'})

        def set_meta(_result):
            return self.s.set_meta(dict(type='test/x-test'))
        d.addCallback(set_meta)

        d.addCallback(lambda _: self.s.meta())
        d.addCallback(self.assertEquals,
                      {'type': 'test/x-test'})
        return d

    def test_headers_read(self):
        stored = []
        def read_headers_callback(grpos, flags, data):
            stored.append((grpos, flags, data))

        task, d = self.s.read_headers(read_headers_callback)
        d.addCallback(lambda _: self.assertEquals(stored, [(0, 0, 'abcd')]))

    def test_headers_write(self):
        d = self.es.write_headers('abcd')

        stored = []
        def read_headers_callback(grpos, flags, data):
            stored.append((grpos, flags, data))
        def clear():
            stored[:] = []

        d.addCallback(lambda _: self.es.read_headers(read_headers_callback)[1])
        d.addCallback(lambda _: self.assertEquals(stored, [(0, 0, 'abcd')]))
        d.addCallback(lambda _: clear())

        d.addCallback(lambda _: self.es.write_headers('efgh', 1, 4))
        d.addCallback(lambda _: self.es.read_headers(read_headers_callback)[1])
        d.addCallback(lambda _: self.assertEquals(stored, [(0, 0, 'abcd'),
                                                           (1, 4, 'efgh')]))
        return d

    def test_seek(self):
        d = self.s.seek(0, 0)
        return d
    test_seek.todo = (NotImplementedError, 'seeking to be implemented later')

    def test_pseek(self):
        d = self.s.pseek(0, 0)
        return d
    test_pseek.todo = (NotImplementedError, 'seeking to be implemented later')

    def test_read_grpos(self):
        stored = []
        def read_callback(grpos, flags, data):
            stored.append((grpos, flags, data))
        def clear():
            stored[:] = []

        task, d = self.s.read(read_callback, 12)
        d.addCallback(lambda _: self.assertEquals(stored,
                                                  [(0, 1, '.1'),
                                                   (5, 2, '.'),
                                                   (10, 2, '.')]))

        d.addCallback(lambda _: clear())
        d.addCallback(lambda _: self.s.read(read_callback, 2)[1])
        d.addCallback(lambda _: self.assertEquals(stored, []))

        # grpos_range is left-inclusive, frame at 15 shouldn't be read
        d.addCallback(lambda _: clear())
        d.addCallback(lambda _: self.s.read(read_callback, 1)[1])
        d.addCallback(lambda _: self.assertEquals(stored, []))

        # ... and now it should
        d.addCallback(lambda _: clear())
        d.addCallback(lambda _: self.s.read(read_callback, 1)[1])
        d.addCallback(lambda _: self.assertEquals(stored,
                                                  [(15, 2, '.')]))

        d.addCallback(lambda _: clear())
        d.addCallback(lambda _: self.s.read(read_callback, 10)[1])
        d.addCallback(lambda _: self.assertEquals(stored,
                                                  [(20, 1, '.2'),
                                                   (25, 2, '.')]))

        d.addCallback(lambda _: clear())
        d.addCallback(lambda _: self.s.read(read_callback, 0)[1])
        d.addCallback(lambda _: self.assertEquals(stored, []))

        d.addCallback(lambda _: clear())
        d.addCallback(lambda _: self.s.read(read_callback, 99)[1])
        d.addCallback(lambda _: self.assertEquals(stored,
                                                  [(30, 2, '.'),
                                                   (35, 2, '.'),
                                                   (40, 1, '!!')]))
        return d

    def test_read_frames(self):
        stored = []
        def read_callback(grpos, flags, data):
            stored.append((grpos, flags, data))
        def clear():
            stored[:] = []

        task, d = self.s.read(read_callback, None, frames=3)
        d.addCallback(lambda _: self.assertEquals(stored,
                                                  [(0, 1, '.1'),
                                                   (5, 2, '.'),
                                                   (10, 2, '.')]))

        d.addCallback(lambda _: clear())
        d.addCallback(lambda _: self.s.read(read_callback, None, frames=0)[1])
        d.addCallback(lambda _: self.assertEquals(stored, []))

        d.addCallback(lambda _: clear())
        d.addCallback(lambda _: self.s.read(read_callback, None, frames=1)[1])
        d.addCallback(lambda _: self.assertEquals(stored,
                                                  [(15, 2, '.')]))

        d.addCallback(lambda _: clear())
        d.addCallback(lambda _: self.s.read(read_callback, None, frames=2)[1])
        d.addCallback(lambda _: self.assertEquals(stored,
                                                  [(20, 1, '.2'),
                                                   (25, 2, '.')]))

        d.addCallback(lambda _: clear())
        d.addCallback(lambda _: self.s.read(read_callback, None, frames=0)[1])
        d.addCallback(lambda _: self.assertEquals(stored, []))

        d.addCallback(lambda _: clear())
        d.addCallback(lambda _: self.s.read(read_callback, None, frames=99)[1])
        d.addCallback(lambda _: self.assertEquals(stored,
                                                  [(30, 2, '.'),
                                                   (35, 2, '.'),
                                                   (40, 1, '!!')]))
        return d

    def test_read_mixed(self):
        stored = []
        def read_callback(grpos, flags, data):
            stored.append((grpos, flags, data))
        def clear():
            stored[:] = []

        task, d = self.s.read(read_callback, None, frames=2)
        d.addCallback(lambda _: self.assertEquals(stored,
                                                  [(0, 1, '.1'),
                                                   (5, 2, '.')]))

        d.addCallback(lambda _: clear())
        d.addCallback(lambda _: self.s.read(read_callback, 4)[1])
        d.addCallback(lambda _: self.assertEquals(stored, []))

        d.addCallback(lambda _: clear())
        d.addCallback(lambda _: self.s.read(read_callback, None, frames=1)[1])
        d.addCallback(lambda _: self.assertEquals(stored,
                                                  [(10, 2, '.')]))

        d.addCallback(lambda _: clear())
        d.addCallback(lambda _: self.s.read(read_callback, 4)[1])
        d.addCallback(lambda _: self.assertEquals(stored, []))

        d.addCallback(lambda _: clear())
        d.addCallback(lambda _: self.s.read(read_callback, None, frames=0)[1])
        d.addCallback(lambda _: self.assertEquals(stored, []))

        d.addCallback(lambda _: clear())
        d.addCallback(lambda _: self.s.read(read_callback, 1)[1])
        d.addCallback(lambda _: self.assertEquals(stored, []))

        d.addCallback(lambda _: clear())
        d.addCallback(lambda _: self.s.read(read_callback, 3)[1])
        d.addCallback(lambda _: self.assertEquals(stored,
                                                  [(15, 2, '.')]))

        d.addCallback(lambda _: clear())
        d.addCallback(lambda _: self.s.read(read_callback, None, frames=1)[1])
        d.addCallback(lambda _: self.assertEquals(stored,
                                                  [(20, 1, '.2')]))

        d.addCallback(lambda _: clear())
        d.addCallback(lambda _: self.s.read(read_callback, None, frames=2)[1])
        d.addCallback(lambda _: self.assertEquals(stored,
                                                  [(25, 2, '.'),
                                                   (30, 2, '.')]))

        d.addCallback(lambda _: clear())
        d.addCallback(lambda _: self.s.read(read_callback, 5)[1])
        d.addCallback(lambda _: self.assertEquals(stored, []))

        d.addCallback(lambda _: clear())
        d.addCallback(lambda _: self.s.read(read_callback, 5)[1])
        d.addCallback(lambda _: self.assertEquals(stored,
                                                  [(35, 2, '.')]))

        d.addCallback(lambda _: clear())
        d.addCallback(lambda _: self.s.read(read_callback, None, frames=1)[1])
        d.addCallback(lambda _: self.assertEquals(stored,
                                                  [(40, 1, '!!')]))

        return d

    def test_trim_grpos(self):
        stored = []
        def read_callback(grpos, flags, data):
            stored.append((grpos, flags, data))
        def clear():
            stored[:] = []

        d = self.s.trim(17)

        d.addCallback(lambda _: self.s.read(read_callback, 21)[1])
        d.addCallback(lambda _: self.assertEquals(stored, []))

        d.addCallback(lambda _: clear())
        d.addCallback(lambda _: self.s.read(read_callback, 20)[1])
        d.addCallback(lambda _: self.assertEquals(stored,
                                                  [(25, 2, '.'),
                                                   (30, 2, '.'),
                                                   (35, 2, '.'),
                                                   (40, 1, '!!')]))
        return d

    def test_trim_frames(self):
        stored = []
        def read_callback(grpos, flags, data):
            stored.append((grpos, flags, data))
        def clear():
            stored[:] = []

        d = self.s.trim(None, frames=4)

        d.addCallback(lambda _: self.s.read(read_callback, 21)[1])
        d.addCallback(lambda _: self.assertEquals(stored, []))

        d.addCallback(lambda _: clear())
        d.addCallback(lambda _: self.s.read(read_callback, 20)[1])
        d.addCallback(lambda _: self.assertEquals(stored,
                                                  [(25, 2, '.'),
                                                   (30, 2, '.'),
                                                   (35, 2, '.'),
                                                   (40, 1, '!!')]))
        return d

    def test_trim_grpos_flagmask_negative(self):
        stored = []
        def read_callback(grpos, flags, data):
            stored.append((grpos, flags, data))
        def clear():
            stored[:] = []

        d = self.s.trim(17, flag_mask=-1)

        d.addCallback(lambda _: self.s.read(read_callback, 19)[1])
        d.addCallback(lambda _: self.assertEquals(stored, []))

        d.addCallback(lambda _: clear())
        d.addCallback(lambda _: self.s.read(read_callback, 22)[1])
        d.addCallback(lambda _: self.assertEquals(stored,
                                                  [(20, 1, '.2'),
                                                   (25, 2, '.'),
                                                   (30, 2, '.'),
                                                   (35, 2, '.'),
                                                   (40, 1, '!!')]))
        return d

    def test_trim_grpos_flagmask_positive(self):
        stored = []
        def read_callback(grpos, flags, data):
            stored.append((grpos, flags, data))
        def clear():
            stored[:] = []

        d = self.s.trim(27, flag_mask=1)

        d.addCallback(lambda _: self.s.read(read_callback, 19)[1])
        d.addCallback(lambda _: self.assertEquals(stored, []))

        d.addCallback(lambda _: clear())
        d.addCallback(lambda _: self.s.read(read_callback, 22)[1])
        d.addCallback(lambda _: self.assertEquals(stored,
                                                  [(20, 1, '.2'),
                                                   (25, 2, '.'),
                                                   (30, 2, '.'),
                                                   (35, 2, '.'),
                                                   (40, 1, '!!')]))
        return d

    def test_read_trim_read(self):
        stored = []
        def read_callback(grpos, flags, data):
            stored.append((grpos, flags, data))
        def clear():
            stored[:] = []

        task, d = self.s.read(read_callback, 26)
        d.addCallback(lambda _: self.assertEquals(stored,
                                                  [(0, 1, '.1'),
                                                   (5, 2, '.'),
                                                   (10, 2, '.'),
                                                   (15, 2, '.'),
                                                   (20, 1, '.2'),
                                                   (25, 2, '.')]))
        d.addCallback(lambda _: clear())

        d.addCallback(lambda _: self.s.trim(20, flag_mask=1))

        d.addCallback(lambda _: self.s.read(read_callback, 10)[1])
        d.addCallback(lambda _: self.assertEquals(stored,
                                                  [(30, 2, '.'),
                                                   (35, 2, '.')]))
        d.addCallback(lambda _: clear())

        d.addCallback(lambda _: self.s.trim(9))

        d.addCallback(lambda _: self.s.read(read_callback, 5)[1])
        d.addCallback(lambda _: self.assertEquals(stored,
                                                  [(40, 1, '!!')]))
        return d

    def test_write(self):
        stored = []
        def read_callback(grpos, flags, data):
            stored.append((grpos, flags, data))
        def clear():
            stored[:] = []

        task, d = self.es.read(read_callback, None, frames=1)
        d.addCallback(lambda _: self.assertEquals(stored, []))

        d.addCallback(lambda _: self.es.write(15, 1, '.3'))
        d.addCallback(lambda _: self.es.write(20, 2, '.'))
        d.addCallback(lambda _: self.es.write(25, 2, '.'))

        d.addCallback(lambda _: self.es.read(read_callback, 9)[1])
        d.addCallback(lambda _: self.assertEquals(stored, []))

        d.addCallback(lambda _: clear())
        d.addCallback(lambda _: self.es.read(read_callback, 10)[1])
        d.addCallback(lambda _: self.assertEquals(stored,
                                                  [(15, 1, '.3')]))

        d.addCallback(lambda _: self.es.write(30, 2, '.'))
        d.addCallback(lambda _: self.es.write(35, 1, '.4'))

        d.addCallback(lambda _: clear())
        d.addCallback(lambda _: self.es.read(read_callback, 16.001)[1])
        d.addCallback(lambda _: self.assertEquals(stored,
                                                  [(20, 2, '.'),
                                                   (25, 2, '.'),
                                                   (30, 2, '.'),
                                                   (35, 1, '.4')]))

        return d

    def test_write_append(self):
        stored = []
        def read_callback(grpos, flags, data):
            stored.append((grpos, flags, data))
        def clear():
            stored[:] = []

        d = self.s.write(45, 2, '.')
        d.addCallback(lambda _: self.s.write(50, 2, '.'))
        d.addCallback(lambda _: self.s.write(55, 2, '.'))


        d.addCallback(lambda _: self.s.read(read_callback, 60)[1])
        d.addCallback(lambda _: self.assertEquals(stored,
                                                  [(0, 1, '.1'),
                                                   (5, 2, '.'),
                                                   (10, 2, '.'),
                                                   (15, 2, '.'),
                                                   (20, 1, '.2'),
                                                   (25, 2, '.'),
                                                   (30, 2, '.'),
                                                   (35, 2, '.'),
                                                   (40, 1, '!!'),
                                                   (45, 2, '.'),
                                                   (50, 2, '.'),
                                                   (55, 2, '.')]))
        return d

    def test_write_append_mixed_read(self):
        stored = []
        def read_callback(grpos, flags, data):
            stored.append((grpos, flags, data))
        def clear():
            stored[:] = []

        task, d = self.s.read(read_callback, 60)
        d.addCallback(lambda _: self.assertEquals(stored,
                                                  [(0, 1, '.1'),
                                                   (5, 2, '.'),
                                                   (10, 2, '.'),
                                                   (15, 2, '.'),
                                                   (20, 1, '.2'),
                                                   (25, 2, '.'),
                                                   (30, 2, '.'),
                                                   (35, 2, '.'),
                                                   (40, 1, '!!')]))

        d.addCallback(lambda _: self.s.write(45, 2, '.'))
        d.addCallback(lambda _: self.s.write(50, 2, '.'))
        d.addCallback(lambda _: self.s.write(55, 2, '.'))


        d.addCallback(lambda _: clear())
        d.addCallback(lambda _: self.s.read(read_callback, 11)[1])
        d.addCallback(lambda _: self.assertEquals(stored,
                                                  [(45, 2, '.'),
                                                   (50, 2, '.')]))

        d.addCallback(lambda _: clear())
        d.addCallback(lambda _: self.s.read(read_callback, 5)[1])
        d.addCallback(lambda _: self.assertEquals(stored,
                                                  [(55, 2, '.')]))
        return d

    def test_subscribe_no_preroll(self):
        box = [None]
        stored = []
        def read_callback(grpos, flags, data):
            stored.append((grpos, flags, data))
        def clear():
            stored[:] = []

        task, d = self.es.read(read_callback, None, frames=1)
        d.addCallback(lambda _: self.assertEquals(stored, []))

        def subscribed(subscription):
            box[0] = subscription
            return subscription
        def subscribe(_result):
            d = self.es.subscribe(read_callback)
            d.addCallback(subscribed)
            return d

        d.addCallback(subscribe)

        d.addCallback(lambda _: self.assertEquals(stored, []))

        d.addCallback(lambda _: self.es.write(15, 1, '.3'))
        d.addCallback(lambda _: self.es.write(20, 2, '.'))
        d.addCallback(lambda _: self.es.write(25, 2, '.'))

        d.addCallback(lambda _: self.assertEquals(stored,
                                                  [(15, 1, '.3'),
                                                   (20, 2, '.'),
                                                   (25, 2, '.')]))

        def unsubscribe(_result):
            d = self.es.unsubscribe(box[0])
            d.addCallback(self.assertEquals, None)
            return d

        d.addCallback(unsubscribe)

        d.addCallback(lambda _: clear())

        d.addCallback(lambda _: self.es.write(30, 2, '.'))
        d.addCallback(lambda _: self.es.write(35, 1, '.4'))
        d.addCallback(lambda _: self.es.write(40, 2, '.'))

        d.addCallback(lambda _: self.assertEquals(stored, []))

        return d

    def test_subscribe_preroll_grpos(self):
        box = [None]
        stored = []
        def read_callback(grpos, flags, data):
            stored.append((grpos, flags, data))
        def clear():
            stored[:] = []

        def subscribed(subscription):
            box[0] = subscription
            return subscription

        d = self.s.subscribe(read_callback, preroll_grpos_range=10)
        d.addCallback(subscribed)

        # d.addCallback(lambda _: self.assertEquals(stored, []))

        d.addCallback(lambda _: self.s.write(45, 2, '.'))
        d.addCallback(lambda _: self.s.write(50, 2, '.'))
        d.addCallback(lambda _: self.s.write(55, 2, '.'))

        d.addCallback(lambda _: self.assertEquals(stored,
                                                  [(30, 2, '.'),
                                                   (35, 2, '.'),
                                                   (40, 1, '!!'),
                                                   (45, 2, '.'),
                                                   (50, 2, '.'),
                                                   (55, 2, '.')]))

        def unsubscribe(_result):
            d = self.s.unsubscribe(box[0])
            d.addCallback(self.assertEquals, None)
            return d

        d.addCallback(unsubscribe)

        d.addCallback(lambda _: clear())

        d.addCallback(lambda _: self.s.write(60, 2, '.'))

        d.addCallback(lambda _: self.assertEquals(stored, []))

        return d

    def test_subscribe_preroll_grpos_flagmask_negative(self):
        stored = []
        def read_callback(grpos, flags, data):
            stored.append((grpos, flags, data))
        def clear():
            stored[:] = []

        d = self.s.subscribe(read_callback, preroll_grpos_range=10,
                             flag_mask=-1)

        d.addCallback(lambda _: self.s.write(45, 2, '.'))

        d.addCallback(lambda _: self.assertEquals(stored,
                                                  [(20, 1, '.2'),
                                                   (25, 2, '.'),
                                                   (30, 2, '.'),
                                                   (35, 2, '.'),
                                                   (40, 1, '!!'),
                                                   (45, 2, '.')]))

        return d

    def test_subscribe_preroll_grpos_flagmask_positive(self):
        stored = []
        def read_callback(grpos, flags, data):
            stored.append((grpos, flags, data))
        def clear():
            stored[:] = []

        d = self.s.subscribe(read_callback, preroll_grpos_range=25,
                             flag_mask=1)

        d.addCallback(lambda _: self.s.write(45, 2, '.'))

        d.addCallback(lambda _: self.assertEquals(stored,
                                                  [(20, 1, '.2'),
                                                   (25, 2, '.'),
                                                   (30, 2, '.'),
                                                   (35, 2, '.'),
                                                   (40, 1, '!!'),
                                                   (45, 2, '.')]))

        return d

    def test_subscribe_preroll_frames(self):
        box = [None]
        stored = []
        def read_callback(grpos, flags, data):
            stored.append((grpos, flags, data))
        def clear():
            stored[:] = []

        def subscribed(subscription):
            box[0] = subscription
            return subscription

        d = self.s.subscribe(read_callback, preroll_frames=3)
        d.addCallback(subscribed)

        # d.addCallback(lambda _: self.assertEquals(stored, []))

        d.addCallback(lambda _: self.s.write(45, 2, '.'))
        d.addCallback(lambda _: self.s.write(50, 2, '.'))
        d.addCallback(lambda _: self.s.write(55, 2, '.'))

        d.addCallback(lambda _: self.assertEquals(stored,
                                                  [(30, 2, '.'),
                                                   (35, 2, '.'),
                                                   (40, 1, '!!'),
                                                   (45, 2, '.'),
                                                   (50, 2, '.'),
                                                   (55, 2, '.')]))

        def unsubscribe(_result):
            d = self.s.unsubscribe(box[0])
            d.addCallback(self.assertEquals, None)
            return d

        d.addCallback(unsubscribe)

        d.addCallback(lambda _: clear())

        d.addCallback(lambda _: self.s.write(60, 2, '.'))

        d.addCallback(lambda _: self.assertEquals(stored, []))

        return d

    def test_subscribe_preroll_from_frame(self):
        box = [None]
        stored = []
        def read_callback(grpos, flags, data):
            stored.append((grpos, flags, data))
        def clear():
            stored[:] = []

        def subscribed(subscription):
            box[0] = subscription
            return subscription

        d = self.s.subscribe(read_callback, preroll_from_frame=6)
        d.addCallback(subscribed)

        d.addCallback(lambda _: self.s.write(45, 2, '.'))
        d.addCallback(lambda _: self.s.write(50, 2, '.'))
        d.addCallback(lambda _: self.s.write(55, 2, '.'))

        d.addCallback(lambda _: self.assertEquals(stored,
                                                  [(30, 2, '.'),
                                                   (35, 2, '.'),
                                                   (40, 1, '!!'),
                                                   (45, 2, '.'),
                                                   (50, 2, '.'),
                                                   (55, 2, '.')]))

        def unsubscribe(_result):
            d = self.s.unsubscribe(box[0])
            d.addCallback(self.assertEquals, None)
            return d

        d.addCallback(unsubscribe)

        d.addCallback(lambda _: clear())

        d.addCallback(lambda _: self.s.write(60, 2, '.'))

        d.addCallback(lambda _: self.assertEquals(stored, []))

        return d

    def test_find_frame_backward_grpos(self):
        d = self.s.find_frame_backward(17)
        d.addCallback(self.assertEquals, 5)
        return d

    def test_find_frame_backward_frames(self):
        d = self.s.find_frame_backward(None, frames=4)
        d.addCallback(self.assertEquals, 5)
        return d

    def test_find_frame_backward_grpos_flagmask_neg(self):
        d = self.s.find_frame_backward(17, flag_mask=-1)
        d.addCallback(self.assertEquals, 4)
        return d

    def test_find_frame_backward_grpos_flagmask_pos(self):
        d = self.s.find_frame_backward(27, flag_mask=1)
        d.addCallback(self.assertEquals, 4)
        return d

    def test_find_frame_backward_frames_flagmask_neg(self):
        d = self.s.find_frame_backward(None, frames=4, flag_mask=-1)
        d.addCallback(self.assertEquals, 4)
        return d

    def test_find_frame_backward_frames_flagmask_pos(self):
        d = self.s.find_frame_backward(None, frames=6, flag_mask=1)
        d.addCallback(self.assertEquals, 4)
        return d

    def test_find_frame_backward_grpos_exceeding(self):
        d = self.s.find_frame_backward(57)
        d.addCallback(self.assertEquals, 0)
        return d

    def test_find_frame_backward_frames_exceeding(self):
        d = self.s.find_frame_backward(None, frames=10)
        d.addCallback(self.assertEquals, 0)
        return d

    def test_find_frame_backward_grpos_exceeding_flagmask_pos(self):
        d = self.s.find_frame_backward(57, flag_mask=2)
        d.addCallback(self.assertEquals, 1)
        return d

    def test_find_frame_backward_frames_exceeding_flagmask_pos(self):
        d = self.s.find_frame_backward(None, frames=10, flag_mask=2)
        d.addCallback(self.assertEquals, 1)
        return d

    def test_find_frame_backward_grpos_after_trim(self):
        # let's just leave the last frame
        d = self.s.trim(1)

        # ... so we find the last frame
        d.addCallback(lambda _: self.s.find_frame_backward(17))

        # ... whose number is 8
        d.addCallback(self.assertEquals, 8)
        return d

    def test_frame_to_grpos(self):
        d = self.s.frame_to_grpos(0)
        d.addCallback(self.assertEquals, 0)

        d.addCallback(lambda _: self.s.frame_to_grpos(3))
        d.addCallback(self.assertEquals, 15)

        d.addCallback(lambda _: self.s.frame_to_grpos(8))
        d.addCallback(self.assertEquals, 40)

        d.addCallback(lambda _: self.s.frame_to_grpos(12))
        return self.assertFailure(d, inmemory.InvalidFrameNumber)

    def test_frame_to_grpos_after_trim_1(self):
        d = self.s.frame_to_grpos(0)
        d.addCallback(self.assertEquals, 0)

        d.addCallback(lambda _: self.s.frame_to_grpos(3))
        d.addCallback(self.assertEquals, 15)

        d.addCallback(lambda _: self.s.frame_to_grpos(8))
        d.addCallback(self.assertEquals, 40)

        d.addCallback(lambda _: self.s.trim(10))

        d.addCallback(lambda _: self.s.frame_to_grpos(8))
        d.addCallback(self.assertEquals, 40)

        d.addCallback(lambda _: self.s.frame_to_grpos(5))
        return self.assertFailure(d, inmemory.InvalidFrameNumber)

    def test_frame_to_grpos_after_trim_2(self):
        d = self.s.frame_to_grpos(0)
        d.addCallback(self.assertEquals, 0)

        d.addCallback(lambda _: self.s.frame_to_grpos(3))
        d.addCallback(self.assertEquals, 15)

        d.addCallback(lambda _: self.s.frame_to_grpos(8))
        d.addCallback(self.assertEquals, 40)

        d.addCallback(lambda _: self.s.trim(10))

        d.addCallback(lambda _: self.s.frame_to_grpos(8))
        d.addCallback(self.assertEquals, 40)

        d.addCallback(lambda _: self.s.frame_to_grpos(12))
        return self.assertFailure(d, inmemory.InvalidFrameNumber)


class TestIMLiveStream(unittest.TestCase):
    def test_no_buffer__no_preroll(self):
        ls = inmemory.IMLiveStream(inmemory.IMServerStream())

        return self._test_subscribe_no_preroll(ls)

    def test_buffer_grpos__no_preroll(self):
        ls = inmemory.IMLiveStream(inmemory.IMServerStream())
        ls.set_buffering(grpos_range=10)

        return self._test_subscribe_no_preroll(ls)

    def test_buffer_frames__no_preroll(self):
        ls = inmemory.IMLiveStream(inmemory.IMServerStream())
        ls.set_buffering(frames=3)

        return self._test_subscribe_no_preroll(ls)

    def test_buffer_grpos_flagmask__no_preroll(self):
        ls = inmemory.IMLiveStream(inmemory.IMServerStream())
        ls.set_buffering(grpos_range=10, flag_mask=1)

        return self._test_subscribe_no_preroll(ls)

    def test_buffer_frames_flagmask__no_preroll(self):
        ls = inmemory.IMLiveStream(inmemory.IMServerStream())
        ls.set_buffering(frames=3, flag_mask=1)

        return self._test_subscribe_no_preroll(ls)

    def _test_subscribe_no_preroll(self, ls):
        box = [None]
        stored = []
        def read_callback(grpos, flags, data):
            stored.append((grpos, flags, data))
        def clear():
            stored[:] = []

        task, d = ls.read(read_callback, None, frames=1)
        d.addCallback(lambda _: self.assertEquals(stored, []))

        def subscribed(subscription):
            box[0] = subscription
            return subscription
        def subscribe(_result):
            d = ls.subscribe(read_callback)
            d.addCallback(subscribed)
            return d

        d.addCallback(subscribe)

        d.addCallback(lambda _: self.assertEquals(stored, []))

        d.addCallback(lambda _: ls.write(10, 2, '.'))
        d.addCallback(lambda _: ls.write(15, 1, '.3'))
        d.addCallback(lambda _: ls.write(20, 2, '.'))
        d.addCallback(lambda _: ls.write(25, 2, '.'))

        d.addCallback(lambda _: self.assertEquals(stored,
                                                  [(10, 2, '.'),
                                                   (15, 1, '.3'),
                                                   (20, 2, '.'),
                                                   (25, 2, '.')]))

        def unsubscribe(_result):
            d = ls.unsubscribe(box[0])
            d.addCallback(self.assertEquals, None)
            return d

        d.addCallback(unsubscribe)

        d.addCallback(lambda _: clear())

        d.addCallback(lambda _: ls.write(30, 2, '.'))
        d.addCallback(lambda _: ls.write(35, 1, '.4'))
        d.addCallback(lambda _: ls.write(40, 2, '.'))

        d.addCallback(lambda _: self.assertEquals(stored, []))

        return d


    def test_no_buffer__preroll_a_lot(self):
        ls = inmemory.IMLiveStream(inmemory.IMServerStream())

        return self._test_subscribe_preroll(ls, 1,
                                            {'preroll_grpos_range': 70})

    def test_buffer_grpos__preroll_a_lot(self):
        ls = inmemory.IMLiveStream(inmemory.IMServerStream())
        ls.set_buffering(grpos_range=10)

        return self._test_subscribe_preroll(ls, 3,
                                            {'preroll_grpos_range': 70})

    def test_buffer_frames__preroll_a_lot(self):
        ls = inmemory.IMLiveStream(inmemory.IMServerStream())
        ls.set_buffering(grpos_range=10)

        return self._test_subscribe_preroll(ls, 3,
                                            {'preroll_grpos_range': 70})

    def test_buffer_grpos_flagmask__preroll_a_lot_1(self):
        ls = inmemory.IMLiveStream(inmemory.IMServerStream())
        ls.set_buffering(grpos_range=10, flag_mask=1)

        return self._test_subscribe_preroll(ls, 4,
                                            {'preroll_grpos_range': 70})

    def test_buffer_grpos_flagmask__preroll_a_lot_2(self):
        ls = inmemory.IMLiveStream(inmemory.IMServerStream())
        ls.set_buffering(grpos_range=20, flag_mask=1)

        return self._test_subscribe_preroll(ls, 8,
                                            {'preroll_grpos_range': 70})

    def test_buffer_frames_flagmask__preroll_a_lot_1(self):
        ls = inmemory.IMLiveStream(inmemory.IMServerStream())
        ls.set_buffering(frames=3, flag_mask=1)

        return self._test_subscribe_preroll(ls, 4,
                                            {'preroll_grpos_range': 70})

    def test_buffer_frames_flagmask__preroll_a_lot_2(self):
        ls = inmemory.IMLiveStream(inmemory.IMServerStream())
        ls.set_buffering(frames=5, flag_mask=1)

        return self._test_subscribe_preroll(ls, 8,
                                            {'preroll_grpos_range': 70})

    def _test_subscribe_preroll(self, ls, should_match, prerol_kw):
        box = [None]
        stored = []
        def read_callback(grpos, flags, data):
            stored.append((grpos, flags, data))
        def clear():
            stored[:] = []

        task, d = ls.read(read_callback, None, frames=1)
        d.addCallback(lambda _: self.assertEquals(stored, []))

        def subscribed(subscription):
            box[0] = subscription
            return subscription
        def subscribe(_result):
            d = ls.subscribe(read_callback, **prerol_kw)
            d.addCallback(subscribed)
            return d

        d.addCallback(lambda _: ls.write(10, 2, '.'))
        d.addCallback(lambda _: ls.write(15, 1, '.3'))
        d.addCallback(lambda _: ls.write(20, 2, '.'))
        d.addCallback(lambda _: ls.write(25, 2, '.'))
        d.addCallback(lambda _: ls.write(30, 2, '.'))
        d.addCallback(lambda _: ls.write(35, 1, '.4'))
        d.addCallback(lambda _: ls.write(40, 2, '.'))
        d.addCallback(lambda _: ls.write(45, 2, '.'))
        d.addCallback(lambda _: ls.write(50, 2, '.'))

        d.addCallback(subscribe)


        to_match = [(10, 2, '.'),
                    (15, 1, '.3'),
                    (20, 2, '.'),
                    (25, 2, '.'),
                    (30, 2, '.'),
                    (35, 1, '.4'),
                    (40, 2, '.'),
                    (45, 2, '.'),
                    (50, 2, '.')][-should_match:]

        d.addCallback(lambda _: self.assertEquals(stored,
                                                  to_match))

        def unsubscribe(_result):
            d = ls.unsubscribe(box[0])
            d.addCallback(self.assertEquals, None)
            return d

        d.addCallback(unsubscribe)

        d.addCallback(lambda _: clear())

        d.addCallback(lambda _: ls.write(55, 1, '.5'))
        d.addCallback(lambda _: ls.write(60, 2, '.'))

        d.addCallback(lambda _: self.assertEquals(stored, []))

        return d


class TestIMStreamGroup(unittest.TestCase):
    def setUp(self):
        s1 = inmemory.IMServerStream()
        s1.meta = dict(comment='This is stream 1', foo='bar')
        s1.params = dict(type='data/x-test-1', format='dat1')
        s1.headers = [(0, 0, 'abcd')]
        s1.data = [(10, 1, '.1'), (15, 2, '.'), (20, 2, '.'), (25, 2, '.'),
                   (30, 1, '.2'), (35, 2, '.'), (40, 2, '.'), (45, 2, '.'),
                   (50, 1, '!!')]

        s2 = inmemory.IMServerStream()
        s2.meta = dict(comment='This is stream 2', foo='baz')
        s2.params = dict(type='data/x-test-2', format='dat2')
        s2.headers = [(0, 0, 'efgh')]
        s2.data = [(6, 1, ':a'), (9, 1, ':b'), (12, 1, ':c'), (15, 1, ':d'),
                   (18, 1, ':e'), (21, 1, ':f'), (24, 1, ':g'), (27, 1, ':h'),
                   (30, 1, ':i'), (33, 1, ':j'), (36, 1, ':k'), (39, 1, ':l'),
                   (42, 1, ':m'), (45, 1, ':n'), (48, 1, ':o'), (51, 1, ':p'),
                   (54, 1, ':q'), ]

        sg = inmemory.IMServerStreamGroup()
        sg.streams = [s2, s1]
        sg.meta = dict(comment='This is a group of stream 1 and stream 2',
                       f1='some field', f2='some other field', f3=3)

        self.sg = inmemory.IMStreamGroup(sg)

        self.eg = inmemory.IMStreamGroup(inmemory.IMServerStreamGroup())

    def test_meta_get(self):
        d = self.sg.meta()
        d.addCallback(self.assertEquals,
                      {'comment': 'This is a group of stream 1 and stream 2',
                       'f1': 'some field',
                       'f2': 'some other field',
                       'f3': 3})
        return d

    def test_meta_set(self):
        d = self.eg.meta()
        d.addCallback(self.assertEquals, {})

        d.addCallback(lambda _: self.eg.set_meta(dict(field='value')))

        d.addCallback(lambda _: self.eg.meta())
        d.addCallback(self.assertEquals,
                      {'field': 'value'})
        return d

    def test_streams(self):
        def got_streams(streams):
            self.assertEquals(len(streams), 2)
            for s in streams:
                self.assertTrue(interfaces.IStream.providedBy(s))

        d = self.sg.streams()
        d.addCallback(got_streams)

        return d

    def test_streams_by_params(self):
        sg, eg = self.sg, self.eg

        def got_one_stream(streams):
            self.assertEquals(len(streams), 1)
            s = streams[0]
            self.assertTrue(interfaces.IStream.providedBy(s))
            return s.params()

        def check_meta(params, check_value):
            self.assertEquals(params, check_value)

        def got_number_streams(streams, number):
            self.assertEquals(len(streams), number)
            return streams

        d = sg.streams_by_params({'type': 'data/x-test-1'})
        d.addCallback(got_one_stream)
        d.addCallback(check_meta, {'type': 'data/x-test-1',
                                   'format': 'dat1'})

        d.addCallback(lambda _: eg.streams_by_params({'type': 'any/x-any'}))
        d.addCallback(got_number_streams, 0)

        d.addCallback(lambda _: sg.streams_by_params({'type': 'any/x-any'}))
        d.addCallback(got_number_streams, 0)

        d.addCallback(lambda _: sg.streams_by_params({}))
        d.addCallback(got_number_streams, 2)

        return d

    def test_make_stream(self):
        eg = self.eg

        def stream_made(stream):
            self.assertTrue(interfaces.IStream.providedBy(stream))
            return stream

        def got_number_streams(streams, number):
            self.assertEquals(len(streams), number)
            return streams

        d = eg.streams()
        d.addCallback(got_number_streams, 0)
        d.addCallback(lambda _: eg.make_stream())
        d.addCallback(stream_made)

        d.addCallback(lambda _: eg.streams())
        d.addCallback(got_number_streams, 1)

        return d

    def test_seek(self):
        d = self.sg.seek(0, 0)
        return d
    test_seek.todo = (NotImplementedError, 'seeking to be implemented later')

    def test_read_to(self):
        d = self.sg.read_to(lambda *a: None, 10)
        return d
    test_read_to.todo = (NotImplementedError,
                         'random access reading to be implemented later')

    def test_subscribe_no_preroll(self):
        sg = self.sg
        stored = []
        box = [None]
        def read_callback(grpos, flags, data, type_):
            stored.append((grpos, flags, data, type_))
        def clear():
            stored[:] = []
        def got_subscription(subscription):
            box[0] = subscription
            return subscription

        type_to_const = {'data/x-test-1': 1, 'data/x-test-2': 2}
        stream_to_const = {}
        const_to_stream = {}

        def got_params(results, streams):
            for (_, params), stream in zip(results, streams):
                const = type_to_const[params['type']]
                stream_to_const[stream] = const
                const_to_stream[const] = stream
        def got_streams(streams):
            dl = defer.DeferredList([s.params() for s in streams],
                                    fireOnOneErrback=1)
            dl.addCallback(got_params, streams)
            return dl


        # build the cb_args_map first
        d = sg.streams()
        d.addCallback(got_streams)

        # now subscribe
        d.addCallback(lambda _: sg.subscribe(read_callback,
                                             cb_args_map=stream_to_const))
        d.addCallback(got_subscription)
        d.addCallback(lambda _: const_to_stream[1].write(55, 2, ','))
        d.addCallback(lambda _: const_to_stream[2].write(57, 1, ':r'))
        d.addCallback(lambda _: const_to_stream[2].write(60, 1, ':s'))
        d.addCallback(lambda _: const_to_stream[1].write(60, 2, '/'))

        d.addCallback(lambda _: self.assertEquals(stored,
                                                  [(55, 2, ',', 1),
                                                   (57, 1, ':r', 2),
                                                   (60, 1, ':s', 2),
                                                   (60, 2, '/', 1)]))

        d.addCallback(lambda _: clear())
        d.addCallback(lambda _: sg.unsubscribe(box[0]))

        d.addCallback(lambda _: const_to_stream[2].write(63, 1, ':t'))
        d.addCallback(lambda _: const_to_stream[1].write(65, 2, '?'))

        d.addCallback(lambda _: self.assertEquals(stored, []))
        return d

    def test_subscribe_preroll_grpos(self):
        sg = self.sg
        stored = []
        box = [None]
        def read_callback(grpos, flags, data, type_):
            stored.append((grpos, flags, data, type_))
        def clear():
            stored[:] = []
        def got_subscription(subscription):
            box[0] = subscription
            return subscription

        type_to_const = {'data/x-test-1': 1, 'data/x-test-2': 2}
        stream_to_const = {}
        const_to_stream = {}

        def got_params(results, streams):
            for (_, params), stream in zip(results, streams):
                const = type_to_const[params['type']]
                stream_to_const[stream] = const
                const_to_stream[const] = stream
        def got_streams(streams):
            dl = defer.DeferredList([s.params() for s in streams],
                                    fireOnOneErrback=1)
            dl.addCallback(got_params, streams)
            return dl


        # build the cb_args_map first
        d = sg.streams()
        d.addCallback(got_streams)

        # now subscribe
        d.addCallback(lambda _: sg.subscribe(read_callback,
                                             preroll_grpos_range=11,
                                             cb_args_map=stream_to_const))
        d.addCallback(got_subscription)
        d.addCallback(lambda _: const_to_stream[1].write(55, 2, ','))
        d.addCallback(lambda _: const_to_stream[2].write(57, 1, ':r'))
        d.addCallback(lambda _: const_to_stream[2].write(60, 1, ':s'))
        d.addCallback(lambda _: const_to_stream[1].write(60, 2, '/'))


        d.addCallback(lambda _: self.assertEquals(set(stored),
                                                  set([(40, 2, '.', 1),
                                                       (45, 1, ':n', 2),
                                                       (45, 2, '.', 1),
                                                       (48, 1, ':o', 2),
                                                       (50, 1, '!!', 1),
                                                       (51, 1, ':p', 2),
                                                       (54, 1, ':q', 2),
                                                       (55, 2, ',', 1),
                                                       (57, 1, ':r', 2),
                                                       (60, 1, ':s', 2),
                                                       (60, 2, '/', 1)])))

        # the following check should pass if interleaving is implemented
        d.addCallback(lambda _: self.assertEquals(set(stored[:4]),
                                                  set([(40, 2, '.', 1),
                                                       (45, 1, ':n', 2),
                                                       (45, 2, '.', 1),
                                                       (48, 1, ':o', 2)])))

        d.addCallback(lambda _: clear())
        d.addCallback(lambda _: sg.unsubscribe(box[0]))

        d.addCallback(lambda _: const_to_stream[2].write(63, 1, ':t'))
        d.addCallback(lambda _: const_to_stream[1].write(65, 2, '?'))

        d.addCallback(lambda _: self.assertEquals(stored, []))
        return d
    test_subscribe_preroll_grpos.todo = 'Implement interleaving!'

    def test_subscribe_preroll_from_frames(self):
        sg = self.sg
        stored = []
        box = [None]
        def read_callback(grpos, flags, data, type_):
            stored.append((grpos, flags, data, type_))
        def clear():
            stored[:] = []
        def got_subscription(subscription):
            box[0] = subscription
            return subscription

        type_to_const = {'data/x-test-1': 1, 'data/x-test-2': 2}
        stream_to_const = {}
        const_to_stream = {}
        pos_frames = {}

        def got_params(results, streams):
            for (_, params), stream in zip(results, streams):
                const = type_to_const[params['type']]
                stream_to_const[stream] = const
                const_to_stream[const] = stream
            pos_frames[const_to_stream[1]] = 6
            pos_frames[const_to_stream[2]] = 13

        def got_streams(streams):
            dl = defer.DeferredList([s.params() for s in streams],
                                    fireOnOneErrback=1)
            dl.addCallback(got_params, streams)
            return dl


        # build the cb_args_map first
        d = sg.streams()
        d.addCallback(got_streams)

        # now subscribe
        d.addCallback(lambda _: sg.subscribe(read_callback,
                                             preroll_from_frames=pos_frames,
                                             cb_args_map=stream_to_const))
        d.addCallback(got_subscription)
        d.addCallback(lambda _: const_to_stream[1].write(55, 2, ','))
        d.addCallback(lambda _: const_to_stream[2].write(57, 1, ':r'))
        d.addCallback(lambda _: const_to_stream[2].write(60, 1, ':s'))
        d.addCallback(lambda _: const_to_stream[1].write(60, 2, '/'))


        d.addCallback(lambda _: self.assertEquals(set(stored),
                                                  set([(40, 2, '.', 1),
                                                       (45, 1, ':n', 2),
                                                       (45, 2, '.', 1),
                                                       (48, 1, ':o', 2),
                                                       (50, 1, '!!', 1),
                                                       (51, 1, ':p', 2),
                                                       (54, 1, ':q', 2),
                                                       (55, 2, ',', 1),
                                                       (57, 1, ':r', 2),
                                                       (60, 1, ':s', 2),
                                                       (60, 2, '/', 1)])))

        # the following check should pass if interleaving is implemented
        d.addCallback(lambda _: self.assertEquals(set(stored[:4]),
                                                  set([(40, 2, '.', 1),
                                                       (45, 1, ':n', 2),
                                                       (45, 2, '.', 1),
                                                       (48, 1, ':o', 2)])))

        d.addCallback(lambda _: clear())
        d.addCallback(lambda _: sg.unsubscribe(box[0]))

        d.addCallback(lambda _: const_to_stream[2].write(63, 1, ':t'))
        d.addCallback(lambda _: const_to_stream[1].write(65, 2, '?'))

        d.addCallback(lambda _: self.assertEquals(stored, []))
        return d
    test_subscribe_preroll_from_frames.todo = 'Implement interleaving!'
