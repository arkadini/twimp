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


from twisted.trial import unittest

from twimp.utils import GeneratorWrapperProtocol, FrameSorter

from helpers import StringTransport


class TestGeneratorWrapperProtocol(unittest.TestCase):
    def setUp(self):
        self.messages = []

    def pop_messages(self):
        ret = self.messages[:]
        self.messages[:] = []
        return ret

    def test_simple_123(self):
        t = StringTransport()
        p = GeneratorWrapperProtocol()

        def _123_gen_handler():
            for n in (1, 2, 3):
                s = yield n
                self.messages.append(s.read(n)[:])

        p.init_handler(_123_gen_handler())

        p.makeConnection(t)

        p.dataReceived('abbccc')

        self.assertEquals(self.pop_messages(), ['a', 'bb', 'ccc'])

    def test_simple_123_parts(self):
        t = StringTransport()
        p = GeneratorWrapperProtocol()

        def _123_gen_handler():
            for n in (1, 2, 3):
                s = yield n
                self.messages.append(s.read(n)[:])

        p.init_handler(_123_gen_handler())

        p.makeConnection(t)

        p.dataReceived('a')
        self.assertEquals(self.pop_messages(), ['a'])

        p.dataReceived('b')
        self.assertEquals(self.pop_messages(), [])

        p.dataReceived('bc')
        self.assertEquals(self.pop_messages(), ['bb'])

        p.dataReceived('cc')
        self.assertEquals(self.pop_messages(), ['ccc'])

    def test_simple_123_partial_writes(self):
        t = StringTransport()
        p = GeneratorWrapperProtocol()

        def _123_gen_handler():
            for n in (1, 2, 3):
                s = yield n
                self.messages.append(s.read(n)[:])

        p.init_handler(_123_gen_handler())

        p.makeConnection(t)

        p.dataReceived('a')
        p.dataReceived('b')
        p.dataReceived('bc')
        p.dataReceived('cc')
        self.assertEquals(self.pop_messages(), ['a', 'bb', 'ccc'])

    def test_simple_123_plus(self):
        t = StringTransport()
        p = GeneratorWrapperProtocol()

        def _123_gen_handler():
            for n in (1, 2, 3):
                s = yield n
                self.messages.append(s.read(n)[:])

        p.init_handler(_123_gen_handler())

        p.makeConnection(t)

        p.dataReceived('abbcccX')

        self.assertEquals(self.pop_messages(), ['a', 'bb', 'ccc'])

    def test_changing_123(self):
        t = StringTransport()
        p = GeneratorWrapperProtocol()

        def _123_gen_handler():
            for n in (1, 2, 3):
                s = yield n
                self.messages.append(s.read(n)[:])

        p.init_handler(_123_gen_handler())

        p.makeConnection(t)

        p.dataReceived('abbccc')

        self.assertEquals(self.pop_messages(), ['a', 'bb', 'ccc'])

        p.init_handler(_123_gen_handler())
        p.dataReceived('abbcccX')

        self.assertEquals(self.pop_messages(), ['a', 'bb', 'ccc'])

        p.init_handler(_123_gen_handler())
        p.dataReceived('YYZZZ-')

        self.assertEquals(self.pop_messages(), ['X', 'YY', 'ZZZ'])

    def test_changing_live(self):
        t = StringTransport()
        p = GeneratorWrapperProtocol()

        def _123_gen_handler():
            for n in (1, 2, 3):
                s = yield n
                self.messages.append(s.read(n)[:])

        def _321_next_gen_handler(next):
            for n in (3, 2, 1):
                s = yield n
                self.messages.append(s.read(n)[:])
            next()

        def next():
            p.init_handler(_123_gen_handler())

        p.init_handler(_321_next_gen_handler(next))

        p.makeConnection(t)

        p.dataReceived('aaabbcabbcccX')

        self.assertEquals(self.pop_messages(), ['aaa', 'bb', 'c',
                                                'a', 'bb', 'ccc'])

    def test_late_init(self):
        t = StringTransport()
        p = GeneratorWrapperProtocol()

        def _123_gen_handler():
            for n in (1, 2, 3):
                s = yield n
                self.messages.append(s.read(n)[:])

        p.init_handler(_123_gen_handler(), do_init=False)
        p.makeConnection(t)
        p.init_handler()

        p.dataReceived('abbcccX')

        self.assertEquals(self.pop_messages(), ['a', 'bb', 'ccc'])


class TestFrameSorter(unittest.TestCase):
    def setUp(self):
        self.frames = []

    def frames_callback(self, grpos, type_, data):
        self.frames.append((grpos, type_, data))

    def clear(self):
        self.frames[:] = []

    def test_one_source_active(self):
        fs = FrameSorter(self.frames_callback, [1, 2])

        fs.add(1, 0, 'a')
        fs.add(1, 1, 'a')
        fs.add(1, 2, 'a')

        self.assertEquals(self.frames, [])

        fs.flush()

        self.assertEquals(self.frames, [(0, 1, 'a'),
                                        (1, 1, 'a'),
                                        (2, 1, 'a')])


    def test_sorted_input(self):
        fs = FrameSorter(self.frames_callback, [1, 2])

        fs.add(1, 0, 'a')
        fs.add(2, 1, 'b')
        fs.add(1, 2, 'a')
        fs.add(2, 3, 'b')
        fs.add(1, 4, 'a')
        fs.add(2, 5, 'b')

        self.assertEquals(self.frames, [(0, 1, 'a'),
                                        (1, 2, 'b'),
                                        (2, 1, 'a'),
                                        (3, 2, 'b'),
                                        (4, 1, 'a')])
        self.clear()

        fs.add(2, 6, 'b')
        fs.add(2, 7, 'b')

        self.assertEquals(self.frames, [])

        fs.add(1, 8, 'a')

        self.assertEquals(self.frames, [(5, 2, 'b'),
                                        (6, 2, 'b'),
                                        (7, 2, 'b')])
        self.clear()

        fs.add(2, 9, 'b')
        fs.add(1, 10, 'a')
        fs.add(2, 11, 'b')


        self.assertEquals(self.frames, [(8, 1, 'a'),
                                        (9, 2, 'b'),
                                        (10, 1, 'a')])
        self.clear()

        fs.flush()

        self.assertEquals(self.frames, [(11, 2, 'b')])

    def test_add_flush_add(self):
        fs = FrameSorter(self.frames_callback, [1, 2])

        fs.add(1, 0, 'a')
        fs.add(2, 1, 'b')
        fs.add(1, 2, 'a')

        self.assertEquals(self.frames, [(0, 1, 'a'),
                                        (1, 2, 'b')])
        self.clear()

        fs.flush()

        self.assertEquals(self.frames, [(2, 1, 'a')])
        self.clear()

        fs.add(2, 3, 'b')
        fs.add(1, 4, 'a')
        fs.add(2, 5, 'b')

        self.assertEquals(self.frames, [(3, 2, 'b'),
                                        (4, 1, 'a')])
        self.clear()

        fs.flush()

        self.assertEquals(self.frames, [(5, 2, 'b')])

    def test_not_sorted_input(self):
        fs = FrameSorter(self.frames_callback, [1, 2])

        fs.add(1, 0, 'a')
        fs.add(2, 1, 'b')
        fs.add(1, 5, 'a')
        fs.add(2, 2, 'b')
        fs.add(1, 10, 'a')
        fs.add(2, 3, 'b')

        self.assertEquals(self.frames, [(0, 1, 'a'),
                                        (1, 2, 'b'),
                                        (2, 2, 'b'),
                                        (3, 2, 'b')])
        self.clear()

        fs.add(2, 4, 'b')
        fs.add(2, 6, 'b')

        self.assertEquals(self.frames, [(4, 2, 'b'),
                                        (5, 1, 'a'),
                                        (6, 2, 'b')])
        self.clear()

        fs.add(1, 15, 'a')

        self.assertEquals(self.frames, [])

        fs.add(2, 7, 'b')
        fs.add(1, 20, 'a')
        fs.add(2, 16, 'b')


        self.assertEquals(self.frames, [(7, 2, 'b'),
                                        (10, 1, 'a'),
                                        (15, 1, 'a'),
                                        (16, 2, 'b')])
        self.clear()

        fs.flush()

        self.assertEquals(self.frames, [(20, 1, 'a')])
