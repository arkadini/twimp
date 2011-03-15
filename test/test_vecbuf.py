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

from twimp.vecbuf import VecBuf, VecBufEOB, flatten


class TestFlatten(unittest.TestCase):
    def assertFlattens(self, to_flatten, target, msg=''):
        return self.assertEquals(flatten(to_flatten), target, msg=msg)

    def test_basic(self):
        self.assertFlattens([], '')
        self.assertFlattens([''], '')
        self.assertFlattens(['', ''], '')
        self.assertFlattens(['abc'], 'abc')
        self.assertFlattens(['ab', 'c'], 'abc')
        self.assertFlattens(['', 'ab', '', 'c', '', ''], 'abc')
        self.assertFlattens([buffer(''), 'ab', '', buffer('c'), ''], 'abc')
        self.assertFlattens([buffer('')], '')
        self.assertFlattens([buffer('a'), buffer('abc', 1, 1), 'c'], 'abc')


class TestVecBuf(unittest.TestCase):
    def assertMatches(self, a, b, msg=''):
        return self.assertEquals(a[:], b[:], msg=msg)

    def test_read_write_simple(self):
        b = VecBuf()

        b.write('abcd')
        self.assertMatches(b.read(4), 'abcd')

        self.assertRaises(VecBufEOB, b.read, 1)

    def test_read_write_mixed(self):
        b = VecBuf()

        b.write('abcd')
        self.assertMatches(b.read(4), 'abcd')

        self.assertRaises(VecBufEOB, b.read, 1)

    def test_read_write_empty_something(self):
        b = VecBuf()

        b.write('')
        b.write('')
        b.write('abcd')

        self.assertMatches(b.read(4), 'abcd')
        self.assertRaises(VecBufEOB, b.read, 1)

    def test_read_empty(self):
        b = VecBuf()

        self.assertMatches(b.read(0), '')
        self.assertRaises(VecBufEOB, b.read, 1)

        b.write('a')
        self.assertMatches(b.read(0), '')
        self.assertMatches(b.read(1), 'a')
        self.assertRaises(VecBufEOB, b.read, 1)

    def test_read_write_empty(self):
        b = VecBuf()

        self.assertMatches(b.read(0), '')

        b.write('')

        self.assertMatches(b.read(0), '')
        self.assertRaises(VecBufEOB, b.read, 1)

        b.write('')

        self.assertRaises(VecBufEOB, b.read, 1)
        self.assertMatches(b.read(0), '')
        self.assertRaises(VecBufEOB, b.read, 1)

    def test_read_write_not_aligned(self):
        b = VecBuf()

        b.write('ab')
        b.write('cd')
        b.write('')
        b.write('ef')
        b.write('gh')
        b.write('ij')

        self.assertMatches(b.read(0), '')
        self.assertMatches(b.read(1), 'a')
        self.assertMatches(b.read(2), 'bc')
        self.assertMatches(b.read(4), 'defg')
        self.assertMatches(b.read(0), '')
        self.assertMatches(b.read(2), 'hi')
        self.assertRaises(VecBufEOB, b.read, 2)
        self.assertMatches(b.read(0), '')
        self.assertMatches(b.read(1), 'j')
        self.assertRaises(VecBufEOB, b.read, 1)

        b.write('')
        b.write('abcd')
        b.write('efgh')
        b.write('ijkl')
        b.write('mnop')

        self.assertMatches(b.read(2), 'ab')
        self.assertMatches(b.read(8), 'cdefghij')
        self.assertMatches(b.read(4), 'klmn')

        self.assertRaises(VecBufEOB, b.read, 3)
        self.assertMatches(b.read(2), 'op')
        self.assertMatches(b.read(0), '')
        self.assertRaises(VecBufEOB, b.read, 1)

    def test_seq_read_empty(self):
        b = VecBuf()
        fl = flatten

        self.assertMatches(fl(b.read_seq(0)), '')
        self.assertRaises(VecBufEOB, b.read_seq, 1)

        b.write('')

        self.assertMatches(fl(b.read_seq(0)), '')
        self.assertRaises(VecBufEOB, b.read_seq, 1)

        b.write_seq([''])

        self.assertMatches(fl(b.read_seq(0)), '')
        self.assertRaises(VecBufEOB, b.read_seq, 1)

        b.write_seq(['', ''])

        self.assertMatches(fl(b.read_seq(0)), '')
        self.assertRaises(VecBufEOB, b.read_seq, 1)

    def test_seq_read_write(self):
        b = VecBuf()
        fl = flatten

        b.write_seq(['ab', 'cd', '', 'ef', 'gh', 'ij'])

        self.assertMatches(b.read(0), '')
        self.assertMatches(b.read(1), 'a')
        self.assertMatches(b.read(2), 'bc')
        self.assertMatches(b.read(4), 'defg')
        self.assertMatches(b.read(0), '')
        self.assertMatches(b.read(2), 'hi')
        self.assertRaises(VecBufEOB, b.read, 2)
        self.assertMatches(b.read(0), '')
        self.assertMatches(b.read(1), 'j')
        self.assertRaises(VecBufEOB, b.read, 1)

        b.write_seq(['', 'abcd', 'efgh', 'ijkl', 'mnop'])

        self.assertMatches(b.read(2), 'ab')
        self.assertMatches(fl(b.read_seq(8)), 'cdefghij')
        self.assertMatches(b.read(4), 'klmn')

        self.assertRaises(VecBufEOB, b.read_seq, 3)
        self.assertMatches(fl(b.read_seq(2)), 'op')
        self.assertRaises(VecBufEOB, b.read, 1)

    def test_seq_read_write_other(self):
        b = VecBuf()
        fl = flatten

        b.write_seq(['ab', 'cd', '', 'ef', 'gh', 'ij'])

        self.assertMatches(b.read(0), '')
        self.assertMatches(b.read(1), 'a')
        self.assertMatches(b.read(2), 'bc')

        b2 = VecBuf()

        b2.write_seq(b.read_seq(6))

        self.assertMatches(b.read(1), 'j')
        self.assertRaises(VecBufEOB, b.read, 1)

        self.assertMatches(b2.read(6), 'defghi')
        self.assertRaises(VecBufEOB, b2.read, 1)

    def test_clone(self):
        b = VecBuf()
        fl = flatten

        b.write_seq(['ab', 'cd', '', 'ef', 'gh', 'ij'])

        self.assertMatches(b.read(0), '')
        self.assertMatches(b.read(1), 'a')
        self.assertMatches(b.read(2), 'bc')

        b2 = b.read_clone(6)

        self.assertMatches(b.read(1), 'j')
        self.assertRaises(VecBufEOB, b.read, 1)

        self.assertMatches(b2.read(6), 'defghi')
        self.assertRaises(VecBufEOB, b2.read, 1)

    def test_peek(self):
        b = VecBuf()
        fl = flatten

        self.assertMatches(b.peek(0), '')
        self.assertRaises(VecBufEOB, b.peek, 1)
        self.assertRaises(VecBufEOB, b.peek_seq, 1)
        self.assertRaises(VecBufEOB, b.read, 1)

        b.write('ab')
        b.write_seq(['cd', '', 'ef'])

        self.assertMatches(b.peek(3), 'abc')
        self.assertMatches(b.peek(3), 'abc')
        self.assertMatches(fl(b.peek_seq(3)), 'abc')
        self.assertMatches(b.read(1), 'a')
        self.assertMatches(b.read(2), 'bc')

        b.write_seq(['gh', 'ij'])

        self.assertMatches(fl(b.peek_seq(7)), 'defghij')
        self.assertMatches(b.peek(7), 'defghij')
        self.assertMatches(fl(b.peek_seq(7)), 'defghij')
        self.assertMatches(b.peek(7), 'defghij')
        self.assertMatches(fl(b.read_seq(7)), 'defghij')

        self.assertMatches(b.peek(0), '')
        self.assertRaises(VecBufEOB, b.peek, 1)
        self.assertRaises(VecBufEOB, b.peek_seq, 1)
        self.assertRaises(VecBufEOB, b.read, 1)

    def test_a_scenario(self):
        b = VecBuf()

        b.write('abcde')
        b.write_seq(['fgh', 'ijkl'])
        b.write('mnopqr')

        self.assertMatches(b.read(1), 'a')
        self.assertMatches(b.read(1), 'b')
        self.assertMatches(b.read(4), 'cdef')
        self.assertMatches(b.read(10), 'ghijklmnop')
        self.assertMatches(b.read(1), 'q')
        self.assertMatches(b.read(1), 'r')

        self.assertRaises(VecBufEOB, b.read, 1)
