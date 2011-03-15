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


import datetime

from twisted.trial import unittest

from twimp import amf0
from twimp.amf0 import decode, encode
from twimp.vecbuf import VecBuf, flatten


def p(s):
    return ''.join(s.split()).decode('hex')

def v(s):
    return VecBuf([p(s)])


simple_data = [
    # number
    ('00 4010000000000000', [4.0]),
    ('00 408f400000000000', [1000.0]),
    ('00 c08f400000000000', [-1000.0]),

    # boolean
    ('01 01', [True]),
    ('01 ff', [True]),
    ('01 00', [False]),

    # short string
    ('02 0003 616263', ['abc']),
    ('02 0001 01', ['\x01']),
    ('02 0000', ['']),

    # short unicode string
    ('02 0005 c48562c487', [u'\u0105b\u0107']),

    # object, simple properties
    ('03'                        # object marker
     '000162 003ff0000000000000' # .b = 1.0
     '000161 004008000000000000' # .a = 3.0
     '000163 004000000000000000' # .c = 2.0
     '000009',                   # empty name + end marker
     [amf0.Object(a=3.0, b=1.0, c=2.0)]),
    # ...
    ('03'                        # object marker
     '000009',                   # empty name + end marker
     [amf0.Object()]),
    ('03'
     '000a636c6173735f6e616d65 0200064f626a656374'
     '000009',
     [amf0.Object(class_name='Object')]),

    # skip movieclip - not supported

    # null
    ('05', [None]),

    # undefined
    ('06', [amf0.undefined]),

    # reference
    ('07 0001', [amf0.Reference(1)]),

    # ecma-array
    ('08 00000003'
     '000162 003ff0000000000000' # 'b': 1.0
     '000161 004008000000000000' # 'a': 3.0
     '000163 004000000000000000' # 'c': 2.0
     '000009',                   # empty name + end marker
     [amf0.ECMAArray([('b', 1.0), ('a', 3.0), ('c', 2.0)])]),

    # strict-array
    ('0a 00000003'
     '00 c08f400000000000'
     '01 00'
     '02 0003 616263',
     [[-1000.0, False, 'abc']]),

    # date
    ('0b 427272085a5f0000 0000',
     [datetime.datetime(2010, 3, 2, 20, 16, 22, tzinfo=amf0.utc)]),

    # long string
    ('0c 00011111' + ('2e' * 69905),
     ['.' * 69905]),

    ]

skip_encoding_tests_simple = [
    4,                          # 255 ways to encode a True boolean value
    10,                         # properties can be encoded in arbitrary order
    ]

complex_data = [
    # multiple simple values
    ('00 c08f400000000000'
     '01 01'
     '02 0003 616263',
     [-1000.0, True, 'abc']),

    # nested strict-arrays
    ('0a 00000005'
     '0a 00000003 00 c08f400000000000 01 01 02 0003 616263'
     '02 0001 00'
     '0a 00000003 00 c08f400000000000 01 00 02 0003 646566'
     '02 0001 01'
     '0a 00000003 00 c08f400000000000 01 01 02 0003 676869'
     '01 01'
     '02 0005 6162636465',
     [[[-1000.0, True, 'abc'],
       '\x00',
       [-1000.0, False, 'def'],
       '\x01',
       [-1000.0, True, 'ghi']],
      True,
      'abcde']),

    # strict-array
    ('0a 00000003'
     '00 c08f400000000000'
     '01 00'
     '02 0003 616263',
     [[-1000.0, False, 'abc']]),

    # Object, nested ecma-array, nested Object
    ('03'
     '000161 08 00000001 000161 03 000009 000009'
     '000009',
     [amf0.Object(a=amf0.ECMAArray([('a', amf0.Object())]))]),

    # ecma-array, nested strict-array
    ('08 00000003'
     '000162 003ff0000000000000' # 'b': 1.0
     '000161 0a 00000003 00 c08f400000000000 01 01 02 0003 616263'
     '000163 004000000000000000' # 'c': 2.0
     '000009',                   # empty name + end marker
     [amf0.ECMAArray([('b', 1.0),
                      ('a', [-1000.0, True, 'abc']),
                      ('c', 2.0)])]),

    ]

class DataTestMixin(object):
    def test_decode(self):
        for i, (encoded, decoded) in enumerate(self.test_data):
            b = v(encoded)
            self.assertEquals(decode(b), decoded, 'failed decoding @ %d' % i)
            self.assertEquals(len(b), 0, 'data left @ %d' % i)

    def test_encode(self):
        for i, (encoded, decoded) in enumerate(self.test_data):
            if i in self.skip_encoding_tests:
                continue
            b = encode(*decoded)
            result = b.read(len(b))
            self.assertEquals(p(encoded), result, 'failed encoding @ %d' % i)

    def test_encode_decode(self):
        for i, (_, decoded) in enumerate(self.test_data):
            b = encode(*decoded)
            self.assertEquals(decode(b), decoded, 'failed re-decoding @ %d' % i)
            self.assertEquals(len(b), 0, 'data left @ %d' % i)

    def test_decode_encode(self):
        for i, (encoded, _) in enumerate(self.test_data):
            if i in self.skip_encoding_tests:
                continue
            b = encode(*decode(v(encoded)))
            result = b.read(len(b))
            self.assertEquals(p(encoded), result, 'failed re-encoding @ %d' % i)
            self.assertEquals(len(b), 0, 'data left @ %d' % i)


class TestSimpleData(unittest.TestCase, DataTestMixin):
    test_data = simple_data
    skip_encoding_tests = skip_encoding_tests_simple

class TestComplexData(unittest.TestCase, DataTestMixin):
    test_data = complex_data
    skip_encoding_tests = ()


decoder_failures = [
    # number
    '00 40100000000000',
    '00 40',
    '00',

    # boolean
    '01',

    # short string
    '02 0003 0102',
    '02 0001',
    '02 00',
    '02',

    # object
    '03 0000 01 01 0000 09',
    '03 000161 01 01 0000',
    '03 000161 01 01',
    '03 000161 01',
    '03 000161',
    '03 00',
    '03',

    # reference
    '07 00',
    '07',

    # ecma-array
    '08 00000001 0000 01 01 0000 09',
    '08 00000001 000161 01 01 0000',
    '08 00000001 000161 01 01',
    '08 00000001 000161 01',
    '08 00000001 000161',
    '08 00000001 00',
    '08 00',
    '08',

    # strict-array
    '0a 00000001 01',
    '0a 00000001',
    '0a 00',
    '0a',

    # date
    '0b 427272085a5f0000 00',
    '0b',

    # long string
    '0c 00011111' + ('2e' * 69904),
    '0c 00000001',
    '0c 00',
    '0c',

    # unsupported or invalid start markers
    '04',
    '09',
    '0d',
    '0e',
    '10',
    '11',

    # and all the rest of the [0; 255] range...
    ] + [chr(x).encode('hex') for x in xrange(0x12, 0xff)]


class DummyObject(object):
    pass

encoder_failures = [
    [amf0.Reference(-1)],
    [amf0.Reference(0x10001)],

    # case of sequence with length exceeding 2**32 is difficult to
    # construct without seriously cheating...

    [DummyObject()],
    [set()],
    [complex()],

    # ... and lots of other types, not tested here
]

class TestFailures(unittest.TestCase):
    def test_decoder(self):
        for i, encoded in enumerate(decoder_failures):
            # assertRaises doesn't accept custom failure message :(...
            self.assertRaises(amf0.DecoderError, decode, v(encoded))

    def test_encoder(self):
        for i, args in enumerate(encoder_failures):
            self.assertRaises(amf0.EncoderError, encode, *args)
