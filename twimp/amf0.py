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


"""
Action Message Format 0 handling library.

Implemented AMF0 -> Python type mapping:

  * number -> float
  * boolean -> bool
  * string -> str, unicode
  * object -> afm0.Object
  * null -> None
  * undefined -> afm0.UndefinedType
  * reference -> afm0.Reference
  * ecma-array -> afm0.ECMAArray
  * strict-array -> list
  * date -> datetime
  * long string -> str, unicode
  * xml document -> afm0.XMLDocument

Implemented Python -> AMF0 type mapping:

  * int, long, float -> number
  * bool -> boolean
  * str, unicode, buffer -> string, long string
  * afm0.Object -> object
  * None -> null
  * afm0.UndefinedType -> undefined
  * afm0.Reference -> reference
  * afm0.ECMAArray, dict -> ecma-array
  * list, tuple -> strict-array
  * datetime -> date
  * afm0.XMLDocument -> xml document
"""


import calendar
import datetime
import struct
import sys
from UserDict import DictMixin

from primitives import _s_double, _s_ushort, _s_ulong_b as _s_ulong
from primitives import _s_date_tz

from vecbuf import VecBuf, VecBufEOB


# A UTC tzinfo class, temporarily here, until full support for
# timezones is implemented...

ZERO = datetime.timedelta(0)
class UTC(datetime.tzinfo):
    """UTC"""

    def utcoffset(self, dt):
        return ZERO

    def tzname(self, dt):
        return "UTC"

    def dst(self, dt):
        return ZERO
utc = UTC()

# ordered dict() necessary, where should it go?...
class OrderedDict(DictMixin, object):
    def __init__(self, other=None, **kw):
        self._keys = []
        self._data = {}
        if other:
            self.update(other)
        self.update(**kw)

    def __getitem__(self, key):
        return self._data[key]

    def __setitem__(self, key, value):
        if key not in self._data:
            self._keys.append(key)
        self._data[key] = value

    def __delitem__(self, key):
        del self._data[key]
        self._keys.remove(key)

    def __iter__(self):
        return iter(self._keys)

    def __contains__(self, key):
        return key in self._data

    def __len__(self):
        return len(self._keys)

    if sys.version_info < (2, 6):
        def __hash__(self):
            raise TypeError('unhashable type: %r' % self.__class__.__name__)
    else:
        __hash__ = None

    def keys(self):
        return self._keys[:]

    def iteritems(self):
        return ((k, self._data.__getitem__(k)) for k in self._keys)

    def copy(self):
        d = OrderedDict()
        d._data = self._data.copy()
        d._keys = self._keys[:]
        return d

    def __repr__(self):
        return '%s(%r)' % (self.__class__.__name__, self.items())


class OrderedObject(OrderedDict):
    def __init__(self, other=None, **kw):
        # overriding __init__ completely!
        OrderedDict.__setattr__(self, '_keys', [])
        OrderedDict.__setattr__(self, '_data', {})
        if other:
            self.update(other)
        self.update(**kw)

    def __getattr__(self, name):
        try:
            return self[name]
        except KeyError:
            raise AttributeError('%r object has no attribute %r' %
                                 (self.__class__.__name__, name))

    def __setattr__(self, name, value):
        if name not in self._data:
            self._keys.append(name)
        self._data[name] = value

    def __delattr__(self, name):
        try:
            del self._data[name]
        except KeyError:
            raise AttributeError(name)
        self._keys.remove(name)

    def copy(self):
        return self.__class__(self)

    def s(self, **kw):
        """Convenience method for initializing/setting in order:
        o = OrderedObject(d=4).s(b=1).s(a=3).s(c=2)
        """
        self.update(**kw)
        return self

    def __repr__(self):
        return '%s(%s)' % (self.__class__.__name__,
                           ', '.join(('%s=%r' % elt) for elt in self.items()))



class DecoderError(ValueError):
    pass

class EncoderError(ValueError):
    pass


class UndefinedType(object):
    def __eq__(self, other):
        return isinstance(other, UndefinedType)

    def __ne__(self, other):
        return not isinstance(other, UndefinedType)

    def __hash__(self):
        return 0

    def __repr__(self):
        return 'undefined'
undefined = UndefinedType()


class Object(OrderedObject):
    """AMF Object class, with chronological attribute ordering, for
    increased compatibility.
    """


class Reference(object):
    def __init__(self, index):
        self.index = index

    def __eq__(self, other):
        return isinstance(other, Reference) and self.index == other.index

    def __ne__(self, other):
        return not self.__eq__(other)

    def __hash__(self):
        return self.index

    def __repr__(self):
        return '%s(%d)' % (self.__class__.__name__, self.index)


class ECMAArray(OrderedDict):
    pass


class XMLDocument(unicode):
    pass


##
# decoder part
#

def _decode_marker(s):
    return ord(s.read(1)[0])

def _decode_number(s):
    return _s_double.unpack(s.read(8))[0]

def _decode_boolean(s):
    return s.read(1)[0] != '\x00'

def _decode_any_string(s, unpacker):
    str_len, = unpacker.unpack(s.read(unpacker.size))
    read = s.read(str_len)

    try:
        return str(read).encode('ascii')
    except UnicodeDecodeError:
        pass

    try:
        return unicode(read, 'utf-8')
    except UnicodeDecodeError:
        raise DecoderError('Invalid string encoding')

def _decode_string(s):
    return _decode_any_string(s, _s_ushort)

def _decode_object_like(s, setter):
    while 1:
        name = _decode_string(s)
        if name == '':
            if _decode_marker(s) != MARK_OBJECT_END:
                raise DecoderError('Missing object end marker')
            break
        value = _decode_single(s, type_dict=object_decoders)
        setter(name, value)

def _decode_object(s):
    ret = Object()
    _decode_object_like(s, lambda k, v: setattr(ret, k, v))
    return ret

def _decode_null(s):
    return None

def _decode_undefined(s):
    return undefined

def _decode_reference(s):
    return Reference(_s_ushort.unpack(s.read(2))[0])

def _decode_ecma_array(s):
    s.read(4)                   # skip unused(?) length
    ret = ECMAArray()
    _decode_object_like(s, lambda k, v: ret.__setitem__(k, v))
    return ret

def _decode_strict_array(s):
    length, = _s_ulong.unpack(s.read(4))
    return [_decode_single(s) for _ in xrange(length)]

def _decode_date(s):
    # TODO: don't ignore timezone
    # FIXME
    milliseconds, tz = _s_date_tz.unpack(s.read(10))
    return datetime.datetime.fromtimestamp(milliseconds / 1000.0, utc)

def _decode_long_string(s):
    return _decode_any_string(s, _s_ulong)

def _decode_xml_document(s):
    return XMLDocument(_decode_long_string(s))

def _decode_typed_object(s):
    raise DecoderError('Typed objects unsupported a.t.m.')

def _decode_unsupported(s):
    raise DecoderError('Unsupported unsupported')


(MARK_NUMBER, MARK_BOOL, MARK_STRING, MARK_OBJECT, MARK_MOVIECLIP, MARK_NULL,
 MARK_UNDEFINED, MARK_REFERENCE, MARK_ECMA_ARRAY, MARK_OBJECT_END,
 MARK_STRICT_ARRAY, MARK_DATE, MARK_LONG_STRING, MARK_UNSUPPORTED,
 MARK_RECORDSET, MARK_XML_DOCUMENT, MARK_TYPED_OBJECT,
 MARK_AVMPLUS_OBJECT) = range(0x12)

object_decoders = {
    MARK_NUMBER:        _decode_number,
    MARK_BOOL:          _decode_boolean,
    MARK_STRING:        _decode_string,
    MARK_OBJECT:        _decode_object,

    MARK_NULL:          _decode_null,
    MARK_UNDEFINED:     _decode_undefined,
    MARK_REFERENCE:     _decode_reference,
    MARK_ECMA_ARRAY:    _decode_ecma_array,

    MARK_STRICT_ARRAY:  _decode_strict_array,
    MARK_DATE:          _decode_date,
    MARK_LONG_STRING:   _decode_long_string,

    MARK_XML_DOCUMENT:  _decode_xml_document,
    MARK_TYPED_OBJECT:  _decode_typed_object,
    }

# def _debug_wrapper(f):
#     def _wrap(*a, **kw):
#         print '(%s) calling %s(%r, %r)' % (len(a[0]), f.__name__, a, kw)
#         ret = f(*a, **kw)
#         print '(%s) got: %r' % (len(a[0]), ret)
#         return ret
#     return _wrap
# object_decoders = dict((k, _debug_wrapper(f))
#                        for (k, f) in object_decoders.items())

decoders = object_decoders.copy()
decoders.update({
        # MARK_MOVIECLIP: _decode_movieclip,
        MARK_UNSUPPORTED: _decode_unsupported,
        # MARK_RECORDSET: _decode_recordset,
        # MARK_AVMPLUS_OBJECT: _decode_avmplus_object,
        })


def _decode_single(s, type_dict=decoders):
    # not checking EOB on marker - decoding exactly one object
    marker = _decode_marker(s)
    decoder = type_dict.get(marker, None)
    if not decoder:
        raise DecoderError('Unsupported marker 0x%02x' % marker)

    return decoder(s)


def _decode(s, type_dict=decoders):
    values = []
    while 1:
        try:
            marker = _decode_marker(s)
        except VecBufEOB:
            break

        decoder = type_dict.get(marker, None)
        if not decoder:
            raise DecoderError('Unsupported marker 0x%02x' % marker)

        values.append(decoder(s))

    return values


##
# encoder part
#

# the following structs are not generic enough to be put in primitives...
_s_m_empty = struct.Struct('>B')
_s_m_len = struct.Struct('>BH')
_s_m_longlen = struct.Struct('>BL')
_s_m_double = struct.Struct('>Bd')
_s_m_boolean = struct.Struct('>BB')
_s_m_reference = _s_m_len
_s_m_time_tz = struct.Struct('>Bdh')
_s_endmarker = struct.Struct('>HB')


def _encode_number(s, value):
    s.write(_s_m_double.pack(MARK_NUMBER, value))

def _encode_boolean(s, value):
    s.write(_s_m_boolean.pack(MARK_BOOL, 1 if value else 0))

def _encode_string(s, value):
    length = len(value)
    if length > 0xffff:
        s.write(_s_m_longlen.pack(MARK_LONG_STRING, length))
    else:
        s.write(_s_m_len.pack(MARK_STRING, length))
    s.write(value)

def _encode_unicode(s, value):
    _encode_string(s, value.encode('utf-8'))

def _encode_null(s, _value):
    s.write(_s_m_empty.pack(MARK_NULL))

def _encode_undefined(s, _value):
    s.write(_s_m_empty.pack(MARK_UNDEFINED))

def _encode_reference(s, value):
    if 0 <= value.index <= 0xffff:
        s.write(_s_m_reference.pack(MARK_REFERENCE, value.index))
        return

    raise EncoderError('Reference not in range [0; 65535]')

def _encode_strict_array(s, value):
    length = len(value)
    if not (0 <= length <= 0xffffffff):
        raise EncoderError('Sequence too long')

    s.write(_s_m_longlen.pack(MARK_STRICT_ARRAY, length))

    for elt in value:
        _encode_single(s, elt)

def _encode_date(s, value):
    # FIXME
    seconds = calendar.timegm(value.utctimetuple())
    s.write(_s_m_time_tz.pack(MARK_DATE, int(seconds * 1000), 0))

def _encode_xml_document(s, value):
    string = value.encode('utf-8')
    s.write(_s_m_longlen.pack(MARK_XML_DOCUMENT, len(string)))
    s.write(string)

def _encode_property_name(s, value):
    s.write(_s_ushort.pack(len(value)))
    s.write(value)

def _encode_object_like_content(s, value):
    for k, v in value.iteritems():
        _encode_property_name(s, k)
        _encode_single(s, v, type_dict=object_encoders)
    s.write(_s_endmarker.pack(0, MARK_OBJECT_END))

def _encode_ecma_array(s, value):
    s.write(_s_m_longlen.pack(MARK_ECMA_ARRAY, len(value)))
    _encode_object_like_content(s, value)

def _encode_object(s, value):
    s.write(_s_m_empty.pack(MARK_OBJECT))
    _encode_object_like_content(s, value)

object_encoders = {
    float: _encode_number,
    int: _encode_number,
    long: _encode_number,
    bool: _encode_boolean,
    str: _encode_string,
    buffer: _encode_string,
    unicode: _encode_unicode,
    Object: _encode_object,
    None.__class__: _encode_null,
    UndefinedType: _encode_undefined,
    Reference: _encode_reference,
    ECMAArray: _encode_ecma_array,
    dict: _encode_ecma_array,
    list: _encode_strict_array,
    tuple: _encode_strict_array,
    datetime.datetime: _encode_date,
    XMLDocument: _encode_xml_document,
    }

encoders = object_encoders

def _encode_single(s, value, type_dict=encoders):
    try:
        value_class = value.__class__
    except AttributeError:
        raise EncoderError("Unable to encode values of type %r" % type(value))

    encoder = type_dict.get(value_class, None)

    if not encoder:
        raise EncoderError('No encoder for values of type %r' % value_class)

    encoder(s, value)

def _encode(s, values, type_dict=encoders):
    for v in values:
        _encode_single(s, v, type_dict=type_dict)

def _encode_variable_name(s, name):
    try:
        name_class = name.__class__
    except AttributeError:
        name_class = None

    value = name
    if name_class == unicode:
        value = name.encode('utf-8')
    elif name_class not in (str, buffer):
        raise EncoderError("Not an acceptable variable name type %r" %
                           type(name))

    if len(value) > 0xffff:
        raise EncoderError("Variable name too long")

    _encode_property_name(s, value)


##
# public interface
#

def decode(data):
    """Decode AMF0-encoded buffer of data.

    @type data: VecBuf

    @returns: list of objects
    """
    try:
        return _decode(data)
    except VecBufEOB:
        raise DecoderError('Incomplete encoded data')

def decode_one(data):
    """Decode a single value from AMF0-encoded buffer of data.

    @type data: VecBuf

    @returns: object
    """
    try:
        return _decode_single(data)
    except VecBufEOB:
        raise DecoderError('Incomplete encoded data')


def encode(*args):
    """Encode given values using AMF0.

    @rtype: VecBuf
    """
    vb = VecBuf()
    _encode(vb, args)
    return vb

def decode_variable(data):
    """Decode a single FLV data variable from AMF0-encoded buffer of data.

    @type data: VecBuf

    @returns: (str, object)
    """
    try:
        return _decode_string(data), _decode_single(data)
    except VecBufEOB:
        raise DecoderError('Incomplete encoded data')

def encode_variable(name, value):
    """Encode given name and value into an FLV data variable using AMF0.

    @type name: str or unicode

    @rtype: VecBuf
    """
    vb = VecBuf()
    _encode_variable_name(vb, name)
    _encode_single(vb, value)
    return vb


__all__ = ['encode', 'decode', 'encode_variable', 'decode_variable',
           'DecoderError', 'EncoderError',
           'ECMAArray', 'Object', 'undefined', 'XMLDocument']
