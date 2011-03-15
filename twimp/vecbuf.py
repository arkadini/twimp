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


class VecBufEOB(Exception):
    pass

class VecBuf(object):
    __slots__ = ('_bufvec', '_buflen', '_offset', '_lenfirst')

    def __init__(self, vecdata=None):
        self._bufvec = []
        self._buflen = 0
        self._offset = 0
        self._lenfirst = 0

        if vecdata is not None:
            self.write_seq(vecdata)

    def __len__(self):
        return self._buflen - self._offset

    def write(self, data):
        self._bufvec.append(buffer(data))
        self._buflen += len(data)
        if len(self._bufvec) == 1:
            self._lenfirst = len(data)

    def write_seq(self, vecdata):
        self._bufvec.extend(buffer(d) for d in vecdata)
        self._buflen += sum(len(d) for d in vecdata)
        if vecdata and len(vecdata) == len(self._bufvec):
            self._lenfirst = len(self._bufvec[0])

    def left(self):
        return len(self)

    def _get(self, bytes):
        new_bufvec_start, buflen_diff, new_offset = 0, 0, 0

        if bytes > (self._buflen - self._offset):
            raise VecBufEOB('Not enough data')
        elif bytes == 0:
            return [''], new_bufvec_start, buflen_diff, self._offset

        ret_bytes = - self._offset
        rows = 0
        for r in self._bufvec:
            rows += 1
            ret_bytes += len(r)
            if ret_bytes >= bytes:
                break
        ret_rows = self._bufvec[0:rows]
        if ret_bytes == bytes:
            # yay, aligned with sub-buffer (end) layout!
            if self._offset != 0:
                ret_rows[0] = buffer(ret_rows[0], self._offset)
            buflen_diff = ret_bytes + self._offset
            new_bufvec_start = rows
            # new_offset = 0
        else:
            if rows == 1:
                # just returning a bit of the first sub-buffer
                ret_rows[0] = buffer(ret_rows[0], self._offset, bytes)
                new_offset = self._offset + bytes
            else:
                # keeping last returned sub-buffer, as only part was read
                new_bufvec_start = rows - 1
                buflen_diff = (ret_bytes + self._offset -
                               len(self._bufvec[new_bufvec_start]))
                ret_rows[0] = buffer(ret_rows[0], self._offset)
                last = ret_rows[-1]
                ret_rows[-1] = buffer(last, 0, len(last) + bytes - ret_bytes)
                new_offset = len(ret_rows[-1])

        return ret_rows, new_bufvec_start, buflen_diff, new_offset

    def read(self, bytes):
        """Read the requested number of bytes from buffer.

        @rtype: str or buffer

        @raises: VecBufEOB if requested more bytes than available
        """
        if bytes < self._lenfirst - self._offset:
            offset, self._offset = self._offset, self._offset + bytes
            return buffer(self._bufvec[0], offset, bytes)
        return flatten(self.read_seq(bytes))

    def peek(self, bytes):
        """Return the requested number of bytes from the start of the
        buffer, without modifying the state of the buffer.

        @rtype: str or buffer

        @raises: VecBufEOB if requested more bytes than available
        """
        if bytes < self._lenfirst - self._offset:
            return buffer(self._bufvec[0], self._offset, bytes)
        return flatten(self.peek_seq(bytes))

    def read_seq(self, bytes):
        """Read the requested number of bytes from buffer, iovec-style.

        @rtype: sequence of buffer

        @raises: VecBufEOB if requested more bytes than available
        """
        data, vec_start, len_diff, offset = self._get(bytes)

        if vec_start > 0:
            self._bufvec[0:vec_start] = []
            if self._bufvec:
                self._lenfirst = len(self._bufvec[0])
            else:
                self._lenfirst = 0
        self._buflen -= len_diff
        self._offset = offset

        return data

    def peek_seq(self, bytes):
        """Return the requested number of bytes from the start of the
        buffer, iovec-style, without modifying the state of the buffer.

        @rtype: sequence of buffer

        @raises: VecBufEOB if requested more bytes than available
        """
        data, vec_start, len_diff, offset = self._get(bytes)
        return data

    def read_clone(self, bytes):
        return VecBuf(self.read_seq(bytes))

    def _debug(self):
        return ('[vb]: len: %d (%d), off: %d, left: %d, vec: %s' %
                (self._buflen, self._lenfirst, self._offset, len(self),
                 self._bufvec))


def semiflatten(bufvec):
    """Return a copy of the given sequence, with all elements rendered
    to strings. Meant to be used with a sequence of buffers.

    @type bufvec: sequence of buffer

    @rtype: sequence of str
    """
    return [elt[:] for elt in bufvec]

def flatten(bufvec):
    """Return a string representing concatentation of the given
    sequence. Meant to be used with a sequence of buffers.

    @type bufvec: sequence of buffer

    @rtype: str
    """
    return ''.join(elt[:] for elt in bufvec)


__all__ = ['VecBuf', 'VecBufEOB', 'flatten']
