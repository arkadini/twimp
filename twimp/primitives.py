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


import struct


_s_uchar = struct.Struct('B')
_s_double_uchar = struct.Struct('BB')
_s_ushort = struct.Struct('>H')
_s_ushort_l = struct.Struct('<H')
_s_ulong_b = struct.Struct('>L')
_s_ulong_l = struct.Struct('<L')
_s_double_ulong_b = struct.Struct('>LL')
_s_double = struct.Struct('>d')

_s_ext_csid = struct.Struct('<BH')
_s_time_size_type = struct.Struct('>HBHBB')
_s_time = struct.Struct('>HB')

_s_set_bw = struct.Struct('>LB')

_s_date_tz = struct.Struct('>dh')
