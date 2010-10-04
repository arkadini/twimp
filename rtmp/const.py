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


RTMP_CHUNK_SIZE = 0x01
RTMP_ABORT = 0x02
RTMP_ACK = 0x03
RTMP_USER_CONTROL = 0x04
RTMP_WINDOW_SIZE = 0x05
RTMP_SET_BANDWIDTH = 0x06

RTMP_AUDIO = 0x08
RTMP_VIDEO = 0x09

RTMP_DATA = 0x12
RTMP_SHARED_OBJ = 0x13
RTMP_COMMAND = 0x14

RTMP_AGGREGATE = 0x16

RTMP_DATA_AMF3 = 0x0f
RTMP_SHARED_OBJ_AMF3 = 0x10
RTMP_COMMAND_AMF3 = 0x11


UCTRL_STREAM_BEGIN = 0x0
UCTRL_STREAM_EOF = 0x1
UCTRL_STREAM_DRY = 0x2
UCTRL_BUFFER_LENGTH = 0x3
UCTRL_STREAM_RECORDED = 0x4
UCTRL_PING = 0x6
UCTRL_PONG = 0x7


##
# Flash audio format ids

AF_PCM_PLATFORM = 0
AF_ADPCM = 1
AF_MP3 = 2
AF_PCM_LITTLE_ENDIAN = 3
AF_NELLYMOSER_16K = 4
AF_NELLYMOSER_8K = 5
AF_NELLYMOSER = 6
AF_ALAW = 7
AF_ULAW = 8
AF_AAC = 10
AF_SPEEX = 11
AF_MP3_8K = 14
AF_DEVICE_SPECIFIC = 15

##
# Flash video format ids

VF_JPEG = 1
VF_H263 = 2
VF_SCREEN = 3
VF_VP6 = 4
VF_VP6_ALPHA = 5
VF_SCREEN_2 = 6
VF_H264 = 7


##
# video, AAC and H264 frame types

FT_VIDEO_KEYFRAME = 1
FT_VIDEO_INTERFAME = 2
FT_VIDEO_DISPOSABLE_INTERFRAME = 3
FT_VIDEO_GENERATED_KEYFRAME = 4
FT_VIDEO_INFO = 5

FT_AAC_HEADER = 0
FT_AAC_DATA = 1

FT_H264_HEADER = 0
FT_H264_NALU = 1
FT_H264_EOS = 2
