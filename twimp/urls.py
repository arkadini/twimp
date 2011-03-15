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


from urlparse import urlparse, urlunparse


def parse_rtmp_url(url, default_port=1935):
    url = url.strip()
    assert url[:5] == 'rtmp:', 'only "rtmp" scheme supported'

    # urlparse doesn't know about rtmp scheme, so we do some parsing
    # "manually" (instead or live-patching urlparse)...
    scheme = url[:4]
    parsed = urlparse(url[5:])
    app = urlunparse(('', '') + parsed[2:])

    host, port = parsed[1], default_port
    if ':' in host:
        host, port = host.split(':', 1)
        try:
            port = int(port)
        except ValueError:
            port = default_port

    # the application "name" seem to not start with a slash
    if app and app[0] == '/':
        app = app[1:]

    return scheme, host, port, app
