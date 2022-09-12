# ----------------------------------------------------------------------------
# MIT License
#
# Copyright (c) 2022 parris3142
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
# ----------------------------------------------------------------------------

import sys

#: The release version
version = '1.0.dev1'
__version__ = version

MIN_PYTHON_VERSION = 3, 8
MIN_PYTHON_VERSION_STR = '.'.join([str(v) for v in MIN_PYTHON_VERSION])

if sys.version_info < MIN_PYTHON_VERSION:
    raise Exception(f"pyglet {version} requires Python {MIN_PYTHON_VERSION_STR} or newer.")

"""
Core Trace function to log to console
"""
from rltrace.Trace import Trace  # NOQA
from rltrace.Trace import LogLevel  # NOQA
from rltrace.UniqueRef import UniqueRef  # NOQA

"""
Elastic extensions to create an Elastic handler
"""
from rltrace.elastic.ElasticFormatter import ElasticFormatter  # NOQA
from rltrace.elastic.ElasticHandler import ElasticHandler  # NOQA
