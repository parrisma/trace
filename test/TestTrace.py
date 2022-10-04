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
import logging
import unittest
import numpy as np
from UtilsForTesting import UtilsForTesting
from rltrace.Trace import Trace
from rltrace.Trace import LogLevel


class TestTrace(unittest.TestCase):
    _run: int

    def __init__(self, *args, **kwargs):
        super(TestTrace, self).__init__(*args, **kwargs)
        return

    @classmethod
    def setUpClass(cls):
        cls._run = 0
        return

    def setUp(self) -> None:
        TestTrace._run += 1
        print(f'- - - - - - C A S E {TestTrace._run} Start - - - - - -')
        return

    def tearDown(self) -> None:
        print(f'- - - - - - C A S E {TestTrace._run} Passed - - - - - -\n')
        return

    @UtilsForTesting.test_case
    def testTraceBasicConstructionAndUse(self):
        try:
            trace: Trace = Trace(log_level=LogLevel.debug)
            trace.log(f'Test Message {np.random.rand()}')
            trace.log(f'Test Message {np.random.rand()}', level=logging.DEBUG)
            trace.log(f'Test Message {np.random.rand()}', level=logging.INFO)
            trace.log(f'Test Message {np.random.rand()}', level=logging.WARNING)
            trace.log(f'Test Message {np.random.rand()}', level=logging.ERROR)
            trace.log(f'Test Message {np.random.rand()}', level=logging.CRITICAL)
            trace.set_log_level(level=logging.WARNING)
            trace.log(f'Test Message {np.random.rand()}')
            trace.set_log_level(level=LogLevel.critical)
        except Exception as e:
            self.fail(f'Unexpected exception {str(e)}')
        return

    @UtilsForTesting.test_case
    def testTraceChangeOfSessionUUID(self):
        try:
            trace: Trace = Trace(log_level=LogLevel.debug)
            session_uuid = trace.session_uuid
            self.assertTrue(session_uuid == trace.session_uuid)
            for _ in range(1000):
                trace.new_session()
                self.assertTrue(session_uuid != trace.session_uuid)
        except Exception as e:
            self.fail(f'Unexpected exception {str(e)}')
        return
