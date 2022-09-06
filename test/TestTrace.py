import io
import unittest
import numpy as np
from IPython.utils.capture import capture_output
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
            trace().info(f'Test Message {np.random.rand()}')
            trace().debug(f'Test Message {np.random.rand()}')
            trace().warning(f'Test Message {np.random.rand()}')
            trace().error(f'Test Message {np.random.rand()}')
            trace().critical(f'Test Message {np.random.rand()}')
        except Exception as e:
            self.fail(f'Unexpected exception {str(e)}')
        return
