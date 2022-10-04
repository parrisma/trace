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
import os
import unittest
import json
import logging
import re
from time import sleep
from typing import Dict, Any, Tuple, List
from datetime import datetime
from logging import LogRecord
from Trace import Trace
from Trace import LogLevel
from elastic.ElasticFormatter import ElasticFormatter
from elastic.ElasticHandler import ElasticHandler
from elastic.ElasticTraceBootStrap import ElasticTraceBootStrap
from elastic.ElasticResources import ElasticResources
from UtilsForTesting import UtilsForTesting
from rltrace.UniqueRef import UniqueRef
from elastic.ESUtil import ESUtil
from gibberish import Gibberish
from functools import partial
import concurrent.futures
from multiprocessing import Process

# We need to run tests in (alphabetical) order.
unittest.TestLoader.sortTestMethodsUsing = lambda self, a, b: (a > b) - (a < b)


class TestElasticTrace(unittest.TestCase):
    _run: int
    _es_connection = None
    _node_port: int = None
    _loaded: bool = False
    _index_name: str = f'index-{UniqueRef().ref}'
    _index_mapping_file: str = ElasticResources.trace_index_definition_file('..\\resources')
    _index_mappings: str = None

    def __init__(self, *args, **kwargs):
        super(TestElasticTrace, self).__init__(*args, **kwargs)
        return

    @classmethod
    def setUpClass(cls) -> None:
        cls._run = 0
        print(f'- - - - - - S T A R T - - - - - -')
        cls._clean_up_test_files()
        try:
            # Get the elastic hostport id.
            port_id = None
            if cls._node_port is None:
                port_id = ESUtil.get_elastic_node_port_number()

            # Load the JSON mappings for the index.
            f = open(cls._index_mapping_file)
            cls._index_mappings = json.load(f)
            f.close()

            # Open connection to elastic
            if cls._es_connection is None:
                cls._es_connection = ESUtil.get_connection(hostname='localhost',
                                                           port_id=str(port_id),
                                                           elastic_user='elastic',
                                                           elastic_password='changeme')
        except Exception as e:
            print(f'Unexpected exception {str(e)}')
        return

    def setUp(self) -> None:
        TestElasticTrace._run += 1
        print(f'- - - - - - C A S E {TestElasticTrace._run} Start - - - - - -')
        self._clean_up_handlers()  # Logger is global so we must reset between tests
        return

    @classmethod
    def tearDownClass(cls) -> None:
        print(f'- - - - - - E N D - - - - - - \n')
        cls._clean_up_test_files()
        cls._delete_all_test_indexes()
        return

    def tearDown(self) -> None:
        print(f'- - - - - - C A S E {TestElasticTrace._run} Passed - - - - - -\n')
        return

    @classmethod
    def _delete_all_test_indexes(cls):
        # Find any residual indices from failed test clean-ups
        try:
            for candidate in cls._es_connection.cat.indices().body.split():
                if re.match(r'(trace_index_.*|index_handler_.*|index-.*)', candidate):
                    if ESUtil.index_exists(es=cls._es_connection,
                                           idx_name=candidate):
                        ESUtil.delete_index(es=cls._es_connection,
                                            idx_name=candidate)
                        print(f'Cleaned up (deleted) test index {candidate}')

        except Exception as e:
            print(f'Failed to find list of test indices with error {str(e)}')
        return

    @staticmethod
    def _clean_up_handlers():
        lgr = logging.getLogger(Trace.trace_unique_name())
        handlers = [h for h in lgr.handlers]  # cannot use lgr.handlers in for loop as we are modifying it
        for handler in handlers:
            if re.match(r'(.*-ConsoleHandler|.*-FileHandler|.*-ElasticDBHandler)', handler.name):
                try:
                    handler.acquire()
                    handler.flush()
                    handler.close()
                except (OSError, ValueError):
                    pass
                finally:
                    handler.release()
                lgr.removeHandler(handler)
                print(f'Cleaned up (Removed) logging handler {handler.name}')
        return

    @staticmethod
    def _clean_up_test_files():
        dirs_to_clean = [[".", r'.*\.log']]
        for dir_to_clean, pattern in dirs_to_clean:
            files_to_delete = [os.path.join(dir_to_clean, f) for f in os.listdir(dir_to_clean) if re.match(pattern, f)]
            for file_to_delete in files_to_delete:
                print(f'- - - - - - deleting test file {files_to_delete}')
                try:
                    os.remove(file_to_delete)
                except Exception as e:
                    print(f'- - - - - - Warning - failed to delete {files_to_delete} with error {str(e)}')
        return

    @staticmethod
    def _generate_test_document(session_uuid: str) -> Dict[str, Any]:
        msg = Gibberish.more_gibber()
        tstamp = ESUtil.datetime_in_elastic_time_format(datetime.now())
        return json.loads(
            f'{{"session_uuid":"{session_uuid}","level":"DEBUG","timestamp":"{tstamp}","message":"{msg}"}}')

    @staticmethod
    def _create_log_record() -> Tuple[LogRecord, str, str, str, str]:
        lr_time = datetime.now()
        lr_level = logging.INFO
        lr_msg = Gibberish.more_gibber()
        lr_name = UniqueRef().ref
        log_record = LogRecord(name=lr_name,
                               level=lr_level,
                               pathname='',
                               lineno=0,
                               msg=lr_msg,
                               args=None,
                               exc_info=None)
        log_record.created = lr_time
        lr_level = ElasticFormatter.level_map[lr_level]
        lr_time = ESUtil.datetime_in_elastic_time_format(lr_time)
        return (log_record, lr_name, lr_level, lr_time, lr_msg)  # NOQA

    @UtilsForTesting.test_case
    def testA1IndexCreateDelete(self):
        try:
            # Test create index
            if not ESUtil.index_exists(es=self._es_connection,
                                       idx_name=self._index_name):
                res = ESUtil.create_index_from_json(es=self._es_connection, idx_name=self._index_name,
                                                    mappings_as_json=self._index_mappings)
                self.assertTrue(True, res)
                self.assertTrue(ESUtil.index_exists(es=self._es_connection, idx_name=self._index_name))

            # Test delete index
            if ESUtil.index_exists(es=self._es_connection,
                                   idx_name=self._index_name):
                res = ESUtil.delete_index(es=self._es_connection,
                                          idx_name=self._index_name)
                self.assertTrue(True, res)
                self.assertFalse(ESUtil.index_exists(es=self._es_connection, idx_name=self._index_name))

            # Reinstate the index for the following tests
            if not ESUtil.index_exists(es=self._es_connection,
                                       idx_name=self._index_name):
                res = ESUtil.create_index_from_json(es=self._es_connection, idx_name=self._index_name,
                                                    mappings_as_json=self._index_mappings)

        except Exception as e:
            self.fail(f'Unexpected exception {str(e)}')
        return

    @UtilsForTesting.test_case
    def testA2ZeroCount(self):
        try:
            res = ESUtil.run_count(es=self._es_connection,
                                   idx_name=self._index_name,
                                   json_query={"match_all": {}})
            self.assertTrue(res == 0)
        except Exception as e:
            self.fail(f'Unexpected exception {str(e)}')
        return

    @UtilsForTesting.test_case
    def testA3EmptySearch(self):
        try:
            res = ESUtil.run_search(es=self._es_connection,
                                    idx_name=self._index_name,
                                    json_query={"match_all": {}})
            self.assertTrue(len(res) == 0)
        except Exception as e:
            self.fail(f'Unexpected exception {str(e)}')
        return

    @UtilsForTesting.test_case
    def testA4WriteDocument(self):
        try:
            session_uuid = UniqueRef().ref
            doc = TestElasticTrace._generate_test_document(session_uuid)
            ESUtil.write_doc_to_index(es=self._es_connection,
                                      idx_name=self._index_name,
                                      document_as_json_map=doc,
                                      wait_for_write_to_complete=True)

            res = ESUtil.run_count(es=self._es_connection,
                                   idx_name=self._index_name,
                                   json_query={"match": {"session_uuid": session_uuid}})
            self.assertTrue(res == 1)
        except Exception as e:
            self.fail(f'Unexpected exception {str(e)}')
        return

    @UtilsForTesting.test_case
    def testA5DeleteDocument(self):
        try:
            session_uuid = UniqueRef().ref
            doc = TestElasticTrace._generate_test_document(session_uuid)
            ESUtil.write_doc_to_index(es=self._es_connection,
                                      idx_name=self._index_name,
                                      document_as_json_map=doc,
                                      wait_for_write_to_complete=True)

            res = ESUtil.run_count(es=self._es_connection,
                                   idx_name=self._index_name,
                                   json_query={"match": {"session_uuid": session_uuid}})
            self.assertTrue(res == 1)

            ESUtil.delete_documents(es=self._es_connection,
                                    idx_name=self._index_name,
                                    json_query={"match": {"session_uuid": session_uuid}},
                                    wait_for_delete_to_complete=True)

            res = ESUtil.run_count(es=self._es_connection,
                                   idx_name=self._index_name,
                                   json_query={"match": {"session_uuid": session_uuid}})
            self.assertTrue(res == 0)

        except Exception as e:
            self.fail(f'Unexpected exception {str(e)}')
        return

    @UtilsForTesting.test_case
    def testA6ElasticFormatter(self):
        try:
            elastic_formatter = ElasticFormatter()
            log_record, lr_name, lr_level, lr_time, lr_msg = self._create_log_record()
            elastic_log_record = elastic_formatter.format(log_record)
            elastic_log_record = json.loads(elastic_log_record)
            self.assertTrue(elastic_log_record[ElasticFormatter.json_log_fields[0]] == lr_name)
            self.assertTrue(elastic_log_record[ElasticFormatter.json_log_fields[1]] == lr_level)
            self.assertTrue(elastic_log_record[ElasticFormatter.json_log_fields[2]] == lr_time)
            self.assertTrue(elastic_log_record[ElasticFormatter.json_log_fields[3]] == lr_msg)
        except Exception as e:
            self.fail(f'Unexpected exception {str(e)}')
        return

    @UtilsForTesting.test_case
    def testA7ElasticLogging(self):
        handler_index_name = f'index_handler_{UniqueRef().ref}'
        try:
            res = ESUtil.create_index_from_json(es=self._es_connection,
                                                idx_name=handler_index_name,
                                                mappings_as_json=self._index_mappings)
            if not res:
                raise Exception(f'failed to create index {handler_index_name} for testing elastic logging')

            elastic_handler = ElasticHandler(es=self._es_connection,
                                             trace_log_index_name=handler_index_name)

            log_record, lr_name, lr_level, lr_time, lr_msg = self._create_log_record()
            elastic_handler.emit(log_record)
            sleep(1)  # log write does not block on write so give time for record to flush before reading it back
            res = ESUtil.run_search(es=self._es_connection,
                                    idx_name=handler_index_name,
                                    json_query={"match": {"session_uuid": f'\"{lr_name}\"'}})
            self.assertTrue(len(res) == 1)
            actual = res[0]["_source"]
            self.assertTrue(actual[ElasticFormatter.json_log_fields[0]] == lr_name)
            self.assertTrue(actual[ElasticFormatter.json_log_fields[1]] == lr_level)
            self.assertTrue(actual[ElasticFormatter.json_log_fields[2]] == lr_time)
            self.assertTrue(actual[ElasticFormatter.json_log_fields[3]] == lr_msg)
        except Exception as e:
            self.fail(f'Unexpected exception {str(e)}')

        return

    @UtilsForTesting.test_case
    def testA8ElasticLoggingViaTrace(self):
        handler_index_name = f'index_handler_{UniqueRef().ref}'
        num_tests: int = 10
        messages: List[str] = []
        try:
            res = ESUtil.create_index_from_json(es=self._es_connection,
                                                idx_name=handler_index_name,
                                                mappings_as_json=self._index_mappings)
            if not res:
                raise Exception(f'failed to create index {handler_index_name} for testing elastic logging')

            elastic_handler = ElasticHandler(es=self._es_connection,
                                             trace_log_index_name=handler_index_name)

            # Create trace logger and attach elastic handler.
            trace: Trace = Trace(log_level=LogLevel.debug)
            trace.enable_handler(elastic_handler)

            # Check no logging entries.
            res = ESUtil.run_count(es=self._es_connection,
                                   idx_name=handler_index_name,
                                   json_query={"match_all": {}})
            self.assertTrue(res == 0)

            for _ in range(num_tests):
                # Create dummy log message
                lr_msg = Gibberish.more_gibber()
                trace.log(lr_msg)
                messages.append(lr_msg)

            sleep(1)  # log write does not block on write so give time for record to flush before reading it back

            # Check there is one logging entries.
            res = ESUtil.run_count(es=self._es_connection,
                                   idx_name=handler_index_name,
                                   json_query={"match_all": {}})
            self.assertTrue(res == num_tests)

            # check the message matches
            res = ESUtil.run_search(es=self._es_connection,
                                    idx_name=handler_index_name,
                                    json_query={"match_all": {}})
            for r, expected_msg in zip(res, messages):
                actual_msg = r['_source']['message']
                self.assertTrue(actual_msg == expected_msg)
        except Exception as e:
            self.fail(f'Unexpected exception {str(e)}')

        return

    @UtilsForTesting.test_case
    def testA9FullBootStrap(self):
        try:
            test_cases = [[None, None, 1], [Trace(log_level=LogLevel.debug), '..\\resources', 2]]
            index_name: str = f'trace_index_{UniqueRef().ref}'

            for trace, index_def, expected_hits in test_cases:
                ebs = ElasticTraceBootStrap(trace=trace,
                                            hostname='localhost',
                                            port_id=None,
                                            elastic_user='elastic',
                                            elastic_password='changeme',
                                            index_definition=index_def,
                                            index_name=index_name)

                sleep(1)
                self.assertTrue(ESUtil.index_exists(es=ebs.elastic_connection, idx_name=ebs.index_name))

                trace = ebs.trace if trace is None else trace
                trace.log(f'{Gibberish.more_gibber()}')
                sleep(2)  # log write does not block on write so give time for record to flush before reading it back

                # Check there is one logging entries.
                res = ESUtil.run_count(es=ebs.elastic_connection,
                                       idx_name=ebs.index_name,
                                       json_query={"match_all": {}})
                self.assertTrue(res == expected_hits)

        except Exception as e:
            raise Exception(f'Unexpected exception {str(e)}')

        return

    @UtilsForTesting.test_case
    def testB1MultiTraceConstruct(self):
        """
        Confirm that no matter how many times Trace is constructed that only one set of handlers is added to
        root logger.
        """
        traces = []
        for _ in range(100):
            traces.append(Trace(log_level=LogLevel.debug, log_dir_name=".", log_file_name="trace.log"))

        # Verify that every Trace has same handlers, which means handlers only created and added once.
        first_trace = traces.pop()
        lgr: logging.Logger = first_trace.__getattribute__('_logger')
        self.assertTrue(len(lgr.handlers) == 2)  # Should be 2 handlers, console + file
        first_console_handler: logging.Handler = None
        first_file_handler: logging.Handler = None
        for handler in lgr.handlers:
            if handler.name == first_trace.trace_file_handler_unique_name:
                first_file_handler = handler
            if handler.name == first_trace.trace_console_handler_unique_name:
                first_console_handler = handler
        self.assertTrue(first_console_handler is not None)
        self.assertTrue(first_file_handler is not None)

        # now verify all created traces are the same as the first, where the logger associated with each is
        # same object so 'is' rather than '=='
        for trace in traces:
            lgr_actual = trace.__getattribute__('_logger')
            self.assertTrue(lgr_actual is lgr)
        return

    @UtilsForTesting.test_case
    def testB2MultiTraceElasticConstruct(self):
        """
        Confirm that no matter how many times ElasticTrace is constructed that only one set of handlers is added to
        root logger.
        """
        traces = []
        index_name: str = f'trace_index_{UniqueRef().ref}'
        for _ in range(100):
            ebs = ElasticTraceBootStrap(trace=None,
                                        hostname='localhost',
                                        port_id=None,
                                        elastic_user='elastic',
                                        elastic_password='changeme',
                                        index_definition='..\\resources',
                                        index_name=index_name,
                                        log_dir_name='.',
                                        log_file_name="trace.log")
            traces.append(ebs.trace)

        # Verify that every Trace has same handlers, which means handlers only created and added once.
        first_elastic_trace = traces.pop()
        lgr: logging.Logger = first_elastic_trace.__getattribute__('_logger')
        self.assertTrue(len(lgr.handlers) == 3)  # Should be 2 handlers, console + file + elastic
        first_console_handler: logging.Handler = None
        first_file_handler: logging.Handler = None
        first_elastic_handler: logging.Handler = None
        for handler in lgr.handlers:
            if handler.name == first_elastic_trace.trace_file_handler_unique_name:
                first_file_handler = handler
            if handler.name == first_elastic_trace.trace_console_handler_unique_name:
                first_console_handler = handler
            if handler.name == ElasticHandler.elastic_handler_unique_name():
                first_elastic_handler = handler
        self.assertTrue(first_console_handler is not None)
        self.assertTrue(first_file_handler is not None)
        self.assertTrue(first_elastic_handler is not None)

        # now verify all created traces are the same as the first, where the logger associated with each is
        # same object so 'is' rather than '=='
        for trace in traces:
            lgr_actual = trace.__getattribute__('_logger')
            self.assertTrue(lgr_actual is lgr)
        return

    class TestLogger:
        def __init__(self,
                     index_name: str):
            # self._trace = ElasticTraceBootStrap(log_level=LogLevel.debug,
            #                                    session_uuid=UniqueRef().ref,
            #                                    index_name=index_name).trace
            self._id = UniqueRef().ref
            return

        @property
        def id(self) -> str:
            return self._id

        def run(self,
                msg: str = ''):
            # self._trace.log(f'{msg}-{self._id}')
            print(f'{msg}-{self._id}')
            return self

    @staticmethod
    def hi():
        print('Hello 123')
        return

    @staticmethod
    def task():
        print('This is another process', flush=True)

    # @UtilsForTesting.test_case
    def B2ProcessPoolExecutor(self):
        # define a task to run in a new process
        p = Process(target=self.task)
        # start the task in a new process
        p.start()
        # wait for the task to complete
        p.join()
        print('Done')
        # End process
        p.close()
        p.kill()
        return


if __name__ == '__main__':
    suite = unittest.TestLoader().loadTestsFromTestCase(TestElasticTrace)
    unittest.TextTestRunner(verbosity=2).run(suite)
