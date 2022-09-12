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

import unittest
import json
import logging
from time import sleep
from typing import Dict, Any, Tuple
from datetime import datetime
from logging import LogRecord
from Trace import Trace
from Trace import LogLevel
from elastic.ElasticFormatter import ElasticFormatter
from elastic.ElasticHandler import ElasticHandler
from elastic.ElasticBootStrap import ElasticBootStrap
from UtilsForTesting import UtilsForTesting
from rltrace.UniqueRef import UniqueRef
from elastic.ESUtil import ESUtil
from gibberish import Gibberish

# We need to run tests in (alphabetical) order.
unittest.TestLoader.sortTestMethodsUsing = lambda self, a, b: (a > b) - (a < b)


class TestElasticTrace(unittest.TestCase):
    _run: int
    _es_connection = None
    _node_port: int = None
    _loaded: bool = False
    _index_name: str = f'index-{UniqueRef().ref}'
    _index_mapping_file: str = '..\\elastic\\k8s-elastic\\elastic-log-index.json'
    _index_mappings: str = None

    def __init__(self, *args, **kwargs):
        super(TestElasticTrace, self).__init__(*args, **kwargs)
        return

    @classmethod
    def setUpClass(cls):
        cls._run = 0
        return

    def setUp(self) -> None:
        TestElasticTrace._run += 1
        print(f'- - - - - - C A S E {TestElasticTrace._run} Start - - - - - -')
        try:
            # Get the elastic hostport id.
            port_id = None
            if self._node_port is None:
                port_id = ESUtil.get_elastic_node_port_number()

            # Load the JSON mappings for the index.
            f = open(self._index_mapping_file)
            self._index_mappings = json.load(f)
            f.close()

            # Open connection to elastic
            if self._es_connection is None:
                self._es_connection = ESUtil.get_connection(hostname='localhost',
                                                            port_id=str(port_id),
                                                            elastic_user='elastic',
                                                            elastic_password='changeme')
        except Exception as e:
            self.fail(f'Unexpected exception {str(e)}')
        return

    def tearDown(self) -> None:
        print(f'- - - - - - C A S E {TestElasticTrace._run} Passed - - - - - -\n')
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
    def testA3ZeroCount(self):
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
        finally:
            if ESUtil.index_exists(es=self._es_connection,
                                   idx_name=handler_index_name):
                res = ESUtil.delete_index(es=self._es_connection,
                                          idx_name=handler_index_name)
                if not res:
                    raise Exception(f'failed to delete index {handler_index_name} while testing elastic logging')

        return

    @UtilsForTesting.test_case
    def testA8ElasticLoggingViaTrace(self):
        handler_index_name = f'index_handler_{UniqueRef().ref}'
        session_uuid = UniqueRef().ref
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

            # Create dummy log message
            lr_msg = Gibberish.more_gibber()

            trace().debug(lr_msg)
            sleep(1)  # log write does not block on write so give time for record to flush before reading it back

            # Check there is one logging entries.
            res = ESUtil.run_count(es=self._es_connection,
                                   idx_name=handler_index_name,
                                   json_query={"match_all": {}})
            self.assertTrue(res == 1)

            # check the message matches
            res = ESUtil.run_search(es=self._es_connection,
                                    idx_name=handler_index_name,
                                    json_query={"match_all": {}})
            actual = res[0]["_source"]
            self.assertTrue(actual[ElasticFormatter.json_log_fields[3]] == lr_msg)

        except Exception as e:
            self.fail(f'Unexpected exception {str(e)}')
        finally:
            if ESUtil.index_exists(es=self._es_connection,
                                   idx_name=handler_index_name):
                res = ESUtil.delete_index(es=self._es_connection,
                                          idx_name=handler_index_name)
                if not res:
                    raise Exception(f'failed to delete index {handler_index_name} while testing elastic logging')

        return

    @UtilsForTesting.test_case
    def testA9FullBootStrap(self):
        try:
            trace: Trace = Trace(log_level=LogLevel.debug)
            ebs = ElasticBootStrap(trace=trace,
                                   hostname='localhost',
                                   port_id=None,
                                   elastic_user='elastic',
                                   elastic_password='changeme',
                                   index_name=f'trace_index_{UniqueRef().ref}')

            sleep(1)
            self.assertTrue(ESUtil.index_exists(es=ebs.elastic_connection, idx_name=ebs.index_name))

            trace().debug(f'{Gibberish.more_gibber()}')
            sleep(2)  # log write does not block on write so give time for record to flush before reading it back

            # Check there is one logging entries.
            res = ESUtil.run_count(es=ebs.elastic_connection,
                                   idx_name=ebs.index_name,
                                   json_query={"match_all": {}})
            self.assertTrue(res == 1)

            res = ESUtil.delete_index(es=ebs.elastic_connection, idx_name=ebs.index_name)
            if not res:
                raise Exception(f'failed to delete index {ebs.index_name} while testing elastic logging')
        except Exception as e:
            self.fail(f'Unexpected exception {str(e)}')
        return

    @UtilsForTesting.test_case
    def testZ9CleanUp(self):
        # Clean up, delete the test index.
        if ESUtil.index_exists(es=self._es_connection,
                               idx_name=self._index_name):
            res = ESUtil.delete_index(es=self._es_connection,
                                      idx_name=self._index_name)
            self.assertTrue(True, res)
            self.assertFalse(ESUtil.index_exists(es=self._es_connection, idx_name=self._index_name))
        return


if __name__ == '__main__':
    suite = unittest.TestLoader().loadTestsFromTestCase(TestElasticTrace)
    unittest.TextTestRunner(verbosity=2).run(suite)
