import io
import time
from datetime import datetime, timezone
import unittest
import numpy as np
import json
from IPython.utils.capture import capture_output
from UtilsForTesting import UtilsForTesting
from rltrace.Trace import Trace
from rltrace.Trace import LogLevel
from rltrace.UniqueRef import UniqueRef
from elastic.ESUtil import ESUtil
from gibberish import Gibberish

# We need to run tests in (alphabetical) order.
unittest.TestLoader.sortTestMethodsUsing = lambda self, a, b: (a > b) - (a < b)


class TestElasticTrace(unittest.TestCase):
    _run: int
    _es_connection = None
    _index_name = f'index-{UniqueRef().ref}'
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
            # Load the JSON mappings for the index.
            f = open(self._index_mapping_file)
            self._index_mappings = json.load(f)
            f.close()

            # Open connection to elastic
            self._es_connection = ESUtil.get_connection(hostname='localhost',
                                                        port_id='30365',
                                                        elastic_user='elastic',
                                                        elastic_password='changeme')
        except Exception as e:
            self.fail(f'Unexpected exception {str(e)}')
        return

    def tearDown(self) -> None:
        print(f'- - - - - - C A S E {TestElasticTrace._run} Passed - - - - - -\n')
        return

    @staticmethod
    def _generate_test_document(session_uuid: str) -> str:
        msg = Gibberish.more_gibber()
        tstamp = ESUtil.datetime_in_elastic_time_format(datetime.now())
        return f'{{"session_uuid":"{session_uuid}","level":"DEBUG","timestamp":"{tstamp}","message":"{msg}"}}'

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
                                      document_as_json=doc,
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
            ESUtil.write_doc_to_index(es=self._es_connection,
                                      idx_name=self._index_name,
                                      document_as_json=f'{{"session_uuid":"{session_uuid}","level":"DEBUG","timestamp":"2000-06-20T18:37:51.000067+0000","message":"\\" \' \\\\"}}',
                                      wait_for_write_to_complete=True)

            res = ESUtil.run_count(es=self._es_connection,
                                   idx_name=self._index_name,
                                   json_query={"match": {"session_uuid": session_uuid}})
            self.assertTrue(res == 1)

            ESUtil.delete_documents(es=self._es_connection,
                                    idx_name=self._index_name,
                                    json_query={"match": {"session_uuid": session_uuid}})

            res = ESUtil.run_count(es=self._es_connection,
                                   idx_name=self._index_name,
                                   json_query={"match": {"session_uuid": session_uuid}})
            self.assertTrue(res == 0)

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
