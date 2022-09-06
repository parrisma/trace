import unittest
import json
import time
from datetime import datetime
from envbuilder import EnvBuilder
from envboot.env import Env
from elastic.ESUtil import ESUtil
from UniqueRef import UniqueRef
from ElasticFormatter import ElasticFormatter
from notificationformatter import NotificationFormatter
from TraceReport import TraceReport


class TestAITrace(unittest.TestCase):
    class DummyLoggingRecord:
        # Proxy/model for the Python logging Record (only relevant fields set here)
        def __init__(self,
                     name: str,
                     level_no: int,
                     message: str,
                     created: datetime):
            self.name = name
            self.levelno = level_no
            self.msg = message
            self.created = created
            return

        # self._trace.log().info("Initiating connection to Elastic DB")
        # self._es = ESUtil.get_connection(hostname=self._hostname, port_id=self._port_id)
        # self._trace.log().info("Connected to {}".format(str(self._es)))

    @classmethod
    def setUpClass(cls):
        cls._env = Env(purge=False)
        cls._trace = cls._env.get_context()[EnvBuilder.TraceContext]
        return

    def test_elastic_formatter(self):
        ef = ElasticFormatter()
        record = TestAITrace.DummyLoggingRecord(name="de741e6c74164189827100ba65eda743",
                                                level_no=10,
                                                message="c5ee7458-1c4a-453a-8582-ebe7744e3623",
                                                created=datetime(year=2000, month=6, day=20,
                                                                 hour=18, minute=37, second=51, microsecond=67)
                                                )
        res = ef.format(record=record)
        self.assertEqual(
            '{"session_uuid":"de741e6c74164189827100ba65eda743","level":"DEBUG","timestamp":"2000-06-20T18:37:51.000067+0000","message":"c5ee7458-1c4a-453a-8582-ebe7744e3623"}',
            res)
        js = json.loads(res)
        self.assertEqual(js['session_uuid'], "de741e6c74164189827100ba65eda743")
        self.assertEqual(js['level'], "DEBUG")
        self.assertEqual(js['timestamp'], "2000-06-20T18:37:51.000067+0000")
        self.assertEqual(js['message'], "c5ee7458-1c4a-453a-8582-ebe7744e3623")
        return

    def test_elastic_formatter_special_character(self):
        """
        Need to ensure JSON special characters are encoded correctly
        """
        ef = ElasticFormatter()
        record = TestAITrace.DummyLoggingRecord(name="de741e6c74164189827100ba65eda743",
                                                level_no=10,
                                                message=chr(34) + chr(32) + chr(39) + chr(32) + chr(92),
                                                created=datetime(year=2000, month=6, day=20,
                                                                 hour=18, minute=37, second=51, microsecond=67)
                                                )
        res = ef.format(record=record)
        self.assertEqual(
            '{"session_uuid":"de741e6c74164189827100ba65eda743","level":"DEBUG","timestamp":"2000-06-20T18:37:51.000067+0000","message":"\\" \' \\\\"}',
            res)
        js = json.loads(res)
        self.assertEqual(js['session_uuid'], "de741e6c74164189827100ba65eda743")
        self.assertEqual(js['level'], "DEBUG")
        self.assertEqual(js['timestamp'], "2000-06-20T18:37:51.000067+0000")
        self.assertEqual(js['message'],
                         chr(34) + chr(32) + chr(39) + chr(32) + chr(92))  # should be un encoded as parsed
        return

    def test_json_insert_args(self):
        str_to_insert_into = "0=<arg0> 2=<arg2> 1=<arg1> 10=<arg10> 2=<arg2><arg2> 20=<arg20>"
        actual = ESUtil.json_insert_args(str_to_insert_into,
                                         arg10="10",
                                         argument0="ShouldIgnoreMe",
                                         arg2="2",
                                         arg0="0",
                                         arg1="1",
                                         ar0="ShouldIgnoreMe")
        self.assertEqual("0=0 2=2 1=1 10=10 2=22 20=<arg20>", actual)
        return

    def test_notification_formatter(self):
        nf = NotificationFormatter()
        actual = nf.format(notification_type_uuid="123",
                           work_ref_uuid="234",
                           session_uuid="345",
                           sink_uuid="456",
                           src_uuid="567",
                           timestamp=datetime(year=2000, month=6, day=20,
                                              hour=18, minute=37, second=51, microsecond=67))
        self.assertEqual(
            '{"notification_type_uuid":"123","work_ref_uuid":"234","session_uuid":"345","sink_uuid":"456","src_uuid":"567","timestamp":"2000-06-20T18:37:51.000067+0000"}'
            , actual)
        return

    def test_notification_log_write_to_elastic(self):
        """
        Write 100 random notification log events to elastic
        """
        tr: TraceReport
        tr = Env.get_context()[EnvBuilder.TraceReport]

        try:
            _num_to_create = 100
            session_uuid = UniqueRef().ref  # session uuid remains fixed.
            for i in range(_num_to_create):
                tr.log_notification(notification_type_uuid=UniqueRef().ref,
                                    work_ref_uuid=UniqueRef().ref,
                                    session_uuid=session_uuid,
                                    sink_uuid=UniqueRef().ref,
                                    src_uuid=UniqueRef().ref)
                self._trace.log().debug(
                    "Written random notification log number {} for session {}".format(i, session_uuid))
            time.sleep(1)
            self.assertEqual(_num_to_create, self._num_docs_in_notification_log_session(session_uuid=session_uuid))
        except Exception as e:
            self.assertFalse("Unexpected Exception while testing notification logging")
        return

    def test_trace_log_write_to_elastic(self):
        """
        Write 100 records to Trace, which is elastic enabled and check that 100 matching records appear in
        the appropriate index.
        """
        _num_to_create = 100
        try:
            session_uuid = UniqueRef().ref
            for i in range(_num_to_create):
                self._trace.log().debug("{}-{}".format(session_uuid, i))
            time.sleep(1)
            self.assertEqual(_num_to_create, self._num_docs_in_trace_log_session(message_uuid=session_uuid))
        except Exception as e:
            self.assertFalse("Unexpected Exception while testing Trace logging")
        return

    def _num_docs_in_trace_log_session(self,
                                       message_uuid: str) -> int:
        """
        How many trace_log entries are there that match the given session_uuid
        :param message_uuid: The message uuid to count documents for
        """
        tr: TraceReport
        tr = Env.get_context()[EnvBuilder.TraceReport]
        return tr.trace_log_count(field_name="message",
                                  session_uuid_pattern="{}*".format(message_uuid))

    def _num_docs_in_notification_log_session(self,
                                              session_uuid: str) -> int:
        """
        How many notification_log entries are there that match the given session_uuid
        :param session_uuid: The session uuid to count documents for
        """
        tr: TraceReport
        tr = Env.get_context()[EnvBuilder.TraceReport]
        return tr.notification_log_count(field_name="session_uuid",
                                         session_uuid_pattern="{}*".format(session_uuid))


if __name__ == "__main__":
    unittest.main()
