import unittest
from unittest.mock import Mock

from app_processor.pyspark_hb_app_processor import parse_configs, get_or_generate_spark_session


class TestAppProcessor(unittest.TestCase):

    def test_parse_configs(self):
        conf = parse_configs("test-conf.yml")
        self.assertEqual('value1', conf['key1'])
        self.assertEqual('value2', conf['key2'])
        self.assertEqual(2, len(conf))

    def test_get_or_generate_spark_session(self):
        test_map = {'key1': 'val1', 'key2': 'val2'}
        test_master_url = "TEST1"
        test_app_name = "TEST_APP"
        mock_spark_session_builder = Mock()
        # we have to do this since we use the builder pattern when creating the SparkSession
        attrs = {'master.return_value': mock_spark_session_builder, 'appName.return_value': mock_spark_session_builder}
        mock_spark_session_builder.configure_mock(**attrs)

        get_or_generate_spark_session(mock_spark_session_builder, test_map, test_master_url, test_app_name)
        mock_spark_session_builder.master.assert_called_with(test_master_url)
        mock_spark_session_builder.appName.assert_called_with(test_app_name)
        self.assertEqual(len(test_map), mock_spark_session_builder.config.call_count)
        mock_spark_session_builder.getOrCreate.assert_called_once()


if __name__ == '__main__':
    unittest.main()
