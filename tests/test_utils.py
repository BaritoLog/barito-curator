from datetime import date
from unittest import TestCase
from unittest.mock import Mock, patch

from elasticsearch import ElasticsearchException
from faker import Faker
from freezegun import freeze_time

from barito_curator.utils import (build_delete_action_for_expired_indices,
                                  connect_to_elasticsearch,
                                  delete_expired_indices,
                                  delete_expired_indices_in_clusters,
                                  filter_expired_indices, is_index_expired)


class IsIndexExpiredTestCase(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.faker = Faker()

    def setUp(self):
        self.topic_name = self.faker.domain_word()
        self.cluster_mock = Mock()
        self.cluster_mock.get_log_retention_days = Mock(return_value=2)

    @freeze_time(date(2020, 1, 5))
    def test_expired(self):
        self.assertTrue(
            is_index_expired(f"{self.topic_name}-2020.01.02",
                             self.cluster_mock))

    @freeze_time(date(2020, 1, 5))
    def test_not_expired(self):
        self.assertFalse(
            is_index_expired(f"{self.topic_name}-2020.01.03",
                             self.cluster_mock))

    def test_index_name(self):
        is_index_expired(f"{self.topic_name}-2020.01.03", self.cluster_mock)
        self.cluster_mock.get_log_retention_days.assert_called_with(
            self.topic_name)

    def test_wrong_format_no_date(self):
        with self.assertRaises(ValueError):
            is_index_expired("test", self.cluster_mock)

    def test_wrong_format_invalid_date(self):
        with self.assertRaises(ValueError):
            is_index_expired("{self.topic-name}-2020.13.01", self.cluster_mock)


class FilterExpiredIndicesTestCase(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.faker = Faker()

    def build_index_list_mock(self, indices):
        index_list_mock = Mock()
        index_list_mock.working_list = Mock(
            side_effect=lambda: index_list_mock.all_indices)
        index_list_mock._IndexList__excludify = Mock()

        index_list_mock.all_indices = indices
        return index_list_mock

    def setUp(self):
        self.index_1_name = f"{self.faker.domain_word()}-2020.01.01"
        self.index_2_name = f"{self.faker.domain_word()}-{self.index_1_name}"
        self.index_list_mock = self.build_index_list_mock([self.index_1_name])
        self.cluster_mock = Mock()
        self.is_index_expired_mock = Mock()
        self.logger_mock = Mock()
        self.logger_mock.warning = Mock()
        self.value_error = ValueError(self.faker.sentence())

    def call_target(self):
        with patch('barito_curator.utils.is_index_expired',
                   self.is_index_expired_mock):
            filter_expired_indices(self.logger_mock, self.index_list_mock,
                                   self.cluster_mock)

    def test_expired(self):
        self.is_index_expired_mock.return_value = True
        self.call_target()
        self.assertFalse(
            self.index_list_mock._IndexList__not_actionable.called)

    def test_not_expired(self):
        self.is_index_expired_mock.return_value = False
        self.call_target()
        self.index_list_mock._IndexList__not_actionable.assert_called_once_with(
            self.index_1_name)

    def test_is_index_expired_call(self):
        self.call_target()
        self.is_index_expired_mock.assert_called_once_with(
            self.index_1_name, self.cluster_mock)

    def prepare_two_indices(self):
        self.index_list_mock.all_indices = [
            self.index_1_name, self.index_2_name
        ]

    def test_expired_any(self):
        self.prepare_two_indices()
        self.is_index_expired_mock.side_effect = (lambda index_name, _: {
            self.index_1_name: False,
            self.index_2_name: True
        }.get(index_name, None))

        self.call_target()
        self.index_list_mock._IndexList__not_actionable.assert_called_once_with(
            self.index_1_name)

    def prepare_value_error_test(self):
        self.prepare_two_indices()
        self.is_index_expired_mock.side_effect = [self.value_error, True]

    def test_value_error_log(self):
        self.prepare_value_error_test()
        self.call_target()
        self.logger_mock.warning.assert_called_once_with(
            f"Unable to check index `{self.index_1_name}`: {self.value_error}")

    def test_value_error_print(self):
        self.prepare_value_error_test()
        self.call_target()
        self.assertTrue(self.index_list_mock._IndexList__not_actionable.called)


class ConnectToElasticSearchTestCase(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.faker = Faker()

    def build_es_connection_mock(self):
        es_connection_mock = Mock()
        es_connection_mock.ping = Mock(return_value=True)
        return es_connection_mock

    def build_es_mock(self, connection):
        return Mock(return_value=connection)

    def setUp(self):
        self.address = self.faker.ipv4()
        self.es_connection_mock = self.build_es_connection_mock()
        self.es_mock = self.build_es_mock(self.es_connection_mock)

    def test_init_call(self):
        with patch('barito_curator.utils.Elasticsearch', self.es_mock):
            connect_to_elasticsearch(self.address)
        self.es_mock.assert_called_once_with(self.address, timeout=300)

    def test_return(self):
        with patch('barito_curator.utils.Elasticsearch', self.es_mock):
            elastic = connect_to_elasticsearch(self.address)
        self.assertEqual(self.es_connection_mock, elastic)

    def test_unpingable(self):
        self.es_connection_mock.ping.return_value = False
        with self.assertRaises(ConnectionError):
            with patch('barito_curator.utils.Elasticsearch', self.es_mock):
                connect_to_elasticsearch(self.address)


class BuildDeleteActionForExpiredIndicesTestCase(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.faker = Faker()

    def setUp(self):
        self.delete_timeout = 10
        self.cluster_mock = Mock()
        self.cluster_mock.address = self.faker.ipv4()
        self.es_connection_mock = Mock()
        self.connect_to_elasticsearch_mock = Mock(
            return_value=self.es_connection_mock)
        self.index_list_mock = Mock()
        self.index_list_init_mock = Mock(return_value=self.index_list_mock)
        self.logger_mock = Mock()
        self.filter_expired_indices_mock = Mock()
        self.delete_indices_mock = Mock()
        self.delete_indices_init_mock = Mock(
            return_value=self.delete_indices_mock)

        @patch('barito_curator.utils.connect_to_elasticsearch',
               self.connect_to_elasticsearch_mock)
        @patch('barito_curator.utils.IndexList', self.index_list_init_mock)
        @patch('barito_curator.utils.filter_expired_indices',
               self.filter_expired_indices_mock)
        @patch('barito_curator.utils.DeleteIndices',
               self.delete_indices_init_mock)
        def func():
            return build_delete_action_for_expired_indices(
                self.logger_mock, self.cluster_mock, self.delete_timeout)

        self.target_return_value = func()

    def test_connect_to_elasticsearch_call(self):
        self.connect_to_elasticsearch_mock.assert_called_once_with(
            self.cluster_mock.address)

    def test_index_list_init_call(self):
        self.index_list_init_mock.assert_called_once_with(
            self.es_connection_mock)

    def test_filter_expired_indices_call(self):
        self.filter_expired_indices_mock.assert_called_once_with(
            self.logger_mock, self.index_list_mock, self.cluster_mock)

    def test_delete_indices_call(self):
        self.delete_indices_init_mock.assert_called_once_with(
            self.index_list_mock, master_timeout=self.delete_timeout)

    def test_return(self):
        self.assertEqual(self.delete_indices_mock, self.target_return_value)


class DeleteExpiredIndices(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.faker = Faker()

    def setUp(self):
        self.delete_timeout = 10
        self.cluster_mock = Mock()
        self.child_logger_mock = Mock()
        self.logger_mock = Mock()
        self.logger_mock.getChild = Mock(return_value=self.child_logger_mock)
        self.delete_action_mock = Mock()
        self.delete_action_mock.do_action = Mock()
        self.build_delete_action_for_expired_indices_mock = Mock(
            return_value=self.delete_action_mock)
        self.elasticsearch_exception = ElasticsearchException(
            self.faker.sentence())

    def call_target(self, cluster, delete_timeout, **kwargs):
        with patch(
                'barito_curator.utils.build_delete_action_for_expired_indices',
                self.build_delete_action_for_expired_indices_mock):
            delete_expired_indices(self.logger_mock, cluster, delete_timeout, **kwargs)

    def test_build_delete_action_for_expired_indices_mock_call(self):
        self.call_target(self.cluster_mock, self.delete_timeout)
        self.build_delete_action_for_expired_indices_mock.assert_called_once_with(
            self.child_logger_mock, self.cluster_mock, self.delete_timeout)

    def test_logger_get_child_prefix(self):
        self.call_target(self.cluster_mock, self.delete_timeout)
        self.logger_mock.getChild.assert_called_once_with(
            f"Cluster `{self.cluster_mock.address}`")

    def test_do_action_call(self):
        self.call_target(self.cluster_mock, self.delete_timeout)
        self.delete_action_mock.do_action.assert_called_once_with()

    def test_do_dry_run_call(self):
        self.call_target(self.cluster_mock, self.delete_timeout, dry_run=True)
        self.delete_action_mock.do_dry_run.assert_called_once_with()

    def test_dry_run_not_doing_any_action(self):
        self.call_target(self.cluster_mock, self.delete_timeout, dry_run=True)
        self.assertFalse(self.delete_action_mock.do_action.called)


class DeleteExpiredIndicesInClustersTestCase(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.faker = Faker()

    def setUp(self):
        self.cluster_1_mock = Mock()
        self.cluster_2_mock = Mock()
        self.logger_mock = Mock()
        self.dry_run = self.faker.pybool()
        self.delete_expired_indices_mock = Mock()
        self.delete_timeout = 10

    def call_target(self, clusters):
        with patch('barito_curator.utils.delete_expired_indices',
                   self.delete_expired_indices_mock):
            delete_expired_indices_in_clusters(self.logger_mock, clusters, self.delete_timeout,
                                               self.dry_run)

    def test_single_cluster(self):
        self.call_target([self.cluster_1_mock])
        self.delete_expired_indices_mock.assert_called_once_with(
            self.logger_mock, self.cluster_1_mock, self.delete_timeout, self.dry_run)

    def test_multiple_clusters(self):
        self.call_target([self.cluster_1_mock, self.cluster_2_mock])
        self.delete_expired_indices_mock.assert_called_with(
            self.logger_mock, self.cluster_2_mock, self.delete_timeout, self.dry_run)

    def test_multiple_clusters_all_deleted(self):
        calls = set()
        self.delete_expired_indices_mock.side_effect = lambda *args: calls.add(
            (*args, ))
        self.call_target([self.cluster_1_mock, self.cluster_2_mock])

        self.assertIn((self.logger_mock, self.cluster_1_mock, self.delete_timeout, self.dry_run),
                      calls)
