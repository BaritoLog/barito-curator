import json
import unittest
from unittest.mock import Mock, patch

import requests
from faker import Faker

from barito_curator.metadata import Cluster, fetch, parse_json_structure


class ClusterTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.faker = Faker()

    def setUp(self):
        self.address = self.faker.ipv4()
        self.default_log_retention_days = self.faker.pyint(min_value=10)
        self.app_1_name = self.faker.domain_word()
        self.app_1_log_retention_days = self.default_log_retention_days - 9
        self.missing_app_2_name = f"{self.app_1_name}-missing"

        self.cluster = Cluster(
            self.address, self.default_log_retention_days,
            {self.app_1_name: self.app_1_log_retention_days})

    def test_address(self):
        self.assertEqual(self.address, self.cluster.address)

    def test_get_log_retention_days_custom(self):
        self.assertEqual(self.app_1_log_retention_days,
                         self.cluster.get_log_retention_days(self.app_1_name))

    def test_get_log_retention_days_default(self):
        self.assertEqual(
            self.default_log_retention_days,
            self.cluster.get_log_retention_days(self.missing_app_2_name))


class ParseJSONTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.faker = Faker()

    def setUp(self):
        self.address = self.faker.ipv4()
        self.default_log_retention_days = self.faker.pyint(min_value=10)
        self.app_1_name = self.faker.domain_word()
        self.app_1_log_retention_days = self.default_log_retention_days - 9
        self.missing_app_2_name = f"{self.app_1_name}-missing"

        json_data = f"""
        [
            {{
                "ipaddress": "{self.address}",
                "log_retention_days": {self.default_log_retention_days},
                "log_retention_days_per_topic": {{
                    "{self.app_1_name}": {self.app_1_log_retention_days}
                }}
            }},
            {{
                "ipaddress": "{self.address}",
                "log_retention_days": {self.default_log_retention_days},
                "log_retention_days_per_topic": {{
                    "{self.app_1_name}": {self.app_1_log_retention_days}
                }}
            }}
        ]
        """
        self.clusters = parse_json_structure(json.loads(json_data))

    def test_parse_json_address(self):
        self.assertEqual(self.clusters[0].address, self.address)

    def test_parse_json_default_log_retention_days(self):
        self.assertEqual(
            self.clusters[0].get_log_retention_days(self.missing_app_2_name),
            self.default_log_retention_days)

    def test_parse_json_app_1_log_retention_days(self):
        self.assertEqual(
            self.clusters[0].get_log_retention_days(self.app_1_name),
            self.app_1_log_retention_days)

    def test_parse_json_cluster_2_address(self):
        self.assertEqual(self.clusters[1].address, self.address)


class FetchTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.faker = Faker()

    def setUp(self):
        self.api_url = self.faker.url()
        self.client_key = self.faker.password(length=128)

        self.response_json_mock = Mock()
        self.response_mock = Mock()
        self.response_mock.json = Mock(return_value=self.response_json_mock)
        self.requests_get_mock = Mock(return_value=self.response_mock)

    def call_target(self):
        with patch('requests.get', self.requests_get_mock):
            return fetch(self.api_url, self.client_key)

    def test_fetch_url(self):
        self.call_target()
        self.requests_get_mock.assert_called_with(self.api_url,
                                                  params=[('client_key',
                                                           self.client_key)])

    def test_fetch_url_json(self):
        response_json = self.call_target()
        self.assertEqual(self.response_json_mock, response_json)
