import requests


def fetch(api_url, client_key):
    return requests.get(api_url, params=[('client_key', client_key)]).json()


def parse_json_structure(json_clusters):
    return [
        Cluster(json_cluster["ipaddress"], json_cluster["log_retention_days"],
                json_cluster["log_retention_days_per_topic"])
        for json_cluster in json_clusters
    ]


def fetch_clusters(api_url, client_key):
    return parse_json_structure(fetch(api_url, client_key))


class Cluster:
    def __init__(self, address, default_log_retention_days,
                 log_retention_days_per_app):
        self.__address = address
        self.__default_log_retention_days = default_log_retention_days
        self.__log_retention_days_per_app = log_retention_days_per_app

    @property
    def address(self):
        return self.__address

    def get_log_retention_days(self, app_name):
        return self.__log_retention_days_per_app.get(
            app_name, self.__default_log_retention_days)
