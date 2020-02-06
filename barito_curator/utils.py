from datetime import datetime, timedelta
from multiprocessing import cpu_count
from multiprocessing.pool import ThreadPool

from curator import DeleteIndices, IndexList
from elasticsearch import Elasticsearch

from barito_curator.metadata import fetch_clusters


def is_index_expired(index_name, cluster):
    splitted_index_name = index_name.rsplit('-', 1)
    if len(splitted_index_name) < 2:
        raise ValueError("No date information in index name")

    topic, index_date_str = splitted_index_name
    index_date = datetime.strptime(index_date_str, "%Y.%m.%d")

    log_retention_days = cluster.get_log_retention_days(topic)
    deletion_date = datetime.today() - timedelta(days=log_retention_days)
    return index_date < deletion_date


def filter_expired_indices(logger, index_list, cluster):
    for index_name in index_list.working_list():
        exclude = True
        try:
            if is_index_expired(index_name, cluster):
                exclude = False
        except ValueError as e:
            logger.warning(f"Unable to check index `{index_name}`: {e}")
        if exclude:
            index_list._IndexList__not_actionable(index_name)


def connect_to_elasticsearch(address):
    elastic = Elasticsearch(address, timeout=300)
    if not elastic.ping():
        raise ConnectionError(f"Unable to ping ElasticSearch: {address}")
    return elastic


def build_delete_action_for_expired_indices(logger, cluster, delete_timeout):
    elastic = connect_to_elasticsearch(cluster.address)
    index_list = IndexList(elastic)
    filter_expired_indices(logger, index_list, cluster)
    return DeleteIndices(index_list, master_timeout=delete_timeout)


def delete_expired_indices(logger, cluster, delete_timeout, dry_run=False):
    child_logger = logger.getChild(f"Cluster `{cluster.address}`")
    try:
        delete_action = build_delete_action_for_expired_indices(
            child_logger, cluster, delete_timeout)
        if dry_run:
            delete_action.do_dry_run()
        else:
            delete_action.do_action()
    except Exception as e:
        child_logger.warning(f"Unable to delete expired indices: {e}")


def delete_expired_indices_in_clusters(logger, clusters, delete_timeout, dry_run=False):
    pool = ThreadPool(cpu_count())
    pool.map(lambda cluster: delete_expired_indices(logger, cluster, delete_timeout, dry_run),
             clusters)


def delete_expired_indices_in_barito(logger,
                                     api_url,
                                     client_key,
                                     delete_timeout,
                                     dry_run=False):
    clusters = fetch_clusters(api_url, client_key)
    delete_expired_indices_in_clusters(logger, clusters, delete_timeout, dry_run)
