import requests
import json
import elasticsearch
import curator
import logging
import sys
import os

logger = logging.getLogger(__name__)

def connect_elasticsearch(cluster):
    ipaddress = cluster["ipaddress"]
    hostname = cluster["hostname"]
    logger.info('Connecting to %s %s' % (hostname, ipaddress))
    try:
        client = elasticsearch.Elasticsearch(ipaddress)
        connected = client.ping()
        if not connected:
            logger.error('Connection to %s %s failed' % (hostname, ipaddress))
            return None
        return client
    except Exception as ex:
        logger.error('Exception %s' % (ex))

def main():

    try:
        req = requests.get('{api_url}?client_key={client_key}'.format(api_url =os.environ['BARITO_API_URL'], client_key=os.environ['BARITO_API_CLIENT_KEY']))
    except requests.exceptions.RequestException as ex:
        logger.error(ex)
        sys.exit(1)

    jsonObject = req.json()

    for cluster in jsonObject:
        log_retention_days = cluster['log_retention_days']
        es = connect_elasticsearch(cluster)
        if es is not None:
            index_list = curator.IndexList(es)
            if len(index_list.indices) == 0:
                logger.warn("No indices found, skipping")
                continue
            try:
                index_list.filter_by_regex(kind='timestring', value='%Y.%m.%d')
                index_list.filter_by_age(source='name', direction='older', timestring='%Y.%m.%d', unit='days', unit_count=log_retention_days)

            except Exception as ex:
                logger.error('Exception %s' % (ex))
                pass

            if index_list.indices:
                logger.info('Delete %d indices' % len(index_list.indices))
                curator.DeleteIndices(index_list, 300).do_action()
            pass
    pass

if __name__ == '__main__':
    main()
