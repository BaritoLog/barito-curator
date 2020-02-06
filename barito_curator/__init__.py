import os
import logging
import argparse

from barito_curator.utils import delete_expired_indices_in_barito


def main():
    parser = argparse.ArgumentParser(
        description='Delete expired indices on Barito.')
    parser.add_argument('-d',
                        '--dry-run',
                        dest='dry_run',
                        action='store_true',
                        help="Log without actually doing actions")
    args = parser.parse_args()

    api_url = os.environ['BARITO_API_URL']
    client_key = os.environ['BARITO_API_CLIENT_KEY']
    if 'DELETE_TIMEOUT' in os.environ:
        delete_timeout = os.environ['DELETE_TIMEOUT']
    else:
        delete_timeout = 3600

    logging.basicConfig(level=logging.INFO)
    delete_expired_indices_in_barito(logging.getLogger(), api_url, client_key, delete_timeout,
                                     args.dry_run)
