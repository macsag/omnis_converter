import logging
import sys
import os
import time
from uuid import uuid4

import redis
import stomp
from elasticsearch import Elasticsearch
from dotenv import load_dotenv

from indexers.codes_cache_indexer import get_data_from_cache
from app_final_converter.final_converter import FinalConverter
from amq_listeners.final_converter_listeners import FinalConverterListener


# set up logging
logging.root.addHandler(logging.StreamHandler(sys.stdout))
logging.root.setLevel(level=logging.DEBUG)

# get environment from CLI
try:
    ENV = sys.argv[1]
except IndexError:
    ENV = ''

# load environment variables
if ENV == 'production':
    dotenv_file = '.env.production'
elif ENV == 'staging3':
    dotenv_file = '.env.staging3'
elif ENV == 'dev':
    dotenv_file = '.env.dev'
else:
    dotenv_file = '.env.local'

load_dotenv(dotenv_file)


# initialize connection pools for Redis
REDIS_HOST = os.getenv('REDIS_HOST')
REDIS_PORT = os.getenv('REDIS_PORT')

indexed_frbr_clusters_by_uuid_conn = redis.Redis(host=REDIS_HOST,
                                                 port=REDIS_PORT,
                                                 db=0)

indexed_frbr_clusters_by_raw_record_id_conn = redis.Redis(host=REDIS_HOST,
                                                          port=REDIS_PORT,
                                                          db=2)

indexed_manifestations_by_uuid_conn = redis.Redis(host=REDIS_HOST,
                                                  port=REDIS_PORT,
                                                  db=3)

redis_conn_for_resolver_cache_conn = redis.Redis(host=REDIS_HOST,
                                                 port=REDIS_PORT,
                                                 db=10,
                                                 decode_responses=True)

# initialize connection pool for ES
ELASTICSEARCH_HOST = os.getenv('ELASTICSEARCH_HOST')
ELASTICSEARCH_PORT = os.getenv('ELASTICSEARCH_PORT')

es_conn_for_resolver_cache = Elasticsearch(hosts=[{'host': ELASTICSEARCH_HOST,
                                                   'port': ELASTICSEARCH_PORT}])

# initialize FinalConverter app instance
converter = FinalConverter(indexed_frbr_clusters_by_uuid_conn,
                           indexed_manifestations_by_uuid_conn,
                           indexed_frbr_clusters_by_raw_record_id_conn,
                           es_conn_for_resolver_cache,
                           redis_conn_for_resolver_cache_conn)

# get data from postgres to cache
POSTGRESQL_HOST = os.getenv('POSTGRESQL_HOST')
POSTGRESQL_PORT = os.getenv('POSTGRESQL_PORT')
POSTGRESQL_USER = os.getenv('POSTGRESQL_USER')
POSTGRESQL_PASSWORD = os.getenv('POSTGRESQL_PASSWORD')

get_data_from_cache(POSTGRESQL_HOST,
                    POSTGRESQL_PORT,
                    POSTGRESQL_USER,
                    POSTGRESQL_PASSWORD,
                    REDIS_HOST,
                    REDIS_PORT)

# initialize ActiveMQ connection and set FinalConverterListener with FinalConverter wrapped
AMQ_HOST = os.getenv('AMQ_HOST')
AMQ_PORT = os.getenv('AMQ_PORT')
AMQ_USER = os.getenv('AMQ_USER')
AMQ_PASSWORD = os.getenv('AMQ_PASSWORD')
AMQ_FINAL_CONVERTER_QUEUE_NAME = os.getenv('AMQ_FINAL_CONVERTER_QUEUE_NAME')
AMQ_INDEXER_QUEUE_NAME = os.getenv('AMQ_INDEXER_QUEUE_NAME')

c = stomp.Connection([(AMQ_HOST, AMQ_PORT)], heartbeats=(0, 0), keepalive=True, auto_decode=False)
c.set_listener('final_converter_listener', FinalConverterListener(converter,
                                                                  AMQ_INDEXER_QUEUE_NAME,
                                                                  c))
c.connect(AMQ_USER, AMQ_PASSWORD, wait=True)
c.subscribe(destination=f'/queue/{AMQ_FINAL_CONVERTER_QUEUE_NAME}',
            ack='client-individual',
            id=f'final_converter_listener-{str(uuid4())}',
            headers={'activemq.prefetchSize': 1})

while True:
    time.sleep(5)
