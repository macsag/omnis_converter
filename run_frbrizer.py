import logging
import sys
import os
import time

import stomp
import redis
from dotenv import load_dotenv

from app_frbrizer.frbrizer import FRBRizer
from amq_listeners.frbrizer_listeners import FRBRizerListener

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

indexed_frbr_clusters_by_titles_conn = redis.Redis(host=REDIS_HOST,
                                                 port=REDIS_PORT,
                                                 db=1)

indexed_frbr_clusters_by_raw_record_id_conn = redis.Redis(host=REDIS_HOST,
                                                          port=REDIS_PORT,
                                                          db=2)

indexed_manifestations_by_uuid_conn = redis.Redis(host=REDIS_HOST,
                                                  port=REDIS_PORT,
                                                  db=3)

# initialize FRBRizer app
frbrizer = FRBRizer(indexed_frbr_clusters_by_uuid_conn,
                    indexed_frbr_clusters_by_titles_conn,
                    indexed_frbr_clusters_by_raw_record_id_conn,
                    indexed_manifestations_by_uuid_conn)

# initialize ActiveMQ connection and set FRBRizerListener with FRBRizer wrapped
AMQ_HOST = os.getenv('AMQ_HOST')
AMQ_PORT = os.getenv('AMQ_PORT')
AMQ_USER = os.getenv('AMQ_USER')
AMQ_PASSWORD = os.getenv('AMQ_PASSWORD')
AMQ_FRBRIZER_QUEUE_NAME = os.getenv('AMQ_FRBRIZER_QUEUE_NAME')
AMQ_FINAL_CONVERTER_QUEUE_NAME = os.getenv('AMQ_FINAL_CONVERTER_QUEUE_NAME')

c = stomp.Connection([(AMQ_HOST, AMQ_PORT)], heartbeats=(0, 0), keepalive=True, auto_decode=False)
c.set_listener('frbrizer_listener', FRBRizerListener(frbrizer,
                                                     AMQ_FINAL_CONVERTER_QUEUE_NAME,
                                                     c))
c.connect(AMQ_USER, AMQ_PASSWORD, wait=True)
c.subscribe(destination=f'/queue/{AMQ_FRBRIZER_QUEUE_NAME}',
            ack='client',
            id='frbrizer_listener',
            headers={'activemq.prefetchSize': 5})

while True:
    time.sleep(5)
