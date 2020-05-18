import redis
from sqlalchemy import create_engine

from indexers.pg_secrets import PG_USER, PG_PASSWORD

to_index = ['language', 'contribution', 'bibliography', 'carrier_type', 'content_type', 'country', 'media_type',
            'publishing_statistics']

dicts_to_index = {'bibliography': {},
                  'carrier_type': {},
                  'content_type': {},
                  'contribution': {},
                  'country': {},
                  'media_type': {},
                  'publishing_statistics': {},
                  'language': {}}

engine = create_engine(f'postgresql://{PG_USER}:{PG_PASSWORD}@192.168.40.233:5432/omnis')
redis_conn = redis.Redis(db=10)

for indexable in to_index:
    with engine.connect() as conn:
        result = conn.execute(f'select id, code, title from {indexable}')
        for row in result:
            dicts_to_index[indexable][row['code']] = f"{row['title']}||{row['id']}"

for dict_name, dict_to_index in dicts_to_index.items():
    redis_conn.hmset(dict_name, dict_to_index)
