import redis
from sqlalchemy import create_engine

def get_data_from_cache(postgresql_host,
                        postgresql_port,
                        postgresql_user,
                        postgresql_password,
                        redis_host,
                        redis_port):

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

    engine = create_engine(
        f'postgresql://{postgresql_user}:{postgresql_password}@{postgresql_host}:{postgresql_port}/omnis')

    redis_conn = redis.Redis(host=redis_host,
                             port=redis_port,
                             db=10)

    for indexable in to_index:
        with engine.connect() as conn:
            result = conn.execute(f'select id, code, title from {indexable}')
            for row in result:
                dicts_to_index[indexable][row['code']] = f"{row['title']}||{row['id']}"

    engine.dispose()

    for dict_name, dict_to_index in dicts_to_index.items():
        redis_conn.hmset(dict_name, dict_to_index)
