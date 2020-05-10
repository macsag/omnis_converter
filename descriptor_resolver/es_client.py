from elasticsearch import Elasticsearch
from elasticsearch_dsl import MultiSearch, Search

client = Elasticsearch(hosts=[{"host": "192.168.40.50", 'port': 9200}])

ms = MultiSearch(using=client)
ms = ms.add(Search().query('match', descr_nlp_id='a0000001003190'))
ms = ms.add(Search().query('match', descr_nlp_id='a1000000699517'))

resp = ms.execute()

print(resp)

for single_resp in resp:
    for hit in single_resp:
        print(hit.descr_name, hit.descr_nlp_id)



