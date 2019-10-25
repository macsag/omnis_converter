from uuid import uuid4
import json
import re

from commons.marc_iso_commons import to_single_value, get_values_by_field_and_subfield, get_values_by_field
from commons.marc_iso_commons import ObjCounter, postprocess

from commons.json_writer import write_to_json


class BnItem(object):
    def __init__(self, bib_object, work, manifestation, expression, buffer):

        # attributes for item_es_index
        self.mock_es_id = str('114' + to_single_value(get_values_by_field(bib_object, '001'))[1:-1])
        self.expression_ids = [str(expression.mock_es_id)]
        self.item_call_number = get_values_by_field_and_subfield(bib_object, ('852', ['h']))
        self.item_count = len(get_values_by_field(bib_object, '852'))
        self.item_deleted_id = []
        self.item_local_bib_id = str(to_single_value(get_values_by_field(bib_object, '001')))
        self.item_local_id = postprocess(str, get_values_by_field_and_subfield(bib_object, ('852', ['8'])))
        self.item_location = str(to_single_value(get_values_by_field_and_subfield(bib_object, ('852', ['c']))))
        self.item_mat_id = int(manifestation.mock_es_id)
        self.item_source = 'DATABN'  # fake source
        self.item_status = 'false'  # fake status
        self.item_url = f'https://katalogi.bn.org.pl/discovery/fulldisplay?docid=alma' \
                        f'{str(to_single_value(get_values_by_field(bib_object, "009")))}' \
                        f'&context=L&vid=48OMNIS_NLOP:48OMNIS_NLOP'
        self.item_work_id = int(work.mock_es_id)
        self.library = {'digital': False, 'name': 'Biblioteka Narodowa', 'id': 10947}  # hardcoded - always the same
        self.metadata_original = str(uuid4())  # some random fake uuid
        self.metadata_source = 'REFERENCE'
        self.modification_time = '2019-10-11T17:45:21.527'  # fake time
        self.phrase_suggest = ['-']
        self.suggest = ['-']
        self.work_ids = [str(work.mock_es_id)]
        self.write_to_dump_file(buffer)

    def __repr__(self):
        return f'BnItem(id={self.mock_es_id}, item_count={self.item_count}, item_url={self.item_url}'

    def serialize_to_es_dump(self):
        dict_item = {"_index": "item", "_type": "item", "_id": self.mock_es_id,
                     "_score": 1, "_source":
                         {"expression_ids": self.expression_ids,
                          "item_call_number": self.item_call_number,
                          "item_count": self.item_count,
                          "item_deleted_id": self.item_deleted_id,
                          "item_local_bib_id": self.item_local_bib_id,
                          "item_local_id": self.item_local_id,
                          "item_location": self.item_location,
                          "item_mat_id": self.item_mat_id,
                          "item_source": self.item_source,
                          "item_status": self.item_status,
                          "item_url": self.item_url,
                          "item_work_id": self.item_work_id,
                          "library": self.library,
                          "metadata_original": self.metadata_original,
                          "metadata_source": self.metadata_source,
                          "modificationTime": self.modification_time,
                          "phrase_suggest": self.phrase_suggest,
                          "suggest": self.suggest,
                          "work_ids": self.work_ids}}
        json_item = json.dumps(dict_item, ensure_ascii=False)

        return json_item

    def write_to_dump_file(self, buffer):
        write_to_json(self.serialize_to_es_dump(), buffer, 'item_buffer')


class MakItem(object):
    def __init__(self, ava_field, library_index, work, expression, manifestation, buffer, num):

        # attributes for manifestation_es_index
        self.mock_es_id = None
        self.expression_ids = [str(expression.mock_es_id)]
        self.item_count = int(ava_field.get_subfields('f')[0])
        self.item_local_bib_id = re.findall('\d+', ava_field.get_subfields('b')[0])[0]
        self.item_mat_id = int(manifestation.mock_es_id)
        self.item_publication = None
        self.item_service_url = None
        self.item_source = 'MAK'  # fake source
        self.item_status = 'false'  # fake status
        self.item_unavailable_count = 0
        self.item_url = ava_field.get_subfields('u')[0]
        self.item_work_id = int(work.mock_es_id)
        self.library = self.get_library(library_index)
        self.metadata_original = str(uuid4())  # some random fake uuid
        self.metadata_source = 'OTHER'
        self.modification_time = '2019-10-11T17:45:21.527'  # fake time
        self.creation_time = '2019-10-11T17:45:21.527'
        self.phrase_suggest = ['-']
        self.suggest = ['-']
        self.work_ids = [str(work.mock_es_id)]

    def __repr__(self):
        return f'MakItem(id={self.mock_es_id}, item_count={self.item_count}, item_url={self.item_url}'

    def get_library(self, library_index):
        library_data = library_index.get(self.item_local_bib_id)
        return {'digital': False, 'name': library_data.source['name'], 'id': int(library_data.es_id)}

    def add(self, mak_item):
        self.item_count += mak_item.item_count

    def serialize_to_es_dump(self):
        dict_item = {"_index": "item", "_type": "item", "_id": self.mock_es_id,
                     "_score": 1, "_source":
                         {"expression_ids": self.expression_ids,
                          "item_count": self.item_count,
                          "item_local_bib_id": self.item_local_bib_id,
                          "item_mat_id": self.item_mat_id,
                          "item_publication": self.item_publication,
                          "item_service_url": self.item_service_url,
                          "item_source": self.item_source,
                          "item_status": self.item_status,
                          "item_unavailable_count": self.item_unavailable_count,
                          "item_url": self.item_url,
                          "item_work_id": self.item_work_id,
                          "library": self.library,
                          "metadata_original": self.metadata_original,
                          "metadata_source": self.metadata_source,
                          "modification_time": self.modification_time,
                          "creation_time": self.creation_time,
                          "phrase_suggest": self.phrase_suggest,
                          "suggest": self.suggest,
                          "work_ids": self.work_ids}}
        json_item = json.dumps(dict_item, ensure_ascii=False)

        return json_item

    def write_to_dump_file(self, buffer):
        write_to_json(self.serialize_to_es_dump(), buffer, 'item_buffer')
