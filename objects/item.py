from uuid import uuid4
import json
import re

from pymarc import Record, Field

from commons.marc_iso_commons import to_single_value, get_values_by_field_and_subfield, get_values_by_field
from commons.marc_iso_commons import postprocess
from objects.helper_objects import ObjCounter

from commons.json_writer import write_to_json


mock_item_ct = {"physical_item":
                    {"item_field_tag": "852",
                     "item_count":
                        {"field": None,
                         "subfields": None},
                    "item_url":
                        {"field": None,
                         "subfields": None,
                         "scheme":
                            {"prefix": "https://katalogi.bn.org.pl/discovery/fulldisplay?docid=alma",
                             "suffix": "&context=L&vid=48OMNIS_NLOP:48OMNIS_NLOP",
                             "infix": {"field": "009", "subfields": None}}},
                    "item_library_code":
                        {"field": None,
                         "subfields": None,
                         "from_ct": "BN"}
                     },
                "digital_item":
                    {"item_field_tag": "856",
                     "item_count":
                         {"field": None,
                         "subfields": None},
                     "item_url":
                         {"field": "this_field",
                          "subfields": ["u"],
                          "scheme": None},
                     "item_library_code":
                         {"field": None,
                          "subfields": None,
                          "from_ct": "POLONA"},
                     }
                }


class FRBRItem(object):
    __slots__ = ['uuid', 'item_raw_record_id', 'item_url',
                 'item_library_code', 'item_count', 'item_count_by_raw_record_id']

    def __init__(self,
                 pymarc_object: Record,
                 pymarc_item_field: Field,
                 item_ct: dict,
                 raw_record_id: str,
                 digital: bool = False):

        self.uuid = str(uuid4())

        # item attributes
        self.item_raw_record_id = None
        self.item_library_code = None
        self.item_url = None
        self.item_count = ObjCounter()

        # item_count_helper_dict for merging and splitting items with the same library code
        # from different raw records (if one or more raw_records from non-reference sources are the same manifestation)
        self.item_count_by_raw_record_id = {}

        # call populate attributes method
        self.populate_attributes(pymarc_object, pymarc_item_field, item_ct, raw_record_id, digital)

    def populate_attributes(self,
                            pymarc_object: Record,
                            pymarc_item_field: Field,
                            item_ct: dict,
                            raw_record_id: str,
                            digital: bool) -> None:

        if digital:
            base_path = 'digital_item'
        else:
            base_path = 'physical_item'

        self.item_raw_record_id = raw_record_id

        # item_library_code
        if item_ct[base_path]['item_library_code']['from_ct']:
            self.item_library_code = item_ct[base_path]['item_library_code']['from_ct']
        else:
            item_library_value = self.get_attribute_value('item_library_code',
                                                          pymarc_object,
                                                          pymarc_item_field,
                                                          item_ct,
                                                          digital)

            self.item_library_code = item_library_value

        # item_url
        if item_ct[base_path]['item_url']['scheme']:
            infix_field = item_ct[base_path]['item_url']['scheme']['infix']['field']
            infix_subfields = item_ct[base_path]['item_url']['scheme']['infix']['subfields']
            infix_scheme = (infix_field, infix_subfields)

            infix_value = to_single_value(get_values_by_field_and_subfield(pymarc_object,
                                                                           infix_scheme))
            prefix_value = item_ct[base_path]['item_url']['scheme']['prefix']
            suffix_value = item_ct[base_path]['item_url']['scheme']['suffix']

            item_url_value = f'{prefix_value}{infix_value}{suffix_value}'
            self.item_url = item_url_value
        else:
            item_url_value = self.get_attribute_value('item_url',
                                                      pymarc_object,
                                                      pymarc_item_field,
                                                      item_ct,
                                                      digital)

            self.item_library_code = item_url_value

        # item_count
        if item_ct[base_path]['item_count']['field']:
            item_count_value = int(self.get_attribute_value('item_count',
                                                        pymarc_object,
                                                        pymarc_item_field,
                                                        item_ct,
                                                        digital))

            self.item_count.add(item_count_value)
        else:
            item_count_value = 1
            self.item_count.add(item_count_value)

        # item_count_helper_dict
        self.item_count_by_raw_record_id.setdefault(raw_record_id, ObjCounter()).add(item_count_value)

    @staticmethod
    def get_attribute_value(attribute: str,
                            pymarc_object: Record,
                            pymarc_item_field: Field,
                            item_ct: dict,
                            digital: bool):

        if digital:
            base_path = 'digital_item'
        else:
            base_path = 'physical_item'

        attribute_field = item_ct[base_path][attribute]['field']
        attribute_subfields = item_ct[base_path][attribute]['subfields']

        attribute_scheme = (attribute_field, attribute_subfields)
        if attribute_field == 'this_field':
            attribute_value = to_single_value(get_values_by_field_and_subfield(pymarc_item_field,
                                                                               (None, attribute_subfields)))
        else:
            attribute_value = to_single_value(get_values_by_field_and_subfield(pymarc_object,
                                                                               attribute_scheme))

        return attribute_value

    def merge_item(self, item):
        for raw_record_id, counter in item.item_count_by_raw_record_id.items():
            self.item_count_by_raw_record_id.setdefault(raw_record_id, ObjCounter()).add(counter.count)
            self.item_count.add(counter.count)

    @staticmethod
    def get_items(pymarc_object: Record, raw_record_id: str, item_ct: dict):
        # create empty dict
        # items are created per library code (one item record per library with item_count for real physical items)
        # {library_code: FRBRItem(), ...}
        dict_of_items = {}

        if item_ct['physical_item']:
            for pymarc_item_field in pymarc_object.get_fields(item_ct['physical_item']['item_field_tag']):
                item_to_append = FRBRItem(pymarc_object, pymarc_item_field, item_ct, raw_record_id)
                item_to_modify = dict_of_items.get(item_to_append.item_library_code)
                if item_to_modify:
                    item_to_modify.merge_item(item_to_append)
                else:
                    dict_of_items[item_to_append.item_library_code] = item_to_append

        if item_ct['digital_item']:
            for pymarc_item_field in pymarc_object.get_fields(item_ct['digital_item']['item_field_tag']):
                item_to_append = FRBRItem(pymarc_object, pymarc_item_field, item_ct, raw_record_id, digital=True)
                item_to_modify = dict_of_items.get(item_to_append.item_library_code)
                if item_to_modify:
                    item_to_modify.merge_item(item_to_append)
                else:
                    dict_of_items[item_to_append.item_library_code] = item_to_append

        return dict_of_items


class FinalItem(object):
    pass


class BnItem(object):
    __slots__ = ['mock_es_id', 'expression_ids', 'item_call_number', 'item_count', 'item_deleted_id',
                 'item_local_bib_id', 'item_local_id', 'item_location', 'item_mat_id', 'item_source',
                 'item_status', 'item_url', 'item_work_id', 'library', 'metadata_original',
                 'metadata_source', 'modification_time', 'phrase_suggest', 'suggest', 'work_ids']

    def __init__(self, bib_object, work, manifestation, expression, buffer):

        # attributes for item_es_index
        self.mock_es_id = str(esid.BN_ITEM_PREFIX + to_single_value(get_values_by_field(bib_object, '001'))[1:])
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
        dict_item = {"_index": "item", "_type": "item", "_id": str(self.mock_es_id),
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


class PolonaItem(object):
    __slots__ = ['mock_es_id', 'expression_ids', 'item_count', 'item_deleted_id',
                 'item_local_bib_id', 'item_local_id', 'item_mat_id', 'item_source',
                 'item_status', 'item_url', 'item_work_id', 'library', 'metadata_original',
                 'metadata_source', 'modification_time', 'phrase_suggest', 'suggest', 'work_ids']

    def __init__(self, bib_object, work, manifestation, expression, buffer):

        # attributes for item_es_index
        self.mock_es_id = str(esid.POLONA_ITEM_PREFIX + to_single_value(get_values_by_field(bib_object, '001'))[1:])
        self.expression_ids = [str(expression.mock_es_id)]
        self.item_count = 1
        self.item_local_bib_id = str(to_single_value(get_values_by_field(bib_object, '001')))
        self.item_local_id = str(to_single_value(get_values_by_field_and_subfield(bib_object, ('856', ['u']))))
        self.item_mat_id = int(manifestation.mock_es_id)
        self.item_url = str(to_single_value(get_values_by_field_and_subfield(bib_object, ('856', ['u']))))
        self.item_work_id = int(work.mock_es_id)
        self.library = {'digital': True, 'name': 'Polona.pl', 'id': 10945}  # hardcoded - always the same
        self.metadata_original = str(uuid4())  # some random fake uuid
        self.metadata_source = 'REFERENCE'
        self.modification_time = '2019-10-11T17:45:21.527'  # fake time
        self.phrase_suggest = ['-']
        self.suggest = ['-']
        self.work_ids = [str(work.mock_es_id)]
        self.write_to_dump_file(buffer)

    def __repr__(self):
        return f'PolonaItem(id={self.mock_es_id}, item_count={self.item_count}, item_url={self.item_url}'

    def serialize_to_es_dump(self):
        dict_item = {"_index": "item", "_type": "item", "_id": str(self.mock_es_id),
                     "_score": 1, "_source":
                         {"expression_ids": self.expression_ids,
                          "item_count": self.item_count,
                          "item_local_bib_id": self.item_local_bib_id,
                          "item_local_id": self.item_local_id,
                          "item_mat_id": self.item_mat_id,
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
    __slots__ = ['mock_es_id', 'expression_ids', 'item_count',
                 'item_local_bib_id', 'item_mat_id', 'item_publication', 'item_service_url', 'item_source',
                 'item_status', 'item_unavailable_count', 'item_url', 'item_work_id', 'library', 'metadata_original',
                 'metadata_source', 'modification_time', 'creation_time', 'phrase_suggest', 'suggest', 'work_ids']

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
        dict_item = {"_index": "item", "_type": "item", "_id": str(self.mock_es_id),
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
