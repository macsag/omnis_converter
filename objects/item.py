from uuid import uuid4

from pymarc import Record, Field

from commons.marc_iso_commons import to_single_value, get_values_by_field_and_subfield
from objects.helper_objects import ObjCounter


class FRBRItem(object):
    __slots__ = ['uuid', 'item_raw_record_id', 'item_url',
                 'item_local_bib_id', 'item_count', 'item_count_by_raw_record_id']

    def __init__(self,
                 pymarc_object: Record,
                 pymarc_item_field: Field,
                 item_ct: dict,
                 raw_record_id: str,
                 digital: bool = False):

        self.uuid = str(uuid4())

        # item attributes
        self.item_raw_record_id = None
        self.item_local_bib_id = None
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

        # item_local_bib_id
        if item_ct[base_path]['item_local_bib_id']['from_ct']:
            self.item_local_bib_id = item_ct[base_path]['item_local_bib_id']['from_ct']
        else:
            item_local_bib_id_value = self.get_attribute_value('item_local_bib_id',
                                                               pymarc_object,
                                                               pymarc_item_field,
                                                               item_ct,
                                                               digital)

            self.item_local_bib_id = item_local_bib_id_value

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

            self.item_url = item_url_value

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
                item_to_modify = dict_of_items.get(item_to_append.item_local_bib_id)
                if item_to_modify:
                    item_to_modify.merge_item(item_to_append)
                else:
                    dict_of_items[item_to_append.item_local_bib_id] = item_to_append

        if item_ct['digital_item']:
            for pymarc_item_field in pymarc_object.get_fields(item_ct['digital_item']['item_field_tag']):
                item_to_append = FRBRItem(pymarc_object, pymarc_item_field, item_ct, raw_record_id, digital=True)
                item_to_modify = dict_of_items.get(item_to_append.item_local_bib_id)
                if item_to_modify:
                    item_to_modify.merge_item(item_to_append)
                else:
                    dict_of_items[item_to_append.item_local_bib_id] = item_to_append

        return dict_of_items


class FinalItem(object):
    """
    FinalItem class represents final item record, which - after serialization to JSON - is sent to indexer.
    It is a wrapper around FRBRItem class.
    Instances of this class live short time in FinalConverter and die after sending message to indexer.
    """
    __slots__ = ['work_ids',
                 'expression_ids',
                 'item_mat_id',
                 'library',
                 'frbr_item']

    def __init__(self,
                 work_ids,
                 expression_ids,
                 item_mat_id,
                 frbr_item):

        self.work_ids = work_ids
        self.expression_ids = expression_ids
        self.item_mat_id = item_mat_id
        self.frbr_item = frbr_item
        self.library = frbr_item.item_local_bib_id

    def collect_data_for_resolver_cache(self, resolver_cache):
        resolver_cache.setdefault('institution_codes',
                                  {}).setdefault(self.library, None)

    def resolve_record(self, resolver_cache: dict) -> None:
        institution_codes = resolver_cache.get('institution_codes')
        if institution_codes:
            library_data = resolver_cache.get(self.frbr_item.item_local_bib_id)
            if library_data:
                self.library = {'digital': library_data['digital'],
                                'name': library_data['name'],
                                'id': library_data['id']}

    def serialize_item_for_bulk_request(self):
        dict_item = {"work_ids": self.work_ids,
                     "expression_ids": self.expression_ids,
                     "item_mat_id": self.item_mat_id,
                     "item_count": self.frbr_item.item_count.count,
                     "item_url": self.frbr_item.item_url,
                     "library": self.frbr_item.library}

        return dict_item

    def prepare_for_indexing_in_es(self,
                                   resolver_cache: dict,
                                   timestamp: int) -> list:

        self.resolve_record(resolver_cache)
        dict_item = self.serialize_item_for_bulk_request()

        request = {"index": {"_index": "item",
                             "_type": "item",
                             "_id": self.frbr_item.uuid,
                             "version": timestamp,
                             "version_type": "external"}}

        bulk_list = [request, dict_item]

        return bulk_list
