from uuid import uuid4
import json
import pprint

from commons.marc_iso_commons import to_single_value, get_values_by_field_and_subfield, get_values_by_field
from commons.marc_iso_commons import postprocess, truncate_title_proper, normalize_publisher
from commons.marc_iso_commons import serialize_to_jsonl_descr
from descriptor_resolver.resolve_record import resolve_field_value, resolve_code, resolve_code_and_serialize, only_values

from objects.item import BnItem


class Manifestation(object):
    def __init__(self, bib_object, work, expression, buffer, descr_index, code_val_index):
        self.uuid = uuid4()

        # attributes for manifestation_es_index
        self.mock_es_id = str('113' + to_single_value(get_values_by_field(bib_object, '001'))[1:-1])

        self.eForm = only_values(resolve_field_value(
                get_values_by_field_and_subfield(bib_object, ('380', ['a'])), descr_index))
        self.expression_ids = [int(expression.mock_es_id)]
        self.items_id = []  # populated after instantiating all the manifestations and mak+ matching
        self.libraries = []  # populated after instantiating all the manifestations and mak+ matching
        self.mat_carrier_type = resolve_code_and_serialize(get_values_by_field_and_subfield(bib_object, ('338', ['b'])),
                                                           'carrier_type_dict',
                                                           code_val_index)
        self.mat_contributor = ''  # todo
        self.mat_digital = 'false'
        self.mat_edition = get_values_by_field(bib_object, '250')
        self.mat_external_id = get_values_by_field_and_subfield(bib_object, ('035', ['a']))
        self.mat_isbn = get_values_by_field_and_subfield(bib_object, ('020', ['a']))
        self.mat_matching_title = ''  # todo
        self.mat_material_type = ''  # todo
        self.mat_media_type = resolve_code_and_serialize(get_values_by_field_and_subfield(bib_object, ('337', ['b'])),
                                                         'media_type_dict',
                                                         code_val_index)
        self.mat_nat_bib = []  # todo
        self.mat_nlp_id = to_single_value(get_values_by_field(bib_object, '001'))
        self.mat_note = []  # todo
        self.mat_number_of_pages = to_single_value(get_values_by_field_and_subfield(bib_object, ('300', ['a'])))
        self.mat_physical_info = get_values_by_field(bib_object, '300')
        self.mat_pub_city = get_values_by_field_and_subfield(bib_object, ('260', ['a']))
        self.mat_pub_country = []
        self.get_pub_country(bib_object, code_val_index)
        self.mat_pub_date_from = None
        self.mat_pub_date_single = None
        self.mat_pub_date_to = None
        self.get_mat_pub_dates(bib_object)
        self.mat_pub_info = get_values_by_field(bib_object, '260')
        self.mat_publisher = []
        self.get_publishers_all(bib_object)
        self.mat_publisher_uniform = []
        self.get_uniform_publishers(bib_object, descr_index)
        self.mat_title_and_resp = get_values_by_field(bib_object, '245')
        self.mat_title_other_info = []  # todo
        self.mat_title_proper = to_single_value(
            postprocess(truncate_title_proper, get_values_by_field_and_subfield(bib_object, ('245', ['a']))))
        self.mat_title_variant = get_values_by_field_and_subfield(bib_object, ('246', ['a', 'b']))
        self.metadata_original = str(uuid4())
        self.metadata_source = 'REFERENCE'
        self.modificationTime = "2019-10-01T13:34:23.580"
        self.phrase_suggest = []  # todo
        self.popularity_join = "owner"
        self.stat_digital = "false"
        self.stat_digital_library_count = 0
        self.stat_item_count = 0  # todo
        self.stat_library_count = 0  # todo
        self.stat_public_domain = 0
        self.suggest = []  # todo
        self.work_creator = only_values(work.main_creator)
        self.work_creators = only_values(work.main_creator)
        self.work_ids = [int(work.mock_es_id)]

        self.bn_items = [self.instantiate_bn_items(bib_object, work, expression, buffer)]
        self.mak_items = self.instantiate_mak_items(bib_object, work)

    def __repr__(self):
        return f'Manifestation(id={self.mock_es_id}, title_and_resp={self.mat_title_and_resp}'

    def get_pub_country(self, bib_object, code_val_index):
        pub_008 = get_values_by_field(bib_object, '008')[0][15:18]
        pub_008 = pub_008[:-1] if pub_008[-1] == ' ' else pub_008
        pub_044_a = get_values_by_field_and_subfield(bib_object, ('044', ['a']))

        country_codes = set()

        country_codes.add(pub_008)
        country_codes.update(pub_044_a)

        self.mat_pub_country.extend(resolve_code_and_serialize(list(country_codes), 'country_dict', code_val_index))

    def get_mat_pub_dates(self, bib_object):
        v_008_06 = get_values_by_field(bib_object, '008')[0][6]
        if v_008_06 in ['r', 's', 'p', 't']:
            v_008_0710 = get_values_by_field(bib_object, '008')[0][7:11].replace('u', '0')
            self.mat_pub_date_single = int(v_008_0710)
        else:
            v_008_0710 = get_values_by_field(bib_object, '008')[0][7:11].replace('u', '0')
            v_008_1114 = get_values_by_field(bib_object, '008')[0][11:15].replace('u', '0')
            self.mat_pub_date_from = int(v_008_0710)
            self.mat_pub_date_to = int(v_008_1114)

    def get_publishers_all(self, bib_object):
        pl = get_values_by_field_and_subfield(bib_object, ('260', ['b']))
        publishers_list = postprocess(normalize_publisher, get_values_by_field_and_subfield(bib_object, ('260', ['b'])))

        self.mat_publisher = publishers_list

    def get_uniform_publishers(self, bib_object, descr_index):
        list_val_710abcdn = set()
        list_710_fields = bib_object.get_fields('710')
        if list_710_fields:
            for field in list_710_fields:
                e_subflds = field.get_subfields('e')
                subflds_4 = field.get_subfields('4')
                if e_subflds or subflds_4:
                    if 'Wyd.' in e_subflds or 'Wydawca' in e_subflds or 'pbl' in subflds_4:
                        list_val_710abcdn.add(' '.join(subfld for subfld in field.get_subfields('a', 'b', 'c', 'd', 'n')))

        resolved_list_710 = resolve_field_value(list(list_val_710abcdn), descr_index)
        serialized = serialize_to_jsonl_descr(resolved_list_710)
        self.mat_publisher_uniform.extend(serialized)

    def get_resolve_and_serialize_libraries(self, lib_index):
        for items_list in (self.bn_items, self.mak_items):
            if items_list:
                for item in items_list:
                    id_to_get = item.library['id']
                    if int(id_to_get) == 10947:
                        self.libraries.append({'digital': 'false',
                                               'localization': {"lon": 21.0055165, "lat": 52.2140166},
                                               'country': 'Polska',
                                               'province': 'mazowieckie',
                                               'city': 'Warszawa',
                                               'name': 'Biblioteka Narodowa',
                                               'id': 10947})
                    lib = lib_index.get(id_to_get)
                    if lib:
                        self.libraries.append(lib.get_serialized())

    def instantiate_bn_items(self, bib_object, work, expression, buffer):
        list_852_fields = bib_object.get_fields('852')
        if list_852_fields:
            i_mock_es_id = str('114' + to_single_value(get_values_by_field(bib_object, '001'))[1:-1])
            i = BnItem(bib_object, work, self, expression, buffer)
            self.items_id.append(i_mock_es_id)
            return i

    def instantiate_mak_items(self, manifestation, work):
        pass

    def serialize_manifestation_for_es_dump(self):
        dict_manifestation = {"_index": "materialization", "_type": "materialization", "_id": self.mock_es_id,
                              "_score": 1, "_source": {
                'eForm': self.eForm,
                'expression_ids': list(self.expression_ids),
                'item_ids': [int(i_id) for i_id in self.items_id],
                'libraries': self.libraries,
                'mat_carrier_type': list(self.mat_carrier_type),
                'mat_contributor': self.mat_contributor,
                'mat_digital': self.mat_digital,
                'mat_edition': self.mat_edition,
                'mat_external_id': list(self.mat_external_id),
                'mat_isbn': self.mat_isbn,
                'mat_matching_title245': '',  # todo
                'mat_material_type': self.mat_material_type,
                'mat_media_type': list(self.mat_media_type),
                'mat_nat_bib': list(),  # todo
                'mat_nlp_id': self.mat_nlp_id,
                'mat_note': self.mat_note,
                'mat_number_of_pages': self.mat_number_of_pages,
                'mat_physical_info': self.mat_physical_info,
                'mat_pub_city': self.mat_pub_city,
                'mat_pub_country': list(self.mat_pub_country),
                'mat_pub_date_from': self.mat_pub_date_from,
                'mat_pub_date_single': self.mat_pub_date_single,
                'mat_pub_date_to': self.mat_pub_date_to,
                'mat_pub_info': self.mat_pub_info,
                'mat_publiher': self.mat_publisher,
                'mat_publisher_uniform': self.mat_publisher_uniform,
                'mat_title_and_resp': self.mat_title_and_resp,
                'mat_title_proper': self.mat_title_proper,
                'metadata_original': self.metadata_original,
                'metadata_source': self.metadata_source,
                'modificationTime': self.modificationTime,
                'phrase_suggest': self.phrase_suggest,
                'popularity-join': self.popularity_join,
                'stat_digital': self.stat_digital,
                'stat_digital_library_count': self.stat_digital_library_count,
                'stat_item_count': self.stat_item_count,
                'stat_library_count': self.stat_library_count,
                'stat_public_domain': self.stat_public_domain,
                'suggest': self.suggest,
                'work_creator': self.work_creator,
                'work_creators': self.work_creators,
                'work_ids': self.work_ids
            }}

        json_manifestation = json.dumps(dict_manifestation, ensure_ascii=False)
        pp = pprint.PrettyPrinter()
        pp.pprint(dict_manifestation)
