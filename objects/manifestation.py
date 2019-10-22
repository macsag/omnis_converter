from uuid import uuid4

from commons.marc_iso_commons import to_single_value, get_values_by_field_and_subfield, get_values_by_field
from commons.marc_iso_commons import postprocess, truncate_title_proper
from descriptor_resolver.resolve_record import resolve_field_value, resolve_code

from objects.item import BnItem


class Manifestation(object):
    def __init__(self, bib_object, work, expression, buffer, descr_index, code_val_index):
        self.uuid = uuid4()

        # attributes for manifestation_es_index
        self.mock_es_id = str('113' + to_single_value(get_values_by_field(bib_object, '001'))[1:-1])

        self.eForm = resolve_field_value(
                get_values_by_field_and_subfield(bib_object, ('380', ['a'])), descr_index)
        self.expression_ids = [int(expression.mock_es_id)]
        self.items_id = []  # populated after instantiating all the manifestations and mak+ matching
        self.libraries = []  # populated after instantiating all the manifestations and mak+ matching
        self.mat_carrier_type = resolve_code(get_values_by_field_and_subfield(bib_object, ('338', ['b'])),
                                             'carrier_type',
                                             code_val_index)
        self.mat_digital = 'false'
        self.mat_external_id = get_values_by_field_and_subfield(bib_object, ('035', ['a']))
        self.mat_isbn = get_values_by_field_and_subfield(bib_object, ('020', ['a']))
        self.mat_marching_title = ''  # todo
        self.mat_media_type = resolve_code(get_values_by_field_and_subfield(bib_object, ('337', ['b'])),
                                             'media_type',
                                             code_val_index)
        self.mat_nat_bib = []  # todo
        self.mat_nlp_id = to_single_value(get_values_by_field(bib_object, '001'))

        self.mat_physical_info = get_values_by_field(bib_object, '300')
        self.mat_pub_city = get_values_by_field_and_subfield(bib_object, ('260', ['a']))
        self.mat_pub_country =

        self.work_ids = [int(work.mock_es_id)]



        self.mat_pub_info = get_values_by_field(bib_object, '260')
        self.mat_pub_city = get_values_by_field_and_subfield(bib_object, ('260', ['a']))

        self.mat_title_proper = to_single_value(
            postprocess(truncate_title_proper, get_values_by_field_and_subfield(bib_object, ('245', ['a']))))

        self.mat_title_and_resp = get_values_by_field(bib_object, '245')
        self.mat_title_variant = get_values_by_field_and_subfield(bib_object, ('246', ['a', 'b']))
        self.mat_edition = get_values_by_field(bib_object, '250')


        self.bn_items = [self.instantiate_bn_items(bib_object, work, expression, buffer)]
        self.mak_items = self.instantiate_mak_items(bib_object, work)

    def __repr__(self):
        return f'Manifestation(id={self.mock_es_id}, title_and_resp={self.mat_title_and_resp}'

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
                'eForm': list(),
                'expression_ids': list(self.expression_ids),
                'item_ids': [int(i_id) for i_id in self.items_id],
                'libraries': list(),  # todo
                'mat_carrier_type': list(),  # todo
                'mat_digital': self.mat_digital,
                'mat_external_id': list(self.mat_external_id),
                'mat_isbn': self.mat_isbn,
                'mat_matching_title245': '',  # todo
                'mat_media_type': list(),  # todo
                'mat_nat_bib': list(),  # todo
                'mat_nlp_id': self.mat_nlp_id,
                'mat_number_of_pages': '',
                'mat_physical_info': self.mat_physical_info,
                'mat_pub_city': self.mat_pub_city,
                'mat_pub_country': self.mat_pub_country,  # todo
                'mat_pub_date_from': int(),
                'mat_pub_date_single': int(),
                'mat_pub_date_to': int(),
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
