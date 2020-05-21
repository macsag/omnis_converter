import time
from datetime import datetime, timezone
from uuid import uuid4
from hashlib import sha1

from commons.marc_iso_commons import to_single_value, get_values_by_field_and_subfield, get_values_by_field
from commons.marc_iso_commons import postprocess, truncate_title_proper, normalize_publisher

from resolvers.descriptor_resolvers import resolve_ids_to_names, resolve_ids_to_dict_objects
from resolvers.descriptor_resolvers import resolve_ids_to_dict_objects_contributors
from resolvers.codes_resolvers import resolve_institution_codes, resolve_codes_to_dict_objects

from manifestation_matcher.manif_matcher import get_data_for_matching


class FRBRManifestation(object):
    def __init__(self,
                 raw_record_id,
                 pymarc_object):
        self.uuid = str(uuid4())
        self.raw_record_id = raw_record_id
        self.multiwork = False

        # manifestation_match_data
        self.manifestation_match_data = get_data_for_matching(pymarc_object)
        self.manifestation_match_data_sha_1 = self.get_sha_1_of_manifestation_match_data()

        # manifestation attributes
        self.mat_nlp_id = to_single_value(get_values_by_field(pymarc_object, '001'))
        self.mat_external_id = get_values_by_field_and_subfield(pymarc_object, ('035', ['a']))
        self.mat_isbn = get_values_by_field_and_subfield(pymarc_object, ('020', ['a']))
        self.mat_carrier_type = get_values_by_field_and_subfield(pymarc_object, ('338', ['b']))
        self.mat_media_type = get_values_by_field_and_subfield(pymarc_object, ('337', ['b']))
        self.mat_number_of_pages = to_single_value(get_values_by_field_and_subfield(pymarc_object, ('300', ['a'])))
        self.mat_physical_info = get_values_by_field(pymarc_object, '300')
        self.mat_title_and_resp = get_values_by_field(pymarc_object, '245')
        self.mat_title_proper = to_single_value(
            postprocess(truncate_title_proper, get_values_by_field_and_subfield(pymarc_object, ('245', ['a', 'b']))))
        self.mat_title_variant = get_values_by_field_and_subfield(pymarc_object, ('246', ['a', 'b']))
        self.mat_edition = get_values_by_field(pymarc_object, '250')
        self.mat_pub_city = get_values_by_field_and_subfield(pymarc_object, ('260', ['a']))
        self.mat_pub_info = get_values_by_field(pymarc_object, '260')

        self.mat_contributor = {}
        self.get_mat_contributors(pymarc_object)

        self.mat_pub_country = []
        self.get_pub_country(pymarc_object)

        self.mat_publisher = []
        self.get_publishers_all(pymarc_object)

        self.mat_publisher_uniform = []
        self.get_uniform_publishers(pymarc_object)

        self.mat_pub_date_from = None
        self.mat_pub_date_single = None
        self.mat_pub_date_to = None
        self.get_mat_pub_dates(pymarc_object)

        self.mat_nat_bib = []  # TODO
        self.mat_note = []  # TODO
        self.mat_title_other_info = []  # TODO
        self.metadata_original = ''  # TODO
        self.suggest = [self.mat_title_proper]  # TODO

        self.popularity_join = 'owner'

        # items
        self.items_by_institution_code = {}

    def __repr__(self):
        return f'FRBRManifestation(id={self.uuid}, raw_record_id={self.raw_record_id})'

    def get_sha_1_of_manifestation_match_data(self):
        manifestation_match_data_byte_array = bytearray()
        mmd = self.manifestation_match_data
        manifestation_match_data_byte_array.extend(mmd.ldr_67.encode('utf-8'))
        manifestation_match_data_byte_array.extend(mmd.val_008_0614.encode('utf-8'))
        manifestation_match_data_byte_array.extend(repr(sorted(mmd.isbn_020_az)).encode('utf-8'))
        manifestation_match_data_byte_array.extend(mmd.title_245.encode('utf-8'))
        manifestation_match_data_byte_array.extend(mmd.title_245_no_offset.encode('utf-8'))
        manifestation_match_data_byte_array.extend(mmd.title_245_with_offset.encode('utf-8'))
        manifestation_match_data_byte_array.extend(repr(sorted(mmd.titles_490)).encode('utf-8'))
        manifestation_match_data_byte_array.extend(mmd.numbers_from_title_245.encode('utf-8'))
        manifestation_match_data_byte_array.extend(mmd.place_pub_260_a_first_word.encode('utf-8'))
        manifestation_match_data_byte_array.extend(str(mmd.num_of_pages_300_a).encode('utf-8'))
        manifestation_match_data_byte_array.extend(str(mmd.b_format).encode('utf-8'))
        manifestation_match_data_byte_array.extend(repr(sorted(mmd.edition)).encode('utf-8'))

        return sha1(manifestation_match_data_byte_array).hexdigest()

    def get_mat_contributors(self, pymarc_object):
        dict_val_7xx = {}

        list_700_fields = pymarc_object.get_fields('700')
        if list_700_fields:
            for field in list_700_fields:
                e_subflds = field.get_subfields('e')
                if e_subflds and 'Autor' not in e_subflds and 'Autor domniemany' not in e_subflds\
                        and 'Wywiad' not in e_subflds:
                    for e_sub in e_subflds:
                        dict_val_7xx.setdefault(e_sub, set()).add(
                            ' '.join(subfld for subfld in field.get_subfields('0')))

        list_710_fields = pymarc_object.get_fields('710')
        if list_710_fields:
            for field in list_710_fields:
                e_subflds = field.get_subfields('e')
                if e_subflds and 'Autor' not in e_subflds and 'Autor domniemany' not in e_subflds \
                        and 'Wywiad' not in e_subflds:
                    for e_sub in e_subflds:
                        dict_val_7xx.setdefault(e_sub, set()).add(
                            ' '.join(subfld for subfld in field.get_subfields('0')))

        list_711_fields = pymarc_object.get_fields('711')
        if list_711_fields:
            for field in list_711_fields:
                j_subflds = field.get_subfields('j')
                if j_subflds and 'Autor' not in j_subflds and 'Autor domniemany' not in j_subflds \
                        and 'Wywiad' not in j_subflds:
                    for j_sub in j_subflds:
                        dict_val_7xx.setdefault(j_sub, set()).add(
                            ' '.join(subfld for subfld in field.get_subfields('0')))

        self.mat_contributor = dict_val_7xx

    def get_pub_country(self, pymarc_object):
        pub_008 = get_values_by_field(pymarc_object, '008')[0][15:18]
        pub_008 = pub_008[:-1] if pub_008[-1] == ' ' else pub_008
        pub_044_a = get_values_by_field_and_subfield(pymarc_object, ('044', ['a']))

        country_codes = set()

        country_codes.add(pub_008)
        country_codes.update(pub_044_a)

        self.mat_pub_country.extend(list(country_codes))

    def get_publishers_all(self, pymarc_object):
        publishers_raw_list = get_values_by_field_and_subfield(pymarc_object, ('260', ['b']))
        publishers_list = postprocess(normalize_publisher, publishers_raw_list)

        self.mat_publisher.extend(publishers_list)

    def get_uniform_publishers(self, pymarc_object):
        list_val_710_0 = set()
        list_710_fields = pymarc_object.get_fields('710')
        if list_710_fields:
            for field in list_710_fields:
                e_subflds = field.get_subfields('e')
                subflds_4 = field.get_subfields('4')
                if e_subflds or subflds_4:
                    if 'Wyd.' in e_subflds or 'Wydawca' in e_subflds or 'pbl' in subflds_4:
                        list_val_710_0.add(' '.join(subfld for subfld in field.get_subfields('0')))

        self.mat_publisher_uniform.extend(list(list_val_710_0))

    def get_mat_pub_dates(self, pymarc_object):
        v_008_06 = get_values_by_field(pymarc_object, '008')[0][6]
        if v_008_06 in ['r', 's', 'p', 't']:
            v_008_0710 = get_values_by_field(pymarc_object,
                                             '008')[0][7:11].replace('u', '0').replace(' ', '0').replace('X', '0')
            try:
                self.mat_pub_date_single = int(v_008_0710)
            except ValueError:
                pass
        else:
            v_008_0710 = get_values_by_field(pymarc_object,
                                             '008')[0][7:11].replace('u', '0').replace(' ', '0').replace('X', '0')
            v_008_1114 = get_values_by_field(pymarc_object,
                                             '008')[0][11:15].replace('u', '0').replace(' ', '0').replace('X', '0')
            try:
                self.mat_pub_date_from = int(v_008_0710)
                self.mat_pub_date_to = int(v_008_1114)
            except ValueError:
                pass


class FinalManifestation(object):
    def __init__(self,
                 work_ids: list,
                 expression_ids: list,
                 frbr_manifestation: FRBRManifestation):

        self.work_ids = work_ids
        self.expression_ids = expression_ids
        self.item_ids = set()
        self.libraries = set()

        # TODO it would be nice to check, what's really used in front-end
        self.work_creator = set()
        self.work_creators = set()

        self.mat_digital = False
        self.stat_digital = False
        self.stat_digital_library_count = 0
        self.stat_item_count = 0
        self.stat_library_count = 0
        self.stat_public_domain = False

        # modification_time
        self.modificationTime = datetime.fromtimestamp(time.time(), tz=timezone.utc).isoformat()

        self.frbr_manifestation = frbr_manifestation

    def collect_data_for_resolver_cache(self,
                                        resolver_cache: dict):

        institution_codes_related_attributes = ['libraries']

        for attribute in institution_codes_related_attributes:
            for institution_code in getattr(self, attribute):
                resolver_cache.setdefault('institution_codes', {}).setdefault(institution_code, None)

        descriptor_related_attributes_frbr_manifestation = ['mat_publisher_uniform']

        for attribute in descriptor_related_attributes_frbr_manifestation:
            for descr_nlp_id in getattr(self.frbr_manifestation, attribute):
                resolver_cache.setdefault('descriptors', {}).setdefault(descr_nlp_id, None)

        # special mat_contributor data collecting
        for contribution_code, contributors_set in self.frbr_manifestation.mat_contributor.items():
            resolver_cache.setdefault('contribution', {}).setdefault(contribution_code, None)
            for descr_nlp_id in contributors_set:
                resolver_cache.setdefault('descriptors', {}).setdefault(descr_nlp_id, None)

        descriptor_related_attributes = ['work_creator', 'work_creators']

        for attribute in descriptor_related_attributes:
            for descr_nlp_id in getattr(self, attribute):
                resolver_cache.setdefault('descriptors', {}).setdefault(descr_nlp_id, None)

        pub_country_related_attributes = ['mat_pub_country']

        for attribute in pub_country_related_attributes:
            for country_code in getattr(self.frbr_manifestation, attribute):
                resolver_cache.setdefault('country', {}).setdefault(country_code, None)

        carrier_type_related_attributes = ['mat_carrier_type']

        for attribute in carrier_type_related_attributes:
            for carrier_type in getattr(self.frbr_manifestation, attribute):
                resolver_cache.setdefault('carrier_type', {}).setdefault(carrier_type, None)

        media_type_related_attributes = ['mat_media_type']

        for attribute in media_type_related_attributes:
            for media_type in getattr(self.frbr_manifestation, attribute):
                resolver_cache.setdefault('media_type', {}).setdefault(media_type, None)

    def resolve_libraries_and_calculate_libraries_stats(self, resolver_cache):
        self.libraries = resolve_institution_codes(list(self.libraries), resolver_cache)
        self.stat_library_count = len(self.libraries)

        for lib in self.libraries:
            digital = lib.get('digital')
            if digital:
                self.stat_digital = True
                self.stat_digital_library_count += 1

    def prepare_for_indexing_in_es(self, resolver_cache, timestamp):
        self.resolve_libraries_and_calculate_libraries_stats(resolver_cache)
        dict_manifestation = self.resolve_and_serialize_manifestation_for_bulk_request(resolver_cache)
        request = {"index": {"_index": "materialization",
                             "_type": "materialization",
                             "_id": self.frbr_manifestation.uuid,
                             "version": timestamp,
                             "version_type": "external"}}

        bulk_list = [request, dict_manifestation]

        return bulk_list

    def resolve_and_serialize_manifestation_for_bulk_request(self, resolver_cache):
        dict_manifestation = {'expression_ids': list(self.expression_ids),
                              'item_ids': list(self.item_ids),
                              'libraries': self.libraries,
                              'mat_carrier_type': resolve_codes_to_dict_objects(
                                  self.frbr_manifestation.mat_carrier_type, 'carrier_type', resolver_cache),
                              'mat_contributor': resolve_ids_to_dict_objects_contributors(
                                  self.frbr_manifestation.mat_contributor,
                                  resolver_cache),
                              'mat_digital': self.mat_digital,
                              'mat_edition': self.frbr_manifestation.mat_edition,
                              'mat_external_id': self.frbr_manifestation.mat_external_id,
                              'mat_isbn': self.frbr_manifestation.mat_isbn,
                              'mat_media_type': resolve_codes_to_dict_objects(
                                  self.frbr_manifestation.mat_media_type, 'media_type', resolver_cache),
                              'mat_nat_bib': self.frbr_manifestation.mat_nat_bib,
                              'mat_nlp_id': self.frbr_manifestation.mat_nlp_id,
                              'mat_note': self.frbr_manifestation.mat_note,
                              'mat_number_of_pages': self.frbr_manifestation.mat_number_of_pages,
                              'mat_physical_info': self.frbr_manifestation.mat_physical_info,
                              'mat_pub_city': self.frbr_manifestation.mat_pub_city,
                              'mat_pub_country': resolve_codes_to_dict_objects(
                                  self.frbr_manifestation.mat_pub_country, 'country', resolver_cache),
                              'mat_pub_date_from': self.frbr_manifestation.mat_pub_date_from,
                              'mat_pub_date_single': self.frbr_manifestation.mat_pub_date_single,
                              'mat_pub_date_to': self.frbr_manifestation.mat_pub_date_to,
                              'mat_pub_info': self.frbr_manifestation.mat_pub_info,
                              'mat_publisher': self.frbr_manifestation.mat_publisher,
                              'mat_publisher_uniform': resolve_ids_to_dict_objects(
                                  self.frbr_manifestation.mat_publisher_uniform, resolver_cache),
                              'mat_title_and_resp': self.frbr_manifestation.mat_title_and_resp,
                              'mat_title_proper': self.frbr_manifestation.mat_title_proper,
                              'metadata_original': self.frbr_manifestation.metadata_original,
                              'modificationTime': self.modificationTime,
                              'popularity-join': self.frbr_manifestation.popularity_join,
                              'stat_digital': self.stat_digital,
                              'stat_digital_library_count': self.stat_digital_library_count,
                              'stat_item_count': self.stat_item_count,
                              'stat_library_count': self.stat_library_count,
                              'stat_public_domain': self.stat_public_domain,
                              'work_creator': resolve_ids_to_names(
                                  self.work_creator, resolver_cache),
                              'work_creators': resolve_ids_to_names(
                                  self.work_creators, resolver_cache),
                              'work_ids': self.work_ids}

        return dict_manifestation





# class Manifestation(object):
#     def __init__(self, bib_object, work, expression, buffer, descr_index, code_val_index):
#
#         self.eForm = only_values(resolve_field_value(
#                 get_values_by_field_and_subfield(bib_object, ('380', ['a'])), descr_index))
#
#         self.get_work_creators(work)

#     def get_work_creators(self, work):
#         if work.main_creator:
#             self.work_creator = only_values(work.main_creator)
#             self.work_creators = only_values(work.main_creator)
#         if work.other_creator:
#             self.work_creator = only_values(work.other_creator)
#             self.work_creators = only_values(work.other_creator)
