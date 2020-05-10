from uuid import uuid4
import json

from commons.marc_iso_commons import get_values_by_field_and_subfield, get_values_by_field, postprocess
from commons.marc_iso_commons import read_marc_from_binary, is_dbn, serialize_to_list_of_values
from commons.marc_iso_commons import serialize_to_jsonl_descr, serialize_to_jsonl_descr_creator, normalize_publisher
from commons.marc_iso_commons import select_number_of_creators
from commons.json_writer import write_to_json
from commons.validators import is_number_of_1xx_fields_valid
from commons.normalization import prepare_name_for_indexing, normalize_title

from descriptor_resolver.resolve_record import resolve_field_value, only_values
from descriptor_resolver.resolve_record import resolve_code, resolve_code_and_serialize

from objects.frbr_cluster import FRBRCluster
from objects.helper_objects import ObjCounter

import config.mock_es_id_prefixes as esid


class FinalWork(object):
    def __init__(self,
                 frbr_cluster: FRBRCluster):
        self.frbr_cluster = frbr_cluster

        # creators for record real metadata
        self.main_creator_real = set()
        self.other_creator_real = set()

        self.titles240 = set()
        self.titles245 = {}
        self.titles245p = set()
        self.titles246_title_orig = {}
        self.titles246_title_other = {}

        # helper dict of creators for control of nonfiling characters
        self.title_with_nonf_chars = {}

        self.language_codes = set()
        self.language_of_orig_codes = {}
        self.language_orig = ''
        self.language_orig_obj = None

        self.pub_country_codes = set()

        self.libraries = []

        # search indexes data
        self.search_adress = set()
        self.search_authors = set()
        self.search_identity = set()
        self.search_title = set()
        self.search_subject = set()
        self.search_formal = set()
        self.search_form = set()
        self.search_note = set()

        # filters data
        self.filter_creator = set()
        self.filter_cultural_group = []
        self.filter_form = []
        self.filter_lang = []
        self.filter_lang_orig = []
        self.filter_nat_bib_code = []
        self.filter_nat_bib_year = []
        self.filter_pub_country = []
        self.filter_pub_date = set()
        self.filter_publisher = set()
        self.filter_publisher_uniform = set()
        self.filter_subject = []
        self.filter_subject_place = []
        self.filter_subject_time = []
        self.filter_time_created = []

        # presentation data
        self.work_presentation_main_creator = []
        self.work_presentation_another_creator = []

        # sort data
        self.sort_author = []

        # work data
        self.work_title_pref = ''
        self.work_title_of_orig_pref = ''
        self.work_title_alt = set()
        self.work_title_of_orig_alt = set()
        self.work_title_index = []

        self.work_main_creator = []
        self.work_other_creator = []
        self.work_other_creator_index = []

        self.work_udc = set()
        self.work_time_created = []
        self.work_form = set()
        self.work_genre = set()
        self.work_cultural_group = set()

        # subject data
        self.work_subject_person = set()
        self.work_subject_corporate_body = set()
        self.work_subject_event = set()
        self.work_subject = set()
        self.work_subject_place = set()
        self.work_subject_time = []
        self.work_subject_domain = set()
        self.work_subject_work = []

        # invariable data
        self.popularity_join = "owner"
        self.stat_digital = False
        self.work_publisher_work = False

        # helper data
        self.work_main_creator_index = []

        # stats

        self.stat_item_count = 0
        self.stat_digital_library_count = 0
        self.stat_library_count = 0
        self.stat_materialization_count = 0
        self.stat_public_domain = False

        # children ids
        self.expression_ids = []
        self.materialization_ids = []
        self.item_ids = []

        # suggestions data
        self.suggest = []
        self.phrase_suggest = []

    def __repr__(self):
        return f'FinalWork(id={self.frbr_cluster.uuid}, title_pref={self.work_title_pref})'

    def join_and_calculate_pure_work_attributes(self,
                                                resolver_cache: dict):
        for work_data_object in self.frbr_cluster.work_data_by_raw_record_id.values():
            # join data without relations to descriptors
            self.join_titles(work_data_object)
            self.join_language_of_orig_codes(work_data_object)
            self.work_udc.update(work_data_object.work_udc)

            # join and calculate subject data
            self.work_subject_person.update(work_data_object.work_subject_person)
            self.work_subject_corporate_body.update(work_data_object.work_subject_corporate_body)
            self.work_subject_event.update(work_data_object.work_subject_event)
            self.work_subject.update(work_data_object.work_subject)
            self.work_subject_place.update(work_data_object.work_subject_place)
            self.work_subject_time = [] #TODO
            self.work_subject_work = [] #TODO

            # join and calculate other descriptor related data
            self.work_genre.update(work_data_object.work_genre)
            self.work_subject_domain.update(work_data_object.work_subject_domain)
            self.work_form.update(work_data_object.work_form)
            self.work_cultural_group.update(work_data_object.work_cultural_group)

        # now, when we have all raw attributes joined, we can calculate some atrributes
        # order of calculations have to be retained (e.g. title_of_orig_pref needs lang_orig to be calculated first)
        self.calculate_lang_orig()
        self.calculate_title_of_orig_pref()
        self.calculate_title_pref()
        self.get_titles_of_orig_alt()
        self.get_titles_alt()

    def join_titles(self, work_data_object):
        for title_lang, title_dict in work_data_object.titles245.items():
            for title, title_count in title_dict.items():
                self.titles245.setdefault(title_lang,
                                          {}).setdefault(title,
                                                         ObjCounter()).add(title_count.count)

        for title_lang, title_dict in work_data_object.titles246_title_orig.items():
            for title, title_count in title_dict.items():
                self.titles246_title_orig.setdefault(title_lang,
                                                     {}).setdefault(title,
                                                                    ObjCounter()).add(title_count.count)

        for title, title_count in work_data_object.titles246_title_other.items():
            self.titles246_title_other.setdefault(title, ObjCounter()).add(title_count.count)

        self.titles240.update(work_data_object.titles240)

        for title, title_full in work_data_object.title_with_nonf_chars.items():
            self.title_with_nonf_chars.setdefault(title, set()).update(title_full)

    def join_language_of_orig_codes(self, work_data_object):
        for lang_code, lang_count in work_data_object.language_of_orig_codes.items():
            self.language_of_orig_codes.setdefault(lang_code, ObjCounter()).add(lang_count.count)

    def get_titles_of_orig_alt(self):
        for title_dict in self.titles246_title_orig.values():
            for title in title_dict.keys():
                if title != self.work_title_of_orig_pref:
                    self.work_title_of_orig_alt.add(title)

    def get_titles_alt(self):
        for title in self.titles246_title_other.keys():
            if title != self.work_title_of_orig_pref and title != self.work_title_pref:
                self.work_title_alt.add(title)
        for title_dict in self.titles245.values():
            for title in title_dict.keys():
                if title != self.work_title_of_orig_pref and title != self.work_title_pref:
                    self.work_title_alt.add(list(self.title_with_nonf_chars.get(title))[0])

    def calculate_title_pref(self):
        polish_titles = self.titles245.get('pol')
        if polish_titles:
            polish_titles_sorted_by_frequency = sorted(polish_titles.items(), key=lambda x: x[1].count)
            self.work_title_pref = list(self.title_with_nonf_chars.get(polish_titles_sorted_by_frequency[0][0]))[0]
        else:
            self.work_title_pref = self.work_title_of_orig_pref

    def calculate_title_of_orig_pref(self):
        orig_titles = self.titles246_title_orig
        if orig_titles:
            orig_titles_flattened = []
            for title_dict in orig_titles.values():
                for title, count in title_dict.items():
                    orig_titles_flattened.append((title, count))
            orig_titles_sorted_by_frequency = sorted(orig_titles_flattened, key=lambda x: x[1].count)
            self.work_title_of_orig_pref = orig_titles_sorted_by_frequency[0][0]
        else:
            orig_titles_from_245 = self.titles245.get(self.language_orig)
            if orig_titles_from_245:
                orig_titles_from_245_sorted_by_frequency = sorted(orig_titles_from_245.items(),
                                                                  key=lambda x: x[1].count)
                self.work_title_of_orig_pref = list(self.title_with_nonf_chars.get(
                    orig_titles_from_245_sorted_by_frequency[0][0]))[0]
            else:
                for title_dict in self.titles245.values():
                    for title in title_dict.keys():
                        self.work_title_of_orig_pref = list(self.title_with_nonf_chars.get(title))[0]
                        break

    def get_language_of_original(self, bib_object):
        lang_008 = get_values_by_field(bib_object, '008')[0][35:38]
        lang_041_h = get_values_by_field_and_subfield(bib_object, ('041', ['h']))

        if lang_008 and not lang_041_h:
            self.language_of_orig_codes.setdefault(lang_008, ObjCounter()).add(1)
        if len(lang_041_h) == 1:
            self.language_of_orig_codes.setdefault(lang_041_h[0], ObjCounter()).add(1)

    def calculate_lang_orig(self):
        try:
            self.language_orig = sorted(self.language_of_orig_codes.items(), key=lambda x: x[1].count)[0][0]
        except IndexError:
            self.language_orig = 'und'

    @staticmethod
    def get_creators_from_manif(bib_object, descr_index):
        list_val_700abcd = set()
        list_val_710abcdn = set()
        list_val_711abcdn = set()

        list_700_fields = bib_object.get_fields('700')
        if list_700_fields:
            for field in list_700_fields:
                e_subflds = field.get_subfields('e')
                if e_subflds:
                    if len(e_subflds) == 1 and e_subflds[0] not in ['Wyd.', 'Wydawca']:
                        list_val_700abcd.add(' '.join(subfld for subfld in field.get_subfields('a', 'b', 'c', 'd')))
                    else:
                        list_val_700abcd.add(' '.join(subfld for subfld in field.get_subfields('a', 'b', 'c', 'd')))

        resolved_list_700 = resolve_field_value(list(list_val_700abcd), descr_index)
        only_values_from_list_700 = only_values(resolved_list_700)

        list_710_fields = bib_object.get_fields('710')
        if list_710_fields:
            for field in list_710_fields:
                e_subflds = field.get_subfields('e')
                if e_subflds:
                    if len(e_subflds) == 1 and e_subflds[0] not in ['Wyd.', 'Wydawca']:
                        list_val_710abcdn.add(' '.join(subfld for subfld in field.get_subfields('a', 'b', 'c', 'd', 'n')))

        resolved_list_710 = resolve_field_value(list(list_val_710abcdn), descr_index)
        only_values_from_list_710 = only_values(resolved_list_710)

        list_711_fields = bib_object.get_fields('711')
        if list_711_fields:
            for field in list_711_fields:
                j_subflds = field.get_subfields('j')
                if j_subflds:
                    if len(j_subflds) == 1 and j_subflds[0] not in ['Wyd.', 'Wydawca']:
                        list_val_711abcdn.add(
                            ' '.join(subfld for subfld in field.get_subfields('a', 'b', 'c', 'd', 'n')))

        resolved_list_711 = resolve_field_value(list(list_val_711abcdn), descr_index)
        only_values_from_list_711 = only_values(resolved_list_711)

        to_return = set()
        to_return.update(only_values_from_list_700)
        to_return.update(only_values_from_list_710)
        to_return.update(only_values_from_list_711)

        return list(to_return)

    def get_uniform_publishers(self, bib_object, descr_index):
        list_val_710abcdn = set()
        list_710_fields = bib_object.get_fields('710')
        if list_710_fields:
            for field in list_710_fields:
                e_subflds = field.get_subfields('e')
                subflds_4 = field.get_subfields('4')
                if e_subflds or subflds_4:
                    if 'Wyd.' in e_subflds or 'Wydawca' in e_subflds or 'pbl' in subflds_4:
                        list_val_710abcdn.add(
                            ' '.join(subfld for subfld in field.get_subfields('a', 'b', 'c', 'd', 'n')))

        resolved_list_710 = resolve_field_value(list(list_val_710abcdn), descr_index)
        only_values_from_list_710 = only_values(resolved_list_710)
        self.filter_publisher_uniform.update(only_values_from_list_710)

    # def convert_to_work(self, manifestations_bn_by_id, buffer, descr_index, code_val_index):
    #         # get simple attributes, without relations to descriptors
    #         self.work_udc.update(get_values_by_field_and_subfield(bib_object, ('080', ['a'])))
    #         self.get_language_of_original(bib_object)
    #         self.get_languages(bib_object)
    #         self.get_pub_country(bib_object)
    #
    #
    #
    #
    #         # get other data related to descriptors
    #         self.work_subject_domain.update(resolve_field_value(
    #             get_values_by_field_and_subfield(bib_object, ('658', ['a'])), descr_index))
    #         self.work_form.update(resolve_field_value(
    #             get_values_by_field_and_subfield(bib_object, ('380', ['a'])), descr_index))
    #         self.work_cultural_group.update(resolve_field_value(
    #             get_values_by_field_and_subfield(bib_object, ('386', ['a'])), descr_index))
    #
    #         # get creators and creators for presentation
    #         self.work_main_creator = serialize_to_jsonl_descr_creator(list(self.main_creator_real))
    #
    #         if len(list(self.main_creator_real)) > 1:
    #             self.work_presentation_main_creator = select_number_of_creators(self.work_main_creator,
    #                                                                             cr_num_end=1)
    #             self.work_presentation_another_creator = select_number_of_creators(self.work_main_creator,
    #                                                                                cr_num_start=1)
    #         else:
    #             if self.main_creator_real:
    #                 self.work_presentation_main_creator = self.work_main_creator
    #                 self.work_presentation_another_creator = []
    #             else:
    #                 self.work_presentation_main_creator = []
    #                 self.work_presentation_another_creator = []
    #
    #         self.work_time_created = []  # todo
    #         self.work_title_index = []  # todo
    #
    #         # get data and create attributes for search indexes
    #         self.search_adress.update(get_values_by_field(bib_object, '260'))
    #
    #         self.search_identity.update(get_values_by_field_and_subfield(bib_object, ('035', ['a'])))
    #         self.search_identity.update(get_values_by_field_and_subfield(bib_object, ('020', ['a'])))
    #         self.search_identity.update(get_values_by_field(bib_object, '001'))
    #
    #         creators_from_manif = self.get_creators_from_manif(bib_object, descr_index)
    #         self.search_authors.update(serialize_to_list_of_values(self.main_creator_real))
    #         self.search_authors.update(creators_from_manif)
    #
    #         self.search_note.update(get_values_by_field(bib_object, '500'))
    #
    #         self.search_subject.update(*[only_values(res_val_list) for res_val_list in
    #                                      [self.work_subject, self.work_subject_place, self.work_subject_domain,
    #                                       self.work_subject_corporate_body, self.work_subject_person,
    #                                       self.work_subject_time, self.work_subject_event]])
    #
    #         self.search_formal.update(*[only_values(res_val_list) for res_val_list in
    #                                     [self.work_cultural_group, self.work_genre]])
    #
    #         self.filter_pub_date.add(get_values_by_field(bib_object, '008')[0][7:11].replace('u', '0').replace(' ', '0').replace('X', '0'))
    #         self.filter_publisher.update(self.get_publishers_all(bib_object))
    #         self.get_uniform_publishers(bib_object, descr_index)
    #         self.filter_creator.update(creators_from_manif)
    #
    #
    #         # that is quite tricky part: upsert_expression method not only instantiates and upserts expression object
    #         # (basing on the manifestation data), but also instantiates manifestation object and item object(s),
    #         # creates expression ids, manifestation ids and item ids
    #         self.upsert_expression(bib_object, buffer, descr_index, code_val_index)
    #
    #     # attributes below can be calculated AFTER getting data from all manifestations
    #     self.calculate_lang_orig()
    #     self.calculate_title_of_orig_pref()
    #     self.calculate_title_pref()
    #
    #     self.get_titles_of_orig_alt()
    #     self.get_titles_alt()
    #
    #     self.language_orig_obj = resolve_code_and_serialize([self.language_orig], 'language_dict', code_val_index)
    #
    #     # calculate filter indexes
    #     self.filter_lang.extend(resolve_code(list(self.language_codes), 'language_dict', code_val_index))
    #     self.filter_lang_orig.extend(resolve_code(list(self.language_of_orig_codes.keys()), 'language_dict',
    #                                               code_val_index))
    #     self.filter_creator.update(serialize_to_list_of_values(self.main_creator_real))
    #     self.filter_nat_bib_code = []  # todo
    #     self.filter_nat_bib_year = []  # todo
    #     self.filter_pub_country.extend(resolve_code(list(self.pub_country_codes), 'country_dict', code_val_index))
    #     self.filter_form.extend(only_values(self.work_form))
    #     self.filter_cultural_group.extend(only_values(self.work_cultural_group))
    #     self.filter_subject.extend(only_values(self.work_subject))
    #     self.filter_subject.extend(only_values(self.work_subject_person))
    #     self.filter_subject.extend(only_values(self.work_subject_corporate_body))
    #     self.filter_subject.extend(only_values(self.work_subject_event))
    #     self.filter_subject.extend(only_values(self.work_subject_work))
    #     self.filter_subject_place.extend(only_values(self.work_subject_place))
    #     self.filter_subject_time = []  # todo
    #     self.filter_time_created = []  # todo
    #
    #     self.search_form.update(self.filter_form)
    #     self.search_form.update(only_values(self.work_genre))
    #
    #     self.search_title.add(self.work_title_of_orig_pref)
    #     self.search_title.update(self.work_title_of_orig_alt)
    #     self.search_title.add(self.work_title_pref)
    #     self.search_title.update(self.work_title_alt)
    #
    #     # get creator for sorting
    #     if self.main_creator:
    #         self.sort_author = list(serialize_to_list_of_values(self.main_creator))[0]
    #     if self.other_creator:
    #         self.sort_author = list(serialize_to_list_of_values(self.other_creator))[0]
    #
    #     # get data for suggestions
    #     self.suggest = [self.work_title_pref]  # todo
    #     self.phrase_suggest = [self.work_title_pref]  # todo

    def get_expr_manif_item_ids_and_counts(self):
        lib_ids = set()

        for expr in self.expressions_dict.values():
            self.expression_ids.append(int(expr.mock_es_id))
            self.materialization_ids.extend([int(m_id) for m_id in list(expr.materialization_ids)])
            self.stat_materialization_count = len(self.materialization_ids)
            self.item_ids.extend([int(i_id) for i_id in list(expr.item_ids)])
            self.stat_item_count += expr.item_count
            self.stat_digital = True if self.stat_digital or expr.stat_digital else False
            self.stat_public_domain = True if self.stat_public_domain or expr.stat_public_domain else False
            self.stat_digital_library_count = 1 if self.stat_digital_library_count == 1 or expr.stat_public_domain == 1 \
                else 0

            for lib in expr.libraries:
                if lib['id'] not in lib_ids:
                    self.libraries.append(lib)
                    lib_ids.add(lib['id'])

        self.stat_library_count = len(self.libraries)

    # 9.1
    def upsert_expression(self, bib_object, buffer, descr_index, code_val_index):
        list_fields_700 = bib_object.get_fields('700')
        list_fields_710 = bib_object.get_fields('710')
        list_fields_711 = bib_object.get_fields('711')

        translators = set()

        if list_fields_700:
            for field in list_fields_700:
                if field.get_subfields('e'):
                    if field.get_subfields('e')[0] in ['Tł.', 'Tł', 'Tłumaczenie']:
                        translators.add(' '.join(field.get_subfields('a', 'b', 'c', 'd')))

        expr_lang = get_values_by_field(bib_object, '008')[0][35:38]
        ldr6 = bib_object.leader[6]

        self.expressions_dict.setdefault((expr_lang, frozenset(translators), ldr6),
                                         Expression()).add(bib_object, self, buffer, descr_index, code_val_index)

    def write_to_dump_file(self, buffer):
        write_to_json(self.serialize_work_for_es_work_dump(), buffer, 'work_buffer')
        write_to_json(self.serialize_work_popularity_object_for_es_work_dump(), buffer, 'work_buffer')

        for jsonl in self.serialize_work_for_es_work_data_dump():
            write_to_json(jsonl, buffer, 'work_data_buffer')

    def serialize_work_for_es_work_dump(self):
        dict_work = {"_index": "work", "_type": "work", "_id": str(self.mock_es_id),
                     "_score": 1, "_source":
                         {'eForm': list(self.filter_form),
                          'expression_ids': list(self.expression_ids),
                          'filter_creator': list(self.filter_creator),
                          'filter_cultural_group': list(self.filter_cultural_group),
                          'filter_form': list(self.filter_form),
                          'filter_lang': list(self.filter_lang),
                          'filter_lang_orig': list(self.filter_lang_orig),
                          'filter_nat_bib_code': [],
                          'filter_nat_bib_year': [],
                          'filter_pub_country': list(self.filter_pub_country),
                          'filter_pub_date': list(self.filter_pub_date),
                          'filter_publisher': list(self.filter_publisher),
                          'filter_publisher_uniform': list(self.filter_publisher_uniform),
                          'filter_subject': list(self.filter_subject),
                          'filter_subject_place': list(self.filter_subject_place),
                          'filter_subject_time': [],
                          'filter_time_created': [],
                          'item_ids': [int(i_id) for i_id in list(self.item_ids)],
                          'libraries': self.libraries,
                          'materialization_ids': list(self.materialization_ids),
                          'modificationTime': self.modificationTime,
                          'phrase_suggest': list(self.phrase_suggest),
                          'popularity-join': self.popularity_join,
                          'search_adress': list(self.search_adress),
                          'search_authors': list(self.search_authors),
                          'search_form': list(self.search_form),
                          'search_formal': list(self.search_formal),
                          'search_identity': list(self.search_identity),
                          'search_note': list(self.search_note),
                          'search_subject': list(self.search_subject),
                          'search_title': list(self.search_title),
                          'sort_author': self.sort_author,
                          'stat_digital': self.stat_digital,
                          'stat_digital_library_count': self.stat_digital_library_count,
                          'stat_item_count': self.stat_item_count,
                          'stat_library_count': self.stat_library_count,
                          'stat_materialization_count': self.stat_materialization_count,
                          'stat_public_domain': self.stat_public_domain,
                          'suggest': list(self.suggest),
                          'work_cultural_group': serialize_to_jsonl_descr(list(self.work_cultural_group)),
                          'work_form': serialize_to_jsonl_descr(list(self.work_form)),
                          'work_genre': serialize_to_jsonl_descr(list(self.work_genre)),
                          'work_main_creator': list(self.work_main_creator),
                          'work_other_creator': list(self.work_other_creator),
                          'work_main_creator_index': list(self.work_main_creator_index),
                          'work_other_creator_index': list(self.work_other_creator_index),
                          'work_presentation_main_creator': list(self.work_presentation_main_creator),
                          'work_presentation_another_creator': list(self.work_presentation_another_creator),
                          'work_publisher_work': self.work_publisher_work,
                          'work_subject': serialize_to_jsonl_descr(list(self.work_subject)),
                          'work_subject_corporate_body': serialize_to_jsonl_descr(
                              list(self.work_subject_corporate_body)),
                          'work_subject_domain': serialize_to_jsonl_descr(list(self.work_subject_domain)),
                          'work_subject_event': serialize_to_jsonl_descr(list(self.work_subject_event)),
                          'work_subject_person': serialize_to_jsonl_descr(list(self.work_subject_person)),
                          'work_subject_place': serialize_to_jsonl_descr(list(self.work_subject_place)),
                          'work_subject_time': list(self.work_subject_time),
                          'work_subject_work': list(self.work_subject_work),
                          'work_time_created': list(self.work_time_created),
                          'work_title_alt': list(self.work_title_alt),
                          'work_title_index': list(self.work_title_index),
                          'work_title_of_orig_alt': list(self.work_title_of_orig_alt),
                          'work_title_of_orig_pref': self.work_title_of_orig_pref,
                          'work_title_pref': self.work_title_pref,
                          'work_udc': list(self.work_udc)
                          }}

        json_work = json.dumps(dict_work, ensure_ascii=False)

        return json_work

    def serialize_work_popularity_object_for_es_work_dump(self):
        dict_work = {"_index": "work", "_type": "work", "_id": f'p{str(self.mock_es_id)}',
                     "_score": 1, "_routing": str(self.mock_es_id), "_source": {
                         "modificationTime": self.modificationTime,
                         "popularity": 0,
                         "popularity-join": {"parent": str(self.mock_es_id), "name": "popularity"}
                     }}

        json_work = json.dumps(dict_work, ensure_ascii=False)

        return json_work

    def serialize_work_for_es_work_data_dump(self):
        dict_work_data_list = []

        for num1, expr in enumerate(self.expressions_dict.values(), start=1):
            for num2, m in enumerate(expr.manifestations, start=1):

                dict_work_data = {"_index": "work_data", "_type": "work_data",
                                  "_id": f'{num2}{num1}{self.mock_es_id}',
                                  "_score": 1, "_source": {
                                      'metadata_original': m.metadata_original,
                                      'metadata_source': self.metadata_source,
                                      'modificationTime': self.modificationTime,
                                      'phrase_suggest': ['-'],
                                      'suggest': ['-'],
                                      'work_form': expr.expr_form,
                                      'work_language_of_orig': self.language_orig_obj,
                                      'work_main_creator': list(self.work_main_creator),
                                      'work_materialization':
                                         {'id': int(m.mock_es_id),
                                          'type': 'materialization',
                                          'value': str(m.mock_es_id)},
                                      'work_multi_work': False,
                                      'work_subject_jhp': [],
                                      'work_time_created': [],
                                      'work_title': expr.expr_title,
                                      'work_work':
                                          {'id': int(self.mock_es_id),
                                           'type': 'work',
                                           'value': str(self.mock_es_id)}}}

                json_work_data = json.dumps(dict_work_data, ensure_ascii=False)
                dict_work_data_list.append(json_work_data)

        return dict_work_data_list
