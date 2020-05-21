import time
from datetime import datetime, timezone

from commons.marc_iso_commons import get_values_by_field_and_subfield, get_values_by_field, postprocess
from commons.marc_iso_commons import serialize_to_jsonl_descr_creator, normalize_publisher
from commons.marc_iso_commons import select_number_of_creators
from commons.json_writer import write_to_json
from commons.normalization import prepare_name_for_indexing, normalize_title

from resolvers.descriptor_resolvers import resolve_ids_to_names, resolve_ids_to_dict_objects
from resolvers.codes_resolvers import resolve_institution_codes, resolve_codes_to_names

from objects.frbr_cluster import FRBRCluster
from objects.manifestation import FinalManifestation
from objects.helper_objects import ObjCounter


class FinalWork(object):
    def __init__(self,
                 frbr_cluster: FRBRCluster,
                 timestamp: int):
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

        self.libraries = set()

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
        self.filter_cultural_group = set()
        self.filter_form = set()
        self.filter_lang = set()
        self.filter_lang_orig = set()
        self.filter_nat_bib_code = []
        self.filter_nat_bib_year = []
        self.filter_pub_country = set()
        self.filter_pub_date = set()
        self.filter_publisher = set()
        self.filter_publisher_uniform = set()
        self.filter_subject = set()
        self.filter_subject_place = set()
        self.filter_subject_time = set()
        self.filter_time_created = set()

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
        self.work_subject_time = []  # TODO
        self.work_subject_domain = set()
        self.work_subject_work = []  # TODO

        self.popularity_join = "owner"
        self.stat_digital = False
        self.work_publisher_work = False

        # helper data
        self.work_main_creator_index = []

        # stats
        self.stat_item_count = 0
        self.stat_digital_library_count = 0
        self.stat_library_count = 0
        self.stat_materialization_count = len(frbr_cluster.manifestations_by_raw_record_id)
        self.stat_public_domain = False

        # children ids
        self.expression_ids = [e_id for e_id in frbr_cluster.expressions.keys()]
        self.materialization_ids = [m_id.get('uuid') for m_id in frbr_cluster.manifestations_by_raw_record_id.values()]
        self.item_ids = set()

        # suggestions data
        self.suggest = []
        self.phrase_suggest = []

        # modification_time
        self.modificationTime = datetime.fromtimestamp(time.time(), tz=timezone.utc).isoformat()

    def __repr__(self):
        return f'FinalWork(id={self.frbr_cluster.uuid}, title_pref={self.work_title_pref})'

    def join_and_calculate_pure_work_attributes(self):
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
            self.work_subject_time = []  # TODO
            self.work_subject_work = []  # TODO

            # join and calculate other descriptor related data
            self.main_creator_real.update(work_data_object.main_creator_real_nlp_id)
            self.other_creator_real.update(work_data_object.other_creator_real_nlp_id)

            self.work_genre.update(work_data_object.work_genre)
            self.work_subject_domain.update(work_data_object.work_subject_domain)
            self.work_form.update(work_data_object.work_form)
            self.work_cultural_group.update(work_data_object.work_cultural_group)

        # now, when we have all raw attributes joined, we can calculate some more complex atrributes
        # order of calculations have to be retained (e.g. title_of_orig_pref needs lang_orig to be calculated first)
        self.calculate_lang_orig()
        self.calculate_title_of_orig_pref()
        self.calculate_title_pref()
        self.get_titles_of_orig_alt()
        self.get_titles_alt()

        if self.main_creator_real:
            self.sort_author = list(self.main_creator_real)[0]
        if self.other_creator_real and not self.main_creator_real:
            self.sort_author = list(self.other_creator_real)[0]

        # get creators and creators for presentation
        if len(self.main_creator_real) > 1:
            list_cr = list(self.main_creator_real)
            self.work_presentation_main_creator = list_cr[0]
            self.work_presentation_another_creator = list_cr[1:]
        else:
            if self.main_creator_real:
                self.work_presentation_main_creator = self.main_creator_real
                self.work_presentation_another_creator = []
            else:
                self.work_presentation_main_creator = []
                self.work_presentation_another_creator = []

        # we can calculate some filter and search attributes as well
        self.filter_creator.update(self.main_creator_real)
        self.filter_creator.update(self.other_creator_real)

        self.filter_cultural_group.update(self.work_cultural_group)
        self.filter_form.update(self.work_form)
        self.filter_lang.update(self.language_codes)
        self.filter_lang_orig.update(self.language_of_orig_codes)

        self.filter_subject.update(self.work_subject)
        self.filter_subject.update(self.work_subject_person)
        self.filter_subject.update(self.work_subject_corporate_body)
        self.filter_subject.update(self.work_subject_event)

        self.filter_subject_place.update(self.work_subject_place)
        self.filter_subject_time.update(self.work_subject_time)
        self.filter_time_created.update(self.work_time_created)

        # some search attributes are joined from multiple "single" atrributes
        self.search_authors.update(self.main_creator_real)
        self.search_authors.update(self.other_creator_real)

        self.search_title.add(self.work_title_pref)
        self.search_title.add(self.work_title_of_orig_pref)
        self.search_title.update(self.work_title_alt)
        self.search_title.update(self.work_title_of_orig_alt)

        self.search_subject.update(self.work_subject)
        self.search_subject.update(self.work_subject_place)
        self.search_subject.update(self.work_subject_time)
        self.search_subject.update(self.work_subject_person)
        self.search_subject.update(self.work_subject_corporate_body)
        self.search_subject.update(self.work_subject_event)

        #self.search_formal = set()
        #self.search_form = set()

        # suggestions
        self.suggest = list(self.search_title)
        self.phrase_suggest = list(self.search_title)

    def join_and_calculate_impure_work_attributes_from_manifestation(self,
                                                                     final_manifestation: FinalManifestation):

        self.pub_country_codes.update(final_manifestation.frbr_manifestation.mat_pub_country)
        for contributor_set in final_manifestation.frbr_manifestation.mat_contributor.values():
            self.filter_creator.update(contributor_set)
        if final_manifestation.frbr_manifestation.mat_pub_date_single:
            self.filter_pub_date.add(final_manifestation.frbr_manifestation.mat_pub_date_single)
        if final_manifestation.frbr_manifestation.mat_pub_date_from:
            self.filter_pub_date.add(final_manifestation.frbr_manifestation.mat_pub_date_from)
        if final_manifestation.frbr_manifestation.mat_pub_date_to:
            self.filter_pub_date.add(final_manifestation.frbr_manifestation.mat_pub_date_to)

        self.filter_publisher.update(final_manifestation.frbr_manifestation.mat_publisher)
        self.filter_publisher_uniform.update(final_manifestation.frbr_manifestation.mat_publisher_uniform)

        #self.filter_nat_bib_code = []
        #self.filter_nat_bib_year = []

    def join_and_calculate_impure_work_attributes_final(self):
        pass

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

    def collect_data_for_resolver_cache(self,
                                        resolver_cache: dict):

        descriptor_related_attributes = ['work_subject_person', 'work_subject_corporate_body',
                                         'work_subject_event', 'work_subject',
                                         'work_subject_place', 'work_subject_work',
                                         'work_genre', 'work_subject_domain',
                                         'work_form', 'work_cultural_group',
                                         'main_creator_real', 'other_creator_real']

        for attribute in descriptor_related_attributes:
            for descr_nlp_id in getattr(self, attribute):
                resolver_cache.setdefault('descriptors', {}).setdefault(descr_nlp_id, None)

        lang_code_related_attributes = ['language_codes']

        for attribute in lang_code_related_attributes:
            for lang_code in getattr(self, attribute):
                resolver_cache.setdefault('language', {}).setdefault(lang_code, None)

        pub_country_related_attributes = ['pub_country_codes']

        for attribute in pub_country_related_attributes:
            for country_code in getattr(self, attribute):
                resolver_cache.setdefault('country', {}).setdefault(country_code, None)

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
    #
    #

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
    #
    #     # get data for suggestions
    #     self.suggest = [self.work_title_pref]  # todo
    #     self.phrase_suggest = [self.work_title_pref]  # todo

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
        dict_work = self.resolve_and_serialize_work_for_bulk_request(resolver_cache)
        request = {"index": {"_index": "work",
                             "_type": "work",
                             "_id": self.frbr_cluster.uuid,
                             "version": timestamp,
                             "version_type": "external"}}

        bulk_list = [request, dict_work]

        return bulk_list

    def resolve_and_serialize_work_for_bulk_request(self, resolver_cache):
        dict_work = {'eForm': resolve_ids_to_names(list(self.filter_form), resolver_cache),
                     'expression_ids': list(self.expression_ids),
                     'filter_creator': resolve_ids_to_names(list(self.filter_creator), resolver_cache),
                     'filter_cultural_group': resolve_ids_to_names(list(self.filter_cultural_group), resolver_cache),
                     'filter_form': resolve_ids_to_names(list(self.filter_form), resolver_cache),
                     'filter_lang': resolve_codes_to_names(list(self.filter_lang), 'language', resolver_cache),
                     'filter_lang_orig': resolve_codes_to_names(list(self.filter_lang_orig), 'language', resolver_cache),
                     'filter_nat_bib_code': [],
                     'filter_nat_bib_year': [],
                     'filter_pub_country': resolve_codes_to_names(list(self.filter_pub_country), 'country', resolver_cache),
                     'filter_pub_date': list(self.filter_pub_date),
                     'filter_publisher': list(self.filter_publisher),
                     'filter_publisher_uniform': resolve_ids_to_names(list(self.filter_publisher_uniform),
                                                                      resolver_cache),
                     'filter_subject': resolve_ids_to_names(list(self.filter_subject), resolver_cache),
                     'filter_subject_place': resolve_ids_to_names(list(self.filter_subject_place), resolver_cache),
                     'filter_subject_time': [],
                     'filter_time_created': [],
                     'item_ids': list(self.item_ids),
                     'libraries': resolve_institution_codes(list(self.libraries), resolver_cache),
                     'materialization_ids': list(self.materialization_ids),
                     'modificationTime': self.modificationTime,
                     'phrase_suggest': list(self.phrase_suggest),
                     'popularity-join': self.popularity_join,
                     'search_adress': list(self.search_adress),
                     'search_authors': resolve_ids_to_names(list(self.search_authors), resolver_cache),
                     'search_form': resolve_ids_to_names(list(self.search_form), resolver_cache),
                     'search_formal': resolve_ids_to_names(list(self.search_formal), resolver_cache),
                     'search_identity': list(self.search_identity),
                     'search_note': list(self.search_note),
                     'search_subject': resolve_ids_to_names(list(self.search_subject), resolver_cache),
                     'search_title': list(self.search_title),
                     'sort_author': self.sort_author,
                     'stat_digital': self.stat_digital,
                     'stat_digital_library_count': self.stat_digital_library_count,
                     'stat_item_count': self.stat_item_count,
                     'stat_library_count': self.stat_library_count,
                     'stat_materialization_count': self.stat_materialization_count,
                     'stat_public_domain': self.stat_public_domain,
                     'suggest': list(self.suggest),
                     'work_cultural_group': resolve_ids_to_dict_objects(list(self.work_cultural_group),
                                                                        resolver_cache),
                     'work_form': resolve_ids_to_dict_objects(list(self.work_form),
                                                              resolver_cache),
                     'work_genre': resolve_ids_to_dict_objects(list(self.work_genre),
                                                               resolver_cache),
                     'work_main_creator': resolve_ids_to_dict_objects(list(self.main_creator_real),
                                                                      resolver_cache),
                     'work_other_creator': resolve_ids_to_dict_objects(list(self.work_other_creator),
                                                                       resolver_cache),
                     'work_presentation_main_creator': resolve_ids_to_dict_objects(
                         list(self.work_presentation_main_creator),
                         resolver_cache),
                     'work_presentation_another_creator': resolve_ids_to_dict_objects(list(
                         self.work_presentation_another_creator),
                         resolver_cache),
                     'work_publisher_work': self.work_publisher_work,
                     'work_subject': resolve_ids_to_dict_objects(list(self.work_subject),
                                                                 resolver_cache),
                     'work_subject_corporate_body': resolve_ids_to_dict_objects(list(self.work_subject_corporate_body),
                                                                                resolver_cache),
                     'work_subject_domain': resolve_ids_to_dict_objects(list(self.work_subject_domain),
                                                                        resolver_cache),
                     'work_subject_event': resolve_ids_to_dict_objects(list(self.work_subject_event),
                                                                       resolver_cache),
                     'work_subject_person': resolve_ids_to_dict_objects(list(self.work_subject_person),
                                                                        resolver_cache),
                     'work_subject_place': resolve_ids_to_dict_objects(list(self.work_subject_place),
                                                                       resolver_cache),
                     'work_subject_time': list(self.work_subject_time),
                     'work_subject_work': list(self.work_subject_work),
                     'work_time_created': list(self.work_time_created),
                     'work_title_alt': list(self.work_title_alt),
                     'work_title_index': list(self.work_title_index),
                     'work_title_of_orig_alt': list(self.work_title_of_orig_alt),
                     'work_title_of_orig_pref': self.work_title_of_orig_pref,
                     'work_title_pref': self.work_title_pref,
                     'work_udc': list(self.work_udc)}

        return dict_work
