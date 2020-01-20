from uuid import uuid4
import json

from commons.marc_iso_commons import get_values_by_field_and_subfield, get_values_by_field, postprocess
from commons.marc_iso_commons import truncate_title_proper, read_marc_from_binary, normalize_title_for_frbr_indexing
from commons.marc_iso_commons import is_dbn, truncate_title_from_246, serialize_to_list_of_values
from commons.marc_iso_commons import serialize_to_jsonl_descr, serialize_to_jsonl_descr_creator, normalize_publisher
from commons.marc_iso_commons import select_number_of_creators
from commons.json_writer import write_to_json

from exceptions.exceptions import TooMany1xxFields, No245FieldFoundOrTooMany245Fields, No008FieldFound

from descriptor_resolver.resolve_record import resolve_field_value, only_values
from descriptor_resolver.resolve_record import resolve_code, resolve_code_and_serialize

from objects.expression import Expression
from objects.helper_objects import ObjCounter


class Work(object):
    __slots__ = ['uuid', 'mock_es_id', 'main_creator', 'other_creator', 'main_creator_real',
                 'titles240', 'titles245', 'titles245p', 'titles246_title_orig',
                 'titles246_title_other', 'language_codes', 'language_of_orig_codes',
                 'language_orig', 'language_orig_obj', 'pub_country_codes', 'expressions_dict',
                 'manifestations_bn_ids', 'manifestations_mak_ids', 'libraries', 'search_adress', 'search_authors',
                 'search_identity', 'search_title', 'search_subject', 'search_formal', 'search_form', 'search_note',
                 'filter_creator', 'filter_cultural_group', 'filter_form', 'filter_lang', 'filter_lang_orig',
                 'filter_nat_bib_code', 'filter_nat_bib_year', 'filter_pub_country', 'filter_pub_date',
                 'filter_publisher', 'filter_publisher_uniform', 'filter_subject', 'filter_subject_place',
                 'filter_subject_time', 'filter_time_created', 'work_presentation_main_creator',
                 'work_presentation_another_creator', 'sort_author', 'work_other_creator',
                 'work_title_pref', 'work_title_of_orig_pref', 'work_title_alt', 'work_title_of_orig_alt',
                 'work_title_index', 'work_main_creator', 'work_other_creator_index', 'work_udc', 'work_time_created',
                 'work_form', 'work_genre', 'work_cultural_group', 'work_subject_person', 'work_subject_corporate_body',
                 'work_subject_event', 'work_subject', 'work_subject_place', 'work_subject_time', 'work_subject_domain',
                 'work_subject_work', 'popularity_join', 'modificationTime', 'stat_digital', 'work_publisher_work',
                 'metadata_source', 'work_main_creator_index', 'stat_item_count', 'stat_digital_library_count',
                 'stat_library_count', 'stat_materialization_count', 'stat_public_domain', 'expression_ids',
                 'materialization_ids', 'item_ids', 'suggest', 'phrase_suggest',
                 'title_with_nonf_chars']

    def __init__(self):
        self.uuid = uuid4()
        self.mock_es_id = int()

        # creators for frbrization process
        self.main_creator = set()
        self.other_creator = set()

        # creators for record real metadata
        self.main_creator_real = set()

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

        self.expressions_dict = {}

        self.manifestations_bn_ids = set()
        self.manifestations_mak_ids = set()

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
        self.modificationTime = "2019-10-01T13:34:23.580"
        self.stat_digital = False
        self.work_publisher_work = False
        self.metadata_source = 'REFERENCE'

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
        return f'Work(id={self.uuid}, title_pref={self.work_title_pref}, children={self.expressions_dict.values()}'

    def get_manifestation_bn_id(self, bib_object):
        self.manifestations_bn_ids.add(get_values_by_field(bib_object, '001')[0])

    def create_mock_es_data_index_id(self):
        self.mock_es_id = str('111' + list(self.manifestations_bn_ids)[0][1:])

    # 3.1.1
    def get_main_creator(self, bib_object, descr_index):
        list_val_100abcd = get_values_by_field_and_subfield(bib_object, ('100', ['0']))
        list_val_110abcdn = get_values_by_field_and_subfield(bib_object, ('110', ['0']))
        list_val_111abcdn = get_values_by_field_and_subfield(bib_object, ('111', ['0']))

        # validate record
        if (len(list_val_100abcd) > 1 or len(list_val_110abcdn) > 1 or len(list_val_111abcdn) > 1) or \
                (list_val_100abcd and list_val_110abcdn and list_val_111abcdn):
            raise TooMany1xxFields
        else:
            # 3.1.1.1 [CHECKED]
            if list_val_100abcd:
                self.main_creator.add(list_val_100abcd[0])
                self.main_creator_real.add(list_val_100abcd[0])
            if list_val_110abcdn:
                self.main_creator.add(list_val_110abcdn[0])
                self.main_creator_real.add(list_val_110abcdn[0])
            if list_val_111abcdn:
                self.main_creator.add(list_val_111abcdn[0])
                self.main_creator_real.add(list_val_111abcdn[0])

            # 3.1.1.2 - if there is no 1XX field, check for 7XX [CHECKED]
            list_val_700abcd = set()
            list_val_710abcdn = set()
            list_val_711abcdn = set()

            list_700_fields = bib_object.get_fields('700')
            if list_700_fields:
                for field in list_700_fields:
                    e_subflds = field.get_subfields('e')
                    if e_subflds:
                        if 'Autor' in e_subflds or 'Autor domniemany' in e_subflds or 'Wywiad' in e_subflds:
                            list_val_700abcd.add(
                                ' '.join(subfld for subfld in field.get_subfields('a', 'b', 'c', 'd')))
                    else:
                        list_val_700abcd.add(
                            ' '.join(subfld for subfld in field.get_subfields('a', 'b', 'c', 'd')))

            resolved_list_700 = resolve_field_value(list(list_val_700abcd), descr_index)
            # only_values_from_list_700 = only_values(resolved_list_700)

            if not self.main_creator:
                self.main_creator.update(resolved_list_700)
            self.main_creator_real.update(resolved_list_700)

            list_710_fields = bib_object.get_fields('710')
            if list_710_fields:
                for field in list_710_fields:
                    e_subflds = field.get_subfields('e')
                    subflds_4 = field.get_subfields('4')
                    if e_subflds:
                        if 'Autor' in e_subflds or 'Autor domniemany' in e_subflds or 'Wywiad' in e_subflds:
                            list_val_710abcdn.add(
                                ' '.join(subfld for subfld in field.get_subfields('a', 'b', 'c', 'd', 'n')))
                    if not e_subflds and not subflds_4:
                        list_val_710abcdn.add(
                            ' '.join(subfld for subfld in field.get_subfields('a', 'b', 'c', 'd', 'n')))

            resolved_list_710 = resolve_field_value(list(list_val_710abcdn), descr_index)
            # only_values_from_list_710 = only_values(resolved_list_710)

            if not self.main_creator:
                self.main_creator.update(resolved_list_710)
            self.main_creator_real.update(resolved_list_710)

            list_711_fields = bib_object.get_fields('711')
            if list_711_fields:
                for field in list_711_fields:
                    j_subflds = field.get_subfields('j')
                    subflds_4 = field.get_subfields('4')
                    if j_subflds:
                        if 'Autor' in j_subflds or 'Autor domniemany' in j_subflds or 'Wywiad' in j_subflds:
                            list_val_711abcdn.add(
                                ' '.join(subfld for subfld in field.get_subfields('a', 'b', 'c', 'd', 'n')))
                    if not j_subflds and not subflds_4:
                        list_val_711abcdn.add(
                            ' '.join(subfld for subfld in field.get_subfields('a', 'b', 'c', 'd', 'n')))

            resolved_list_711 = resolve_field_value(list(list_val_711abcdn), descr_index)
            # only_values_from_list_711 = only_values(resolved_list_711)

            if not self.main_creator:
                self.main_creator.update(resolved_list_711)
            self.main_creator_real.update(resolved_list_711)

    # 3.1.2
    def get_other_creator(self, bib_object, descr_index):
        if not self.main_creator:
            list_val_700abcd = set()
            list_val_710abcdn = set()
            list_val_711abcdn = set()

            list_700_fields = bib_object.get_fields('700')
            if list_700_fields:
                for field in list_700_fields:
                    e_subflds = field.get_subfields('e')
                    if e_subflds:
                        e_sub_joined = ' '.join(e_sub for e_sub in e_subflds)
                        if 'Red' in e_sub_joined or 'Oprac' in e_sub_joined or 'Wybór' in e_sub_joined:
                            list_val_700abcd.add(
                                ' '.join(subfld for subfld in field.get_subfields('a', 'b', 'c', 'd')))

            resolved_list_700 = resolve_field_value(list(list_val_700abcd), descr_index)
            # only_values_from_list_700 = only_values(resolved_list_700)

            self.other_creator.update(resolved_list_700)

            list_710_fields = bib_object.get_fields('710')
            if list_710_fields:
                for field in list_710_fields:
                    e_subflds = field.get_subfields('e')
                    if e_subflds:
                        e_sub_joined = ' '.join(e_sub for e_sub in e_subflds)
                        if 'Red' in e_sub_joined or 'Oprac' in e_sub_joined or 'Wybór' in e_sub_joined:
                            list_val_710abcdn.add(
                                ' '.join(subfld for subfld in field.get_subfields('a', 'b', 'c', 'd', 'n')))

            resolved_list_710 = resolve_field_value(list(list_val_710abcdn), descr_index)
            # only_values_from_list_710 = only_values(resolved_list_710)

            self.other_creator.update(resolved_list_710)

            list_711_fields = bib_object.get_fields('711')
            if list_711_fields:
                for field in list_711_fields:
                    j_subflds = field.get_subfields('j')
                    if j_subflds:
                        j_sub_joined = ' '.join(j_sub for j_sub in j_subflds)
                        if 'Red' in j_sub_joined or 'Oprac' in j_sub_joined or 'Wybór' in j_sub_joined:
                            list_val_711abcdn.add(
                                ' '.join(subfld for subfld in field.get_subfields('a', 'b', 'c', 'd', 'n')))

            resolved_list_711 = resolve_field_value(list(list_val_711abcdn), descr_index)
            # only_values_from_list_711 = only_values(resolved_list_711)

            self.other_creator.update(resolved_list_711)

    # 3.1.3
    def get_titles(self, bib_object):
        # get 245 field
        title_245_raw = bib_object.get_fields('245')
        field_008_raw = bib_object.get_fields('008')

        # validate record
        if len(title_245_raw) > 1 or not title_245_raw:
            raise No245FieldFoundOrTooMany245Fields
        if not field_008_raw:
            raise No008FieldFound

        # get titles from 245 field
        lang_008 = field_008_raw[0].value()[35:38]

        list_val_245ab = postprocess(truncate_title_proper, get_values_by_field_and_subfield(bib_object,
                                                                                             ('245', ['a', 'b'])))
        title_245_raw_ind = title_245_raw[0].indicators
        list_val_245a = postprocess(truncate_title_proper, get_values_by_field_and_subfield(bib_object,
                                                                                            ('245', ['a'])))
        val_245a_last_char = get_values_by_field_and_subfield(bib_object, ('245', ['a']))[0][-1]
        list_val_245p = postprocess(truncate_title_proper, get_values_by_field_and_subfield(bib_object, ('245', ['p'])))

        if val_245a_last_char == '=' and not list_val_245p:
            to_add = list_val_245a[0]

            try:
                self.titles245.setdefault(lang_008,
                                          {}).setdefault(to_add[int(title_245_raw_ind[1]):],
                                                         ObjCounter()).add(1)

                self.title_with_nonf_chars.setdefault(to_add[int(title_245_raw_ind[1]):],
                                                      set()).add(to_add)
            except ValueError as err:
                #print(err)
                self.titles245.setdefault(lang_008,
                                          {}).setdefault(to_add,
                                                         ObjCounter()).add(1)

                self.title_with_nonf_chars.setdefault(to_add,
                                                      set()).add(to_add)
        if list_val_245p:
            to_add = list_val_245p[0]

            self.titles245.setdefault(lang_008,
                                      {}).setdefault(to_add,
                                                     ObjCounter()).add(1)

            self.title_with_nonf_chars.setdefault(to_add,
                                                  set()).add(to_add)

        else:
            to_add = list_val_245ab[0]

            try:
                self.titles245.setdefault(lang_008, {}).setdefault(to_add[int(title_245_raw_ind[1]):],
                                                                   ObjCounter()).add(1)

                self.title_with_nonf_chars.setdefault(to_add[int(title_245_raw_ind[1]):],
                                                      set()).add(to_add)

            except ValueError as err:
                #print(err)
                self.titles245.setdefault(lang_008, {}).setdefault(to_add,
                                                                   ObjCounter()).add(1)

                self.title_with_nonf_chars.setdefault(to_add,
                                                      set()).add(to_add)

        # get titles from 246 fields
        list_fields_246 = bib_object.get_fields('246')
        list_val_246_title_orig = []
        list_val_246_other = []

        if list_fields_246:
            for field in list_fields_246:
                if field.get_subfields('i') and field.get_subfields('a', 'b'):
                    i_value = field.get_subfields('i')[0]
                    if 'Tyt. oryg' in i_value or 'Tytuł oryginału' in i_value:
                        list_val_246_title_orig.append(' '.join(field.get_subfields('a', 'b')))
                    else:
                        list_val_246_other.append(' '.join(field.get_subfields('a', 'b')))
                if not field.get_subfields('i') and field.get_subfields('a', 'b'):
                    list_val_246_other.append(' '.join(field.get_subfields('a', 'b')))

        list_val_246_title_orig = postprocess(truncate_title_from_246, list_val_246_title_orig)
        lang_041_h = get_values_by_field_and_subfield(bib_object, ('041', ['h']))

        if len(lang_041_h) == 1 and len(list_val_246_title_orig) == 1:
            self.titles246_title_orig.setdefault(lang_041_h[0], {}).setdefault(list_val_246_title_orig[0],
                                                                               ObjCounter()).add(1)

        list_val_246_other = postprocess(truncate_title_from_246, list_val_246_other)
        for val in list_val_246_other:
            self.titles246_title_other.setdefault(val, ObjCounter()).add(1)

        # get title from 240 field
        title_240_raw_list = bib_object.get_fields('240')
        if title_240_raw_list:
            title_240_raw = title_240_raw_list[0]

            list_val_240 = get_values_by_field_and_subfield(bib_object, ('240', ['a', 'b']))

            try:
                self.titles240.add(list_val_240[0][int(title_240_raw.indicators[1]):])

                self.title_with_nonf_chars.setdefault(list_val_240[0][int(title_240_raw.indicators[1]):],
                                                      set()).add(list_val_240[0])
            except ValueError as err:
                #print(err)
                self.titles240.add(list_val_240[0])
                self.title_with_nonf_chars.setdefault(list_val_240[0], set()).add(list_val_240[0])

    def merge_titles(self, matched_work):

        for title_lang, title_dict in self.titles245.items():
            for title, title_count in title_dict.items():
                matched_work.titles245.setdefault(title_lang, {}).setdefault(title,
                                                                             ObjCounter()).add(title_count.count)

        for title_lang, title_dict in self.titles246_title_orig.items():
            for title, title_count in title_dict.items():
                matched_work.titles246_title_orig.setdefault(title_lang,
                                                             {}).setdefault(title,
                                                                            ObjCounter()).add(title_count.count)

        for title, title_count in self.titles246_title_other.items():
            matched_work.titles246_title_other.setdefault(title, ObjCounter()).add(title_count.count)

        matched_work.titles240.update(self.titles240)

        for title, title_full in self.title_with_nonf_chars.items():
            matched_work.title_with_nonf_chars.setdefault(title, set()).update(title_full)

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
                self.work_title_of_orig_pref = list(self.title_with_nonf_chars.get(orig_titles_from_245_sorted_by_frequency[0][0]))[0]
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

    def get_languages(self, bib_object):
        lang_008 = get_values_by_field(bib_object, '008')[0][35:38]
        lang_041_h = get_values_by_field_and_subfield(bib_object, ('041', ['h']))

        self.language_codes.update([lang_008])
        self.language_codes.update(lang_041_h)

    def calculate_lang_orig(self):
        try:
            self.language_orig = sorted(self.language_of_orig_codes.items(), key=lambda x: x[1].count)[0][0]
        except IndexError:
            self.language_orig = 'und'

    def merge_manif_bn_ids(self, matched_work):
        matched_work.manifestations_bn_ids.update(self.manifestations_bn_ids)

    def match_with_existing_work_and_index(self, works_by_uuid, works_by_titles):
        candidate_works_by_245_title = {}
        candidate_works_by_246_title_orig = {}
        candidate_works_by_246_title_other = {}
        candidate_works_by_240_title = {}

        for title_dict in self.titles245.values():
            for title in title_dict.keys():
                # no such title - append empty list
                normalized_title = normalize_title_for_frbr_indexing(title)
                if normalized_title not in works_by_titles:
                    candidate_works_by_245_title.setdefault(title, [])
                # title found - append candidate uuids
                else:
                    candidate_works_by_245_title.setdefault(title, []).extend(works_by_titles.get(normalized_title))

        for title_dict in self.titles246_title_orig.values():
            for title in title_dict.keys():
                # no such title - append empty list
                normalized_title = normalize_title_for_frbr_indexing(title)
                if normalized_title not in works_by_titles:
                    candidate_works_by_246_title_orig.setdefault(title, [])
                # title found - append candidate uuids
                else:
                    candidate_works_by_246_title_orig.setdefault(title, []).extend(works_by_titles.get(normalized_title))

        for title in self.titles246_title_other.keys():
            # no such title - append empty list
            normalized_title = normalize_title_for_frbr_indexing(title)
            if normalized_title not in works_by_titles:
                candidate_works_by_246_title_other.setdefault(title, [])
            # title found - append candidate uuids
            else:
                candidate_works_by_246_title_other.setdefault(title, []).extend(works_by_titles.get(normalized_title))

        for title in list(self.titles240):
            # no such title - append empty list
            normalized_title = normalize_title_for_frbr_indexing(title)
            if normalized_title not in works_by_titles:
                candidate_works_by_240_title.setdefault(title, [])
            # title found - append candidate uuids
            else:
                candidate_works_by_240_title.setdefault(title, []).extend(works_by_titles.get(normalized_title))

        matched_uuids = set()

        if candidate_works_by_245_title:
            for title, uuids_list in candidate_works_by_245_title.items():
                for uuid in uuids_list:
                    candidate_work = works_by_uuid.get(uuid)
                    if candidate_work.main_creator:
                        if candidate_work.main_creator & self.main_creator:
                            matched_uuids.add(uuid)
                    if candidate_work.other_creator:
                        if candidate_work.other_creator == self.other_creator:
                            matched_uuids.add(uuid)
                    if not candidate_work.main_creator and not candidate_work.other_creator \
                            and not self.main_creator and not self.other_creator:
                        matched_uuids.add(uuid)

        if candidate_works_by_246_title_orig:
            for title, uuids_list in candidate_works_by_246_title_orig.items():
                for uuid in uuids_list:
                    candidate_work = works_by_uuid.get(uuid)
                    if candidate_work.main_creator:
                        if candidate_work.main_creator & self.main_creator:
                            matched_uuids.add(uuid)
                    if candidate_work.other_creator:
                        if candidate_work.other_creator == self.other_creator:
                            matched_uuids.add(uuid)
                    if not candidate_work.main_creator and not candidate_work.other_creator \
                            and not self.main_creator and not self.other_creator:
                        matched_uuids.add(uuid)

        if candidate_works_by_240_title:
            for title, uuids_list in candidate_works_by_240_title.items():
                for uuid in uuids_list:
                    candidate_work = works_by_uuid.get(uuid)
                    if candidate_work.main_creator:
                        if candidate_work.main_creator & self.main_creator:
                            matched_uuids.add(uuid)
                    if candidate_work.other_creator:
                        if candidate_work.other_creator == self.other_creator:
                            matched_uuids.add(uuid)
                    if not candidate_work.main_creator and not candidate_work.other_creator \
                            and not self.main_creator and not self.other_creator:
                        matched_uuids.add(uuid)

        if candidate_works_by_246_title_other:
            for title, uuids_list in candidate_works_by_246_title_other.items():
                for uuid in uuids_list:
                    candidate_work = works_by_uuid.get(uuid)
                    if candidate_work.main_creator:
                        if candidate_work.main_creator & self.main_creator:
                            matched_uuids.add(uuid)
                    if candidate_work.other_creator:
                        if candidate_work.other_creator == self.other_creator:
                            matched_uuids.add(uuid)
                    if not candidate_work.main_creator and not candidate_work.other_creator \
                            and not self.main_creator and not self.other_creator:
                        matched_uuids.add(uuid)

        matched_uuids = list(matched_uuids)

        # no candidates found - new work to add
        if len(matched_uuids) == 0:
            # index new work by titles and by uuid
            for title_dict in self.titles245.values():
                for title in title_dict.keys():
                    works_by_titles.setdefault(normalize_title_for_frbr_indexing(title), set()).add(self.uuid)
            for title_dict in self.titles246_title_orig.values():
                for title in title_dict.keys():
                    works_by_titles.setdefault(normalize_title_for_frbr_indexing(title), set()).add(self.uuid)
            for title in self.titles240:
                works_by_titles.setdefault(normalize_title_for_frbr_indexing(title), set()).add(self.uuid)
            for title in self.titles246_title_other.keys():
                works_by_titles.setdefault(normalize_title_for_frbr_indexing(title), set()).add(self.uuid)

            works_by_uuid.setdefault(self.uuid, self)
            #print('Added new work.')

        # one candidate found - merge with existing work and index by all titles
        if len(matched_uuids) == 1:
            matched_work = works_by_uuid.get(matched_uuids[0])
            self.merge_titles(matched_work)
            self.merge_manif_bn_ids(matched_work)
            self.index_matched_work_by_titles(matched_work, works_by_titles)
            #print('One candidate found - merged works.')

        # more than one candidate found
        if len(matched_uuids) > 1:
            matched_work = works_by_uuid.get(matched_uuids[0])
            self.merge_titles(matched_work)
            self.merge_manif_bn_ids(matched_work)
            self.index_matched_work_by_titles(matched_work, works_by_titles)
            #print(f'{len(matched_uuids)} candidates found, merged with first one.')

    def try_to_merge_possible_duplicates_using_broader_context(self, works_by_uuid, works_by_titles):
        candidate_works_by_245_title = {}
        candidate_works_by_246_title_orig = {}
        candidate_works_by_246_title_other = {}
        candidate_works_by_240_title = {}

        for title_dict in self.titles245.values():
            for title in title_dict.keys():
                # no such title - append empty list
                normalized_title = normalize_title_for_frbr_indexing(title)
                if normalized_title not in works_by_titles:
                    candidate_works_by_245_title.setdefault(title, [])
                # title found - append candidate uuids
                else:
                    candidate_works_by_245_title.setdefault(title, []).extend(works_by_titles.get(normalized_title))

        for title_dict in self.titles246_title_orig.values():
            for title in title_dict.keys():
                normalized_title = normalize_title_for_frbr_indexing(title)
                # no such title - append empty list
                if normalized_title not in works_by_titles:
                    candidate_works_by_246_title_orig.setdefault(title, [])
                # title found - append candidate uuids
                else:
                    candidate_works_by_246_title_orig.setdefault(title, []).extend(works_by_titles.get(normalized_title))

        for title in self.titles246_title_other.keys():
            # no such title - append empty list
            normalized_title = normalize_title_for_frbr_indexing(title)
            if normalized_title not in works_by_titles:
                candidate_works_by_246_title_other.setdefault(title, [])
            # title found - append candidate uuids
            else:
                candidate_works_by_246_title_other.setdefault(title, []).extend(works_by_titles.get(normalized_title))

        for title in list(self.titles240):
            # no such title - append empty list
            normalized_title = normalize_title_for_frbr_indexing(title)
            if normalized_title not in works_by_titles:
                candidate_works_by_240_title.setdefault(title, [])
            # title found - append candidate uuids
            else:
                candidate_works_by_240_title.setdefault(title, []).extend(works_by_titles.get(normalized_title))

        matched_uuids = set()

        if candidate_works_by_245_title:
            for title, uuids_list in candidate_works_by_245_title.items():
                for uuid in uuids_list:
                    if uuid != self.uuid:
                        candidate_work = works_by_uuid.get(uuid)
                        if candidate_work:
                            if candidate_work.main_creator:
                                if candidate_work.main_creator & self.main_creator:
                                    matched_uuids.add(uuid)
                            if candidate_work.other_creator:
                                if candidate_work.other_creator == self.other_creator:
                                    matched_uuids.add(uuid)
                            if not candidate_work.main_creator and not candidate_work.other_creator \
                                    and not self.main_creator and not self.other_creator:
                                matched_uuids.add(uuid)

        if candidate_works_by_246_title_orig:
            for title, uuids_list in candidate_works_by_246_title_orig.items():
                for uuid in uuids_list:
                    if uuid != self.uuid:
                        candidate_work = works_by_uuid.get(uuid)
                        if candidate_work:
                            if candidate_work.main_creator:
                                if candidate_work.main_creator & self.main_creator:
                                    matched_uuids.add(uuid)
                            if candidate_work.other_creator:
                                if candidate_work.other_creator == self.other_creator:
                                    matched_uuids.add(uuid)
                            if not candidate_work.main_creator and not candidate_work.other_creator \
                                    and not self.main_creator and not self.other_creator:
                                matched_uuids.add(uuid)

        if candidate_works_by_240_title:
            for title, uuids_list in candidate_works_by_240_title.items():
                for uuid in uuids_list:
                    if uuid != self.uuid:
                        candidate_work = works_by_uuid.get(uuid)
                        if candidate_work:
                            if candidate_work.main_creator:
                                if candidate_work.main_creator & self.main_creator:
                                    matched_uuids.add(uuid)
                            if candidate_work.other_creator:
                                if candidate_work.other_creator == self.other_creator:
                                    matched_uuids.add(uuid)
                            if not candidate_work.main_creator and not candidate_work.other_creator \
                                    and not self.main_creator and not self.other_creator:
                                matched_uuids.add(uuid)

        if candidate_works_by_246_title_other:
            for title, uuids_list in candidate_works_by_246_title_other.items():
                for uuid in uuids_list:
                    if uuid != self.uuid:
                        candidate_work = works_by_uuid.get(uuid)
                        if candidate_work:
                            if candidate_work.main_creator:
                                if candidate_work.main_creator & self.main_creator:
                                    matched_uuids.add(uuid)
                            if candidate_work.other_creator:
                                if candidate_work.other_creator == self.other_creator:
                                    matched_uuids.add(uuid)
                            if not candidate_work.main_creator and not candidate_work.other_creator \
                                    and not self.main_creator and not self.other_creator:
                                matched_uuids.add(uuid)

        matched_uuids = list(matched_uuids)

        # one candidate found - merge with existing work and delete duplicate (only in indexed works by uuid)
        if len(matched_uuids) == 1 and matched_uuids[0] != self.uuid:
            matched_work = works_by_uuid.get(matched_uuids[0])
            if matched_work:
                #print(
                    #f'{self.titles245} | {self.titles246_title_orig} | {self.titles246_title_other} | {self.titles240}')
                #print(f'{self.main_creator} | {self.other_creator}')
                #print(
                    #f'{matched_work.titles245} | {matched_work.titles246_title_orig} | {matched_work.titles246_title_other} | {matched_work.titles240}')
                #print(f'{matched_work.main_creator} | {matched_work.other_creator}')
                self.merge_titles(matched_work)
                self.merge_manif_bn_ids(matched_work)
                #print('Merged works using broader context!')

                return True

        # more than one candidate found
        if len(matched_uuids) > 1:
            #print(f'{len(matched_uuids)} candidates found using broader context.')

            return False

        # no candidates found - there is no duplicate
        return False

    @staticmethod
    def index_matched_work_by_titles(matched_work, works_by_titles):
        for title_dict in matched_work.titles245.values():
            for title in title_dict.keys():
                works_by_titles.setdefault(normalize_title_for_frbr_indexing(title), set()).add(matched_work.uuid)
        for title_dict in matched_work.titles246_title_orig.values():
            for title in title_dict.keys():
                works_by_titles.setdefault(normalize_title_for_frbr_indexing(title), set()).add(matched_work.uuid)
        for title in matched_work.titles240:
            works_by_titles.setdefault(normalize_title_for_frbr_indexing(title), set()).add(matched_work.uuid)
        for title in matched_work.titles246_title_other.keys():
            works_by_titles.setdefault(normalize_title_for_frbr_indexing(title), set()).add(matched_work.uuid)

    def get_pub_country(self, bib_object):
        pub_008 = get_values_by_field(bib_object, '008')[0][15:18]
        pub_008 = pub_008[:-1] if pub_008[-1] == ' ' else pub_008
        pub_044_a = get_values_by_field_and_subfield(bib_object, ('044', ['a']))

        self.pub_country_codes.update([pub_008])
        self.pub_country_codes.update(pub_044_a)

    @staticmethod
    def get_publishers_all(bib_object):
        pl = get_values_by_field_and_subfield(bib_object, ('260', ['b']))
        publishers_list = postprocess(normalize_publisher, get_values_by_field_and_subfield(bib_object, ('260', ['b'])))

        return publishers_list



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

    def convert_to_work(self, manifestations_bn_by_id, buffer, descr_index, code_val_index):
        self.create_mock_es_data_index_id()

        # get values from all reference manifestations
        for m_id in self.manifestations_bn_ids:

            # get manifestation by bn id from the index and read it (they're stored as iso binary)
            bib_object = read_marc_from_binary(manifestations_bn_by_id.get(m_id))

            # get simple attributes, without relations to descriptors
            self.work_udc.update(get_values_by_field_and_subfield(bib_object, ('080', ['a'])))
            self.get_language_of_original(bib_object)
            self.get_languages(bib_object)
            self.get_pub_country(bib_object)

            # check if manifestation is catalogued using DBN - if so, get subject and genre data
            if is_dbn(bib_object):

                self.work_subject_person.update(resolve_field_value(
                    get_values_by_field_and_subfield(bib_object, ('600', ['a', 'b', 'c', 'd'])), descr_index))
                self.work_subject_corporate_body.update(resolve_field_value(
                    get_values_by_field_and_subfield(bib_object, ('610', ['a', 'b', 'c', 'd', 'n', 'p'])), descr_index))
                self.work_subject_event.update(resolve_field_value(
                    get_values_by_field_and_subfield(bib_object, ('611', ['a', 'b', 'c', 'd', 'n', 'p'])), descr_index))
                self.work_subject.update(resolve_field_value(
                    get_values_by_field_and_subfield(bib_object, ('650', ['a', 'b', 'c', 'd'])), descr_index))
                self.work_subject_place.update(resolve_field_value(
                    get_values_by_field_and_subfield(bib_object, ('651', ['a', 'b', 'c', 'd'])), descr_index))
                self.work_subject_time = []
                self.work_subject_work = []
                self.work_genre.update(resolve_field_value(
                    get_values_by_field_and_subfield(bib_object, ('655', ['a', 'b', 'c', 'd'])), descr_index))

            # get other data related to descriptors
            self.work_subject_domain.update(resolve_field_value(
                get_values_by_field_and_subfield(bib_object, ('658', ['a'])), descr_index))
            self.work_form.update(resolve_field_value(
                get_values_by_field_and_subfield(bib_object, ('380', ['a'])), descr_index))
            self.work_cultural_group.update(resolve_field_value(
                get_values_by_field_and_subfield(bib_object, ('386', ['a'])), descr_index))

            # get creators and creators for presentation
            self.work_main_creator = serialize_to_jsonl_descr_creator(list(self.main_creator_real))
            self.work_main_creator_index = []  # todo
            self.work_other_creator_index = []  # todo

            if len(list(self.main_creator_real)) > 1:
                self.work_presentation_main_creator = select_number_of_creators(self.work_main_creator,
                                                                                cr_num_end=1)
                self.work_presentation_another_creator = select_number_of_creators(self.work_main_creator,
                                                                                   cr_num_start=1)
            else:
                if self.main_creator_real:
                    self.work_presentation_main_creator = self.work_main_creator
                    self.work_presentation_another_creator = []
                else:
                    self.work_presentation_main_creator = []
                    self.work_presentation_another_creator = []

            self.work_time_created = []  # todo
            self.work_title_index = []  # todo

            # get data and create attributes for search indexes
            self.search_adress.update(get_values_by_field(bib_object, '260'))

            self.search_identity.update(get_values_by_field_and_subfield(bib_object, ('035', ['a'])))
            self.search_identity.update(get_values_by_field_and_subfield(bib_object, ('020', ['a'])))
            self.search_identity.update(get_values_by_field(bib_object, '001'))

            creators_from_manif = self.get_creators_from_manif(bib_object, descr_index)
            self.search_authors.update(serialize_to_list_of_values(self.main_creator_real))
            self.search_authors.update(creators_from_manif)

            self.search_note.update(get_values_by_field(bib_object, '500'))

            self.search_subject.update(*[only_values(res_val_list) for res_val_list in
                                         [self.work_subject, self.work_subject_place, self.work_subject_domain,
                                          self.work_subject_corporate_body, self.work_subject_person,
                                          self.work_subject_time, self.work_subject_event]])

            self.search_formal.update(*[only_values(res_val_list) for res_val_list in
                                        [self.work_cultural_group, self.work_genre]])

            self.filter_pub_date.add(get_values_by_field(bib_object, '008')[0][7:11].replace('u', '0').replace(' ', '0').replace('X', '0'))
            self.filter_publisher.update(self.get_publishers_all(bib_object))
            self.get_uniform_publishers(bib_object, descr_index)
            self.filter_creator.update(creators_from_manif)


            # that is quite tricky part: upsert_expression method not only instantiates and upserts expression object
            # (basing on the manifestation data), but also instantiates manifestation object and item object(s),
            # creates expression ids, manifestation ids and item ids
            self.upsert_expression(bib_object, buffer, descr_index, code_val_index)

        # attributes below can be calculated AFTER getting data from all manifestations
        self.calculate_lang_orig()
        self.calculate_title_of_orig_pref()
        self.calculate_title_pref()

        self.get_titles_of_orig_alt()
        self.get_titles_alt()

        self.language_orig_obj = resolve_code_and_serialize([self.language_orig], 'language_dict', code_val_index)

        # calculate filter indexes
        self.filter_lang.extend(resolve_code(list(self.language_codes), 'language_dict', code_val_index))
        self.filter_lang_orig.extend(resolve_code(list(self.language_of_orig_codes.keys()), 'language_dict',
                                                  code_val_index))
        self.filter_creator.update(serialize_to_list_of_values(self.main_creator_real))
        self.filter_nat_bib_code = []  # todo
        self.filter_nat_bib_year = []  # todo
        self.filter_pub_country.extend(resolve_code(list(self.pub_country_codes), 'country_dict', code_val_index))
        self.filter_form.extend(only_values(self.work_form))
        self.filter_cultural_group.extend(only_values(self.work_cultural_group))
        self.filter_subject.extend(only_values(self.work_subject))
        self.filter_subject.extend(only_values(self.work_subject_person))
        self.filter_subject.extend(only_values(self.work_subject_corporate_body))
        self.filter_subject.extend(only_values(self.work_subject_event))
        self.filter_subject.extend(only_values(self.work_subject_work))
        self.filter_subject_place.extend(only_values(self.work_subject_place))
        self.filter_subject_time = []  # todo
        self.filter_time_created = []  # todo

        self.search_form.update(self.filter_form)
        self.search_form.update(only_values(self.work_genre))

        self.search_title.add(self.work_title_of_orig_pref)
        self.search_title.update(self.work_title_of_orig_alt)
        self.search_title.add(self.work_title_pref)
        self.search_title.update(self.work_title_alt)

        # get creator for sorting
        if self.main_creator:
            self.sort_author = list(serialize_to_list_of_values(self.main_creator))[0]
        if self.other_creator:
            self.sort_author = list(serialize_to_list_of_values(self.other_creator))[0]

        # get data for suggestions
        self.suggest = [self.work_title_pref]  # todo
        self.phrase_suggest = [self.work_title_pref]  # todo

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
        dict_work = {"_index": "work", "_type": "work", "_id": self.mock_es_id,
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
