from uuid import uuid4
from collections import Counter
import json
import pprint

from commons.marc_iso_commons import get_values_by_field_and_subfield, get_values_by_field, postprocess
from commons.marc_iso_commons import truncate_title_proper, get_rid_of_punctuation, read_marc_from_binary
from commons.marc_iso_commons import is_dbn, truncate_title_from_246
from exceptions.exceptions import TooMany1xxFields, No245FieldFoundOrTooMany245Fields

from objects.expression import Expression


class ObjCounter(object):
    __slots__ = 'count'

    def __init__(self):
        self.count = 0

    def __repr__(self):
        return f'TitleCounter(title_count={self.count}'

    def add(self, number_to_add):
        self.count += number_to_add


class Work(object):
    def __init__(self):
        self.uuid = uuid4()
        self.mock_es_id = int()

        self.main_creator = []
        self.other_creator = []

        self.titles240 = set()
        self.titles245 = {}
        self.titles246_title_orig = {}
        self.titles246_title_other = {}

        self.language_codes = set()
        self.language_of_orig_codes = {}
        self.language_orig = ''

        self.expressions_dict = {}

        self.manifestations_bn_ids = set()
        self.manifestations_mak_ids = set()

        self.item_ids = []

        self.expressions = []

        # search indexes data
        self.search_adress = set()
        self.search_authors = set()
        self.search_identity = set()
        self.search_title = set()
        self.search_subject = set()
        self.search_formal = set()

        # filters data
        self.filter_creator = []
        self.filter_nat_bib_year = []
        self.filter_lang = []
        self.filter_lang_orig = []
        self.filter_nat_bib_code = []
        self.filter_pub_date = []

        # presentation data
        self.work_presentation_main_creator = []

        # sort data
        self.sort_author = []

        # work data
        self.work_title_pref = ''
        self.work_title_of_orig_pref = ''
        self.work_title_alt = []
        self.work_title_of_orig_alt = []
        self.work_title_index = []

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

        # invariable data
        self.popularity_join = "owner"
        self.modificationTime = "2019-10-01T13:34:23.580"
        self.stat_digital = "false"

        # helper data
        self.work_main_creator_index = []

        # stats
        self.stat_digital_library_count = 0

        # children ids
        self.expression_ids = []
        self.materialization_ids = []
        self.item_ids = []

    def __repr__(self):
        return f'Work(id={self.uuid}, lang={self.work_title_pref}, children={self.expressions_dict.values()}'

    def get_manifestation_bn_id(self, pymarc_object):
        self.manifestations_bn_ids.add(get_values_by_field(pymarc_object, '001')[0])

    def create_mock_es_data_index_id(self):
        self.mock_es_id = str('111' + list(self.manifestations_bn_ids)[0][1:-1])

    def create_mock_es_data_work_index_id(self):
        pass

    # 3.1.1
    def get_main_creator(self, pymarc_object):
        list_val_100abcd = postprocess(get_rid_of_punctuation,
                                       get_values_by_field_and_subfield(pymarc_object, ('100', ['a', 'b', 'c', 'd'])))
        list_val_110abcdn = get_values_by_field_and_subfield(pymarc_object, ('110', ['a', 'b', 'c', 'd', 'n']))
        list_val_111abcdn = get_values_by_field_and_subfield(pymarc_object, ('111', ['a', 'b', 'c', 'd', 'n']))

        # validate record
        if (len(list_val_100abcd) > 1 or len(list_val_110abcdn) > 1 or len(list_val_111abcdn) > 1) or \
                (list_val_100abcd and list_val_110abcdn and list_val_111abcdn):
            raise TooMany1xxFields
        else:
            # 3.1.1.1
            if list_val_100abcd:
                self.main_creator.append(list_val_100abcd[0])
            if list_val_110abcdn:
                self.main_creator.append(list_val_110abcdn[0])
            if list_val_111abcdn:
                self.main_creator.append(list_val_111abcdn[0])

            # 3.1.1.2 - if there is no 1XX field, check for 7XX
            if not self.main_creator:
                pass

    # 3.1.2
    def get_other_creator(self, pymarc_object):
        pass

    # 3.1.3
    def get_titles(self, pymarc_object):
        # get title from 245 field
        list_val_245ab = postprocess(truncate_title_proper, get_values_by_field_and_subfield(pymarc_object, ('245', ['a', 'b'])))
        lang_008 = get_values_by_field(pymarc_object, '008')[0][35:38]

        # validate record
        if len(list_val_245ab) > 1 or not list_val_245ab:
            raise No245FieldFoundOrTooMany245Fields
        # append title
        else:
            self.titles245.setdefault(lang_008, {}).setdefault(list_val_245ab[0], ObjCounter()).add(1)

        # get titles from 246 fields
        list_fields_246 = pymarc_object.get_fields('246')
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
        lang_041_h = get_values_by_field_and_subfield(pymarc_object, ('041', ['h']))

        if len(lang_041_h) == 1 and len(list_val_246_title_orig) == 1:
            self.titles246_title_orig.setdefault(lang_041_h[0], {}).setdefault(list_val_246_title_orig[0],
                                                                               ObjCounter()).add(1)

        list_val_246_other = postprocess(truncate_title_from_246, list_val_246_other)
        for val in list_val_246_other:
            self.titles246_title_other.setdefault(val, ObjCounter()).add(1)

        # get title from 240 field
        list_val_240 = get_values_by_field_and_subfield(pymarc_object, ('240', ['a', 'b']))
        self.titles240.update(list_val_240)

    def calculate_title_pref(self):
        polish_titles = self.titles245.get('pol')
        if polish_titles:
            polish_titles_sorted_by_frequency = sorted(polish_titles.items(), key=lambda x: x[1].count)
            self.work_title_pref = polish_titles_sorted_by_frequency[0][0]
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
                orig_titles_from_245_sorted_by_frequency = sorted(orig_titles_from_245.items(), key=lambda x: x[1].count)
                self.work_title_of_orig_pref = orig_titles_from_245_sorted_by_frequency[0][0]
            else:
                pass

    def get_language_of_original(self, pymarc_object):
        lang_008 = get_values_by_field(pymarc_object, '008')[0][35:38]
        lang_041_h = get_values_by_field_and_subfield(pymarc_object, ('041', ['h']))

        if lang_008 and not lang_041_h:
            self.language_of_orig_codes.setdefault(lang_008, ObjCounter()).add(1)
        if len(lang_041_h) == 1:
            self.language_of_orig_codes.setdefault(lang_041_h[0], ObjCounter()).add(1)

    def get_languages(self, pymarc_object):
        lang_008 = get_values_by_field(pymarc_object, '008')[0][35:38]
        lang_041_h = get_values_by_field_and_subfield(pymarc_object, ('041', ['h']))

        self.language_codes.update([lang_008])
        self.language_codes.update(lang_041_h)

    def calculate_lang_orig(self):
        self.language_orig = sorted(self.language_of_orig_codes.items(), key=lambda x: x[1].count)[0][0]

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
        matched_work.titles240.update(self.titles240)

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
                if title not in works_by_titles:
                    candidate_works_by_245_title.setdefault(title, [])
                # title found - append candidate uuids
                else:
                    candidate_works_by_245_title.setdefault(title, []).extend(works_by_titles.get(title))

        for title_dict in self.titles246_title_orig.values():
            for title in title_dict.keys():
                # no such title - append empty list
                if title not in works_by_titles:
                    candidate_works_by_246_title_orig.setdefault(title, [])
                # title found - append candidate uuids
                else:
                    candidate_works_by_246_title_orig.setdefault(title, []).extend(works_by_titles.get(title))

        for title in list(self.titles240):
            # no such title - append empty list
            if title not in works_by_titles:
                candidate_works_by_240_title.setdefault(title, [])
            # title found - append candidate uuids
            else:
                candidate_works_by_240_title.setdefault(title, []).extend(works_by_titles.get(title))


        matched_uuids = []

        if candidate_works_by_245_title:
            for title, uuids_list in candidate_works_by_245_title.items():
                for uuid in uuids_list:
                    candidate_work = works_by_uuid.get(uuid)
                    if candidate_work.main_creator == self.main_creator:
                        matched_uuids.append(uuid)
        if candidate_works_by_246_title_orig:
            for title, uuids_list in candidate_works_by_246_title_orig.items():
                for uuid in uuids_list:
                    candidate_work = works_by_uuid.get(uuid)
                    if candidate_work.main_creator == self.main_creator:
                        matched_uuids.append(uuid)
        if candidate_works_by_240_title:
            for title, uuids_list in candidate_works_by_240_title.items():
                for uuid in uuids_list:
                    candidate_work = works_by_uuid.get(uuid)
                    if candidate_work.main_creator == self.main_creator:
                        matched_uuids.append(uuid)


        # no candidates found - new work to add
        if len(Counter(matched_uuids)) == 0:
            # index new work by titles and by uuid
            for title_dict in self.titles245.values():
                for title in title_dict.keys():
                    works_by_titles.setdefault(title, set()).add(self.uuid)
            for title_dict in self.titles246_title_orig.values():
                for title in title_dict.keys():
                    works_by_titles.setdefault(title, set()).add(self.uuid)
            for title in self.titles240:
                works_by_titles.setdefault(title, set()).add(self.uuid)

            works_by_uuid.setdefault(self.uuid, self)
            print('Added new work.')

        # one candidate found - merge with existing work and index by all titles
        if len(Counter(matched_uuids)) == 1:
            matched_work = works_by_uuid.get(matched_uuids[0])
            self.merge_titles(matched_work)
            self.merge_manif_bn_ids(matched_work)
            self.index_matched_work_by_titles(matched_work, works_by_titles)

    @staticmethod
    def index_matched_work_by_titles(matched_work, works_by_titles):
        for title_dict in matched_work.titles245.values():
            for title in title_dict.keys():
                works_by_titles.setdefault(title, set()).add(matched_work.uuid)
        for title_dict in matched_work.titles246_title_orig.values():
            for title in title_dict.keys():
                works_by_titles.setdefault(title, set()).add(matched_work.uuid)
        for title in matched_work.titles240:
            works_by_titles.setdefault(title, set()).add(matched_work.uuid)

    def convert_to_work(self, manifestations_bn_by_id):
        # get values from all reference manifestations
        for m_id in self.manifestations_bn_ids:

            # get manifestation by bn id from the index and read it (they're stored as iso binary)
            m_object = read_marc_from_binary(manifestations_bn_by_id.get(m_id))

            # get simple attributes, without relations to descriptors
            self.work_udc.update(get_values_by_field_and_subfield(m_object, ('080', ['a'])))
            self.get_language_of_original(m_object)
            self.get_languages(m_object)


            # check if manifestation is catalogued using DBN - if so, get subject data
            if is_dbn(m_object):

                self.work_subject_person.update(get_values_by_field_and_subfield(m_object,
                                                                                 ('600',
                                                                                  ['a', 'b', 'c', 'd', 'n', 'p'])))
                self.work_subject_corporate_body.update(get_values_by_field_and_subfield(m_object,
                                                                                         ('610',
                                                                                          ['a', 'b', 'c', 'd',
                                                                                           'n', 'p'])))
                self.work_subject_event.update(get_values_by_field_and_subfield(m_object,
                                                                                ('611',
                                                                                 ['a', 'b', 'c', 'd', 'n', 'p'])))
                self.work_subject.update(get_values_by_field_and_subfield(m_object,
                                                                          ('650',
                                                                           ['a', 'b', 'c', 'd', 'n', 'p'])))
                self.work_subject_place.update(get_values_by_field_and_subfield(m_object,
                                                                                ('651', ['a', 'b', 'c', 'd'])))
                self.work_genre.update(get_values_by_field_and_subfield(m_object, ('655', ['a', 'b', 'c', 'd'])))

            # get other data related to descriptors
            self.work_subject_domain.update(get_values_by_field_and_subfield(m_object, ('658', ['a'])))
            self.work_form.update(get_values_by_field_and_subfield(m_object, ('380', ['a'])))
            self.work_cultural_group.update(get_values_by_field_and_subfield(m_object, ('386', ['a'])))

            # get data and create attributes for search indexes
            self.search_adress.update(get_values_by_field(m_object, '260'))

            self.search_identity.update(get_values_by_field_and_subfield(m_object, ('035', ['a'])))
            self.search_identity.update(get_values_by_field_and_subfield(m_object, ('020', ['a'])))
            self.search_identity.update(get_values_by_field(m_object, '001'))

            self.search_subject.update(self.work_subject, self.work_subject_place, self.work_subject_domain,
                                       self.work_subject_corporate_body, self.work_subject_person,
                                       self.work_subject_time, self.work_subject_event)

            self.search_formal.update(self.work_cultural_group, self.work_genre)

            # that is quite tricky part: upsert_expression function not only instantiates and upserts expression object
            # (basing on the manifestation data), but also instantiates manifestation object and item object(s),
            # creates expression ids, manifestation ids, item ids and inserts them accordingly into each FRBR object
            self.upsert_expression(m_object)

            # calculate data
        self.calculate_lang_orig()
        self.calculate_title_of_orig_pref()
        self.calculate_title_pref()

        self.create_mock_es_data_index_id()

    # 9.1
    def upsert_expression(self, m_object):
        list_fields_700 = m_object.get_fields('700')
        list_fields_710 = m_object.get_fields('710')
        list_fields_711 = m_object.get_fields('711')

        translators = set()

        if list_fields_700:
            for field in list_fields_700:
                if field.get_subfields('e'):
                    if field.get_subfields('e')[0] in ['Tł.', 'Tł', 'Tłumaczenie']:
                        translators.add(' '.join(field.get_subfields('a', 'b', 'c', 'd')))

        expr_lang = get_values_by_field(m_object, '008')[0][35:38]
        ldr6 = m_object.leader[6]

        self.expressions_dict.setdefault((expr_lang, frozenset(translators), ldr6), Expression(self)).add(m_object)

    def serialize_work_for_es_dump(self):
        dict_work = {"_index": "work", "_type": "work", "_id": self.mock_es_id,
                     "_score": 1, "_source":
                         {'expression_ids': [0],
                          'filter_creator': 'todo',
                          'filter_lang': list(self.language_codes),
                          'filter_lang_orig': [self.language_orig],
                          'filter_nat_bib_code': 'todo',
                          'filter_nat_bib_year': 'todo',
                          'filter_pub_date': 'todo',
                          'filter_publisher': 'todo',
                          'filter_subject': 'todo',
                          'item_ids': 'todo',
                          'work_udc': list(self.work_udc),
                          'work_subject_person': list(self.work_subject_person),
                          'work_subject_corporate_body': list(self.work_subject_corporate_body),
                          'work_subject_event': list(self.work_subject_event),
                          'work_subject': list(self.work_subject),
                          'work_subject_place': list(self.work_subject_place),
                          'search_adress': list(self.search_adress),
                          'search_identity': list(self.search_identity),
                          'search_subject': list(self.search_subject),
                          'search_formal': list(self.search_formal),
                          'work_title_pref': self.work_title_pref,
                          'work_title_of_orig_pref': self.work_title_of_orig_pref,

                          }}

        json_work = json.dumps(dict_work, ensure_ascii=False)
        pp = pprint.PrettyPrinter()
        pp.pprint(dict_work)
