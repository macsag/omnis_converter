from pymarc import Record

import exceptions.exceptions as oe

from commons.marc_iso_commons import get_values_by_field_and_subfield, get_values_by_field, postprocess
from commons.marc_iso_commons import is_dbn

from commons.normalization import normalize_title

from objects.helper_objects import ObjCounter


class WorkData(object):
    """
    Class WorkData contains attributes of FRBRWork, which can be obtained directly from a raw bibliographic record.
    Each raw record can produce one or more WorkData instances (exactly one per FRBRCluster instance).

    raw record -> [FRBRCluster containing WorkData, FRBRCluster containing WorkData, ...]

    In final conversion all of the WorkData instances from one FRBRCluster is joined and FinalWork instance is produced.

    Some of the WorkData attributes serve for splitting and joining FRBRClusters as well:
    'main_creator_for_cluster', 'other_creator_for_cluster', 'titles_for_cluster', 'work_match_data_sha_1'
    are used in this process.
    """
    def __init__(self, frbr_cluster, pymarc_object: Record):
        # data needed for merging and splitting FRBRClusters and deleting work_data from FRBRCluster
        self.raw_record_id = frbr_cluster.original_raw_record_id
        self.main_creator_for_cluster = frbr_cluster.main_creator_nlp_id
        self.other_creator_for_cluster = frbr_cluster.other_creator_nlp_id
        self.titles_for_cluster = frbr_cluster.titles

        self.work_match_data_sha_1 = frbr_cluster.work_match_data_sha_1_nlp_id

        # helper dict of titles for control of nonfiling characters
        # used for generating real titles for front-end
        self.title_with_nonf_chars = {}

        self.language_codes = set()
        self.language_of_orig_codes = {}
        self.language_orig = ''
        self.language_orig_obj = None

        # raw work_data attributes (no need for calculations or joins)
        self.main_creator_real_nlp_id = set()
        self.other_creator_real_nlp_id = set()

        self.titles240 = set()
        self.titles245 = {}
        self.titles245p = set()
        self.titles246_title_orig = {}
        self.titles246_title_other = {}

        self.work_time_created = []
        self.work_form = set()
        self.work_genre = set()

        self.work_udc = set()

        self.work_cultural_group = set()
        self.work_subject_person = set()
        self.work_subject_corporate_body = set()
        self.work_subject_event = set()
        self.work_subject_time = []
        self.work_subject = set()
        self.work_subject_place = set()
        self.work_subject_domain = set()
        self.work_subject_work = []

        # call method to populate the attributes
        self.get_attributes_from_pymarc_object(pymarc_object)

    def __repr__(self):
        return f'WorkData(raw_record_id={self.raw_record_id})'

    def get_main_creator_real_nlp_id(self, pymarc_object: Record):
        list_val_100_0 = get_values_by_field_and_subfield(pymarc_object, ('100', ['0']))
        list_val_110_0 = get_values_by_field_and_subfield(pymarc_object, ('110', ['0']))
        list_val_111_0 = get_values_by_field_and_subfield(pymarc_object, ('111', ['0']))

        if list_val_100_0:
            self.main_creator_real_nlp_id.add(list_val_100_0[0])
        if list_val_110_0:
            self.main_creator_real_nlp_id.add(list_val_110_0[0])
        if list_val_111_0:
            self.main_creator_real_nlp_id.add(list_val_111_0[0])

        list_val_700_0 = set()
        list_val_710_0 = set()
        list_val_711_0 = set()

        main_creators_to_add = set()

        list_700_fields = pymarc_object.get_fields('700')
        if list_700_fields:
            for field in list_700_fields:
                e_subflds = field.get_subfields('e')
                t_subflds = field.get_subfields('t')
                if e_subflds and not t_subflds:
                    if 'Autor' in e_subflds or 'Autor domniemany' in e_subflds or 'Wywiad' in e_subflds:
                        value_to_add = field.get_subfields('0')
                        if value_to_add:
                            list_val_700_0.add(value_to_add[0])
                        else:
                            raise oe.DescriptorNotResolved
                if not e_subflds and not t_subflds:
                    value_to_add = field.get_subfields('0')
                    if value_to_add:
                        list_val_700_0.add(value_to_add[0])
                    else:
                        raise oe.DescriptorNotResolved

        main_creators_to_add.update(list_val_700_0)

        list_710_fields = pymarc_object.get_fields('710')
        if list_710_fields:
            for field in list_710_fields:
                e_subflds = field.get_subfields('e')
                subflds_4 = field.get_subfields('4')
                t_subflds = field.get_subfields('t')
                if e_subflds and not t_subflds:
                    if 'Autor' in e_subflds or 'Autor domniemany' in e_subflds or 'Wywiad' in e_subflds:
                        value_to_add = field.get_subfields('0')
                        if value_to_add:
                            list_val_710_0.add(value_to_add[0])
                        else:
                            raise oe.DescriptorNotResolved
                if not e_subflds and not subflds_4 and not t_subflds:
                    value_to_add = field.get_subfields('0')
                    if value_to_add:
                        list_val_710_0.add(value_to_add[0])
                    else:
                        raise oe.DescriptorNotResolved

        main_creators_to_add.update(list_val_710_0)

        list_711_fields = pymarc_object.get_fields('711')
        if list_711_fields:
            for field in list_711_fields:
                j_subflds = field.get_subfields('j')
                subflds_4 = field.get_subfields('4')
                t_subflds = field.get_subfields('t')
                if j_subflds and not t_subflds:
                    if 'Autor' in j_subflds or 'Autor domniemany' in j_subflds or 'Wywiad' in j_subflds:
                        value_to_add = field.get_subfields('0')
                        if value_to_add:
                            list_val_711_0.add(value_to_add[0])
                        else:
                            raise oe.DescriptorNotResolved
                if not j_subflds and not subflds_4 and not t_subflds:
                    value_to_add = field.get_subfields('0')
                    if value_to_add:
                        list_val_711_0.add(value_to_add[0])
                    else:
                        raise oe.DescriptorNotResolved

        main_creators_to_add.update(list_val_711_0)

        for main_creator in main_creators_to_add:
            self.main_creator_real_nlp_id.add(main_creator)

    def get_other_creator_real_nlp_id(self, pymarc_object: Record) -> None:
        list_val_700_0 = set()
        list_val_710_0 = set()
        list_val_711_0 = set()

        other_creators_to_add = set()

        list_700_fields = pymarc_object.get_fields('700')
        if list_700_fields:
            for field in list_700_fields:
                e_subflds = field.get_subfields('e')
                if e_subflds:
                    e_sub_joined = ' '.join(e_sub for e_sub in e_subflds)
                    if 'Red' in e_sub_joined or 'Oprac' in e_sub_joined or 'Wybór' in e_sub_joined:
                        value_to_add = field.get_subfields('0')
                        if value_to_add:
                            list_val_700_0.add(value_to_add[0])
                        else:
                            raise oe.DescriptorNotResolved

        other_creators_to_add.update(list_val_700_0)

        list_710_fields = pymarc_object.get_fields('710')
        if list_710_fields:
            for field in list_710_fields:
                e_subflds = field.get_subfields('e')
                if e_subflds:
                    e_sub_joined = ' '.join(e_sub for e_sub in e_subflds)
                    if 'Red' in e_sub_joined or 'Oprac' in e_sub_joined or 'Wybór' in e_sub_joined:
                        value_to_add = field.get_subfields('0')
                        if value_to_add:
                            list_val_710_0.add(value_to_add[0])
                        else:
                            raise oe.DescriptorNotResolved

        other_creators_to_add.update(list_val_710_0)

        list_711_fields = pymarc_object.get_fields('711')
        if list_711_fields:
            for field in list_711_fields:
                j_subflds = field.get_subfields('j')
                if j_subflds:
                    j_sub_joined = ' '.join(j_sub for j_sub in j_subflds)
                    if 'Red' in j_sub_joined or 'Oprac' in j_sub_joined or 'Wybór' in j_sub_joined:
                        value_to_add = field.get_subfields('0')
                        if value_to_add:
                            list_val_711_0.add(value_to_add[0])
                        else:
                            raise oe.DescriptorNotResolved

        other_creators_to_add.update(list_val_711_0)

        for other_creator in other_creators_to_add:
            self.other_creator_real_nlp_id.add(other_creator)

    # 3.1.3
    def get_titles(self, pymarc_object: Record):
        # get 245 field
        title_245_raw = pymarc_object.get_fields('245')
        field_008_raw = pymarc_object.get_fields('008')

        if not field_008_raw:
            raise oe.No008FieldFound

        # get titles from 245 field
        lang_008 = field_008_raw[0].value()[35:38]

        list_val_245ab = postprocess(normalize_title, get_values_by_field_and_subfield(pymarc_object,
                                                                                       ('245', ['a', 'b'])))
        title_245_raw_ind = title_245_raw[0].indicators
        list_val_245a = postprocess(normalize_title, get_values_by_field_and_subfield(pymarc_object,
                                                                                      ('245', ['a'])))
        val_245a_last_char = get_values_by_field_and_subfield(pymarc_object, ('245', ['a']))[0].strip()[-1]
        list_val_245p = postprocess(normalize_title, get_values_by_field_and_subfield(pymarc_object, ('245', ['p'])))

        if val_245a_last_char == '=' and not list_val_245p:
            to_add = list_val_245a[0]

            try:
                self.titles245.setdefault(lang_008,
                                          {}).setdefault(to_add[int(title_245_raw_ind[1]):],
                                                         ObjCounter()).add(1)

                self.title_with_nonf_chars.setdefault(to_add[int(title_245_raw_ind[1]):],
                                                      set()).add(to_add)
            # sometimes there is invalid value in second indicator (non-numeric character)
            # so we must catch the exception and assume, that there is no non-filing character at all
            # thus, second indicator == 0
            except ValueError:
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

        if val_245a_last_char != '=' and not list_val_245p:
            to_add = list_val_245ab[0]

            try:
                self.titles245.setdefault(lang_008, {}).setdefault(to_add[int(title_245_raw_ind[1]):],
                                                                   ObjCounter()).add(1)

                self.title_with_nonf_chars.setdefault(to_add[int(title_245_raw_ind[1]):],
                                                      set()).add(to_add)

            except ValueError as err:
                self.titles245.setdefault(lang_008, {}).setdefault(to_add,
                                                                   ObjCounter()).add(1)

                self.title_with_nonf_chars.setdefault(to_add,
                                                      set()).add(to_add)

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

        list_val_246_title_orig = postprocess(normalize_title, list_val_246_title_orig)
        lang_041_h = get_values_by_field_and_subfield(pymarc_object, ('041', ['h']))

        if len(lang_041_h) == 1 and len(list_val_246_title_orig) == 1:
            self.titles246_title_orig.setdefault(lang_041_h[0], {}).setdefault(list_val_246_title_orig[0],
                                                                               ObjCounter()).add(1)

        list_val_246_other = postprocess(normalize_title, list_val_246_other)
        for val in list_val_246_other:
            self.titles246_title_other.setdefault(val, ObjCounter()).add(1)

        # get title from 240 field
        title_240_raw_list = pymarc_object.get_fields('240')
        if title_240_raw_list:
            title_240_raw = title_240_raw_list[0]

            list_val_240 = get_values_by_field_and_subfield(pymarc_object, ('240', ['a', 'b']))

            try:
                self.titles240.add(list_val_240[0][int(title_240_raw.indicators[1]):])

                self.title_with_nonf_chars.setdefault(list_val_240[0][int(title_240_raw.indicators[1]):],
                                                      set()).add(list_val_240[0])
            except ValueError:
                self.titles240.add(list_val_240[0])
                self.title_with_nonf_chars.setdefault(list_val_240[0], set()).add(list_val_240[0])

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

    def get_attributes_from_pymarc_object(self, pymarc_object):
        # get creators and titles
        self.get_main_creator_real_nlp_id(pymarc_object)
        self.get_other_creator_real_nlp_id(pymarc_object)
        self.get_titles(pymarc_object)

        # get simple attributes, without relations to descriptors (literals or codes)
        self.work_udc.update(get_values_by_field_and_subfield(pymarc_object, ('080', ['a'])))
        self.get_language_of_original(pymarc_object)
        self.get_languages(pymarc_object)

        # check if manifestation is catalogued using DBN - if so, get subject and genre data in DBN
        # nlp_ids are used instead of preferred names
        if is_dbn(pymarc_object):

            self.work_subject_person.update(
                get_values_by_field_and_subfield(pymarc_object, ('600', ['0'])))
            self.work_subject_corporate_body.update(
                get_values_by_field_and_subfield(pymarc_object, ('610', ['0'])))
            self.work_subject_event.update(
                get_values_by_field_and_subfield(pymarc_object, ('611', ['0'])))
            self.work_subject.update(
                get_values_by_field_and_subfield(pymarc_object, ('650', ['0'])))
            self.work_subject_place.update(
                get_values_by_field_and_subfield(pymarc_object, ('651', ['0'])))
            self.work_subject_time = []
            self.work_subject_work = []
            self.work_genre.update(
                get_values_by_field_and_subfield(pymarc_object, ('655', [['0']])))

        # get other data related to descriptors
        self.work_subject_domain.update(
            get_values_by_field_and_subfield(pymarc_object, ('658', ['0'])))
        self.work_form.update(
            get_values_by_field_and_subfield(pymarc_object, ('380', ['0'])))
        self.work_cultural_group.update(
            get_values_by_field_and_subfield(pymarc_object, ('386', ['0'])))

        self.work_time_created = []  # todo


