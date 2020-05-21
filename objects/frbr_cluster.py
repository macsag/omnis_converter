from __future__ import annotations

import pickle
from typing import Union, List
from uuid import uuid4
from hashlib import sha1

import redis
from pymarc import Record

import exceptions.exceptions as oe

from commons.marc_iso_commons import get_values_by_field_and_subfield, get_values_by_field, postprocess
from commons.validators import is_number_of_1xx_fields_valid, is_field_245_valid, is_fields_1xx_present
from commons.normalization import prepare_name_for_indexing, normalize_title

from objects.expression import FRBRExpression
from objects.manifestation import FRBRManifestation
from objects.item import FRBRItem
from objects.helper_objects import ObjCounter

from objects.work_data import WorkData
from objects.expression_data import ExpressionData


class FRBRCluster(object):
    """
    FRBRCluster class represents...
    """
    __slots__ = ['uuid',
                 'main_creator',
                 'other_creator',
                 'titles',
                 'main_creator_nlp_id',
                 'other_creator_nlp_id',
                 'work_match_data_sha_1_nlp_id',
                 'work_match_data_sha_1',
                 'expression_distinctive_tuple_from_original_raw_record',
                 'expression_match_data_sha_1',
                 'expression_match_data_sha_1_nlp_id',
                 'expressions_by_distinctive_tuple',
                 'expression_distinctive_tuple_from_original_raw_record_nlp_id',
                 'manifestation_from_original_raw_record',
                 'original_raw_record_id',
                 'work_data_by_raw_record_id',
                 'expressions',
                 'manifestations_by_raw_record_id',
                 'expressions_by_raw_record_id',
                 'expression_data_from_original_raw_record',
                 'items_by_institution_code',
                 'stub']

    def __init__(self):
        self.uuid = str(uuid4())

        # data for frbrization process
        # work matching
        self.main_creator = {}
        self.other_creator = {}
        self.titles = {}

        self.main_creator_nlp_id = {}
        self.other_creator_nlp_id = {}

        self.work_match_data_sha_1 = None
        self.work_match_data_sha_1_nlp_id = None

        # expression matching
        self.expression_distinctive_tuple_from_original_raw_record = None
        self.expression_distinctive_tuple_from_original_raw_record_nlp_id = None

        self.expression_match_data_sha_1 = None
        self.expression_match_data_sha_1_nlp_id = None

        self.expressions_by_distinctive_tuple = {}

        # manifestation creation
        self.manifestation_from_original_raw_record = None
        self.original_raw_record_id = None

        # work data by raw record id
        self.work_data_by_raw_record_id = {}

        # children
        self.expressions = {}

        # helper attributes
        self.manifestations_by_raw_record_id = {}
        self.expressions_by_raw_record_id = {}
        self.expression_data_from_original_raw_record = None
        self.items_by_institution_code = {}
        self.stub = True

    def __repr__(self):
        # creator = first most frequent main_creator or first other_creator
        # title = first most frequent title
        return f'Work(id={self.uuid}, ' \
               f'creator={self.main_creator if self.main_creator else self.other_creator}, ' \
               f'title={list(self.titles.keys())[0]})'

    # 3.1.1
    # DEPRECATED
    def get_main_creator(self, bib_object):
        # get lists of 1XX fields from parsed raw record
        list_val_100abcd = get_values_by_field_and_subfield(bib_object, ('100', ['a', 'b', 'c', 'd']))
        list_val_110abcdn = get_values_by_field_and_subfield(bib_object, ('110', ['a', 'b', 'c', 'd', 'n']))
        list_val_111abcdn = get_values_by_field_and_subfield(bib_object, ('111', ['a', 'b', 'c', 'd', 'n']))

        # validate number of 1XX fields in parsed raw record record and raise exception if record is not valid
        # exception is handled at the frbrizer level
        is_number_of_1xx_fields_valid(list_val_100abcd, list_val_110abcdn, list_val_111abcdn)

        # 3.1.1.1 - there is 1XX field
        if list_val_100abcd or list_val_110abcdn or list_val_111abcdn:
            if list_val_100abcd:
                self.main_creator.setdefault(prepare_name_for_indexing(list_val_100abcd[0]), ObjCounter()).add(1)
            if list_val_110abcdn:
                self.main_creator.setdefault(prepare_name_for_indexing(list_val_110abcdn[0]), ObjCounter()).add(1)
            if list_val_111abcdn:
                self.main_creator.setdefault(prepare_name_for_indexing(list_val_111abcdn[0]), ObjCounter()).add(1)

        # 3.1.1.2 - if there is no 1XX field, check for 7XX [CHECKED]
        else:
            list_val_700abcd = set()
            list_val_710abcdn = set()
            list_val_711abcdn = set()

            main_creators_to_add = set()

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

            main_creators_to_add.update(postprocess(prepare_name_for_indexing, list_val_700abcd))

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

            main_creators_to_add.update(postprocess(prepare_name_for_indexing, list_val_710abcdn))

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

            main_creators_to_add.update(postprocess(prepare_name_for_indexing, list_val_711abcdn))

            for main_creator in main_creators_to_add:
                self.main_creator.setdefault(main_creator, ObjCounter()).add(1)

    # preferred method to use for frbrizing - thanks to nlp_id usage, when main_creator name changes,
    # FRBRCluster produced from updated record can still be matched with the existing one - nlp_id does not change
    def get_main_creator_nlp_id(self, pymarc_object: Record):
        # get lists of 1XX fields from parsed raw record
        list_val_100_0 = get_values_by_field_and_subfield(pymarc_object, ('100', ['0']))
        list_val_110_0 = get_values_by_field_and_subfield(pymarc_object, ('110', ['0']))
        list_val_111_0 = get_values_by_field_and_subfield(pymarc_object, ('111', ['0']))

        # validate number of 1XX fields in parsed raw record record and raise exception if record is not valid
        # exception is handled at the analyzer level
        is_number_of_1xx_fields_valid(list_val_100_0, list_val_110_0, list_val_111_0)

        if not list_val_100_0 and not list_val_110_0 and not list_val_111_0 and is_fields_1xx_present(pymarc_object):
            raise oe.DescriptorNotResolved

        # 3.1.1.1 - there is 1XX field
        if list_val_100_0 or list_val_110_0 or list_val_111_0:
            if list_val_100_0:
                self.main_creator_nlp_id.setdefault(list_val_100_0[0], ObjCounter()).add(1)
            if list_val_110_0:
                self.main_creator_nlp_id.setdefault(list_val_110_0[0], ObjCounter()).add(1)
            if list_val_111_0:
                self.main_creator_nlp_id.setdefault(list_val_111_0[0], ObjCounter()).add(1)


        # 3.1.1.2 - if there is no 1XX field, check for 7XX
        else:
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
                self.main_creator_nlp_id.setdefault(main_creator, ObjCounter()).add(1)

    # 3.1.2
    # DEPRECATED
    def get_other_creator(self, bib_object):
        if not self.main_creator:
            list_val_700abcd = set()
            list_val_710abcdn = set()
            list_val_711abcdn = set()

            other_creators_to_add = set()

            list_700_fields = bib_object.get_fields('700')
            if list_700_fields:
                for field in list_700_fields:
                    e_subflds = field.get_subfields('e')
                    if e_subflds:
                        e_sub_joined = ' '.join(e_sub for e_sub in e_subflds)
                        if 'Red' in e_sub_joined or 'Oprac' in e_sub_joined or 'Wybór' in e_sub_joined:
                            list_val_700abcd.add(
                                ' '.join(subfld for subfld in field.get_subfields('a', 'b', 'c', 'd')))

            other_creators_to_add.update(postprocess(prepare_name_for_indexing, list_val_700abcd))

            list_710_fields = bib_object.get_fields('710')
            if list_710_fields:
                for field in list_710_fields:
                    e_subflds = field.get_subfields('e')
                    if e_subflds:
                        e_sub_joined = ' '.join(e_sub for e_sub in e_subflds)
                        if 'Red' in e_sub_joined or 'Oprac' in e_sub_joined or 'Wybór' in e_sub_joined:
                            list_val_710abcdn.add(
                                ' '.join(subfld for subfld in field.get_subfields('a', 'b', 'c', 'd', 'n')))

            other_creators_to_add.update(postprocess(prepare_name_for_indexing, list_val_710abcdn))

            list_711_fields = bib_object.get_fields('711')
            if list_711_fields:
                for field in list_711_fields:
                    j_subflds = field.get_subfields('j')
                    if j_subflds:
                        j_sub_joined = ' '.join(j_sub for j_sub in j_subflds)
                        if 'Red' in j_sub_joined or 'Oprac' in j_sub_joined or 'Wybór' in j_sub_joined:
                            list_val_711abcdn.add(
                                ' '.join(subfld for subfld in field.get_subfields('a', 'b', 'c', 'd', 'n')))

            other_creators_to_add.update(postprocess(prepare_name_for_indexing, list_val_711abcdn))

            for other_creator in other_creators_to_add:
                self.other_creator.setdefault(other_creator, ObjCounter()).add(1)

    # preferred method to use for frbrizing - thanks to nlp_id usage, when main_creator name changes,
    # FRBRCluster produced from updated record can still be matched with the existing one - nlp_id does not change
    def get_other_creator_nlp_id(self, pymarc_object: Record) -> None:
        if not self.main_creator_nlp_id:
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
                self.other_creator_nlp_id.setdefault(other_creator, ObjCounter()).add(1)

    # 3.1.3
    # to be able to track changes on merging, splitting and deleting FRBR clusters,
    # unique titles can be added only once per raw record (counter.count for each NEW title after merge must == 1)
    def get_titles(self, bib_object):
        titles_to_add = set()

        # get list of raw 245 fields
        titles_245_raw = bib_object.get_fields('245')

        # validate record
        is_field_245_valid(titles_245_raw)

        # get titles from 245 field
        list_val_245ab = postprocess(normalize_title, get_values_by_field_and_subfield(bib_object,
                                                                                             ('245', ['a', 'b'])))
        title_245_raw_ind = titles_245_raw[0].indicators
        list_val_245a = postprocess(normalize_title, get_values_by_field_and_subfield(bib_object,
                                                                                            ('245', ['a'])))
        val_245a_last_char = get_values_by_field_and_subfield(bib_object, ('245', ['a']))[0].strip()[-1]
        list_val_245p = postprocess(normalize_title, get_values_by_field_and_subfield(bib_object, ('245', ['p'])))

        if val_245a_last_char == '=' and not list_val_245p:
            to_add = prepare_name_for_indexing(list_val_245a[0])

            try:
                titles_to_add.add(to_add[int(title_245_raw_ind[1]):])
            except ValueError:
                pass
            titles_to_add.add(to_add)

        if list_val_245p:
            # add only title from last subfield |p
            to_add = prepare_name_for_indexing(list_val_245p[-1])

            titles_to_add.add(to_add)

        if val_245a_last_char != '=' and not list_val_245p:
            to_add = prepare_name_for_indexing(list_val_245ab[0])

            try:
                titles_to_add.add(to_add[int(title_245_raw_ind[1]):])
            except ValueError:
                pass
            titles_to_add.add(to_add)

        # get titles from 246 fields
        list_fields_246ab = postprocess(normalize_title, get_values_by_field_and_subfield(bib_object,
                                                                                          ('246', ['a', 'b'])))
        for title in list_fields_246ab:
            titles_to_add.add(prepare_name_for_indexing(title))

        # get title from 240 field
        title_240_raw_list = bib_object.get_fields('240')
        if title_240_raw_list:
            title_240_raw = title_240_raw_list[0]

            list_val_240 = postprocess(normalize_title,
                                       get_values_by_field_and_subfield(bib_object, ('240', ['a', 'b'])))

            try:
                titles_to_add.add(prepare_name_for_indexing(list_val_240[0][int(title_240_raw.indicators[1]):]))
            except ValueError:
                pass

            titles_to_add.add(prepare_name_for_indexing(list_val_240[0]))

        # finally add all unique titles from single raw record with counters
        for title in titles_to_add:
            self.titles.setdefault(title, ObjCounter()).add(1)

    # DEPRECATED
    def get_expression_distinctive_tuple(self, bib_object, is_multiwork=False):
        if not is_multiwork:
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

            expression_distinctive_tuple = (expr_lang, frozenset(translators), ldr6)

            self.expression_distinctive_tuple_from_original_raw_record = expression_distinctive_tuple
        else:
            # TODO get expressions when there is more than one work in raw record
            pass

    def get_expression_distinctive_tuple_nlp_id(self, pymarc_object: Record, is_multiwork: bool = False) -> None:
        if not is_multiwork:
            list_fields_700 = pymarc_object.get_fields('700')
            list_fields_710 = pymarc_object.get_fields('710')
            list_fields_711 = pymarc_object.get_fields('711')

            translators = set()

            if list_fields_700:
                for field in list_fields_700:
                    e_subflds = field.get_subfields('e')
                    if e_subflds:
                        e_sub_joined = ' '.join(e_sub for e_sub in e_subflds)
                        if 'Tł' in e_sub_joined or 'Tł.' in e_sub_joined or 'Tłumaczenie' in e_sub_joined:
                            value_to_add = field.get_subfields('0')
                            if value_to_add:
                                translators.add(value_to_add[0])
                            else:
                                raise oe.DescriptorNotResolved

            if list_fields_710:
                for field in list_fields_710:
                    e_subflds = field.get_subfields('e')
                    if e_subflds:
                        e_sub_joined = ' '.join(e_sub for e_sub in e_subflds)
                        if 'Tł' in e_sub_joined or 'Tł.' in e_sub_joined or 'Tłumaczenie' in e_sub_joined:
                            value_to_add = field.get_subfields('0')
                            if value_to_add:
                                translators.add(value_to_add[0])
                            else:
                                raise oe.DescriptorNotResolved

            if list_fields_711:
                for field in list_fields_711:
                    j_subflds = field.get_subfields('j')
                    if j_subflds:
                        j_sub_joined = ' '.join(j_sub for j_sub in j_subflds)
                        if 'Tł' in j_sub_joined or 'Tł.' in j_sub_joined or 'Tłumaczenie' in j_sub_joined:
                            value_to_add = field.get_subfields('0')
                            if value_to_add:
                                translators.add(value_to_add[0])
                            else:
                                raise oe.DescriptorNotResolved

            expr_lang = get_values_by_field(pymarc_object, '008')[0][35:38]
            ldr6 = pymarc_object.leader[6]

            expression_distinctive_tuple = (expr_lang, frozenset(sorted(translators)), ldr6)

            self.expression_distinctive_tuple_from_original_raw_record_nlp_id = expression_distinctive_tuple
        else:
            # TODO get expressions when there is more than one work in raw record
            pass

    # DEPRECATED
    def get_sha_1_of_work_match_data(self):
        work_match_data_byte_array = bytearray()
        work_match_data_byte_array.extend(repr(sorted(self.main_creator.keys())).encode('utf-8'))
        work_match_data_byte_array.extend(repr(sorted(self.other_creator.keys())).encode('utf-8'))
        work_match_data_byte_array.extend(repr(sorted(self.titles.keys())).encode('utf-8'))

        self.work_match_data_sha_1 = sha1(work_match_data_byte_array).hexdigest()

    def get_sha_1_of_work_match_data_nlp_id(self):
        work_match_data_byte_array = bytearray()
        work_match_data_byte_array.extend(repr(sorted(self.main_creator_nlp_id.keys())).encode('utf-8'))
        work_match_data_byte_array.extend(repr(sorted(self.other_creator_nlp_id.keys())).encode('utf-8'))
        work_match_data_byte_array.extend(repr(sorted(self.titles.keys())).encode('utf-8'))

        self.work_match_data_sha_1_nlp_id = sha1(work_match_data_byte_array).hexdigest()

    # DEPRECATED
    def get_sha_1_of_expression_match_data(self):
        expression_match_data_byte_array = bytearray()
        expression_match_data_byte_array.extend(
            repr(self.expression_distinctive_tuple_from_original_raw_record).encode('utf-8'))

        self.expression_match_data_sha_1 = sha1(expression_match_data_byte_array).hexdigest()

    def get_sha_1_of_expression_match_data_nlp_id(self):
        expression_match_data_byte_array = bytearray()
        expression_match_data_byte_array.extend(
            repr(self.expression_distinctive_tuple_from_original_raw_record_nlp_id).encode('utf-8'))

        self.expression_match_data_sha_1_nlp_id = sha1(expression_match_data_byte_array).hexdigest()

    def check_changes_in_match_data(self, frbr_cluster_match_info):
        pass

    def get_raw_record_id(self, bib_object):
        self.original_raw_record_id = bib_object.get_fields('001')[0].value()

    def create_manifestation(self, pymarc_object):
        self.manifestation_from_original_raw_record = FRBRManifestation(self.original_raw_record_id, pymarc_object)

    def create_expression_and_add_manifestation(self):
        expression_to_add = FRBRExpression(self.expression_distinctive_tuple_from_original_raw_record_nlp_id,
                                           self.expression_match_data_sha_1_nlp_id,
                                           self.expression_data_from_original_raw_record)

        self.expressions_by_distinctive_tuple.setdefault(self.expression_distinctive_tuple_from_original_raw_record,
                                                         expression_to_add.uuid)
        self.expressions.setdefault(expression_to_add.uuid,
                                    expression_to_add).manifestations.setdefault(self.manifestation_from_original_raw_record.uuid,
                                                                                 self.manifestation_from_original_raw_record.raw_record_id)
        self.expressions_by_raw_record_id.setdefault(
            self.manifestation_from_original_raw_record.raw_record_id,
            self.expressions_by_distinctive_tuple.get(
                self.expression_distinctive_tuple_from_original_raw_record))

    def get_work_data_from_single_work_record(self, pymarc_object):
        self.work_data_by_raw_record_id.setdefault(self.original_raw_record_id, WorkData(self, pymarc_object))

    def get_expression_data_from_single_work_record(self, pymarc_object):
        self.expression_data_from_original_raw_record = ExpressionData(self, pymarc_object)

    def merge_frbr_clusters_and_reindex(self,
                                        matched_clusters_to_merge_with: List[FRBRCluster],
                                        indexed_frbr_clusters_by_uuid: Union[dict, redis.Redis],
                                        indexed_frbr_clusters_by_titles: Union[dict, redis.Redis],
                                        indexed_frbr_clusters_by_raw_record_id: Union[dict, redis.Redis]) -> set:

        matched_frbr_cluster = matched_clusters_to_merge_with[0]

        expressions_to_delete_from_es = set()

        # perform these actions only if merging stub with existing frbr_cluster
        # in this case stub is never indexed and dies after merging
        if self.stub:
            self.merge_titles(matched_frbr_cluster)
            self.merge_manifestations_by_original_raw_id(matched_frbr_cluster)
            self.merge_children(matched_frbr_cluster)
            self.merge_work_data_by_raw_record_id(matched_frbr_cluster,
                                                  indexed_frbr_clusters_by_raw_record_id)
            matched_frbr_cluster.index_frbr_cluster_by_titles(indexed_frbr_clusters_by_titles)

            if len(matched_clusters_to_merge_with) > 1:
                matched_frbr_cluster.merge_frbr_clusters_and_reindex(matched_clusters_to_merge_with[1:],
                                                                     indexed_frbr_clusters_by_uuid,
                                                                     indexed_frbr_clusters_by_titles,
                                                                     indexed_frbr_clusters_by_raw_record_id)

        # perform these actions only if merging two or more existing frbr_clusters
        # in this case all matched frbr_clusters will be merged with last frbr_cluster on the list
        # and will die after that (they will be unindexed and deleted from the main index)
        else:
            self.unindex_frbr_cluster_by_titles_before_merge(indexed_frbr_clusters_by_titles)
            self.merge_titles(matched_frbr_cluster)
            self.merge_manifestations_by_original_raw_id(matched_frbr_cluster)
            expressions_to_delete_from_merging_children = self.merge_children(matched_frbr_cluster)
            expressions_to_delete_from_es.update(expressions_to_delete_from_merging_children)
            self.merge_work_data_by_raw_record_id(matched_frbr_cluster,
                                                  indexed_frbr_clusters_by_raw_record_id)

            if not len(matched_clusters_to_merge_with) > 1:
                matched_frbr_cluster.index_frbr_cluster_by_titles(indexed_frbr_clusters_by_titles)

            if type(indexed_frbr_clusters_by_uuid) == dict:
                indexed_frbr_clusters_by_uuid.pop(self.uuid)
            else:
                indexed_frbr_clusters_by_uuid.delete(self.uuid)

            if len(matched_clusters_to_merge_with) > 1:
                expressions_to_delete_from_es_from_recursion = matched_frbr_cluster.merge_frbr_clusters_and_reindex(
                    matched_clusters_to_merge_with[1:],
                    indexed_frbr_clusters_by_uuid,
                    indexed_frbr_clusters_by_titles,
                    indexed_frbr_clusters_by_raw_record_id)
                expressions_to_delete_from_es.update(expressions_to_delete_from_es_from_recursion)

        if type(indexed_frbr_clusters_by_uuid) != dict:
            matched_frbr_cluster = matched_clusters_to_merge_with[-1]
            indexed_frbr_clusters_by_uuid.set(matched_frbr_cluster.uuid, pickle.dumps(matched_frbr_cluster))

        return expressions_to_delete_from_es

    def merge_titles(self, matched_frbr_cluster: FRBRCluster) -> None:
        for title, counter in self.titles.items():
            matched_frbr_cluster.titles.setdefault(title, ObjCounter()).add(counter.count)

    def merge_manifestations_by_original_raw_id(self,
                                                matched_frbr_cluster: FRBRCluster,
                                                ) -> None:
        matched_frbr_cluster.manifestations_by_raw_record_id.update(self.manifestations_by_raw_record_id)

    def merge_work_data_by_raw_record_id(self,
                                         matched_frbr_cluster: FRBRCluster,
                                         indexed_frbr_clusters_by_raw_record_id: Union[dict, redis.Redis]) -> None:

        for raw_record_id, work_data in self.work_data_by_raw_record_id.items():
            matched_frbr_cluster.work_data_by_raw_record_id.update({raw_record_id: work_data})
            matched_frbr_cluster.index_frbr_cluster_by_raw_record_id_single(raw_record_id,
                                                                            indexed_frbr_clusters_by_raw_record_id)

    def merge_children(self,
                       matched_frbr_cluster: FRBRCluster) -> set:

        expressions_to_delete_from_es = set()

        # merge strategy for merging two already existing frbr_clusters
        if not self.stub:

            # iterate over expressions_by_distinctive_tuple
            for expression_distinctive_tuple, expression_uuid in self.expressions_by_distinctive_tuple.items():

                # check if expression from existing frbr_cluster already exists in existing matched frbr_cluster
                expression_to_merge_with_uuid = matched_frbr_cluster.expressions_by_distinctive_tuple.get(
                    expression_distinctive_tuple)

                # expression already exists
                # move manifestations to existing expression in existing matched frbr_cluster
                if expression_to_merge_with_uuid:
                    expression_to_merge_with_object = matched_frbr_cluster.expressions.get(
                        expression_to_merge_with_uuid)

                    for manifestation_uuid, manifestation_raw_record_id in self.expressions.get(
                            expression_uuid).manifestations.items():

                        # move manifestations
                        expression_to_merge_with_object.manifestations.setdefault(
                            manifestation_uuid,
                            manifestation_raw_record_id)

                        # create entries in lookup table
                        matched_frbr_cluster.expressions_by_raw_record_id.setdefault(
                            manifestation_raw_record_id,
                            expression_to_merge_with_object.uuid)

                    # get expression uuid to delete
                    expressions_to_delete_from_es.add(expression_uuid)

                    # move expression_data_by_raw_id
                    for raw_id, expression_data in self.expressions.get(
                            expression_uuid).expression_data_by_raw_record_id.items():
                        expression_to_merge_with_object.expression_data_by_raw_record_id.setdefault(
                            raw_id,
                            expression_data)

                # expression does not exist
                # move existing expression with manifestations to existing matched frbr_cluster
                else:
                    expression_to_move = self.expressions.get(expression_uuid)

                    matched_frbr_cluster.expressions.update(
                        {expression_uuid: expression_to_move})

                    # create entries in lookup tables
                    matched_frbr_cluster.expressions_by_distinctive_tuple.update(
                        {expression_distinctive_tuple: expression_uuid})

                    for manifestation_raw_record_id in expression_to_move.manifestations.values():
                        matched_frbr_cluster.expressions_by_raw_record_id.setdefault(manifestation_raw_record_id,
                                                                                     expression_to_move.uuid)

        # merge strategy for merging stub frbr_cluster with existing matched frbr_cluster
        else:
            # check if expression from stub already exists in existing matched frbr_cluster
            expression_to_merge_with_uuid = matched_frbr_cluster.expressions_by_distinctive_tuple.get(
                self.expression_distinctive_tuple_from_original_raw_record)

            # expression already exists
            # append new manifestation to existing expression in existing matched frbr_cluster
            if expression_to_merge_with_uuid:
                expression_to_merge_with_object = matched_frbr_cluster.expressions.get(
                    expression_to_merge_with_uuid)

                # append new manifestation
                expression_to_merge_with_object.manifestations.setdefault(
                    self.manifestation_from_original_raw_record.uuid,
                    self.manifestation_from_original_raw_record.raw_record_id)

                # append new expression_data
                expression_to_merge_with_object.expression_data_by_raw_record_id.setdefault(
                    self.expression_data_from_original_raw_record.raw_record_id,
                    self.expression_data_from_original_raw_record)

                # create entry in lookup table (used for (re)indexing frbr_cluster by raw_record_id)
                matched_frbr_cluster.expressions_by_raw_record_id.setdefault(
                    self.manifestation_from_original_raw_record.raw_record_id, expression_to_merge_with_object.uuid)

            # expression does not exist
            # create new expression, append new manifestation to the expression
            # and add it to existing matched frbr_cluster
            else:
                self.create_expression_and_add_manifestation()
                matched_frbr_cluster.expressions.update(self.expressions)

                # create entries in lookup tables
                matched_frbr_cluster.expressions_by_distinctive_tuple.update(self.expressions_by_distinctive_tuple)
                matched_frbr_cluster.expressions_by_raw_record_id.setdefault(
                    self.manifestation_from_original_raw_record.raw_record_id,
                    matched_frbr_cluster.expressions_by_distinctive_tuple.get(
                        self.expression_distinctive_tuple_from_original_raw_record))

        return expressions_to_delete_from_es

    # index entire cluster by titles
    # to avoid unnecessary requests (in case of merging) only new (with counter.prev_count == 0) titles are indexed
    # in production dict is replaced by Redis set (SADD)
    def index_frbr_cluster_by_titles(self,
                                     indexed_frbr_clusters_by_titles: Union[dict, redis.Redis]) -> None:
        if type(indexed_frbr_clusters_by_titles) == dict:
            for title, counter in self.titles.items():
                if counter.prev_count == 0:
                    indexed_frbr_clusters_by_titles.setdefault(title, set()).add(self.uuid)
        else:
            p = indexed_frbr_clusters_by_titles.pipeline(transaction=False)
            for title, counter in self.titles.items():
                if counter.prev_count == 0:
                    p.sadd(title, self.uuid)
            p.execute()

    def unindex_frbr_cluster_by_titles_before_merge(self,
                                                    indexed_frbr_clusters_by_titles: Union[dict, redis.Redis]) -> None:
        if type(indexed_frbr_clusters_by_titles) == dict:
            for title in self.titles.keys():
                uuids_by_title = indexed_frbr_clusters_by_titles.get(title)
                uuids_by_title.discard(self.uuid)
        else:
            p = indexed_frbr_clusters_by_titles.pipeline(transaction=False)
            for title in self.titles.keys():
                p.srem(title, self.uuid)
            p.execute()

    def index_frbr_cluster_by_uuid(self, indexed_frbr_clusters_by_uuid: Union[dict, redis.Redis]) -> None:
        if type(indexed_frbr_clusters_by_uuid) == dict:
            indexed_frbr_clusters_by_uuid.setdefault(self.uuid, self)
        else:
            indexed_frbr_clusters_by_uuid.set(self.uuid, pickle.dumps(self))

    # used only for indexing stub frbr_cluster
    def index_frbr_cluster_by_raw_record_ids(self,
                                             indexed_frbr_clusters_by_raw_record_id: Union[dict, redis.Redis]) -> None:
        for expression_uuid, expression in self.expressions.items():
            for manifestation_uuid, manifestation_raw_record_id in expression.manifestations.items():
                update_data = {'work_match_data':
                                   {self.work_data_by_raw_record_id.get(
                                       manifestation_raw_record_id).work_match_data_sha_1: self.uuid},
                               'expression_match_data':
                                   {expression.expression_match_data_sha_1: expression_uuid},
                               'manifestation_match_data':
                                   {self.manifestation_from_original_raw_record.manifestation_match_data_sha_1:
                                    manifestation_uuid}}

                if type(indexed_frbr_clusters_by_raw_record_id) == dict:
                    indexed_frbr_clusters_by_raw_record_id[manifestation_raw_record_id] = update_data
                else:
                    indexed_frbr_clusters_by_raw_record_id.set(manifestation_raw_record_id,
                                                               pickle.dumps(update_data))

    def index_frbr_cluster_by_raw_record_id_single(self,
                                                   raw_record_id,
                                                   indexed_frbr_clusters_by_raw_record_id: Union[dict, redis.Redis]):
        expression = self.expressions.get(self.expressions_by_raw_record_id.get(raw_record_id))
        manifestation = self.manifestations_by_raw_record_id.get(raw_record_id)

        update_data = {'work_match_data':
                           {self.work_data_by_raw_record_id.get(
                               raw_record_id).work_match_data_sha_1: self.uuid},
                       'expression_match_data':
                           {expression.expression_match_data_sha_1: expression.uuid},
                       'manifestation_match_data':
                           {manifestation.get('manifestation_match_data_sha_1'): manifestation.get('uuid')}}

        if type(indexed_frbr_clusters_by_raw_record_id) == dict:
            to_update_data = indexed_frbr_clusters_by_raw_record_id.get(raw_record_id)

            if to_update_data:
                for match_data_type, match_data in update_data.items():
                    for sha_1, uuid in match_data.items():
                        mt = to_update_data.get(match_data_type)
                        mt[sha_1] = uuid
            else:
                indexed_frbr_clusters_by_raw_record_id[raw_record_id] = update_data


        else:
            to_update_raw = indexed_frbr_clusters_by_raw_record_id.get(raw_record_id)
            if to_update_raw:
                to_update_unpickled = pickle.loads(to_update_raw)

                for match_data_type, match_data in update_data.items():
                    for sha_1, uuid in match_data.items():
                        mt = to_update_unpickled.get(match_data_type)
                        mt[sha_1] = uuid

                updated_pickled = pickle.dumps(to_update_unpickled)
                indexed_frbr_clusters_by_raw_record_id.set(raw_record_id, updated_pickled)
            else:
                update_data_pickled = pickle.dumps(update_data)
                indexed_frbr_clusters_by_raw_record_id.set(raw_record_id, update_data_pickled)

    def set_manifestation_by_raw_record_id_based_on_original_raw_record(self):
        self.manifestations_by_raw_record_id.setdefault(
            self.original_raw_record_id, {}).update(
            {'uuid': self.manifestation_from_original_raw_record.uuid,
             'manifestation_match_data_sha_1': self.manifestation_from_original_raw_record.manifestation_match_data_sha_1})

    def create_items(self, pymarc_object, item_conversion_table):
        self.items_by_institution_code = FRBRItem.get_items(pymarc_object, self.original_raw_record_id, item_conversion_table)

    def append_items_to_manifestation(self):
        self.manifestation_from_original_raw_record.items_by_institution_code = self.items_by_institution_code

    def match_work(self,
                   indexed_frbr_clusters_by_uuid: Union[dict, redis.Redis],
                   indexed_frbr_clusters_by_titles: Union[dict, redis.Redis]):

        if type(indexed_frbr_clusters_by_titles) == dict:
            candidate_clusters = set()
            matched_clusters = set()

            for title in self.titles.keys():
                candidates_per_title = indexed_frbr_clusters_by_titles.get(title)
                if candidates_per_title:
                    candidate_clusters.update(candidates_per_title)

            for candidate_uuid in candidate_clusters:
                candidate_cluster = indexed_frbr_clusters_by_uuid.get(candidate_uuid)
                if candidate_cluster:
                    if candidate_cluster.main_creator:
                        if candidate_cluster.main_creator.keys() == self.main_creator.keys():
                            matched_clusters.add(candidate_uuid)
                    if candidate_cluster.other_creator:
                        if candidate_cluster.other_creator.keys() == self.other_creator.keys():
                            matched_clusters.add(candidate_uuid)
                    if not candidate_cluster.main_creator and not candidate_cluster.other_creator \
                            and not self.main_creator and not self.other_creator:
                        matched_clusters.add(candidate_uuid)

            matched_clusters = list(matched_clusters)
            return matched_clusters

        else:
            candidate_clusters_uuids = set()
            matched_clusters = set()

            candidates_per_title = indexed_frbr_clusters_by_titles.sunion(list(self.titles.keys()))
            for candidate in candidates_per_title:
                if candidate:
                    candidate_clusters_uuids.add(candidate.decode('utf-8'))

            candidate_clusters_objects = indexed_frbr_clusters_by_uuid.mget(list(candidate_clusters_uuids))

            for candidate in candidate_clusters_objects:
                if candidate:
                    unpickled_candidate = pickle.loads(candidate)
                    if unpickled_candidate.main_creator:
                        if unpickled_candidate.main_creator.keys() == self.main_creator.keys():
                            matched_clusters.add(unpickled_candidate)
                    if unpickled_candidate.other_creator:
                        if unpickled_candidate.other_creator.keys() == self.other_creator.keys():
                            matched_clusters.add(unpickled_candidate)
                    if not unpickled_candidate.main_creator and not unpickled_candidate.other_creator \
                            and not self.main_creator and not self.other_creator:
                        matched_clusters.add(unpickled_candidate)

            matched_clusters = list(matched_clusters)
            matched_clusters.sort(key=lambda x: len(x.manifestations_by_raw_record_id), reverse=True)
            return matched_clusters

    def match_work_and_index(self,
                             indexed_frbr_clusters_by_uuid: Union[dict, redis.Redis],
                             indexed_frbr_clusters_by_titles: Union[dict, redis.Redis],
                             indexed_frbr_clusters_by_raw_record_id: Union[dict, redis.Redis],
                             indexed_manifestations_by_uuid: Union[dict, redis.Redis],
                             pymarc_object,
                             item_conversion_table) -> tuple:

        matched_clusters = self.match_work(indexed_frbr_clusters_by_uuid,
                                           indexed_frbr_clusters_by_titles)

        self.create_manifestation(pymarc_object)
        self.create_items(pymarc_object, item_conversion_table)
        self.append_items_to_manifestation()
        self.index_manifestation(indexed_manifestations_by_uuid)
        self.set_manifestation_by_raw_record_id_based_on_original_raw_record()

        # no matches for this FRBRCluster stub
        if not matched_clusters:
            self.stub = False
            self.create_expression_and_add_manifestation()

            self.index_frbr_cluster_by_titles(indexed_frbr_clusters_by_titles)
            self.index_frbr_cluster_by_raw_record_ids(indexed_frbr_clusters_by_raw_record_id)
            self.index_frbr_cluster_by_uuid(indexed_frbr_clusters_by_uuid)

            return [self.uuid], list()

        # there are one or more matches for this FRBRCluster stub
        else:
            print(f"Matching! - matched clusters = {len(matched_clusters)}. - {matched_clusters}")

            if type(indexed_frbr_clusters_by_uuid) == dict:
                matched_clusters_to_merge_with = [indexed_frbr_clusters_by_uuid.get(
                    matched_cluster) for matched_cluster in matched_clusters]
            else:
                matched_clusters_to_merge_with = matched_clusters

            expressions_to_delete_from_es = self.merge_frbr_clusters_and_reindex(
                matched_clusters_to_merge_with,
                indexed_frbr_clusters_by_uuid,
                indexed_frbr_clusters_by_titles,
                indexed_frbr_clusters_by_raw_record_id)

            return [cluster.uuid for cluster in matched_clusters_to_merge_with], list(expressions_to_delete_from_es)

    def index_manifestation(self, indexed_manifestations_by_uuid: Union[dict, redis.Redis]) -> None:
        if type(indexed_manifestations_by_uuid) == dict:
            indexed_manifestations_by_uuid[
                self.manifestation_from_original_raw_record.uuid] = self.manifestation_from_original_raw_record
        else:
            indexed_manifestations_by_uuid.set(self.manifestation_from_original_raw_record.uuid,
                                               pickle.dumps(self.manifestation_from_original_raw_record))

    def rebuild_work_and_expression_data(self, new_work_data, new_expression_data):
        # rebuild work_data
        self.work_data_by_raw_record_id[new_work_data.raw_record_id] = new_work_data

        # rebuild expression_data
        expression_to_rebuild_uuid = self.expressions_by_raw_record_id.get(new_expression_data.raw_record_id)
        expression_to_rebuild = self.expressions.get(expression_to_rebuild_uuid)
        expression_to_rebuild.expression_data_by_raw_record_id[new_expression_data.raw_record_id] = new_expression_data
