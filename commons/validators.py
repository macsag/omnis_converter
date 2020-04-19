from pymarc import Record

import config.document_types as conf_dt
from commons.marc_iso_commons import get_values_by_field_and_subfield, get_values_by_field

from exceptions.exceptions import TooMany1xxFields, No245FieldFound, TooMany245Fields


def is_number_of_1xx_fields_valid(list_val_100abcd: list,
                                  list_val_110abcdn: list,
                                  list_val_111abcdn: list) -> None:
    if (len(list_val_100abcd) > 1 or len(list_val_110abcdn) > 1 or len(list_val_111abcdn) > 1) \
            or (list_val_100abcd and list_val_110abcdn and list_val_111abcdn):
        raise TooMany1xxFields


def is_field_245_valid(title_245_raw: list) -> None:
    if len(title_245_raw) > 1:
        raise TooMany245Fields
    if not title_245_raw:
        raise No245FieldFound


# 1
# bib document type validator
def is_document_type(pymarc_object: Record) -> bool:
    val_380a = get_values_by_field_and_subfield(pymarc_object, ('380', ['a']))
    val_ldr67 = pymarc_object.leader[6:8]

    if val_ldr67 in conf_dt.VALUES_LDR67_TO_CHECK:
        for value in conf_dt.VALUES_380A_TO_CHECK:
            if value in val_380a:
                return True
        else:
            return False
    else:
        return False


# 2.1 bib single or multi work analyser
def is_single_or_multi_work(pymarc_object: Record) -> str:
    # each and every record MUST have these fields, if it hasn't, it should be treated as invalid and skipped
    try:
        val_245a_last_char = get_values_by_field_and_subfield(pymarc_object, ('245', ['a']))[0][-1]
        val_245a = get_values_by_field_and_subfield(pymarc_object, ('245', ['a']))[0]
    except IndexError:
        return 'invalid_record'

    list_val_245c = get_values_by_field_and_subfield(pymarc_object, ('245', ['c']))
    val_245c = list_val_245c[0] if list_val_245c else ''

    list_val_245b = get_values_by_field_and_subfield(pymarc_object, ('245', ['b']))
    val_245b = list_val_245b[0] if list_val_245b else ''

    list_val_730 = get_values_by_field(pymarc_object, '730')
    list_val_501 = get_values_by_field(pymarc_object, '501')
    list_val_505 = get_values_by_field(pymarc_object, '505')
    list_val_740 = get_values_by_field(pymarc_object, '740')
    list_val_700t = get_values_by_field_and_subfield(pymarc_object, ('700', ['t']))
    list_val_710t = get_values_by_field_and_subfield(pymarc_object, ('710', ['t']))
    list_val_711t = get_values_by_field_and_subfield(pymarc_object, ('711', ['t']))
    list_val_246i = get_values_by_field_and_subfield(pymarc_object, ('246', ['i']))

    is_2_1_1_1 = val_245a_last_char != ';' and ' ; ' not in val_245a and ' ; ' not in val_245b and ' / 'not in val_245c
    is_2_1_1_2 = True if not list_val_730 or (len(list_val_730) == 1 and 'Katalog wystawy' in list_val_730[0]) else False
    is_2_1_1_3 = True if not list_val_501 and not list_val_505 and not list_val_740 else False
    is_2_1_1_4 = True if not list_val_700t and not list_val_710t and not list_val_711t else False
    is_2_1_1_5 = True if len([x for x in list_val_246i if 'Tyt. oryg.' in x or 'Tytuł oryginału' in x]) < 2 else False

    if is_2_1_1_1 and is_2_1_1_2 and is_2_1_1_3 and is_2_1_1_4 and is_2_1_1_5:
        return 'single_work'
    if not is_2_1_1_1 and (not is_2_1_1_4 or not is_2_1_1_3):
        return 'multi_work_A1'
