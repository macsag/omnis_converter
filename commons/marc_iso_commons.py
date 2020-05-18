import re
from typing import Union

from pymarc import MARCReader, Record, Field


def read_marc_from_file(file):
    with open(file, 'rb') as fp:
        rdr = MARCReader(fp, to_unicode=True, force_utf8=True, utf8_handling='ignore', permissive=True)
        for rcd in rdr:
            yield rcd


def read_marc_from_binary(data_chunk):
    marc_rdr = MARCReader(data_chunk, to_unicode=True, force_utf8=True, utf8_handling='ignore', permissive=True)
    for rcd in marc_rdr:
        return rcd


def get_values_by_field(marc21_record, field):
    return [v.value() for v in marc21_record.get_fields(field)]


def get_values_by_field_and_subfield(pymarc_record_or_field: Union[Record, Field], field_and_subfields: tuple) -> list:
    """
    Get values by field or by field and subfield or from field or from field and subfield.
    Returns list of values or empty list.
    """
    values_to_return = []
    field, subfields = field_and_subfields[0], field_and_subfields[1]

    if type(pymarc_record_or_field) == Record:
        field, subfields = field_and_subfields[0], field_and_subfields[1]

        if subfields:
            if field in pymarc_record_or_field:
                raw_objects_fields_list = pymarc_record_or_field.get_fields(field)

                for raw_object_field in raw_objects_fields_list:
                    to_append = ' '.join(subfield for subfield in raw_object_field.get_subfields(*subfields))
                    if to_append:
                        values_to_return.append(to_append)
        else:
            if field in pymarc_record_or_field:
                for value in pymarc_record_or_field.get_fields(field):
                    values_to_return.append(value.value())

    else:
        to_append = ' '.join(subfield for subfield in pymarc_record_or_field.get_subfields(*subfields))
        if to_append:
            values_to_return.append(to_append)

    return values_to_return


def truncate_title_proper(value):
    if value[-3:] in ['  /', '  :', '  =', '  .']:
        return value[:-3]
    if value[-2:] in [' /', ' :', ' =', ' .']:
        return value[:-2]
    if value[-1:] in [',', '/', ':', '=', '.']:
        return value[:-1]
    else:
        return value


def truncate_title_from_246(value):
    if value[-1] in [',']:
        return value[:-1]
    if value[-2:] in [', ']:
        return value[:-2]

    match = re.search(r',\s*\d+', value)  # supposedly there is no need to compile pattern, python caches it by default
    if match:
        return value.replace(match.group(0), '')
    else:
        return value


def normalize_title_for_frbr_indexing(title: str):
    match = re.finditer(r'\W', title)
    for m_object in match:
        title = title.replace(m_object.group(0), ' ')
    title = title.replace('   ', ' ').replace('  ', ' ')
    title = title.upper()
    match = re.search(r'^\W+', title)
    if match:
        title = title[match.span(0)[1]:]
    match = re.search(r'\W+$', title)
    if match:
        title = title[:match.span(0)[0]]
    return title


def to_single_value(list_of_values):
    if list_of_values:
        return list_of_values[0]
    else:
        return ''


def normalize_publisher(val):
    return val[:-1] if val[-1] == ',' else val


def normalize_edition_for_matching(value):
    return value.replace(' ', '').replace('.', '').replace('[', '').replace(']', '').replace(',', '')


def get_rid_of_punctuation(value):
    return ''.join(char.replace(',', '').replace('.', '') for char in value)


def postprocess(postprocess_method, list_of_values):
    if list_of_values:
        processed_list_of_values = []

        for value in list_of_values:
            processed_list_of_values.append(postprocess_method(value))

        return processed_list_of_values
    else:
        return list_of_values


def is_dbn(marc_object):
    is_600_dbn = False if get_values_by_field_and_subfield(marc_object, ('600', ['x', 'y', 'z'])) else True
    is_610_dbn = False if get_values_by_field_and_subfield(marc_object, ('610', ['x', 'y', 'z'])) else True
    is_611_dbn = False if get_values_by_field_and_subfield(marc_object, ('611', ['x', 'y', 'z'])) else True
    is_630_dbn = False if get_values_by_field_and_subfield(marc_object, ('630', ['x', 'y', 'z'])) else True
    is_650_dbn = False if get_values_by_field_and_subfield(marc_object, ('650', ['x', 'y', 'z'])) else True
    is_651_dbn = False if get_values_by_field_and_subfield(marc_object, ('651', ['x', 'y', 'z'])) else True
    is_655_dbn = False if get_values_by_field_and_subfield(marc_object, ('655', ['x', 'y', 'z'])) else True

    return True if is_600_dbn and is_610_dbn and is_611_dbn and is_630_dbn and is_650_dbn and is_651_dbn and is_655_dbn else False


def serialize_to_jsonl_descr_creator(subfields_zero_list):
    if subfields_zero_list:
        list_to_return = []
        dict_to_update = {'key': 'Autor', 'value': []}

        for subfield_zero in subfields_zero_list:
            d_id, d_type, d_value = subfield_zero.split('^^')
            dict_to_update['value'].append({'id': int(d_id), 'type': d_type, 'value': d_value})
        list_to_return.append(dict_to_update)
        return list_to_return
    else:
        return subfields_zero_list


def select_number_of_creators(list_of_dicts_of_creators: list, cr_num_start=None, cr_num_end=None):
    if list_of_dicts_of_creators:
        list_to_return = []

        dict_to_update = {'key': 'Autor', 'value': []}
        cr_list = list_of_dicts_of_creators[0].get('value')

        if not cr_num_start and cr_num_end:
            cr_list = cr_list[:cr_num_end]
        if cr_num_start and not cr_num_end:
            cr_list = cr_list[cr_num_start:]
        dict_to_update['value'].extend(cr_list)
        list_to_return.append(dict_to_update)

        return list_to_return
    else:
        return list_of_dicts_of_creators
