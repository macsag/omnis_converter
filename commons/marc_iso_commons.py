from pymarc import MARCReader
import re


def read_marc_from_file(file):
    with open(file, 'rb') as fp:
        rdr = MARCReader(fp, to_unicode=True, force_utf8=True, utf8_handling='ignore')
        for rcd in rdr:
            yield rcd


def read_marc_from_binary(data_chunk):
    marc_rdr = MARCReader(data_chunk, to_unicode=True, force_utf8=True, utf8_handling='ignore')
    for rcd in marc_rdr:
        return rcd


def get_values_by_field(marc21_record, field):
    return [v.value() for v in marc21_record.get_fields(field)]


def get_values_by_field_and_subfield(marc21_record, field_and_subfields):
    values_to_return = []

    field, subfields = field_and_subfields[0], field_and_subfields[1]

    if field in marc21_record:
        raw_objects_fields_list = marc21_record.get_fields(field)

        for raw_object_field in raw_objects_fields_list:
            to_append = ' '.join(subfield for subfield in raw_object_field.get_subfields(*subfields))
            if to_append:
                values_to_return.append(to_append)

    return values_to_return


def truncate_title_proper(value):
    if value[-2:] in [' /', ' :', ' =']:
        return value[:-2]
    if value[-1:] in [',']:
        return value[:-1]
    else:
        return value


def truncate_title_from_246(value):
    if value[-1] in [',']:
        return value[:-1]
    if value[-2:] in [', ']:
        return value[:-2]

    match = re.search(',\s*\d+', value)  # supposedly there is no need to compile pattern, python caches it by default
    if match:
        return value.replace(match.group(0), '')
    else:
        return value


def to_single_value(list_of_values):
    if list_of_values:
        return list_of_values[0]


def normalize_publisher(val):
    return val[:-1] if val[-1] == ',' else val


def get_rid_of_punctuation(value):
    return ''.join(char.replace(',', '').replace('.', '') for char in value)


def prepare_name_for_indexing(value):
    value = ''.join(char.replace('  ', ' ').replace(',', '').replace('.', '') for char in value)
    if value[0] == ' ':
        value = value[1:]
    if value[-1] == ' ':
        value = value[:-1]

    return value


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


def create_jsonlines_like_dict(list_of_values, marc_object, marc_tag):
    if list_of_values:
        helper_dict = {'600' : 'personal_descriptor', '610': 'corporate_descriptor', '611': 'event_descriptor',
                   '650': 'subject_descriptor', '651': 'geographical_descriptor', '655': 'form_descriptor'}



        processed_list_of_values = []

        for value in list_of_values:
            # nlp_id = get_values_by_field_and_subfield(marc_object, (marc_tag, ['0']))[0]
            # es_id = get_es_id(nlp_id)
            processed_list_of_values.append(tuple({'id': 111, 'type': helper_dict.get(marc_tag), 'value': value}))

            return processed_list_of_values
    else:
        return list_of_values


class ObjCounter(object):
    __slots__ = 'count'

    def __init__(self):
        self.count = 0

    def __repr__(self):
        return f'ObjCounter(title_count={self.count}'

    def add(self, number_to_add):
        self.count += number_to_add


def serialize_to_jsonl_descr(subfields_zero_list):
    if subfields_zero_list:
        list_to_return = []
        for subfield_zero in subfields_zero_list:
            d_id, d_type, d_value = subfield_zero.split('^^')
            list_to_return.append({'id': int(d_id), 'type': d_type, 'value': d_value})
        return list_to_return
    else:
        return subfields_zero_list


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


def serialize_to_list_of_values(subfields_zero_list):
    if subfields_zero_list:
        list_to_return = []

        for subfield_zero in subfields_zero_list:
            d_id, d_type, d_value = subfield_zero.split('^^')
            list_to_return.append(d_value)

        return list_to_return
    else:
        return subfields_zero_list
