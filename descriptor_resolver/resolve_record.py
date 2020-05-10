from commons.normalization import prepare_name_for_indexing
from exceptions.exceptions import DescriptorNotResolved

FIELDS_TO_CHECK = [('100', ['a', 'b', 'c', 'd']),
                   ('110', ['a', 'b', 'c', 'd', 'n']),
                   ('111', ['a', 'b', 'c', 'd', 'n']),
                   ('700', ['a', 'b', 'c', 'd', 'n', 'p']),
                   ('711', ['a', 'b', 'c', 'd', 'n', 'p'])]





def resolve_record(marc_record, descr_index):
    for marc_field_and_subfields in FIELDS_TO_CHECK:
        fld, subflds = marc_field_and_subfields[0], marc_field_and_subfields[1]

        if fld in marc_record:
            raw_objects_flds_list = marc_record.get_fields(fld)

            for raw_fld in raw_objects_flds_list:
                name_for_prepare = ' '.join(subfld for subfld in raw_fld.get_subfields(*subflds))
                if name_for_prepare:
                    term_to_search = prepare_name_for_indexing(name_for_prepare)
                else:
                    raise DescriptorNotResolved

                if term_to_search in descr_index:
                    descr = descr_index.get(term_to_search)
                    identifier = f'{descr[0]}^^{descr[1]}^^{descr[2]}'

                    marc_record.remove_field(raw_fld)
                    raw_fld.add_subfield('0', identifier)
                    marc_record.add_ordered_field(raw_fld)
                else:
                    #print(term_to_search)
                    raise DescriptorNotResolved

    return marc_record


def resolve_field_value(field_value_list, descr_index):
    if field_value_list:
        list_to_return = []
        for val in field_value_list:
            if val:
                term_to_search = prepare_name_for_indexing(val)

                if term_to_search in descr_index:
                    descr = descr_index.get(term_to_search)
                    val_with_id = f'{descr[0]}^^{descr[1]}^^{descr[2]}'
                    list_to_return.append(val_with_id)

        return list_to_return

    else:
        return field_value_list


def resolve_code(code_list, code_type, code_val_index):
    if code_list:
        list_to_return = []

        for code in code_list:
            if code in code_val_index[code_type]:
                val = code_val_index[code_type].get(code).get('name')
                list_to_return.append(val)

        return list_to_return
    else:
        return code_list


def resolve_code_and_serialize(code_list, code_type, code_val_index):
    if code_list:
        list_to_return = []

        for code in code_list:
            if code in code_val_index[code_type]:
                val = code_val_index[code_type].get(code).get('name')
                c_id = code_val_index[code_type].get(code).get('id')
                list_to_return.append({'id': c_id, 'type': code_type.replace('_dict', ''), 'value': val})

        return list_to_return
    else:
        return code_list


def only_values(resolved_values_list):
    if resolved_values_list:
        list_to_return = []

        for res_val in resolved_values_list:
            d_id, d_type, d_val = res_val.split('^^')
            list_to_return.append(d_val)

        return list_to_return
    else:
        return resolved_values_list
