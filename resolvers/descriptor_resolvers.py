def resolve_ids_to_names(list_of_ids, resolver_cache):
    descriptors = resolver_cache.get('descriptors')

    resolved_list = []

    if descriptors:
        for descr_nlp_id in list_of_ids:
            resolved_descr = descriptors.get(descr_nlp_id)
            if resolved_descr:
                resolved_list.append(resolved_descr.get('value'))

    return resolved_list


def resolve_ids_to_dict_objects(list_of_ids, resolver_cache):
    descriptors = resolver_cache.get('descriptors')

    resolved_list = []

    if descriptors:
        for descr_nlp_id in list_of_ids:
            resolved_descr = descriptors.get(descr_nlp_id)
            if resolved_descr:
                resolved_list.append(resolved_descr)

    return resolved_list


def resolve_ids_to_dict_objects_main_creator(subfields_zero_list):
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
