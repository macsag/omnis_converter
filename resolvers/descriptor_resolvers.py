def resolve_ids_to_names(list_of_ids, resolver_cache):
    descriptors = resolver_cache.get('descriptors')

    resolved_list = []

    if descriptors:
        for descr_nlp_id in list_of_ids:
            resolved_descr = descriptors.get(descr_nlp_id)
            if resolved_descr:
                resolved_list.append(resolved_descr.get('value'))

    return resolved_list
