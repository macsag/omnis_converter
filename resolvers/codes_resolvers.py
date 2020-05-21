def resolve_institution_codes(institutions_list: list,
                              resolver_cache: dict) -> list:

    institution_codes = resolver_cache.get('institution_codes')

    resolved_list = []

    if institution_codes:
        for inst_code in institutions_list:
            print(inst_code)
            resolved_institution = institution_codes.get(inst_code)
            print(resolved_institution)
            if resolved_institution:
                resolved_list.append(resolved_institution)

    return resolved_list


def resolve_codes_to_names(list_of_codes: list,
                           code_type: str,
                           resolver_cache: dict) -> list:
    resolved_list = []
    codes = None

    if code_type == 'language':
        codes = resolver_cache.get('language_codes')
    if code_type == 'country':
        codes = resolver_cache.get('country')

    if codes:
        for code in list_of_codes:
            resolved_code = codes.get(code)
            if resolved_code:
                resolved_list.append(resolved_code.split('||')[0])

    return resolved_list


def resolve_codes_to_dict_objects(list_of_codes: list,
                                  code_type: str,
                                  resolver_cache: dict):
    resolved_list = []
    codes = None

    if code_type == 'language':
        codes = resolver_cache.get('language_codes')
    if code_type == 'country':
        codes = resolver_cache.get('country')
    if code_type == 'carrier_type':
        codes = resolver_cache.get('carrier_type')

    if codes:
        for code in list_of_codes:
            resolved_code = codes.get(code)
            if resolved_code:
                c_name, c_id = resolved_code.split('||')
                resolved_list.append({'id': c_id,
                                      'type': code_type,
                                      'value': c_name})

    return resolved_list
