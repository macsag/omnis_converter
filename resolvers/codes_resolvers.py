def resolve_institution_codes(institutions_list: list,
                              resolver_cache: dict) -> list:

    institution_codes = resolver_cache.get('institution_codes')

    resolved_list = []

    if institution_codes:
        for inst_code in institutions_list:
            resolved_institution = institution_codes.get(inst_code)
            if resolved_institution:
                resolved_list.append(resolved_institution)

    return resolved_list


def resolve_codes_to_names(list_of_codes: list,
                           code_type: str,
                           resolver_cache):
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
                resolved_list.append(resolved_code)

    return resolved_list
