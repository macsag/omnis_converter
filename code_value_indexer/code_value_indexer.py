def code_value_indexer(file_in):
    bibliography_list, bibliography_dict = [], {}
    carrier_type_list, carrier_type_dict = [], {}
    content_type_list, content_type_dict = [], {}
    contribution_list, contribution_dict = [], {}
    country_list, country_dict = [], {}
    media_type_list, media_type_dict = [], {}
    publishing_statistics_list, publishing_statistics_dict = [], {}
    topic_category_list, topic_category_dict = [], {}
    language_list, language_dict = [], {}
    gender_list, gender_dict = [], {}

    with open(file_in, 'r', encoding='utf-8') as fp:
        for line in fp:
            begin, end = line.split(' values ')
            end = end[1:-2]
            value, code, status = end.split('\',')
            value = value[1:]
            code = code[1:]

            if 'bibliography' in begin:
                bibliography_list.append((code, value))
            if 'carrier_type' in begin:
                carrier_type_list.append((code, value))
            if 'content_type' in begin:
                content_type_list.append((code, value))
            if 'contribution' in begin:
                contribution_list.append((code, value))
            if 'country' in begin:
                country_list.append((code, value))
            if 'media_type' in begin:
                media_type_list.append((code, value))
            if 'publishing_statistics' in begin:
                publishing_statistics_list.append((code, value))
            if 'topic_category' in begin:
                topic_category_list.append((code, value))
            if 'language' in begin:
                language_list.append((code, value))
            if 'gender' in begin:
                gender_list.append((code, value))

    for num, elem in enumerate(bibliography_list, start=1):
        bibliography_dict.setdefault(elem[0], {'name': elem[1], 'id': num})
    for num, elem in enumerate(carrier_type_list, start=1):
        carrier_type_dict.setdefault(elem[0], {'name': elem[1], 'id': num})
    for num, elem in enumerate(content_type_list, start=1):
        content_type_dict.setdefault(elem[0], {'name': elem[1], 'id': num})
    for num, elem in enumerate(contribution_list, start=1):
        contribution_dict.setdefault(elem[0], {'name': elem[1], 'id': num})
    for num, elem in enumerate(country_list, start=1):
        country_dict.setdefault(elem[0], {'name': elem[1], 'id': num})
    for num, elem in enumerate(media_type_list, start=1):
        media_type_dict.setdefault(elem[0], {'name': elem[1], 'id': num})
    for num, elem in enumerate(publishing_statistics_list, start=1):
        publishing_statistics_dict.setdefault(elem[0], {'name': elem[1], 'id': num})
    for num, elem in enumerate(topic_category_list, start=1):
        topic_category_dict.setdefault(elem[0], {'name': elem[1], 'id': num})
    for num, elem in enumerate(language_list, start=1):
        language_dict.setdefault(elem[0], {'name': elem[1], 'id': num})
    for num, elem in enumerate(gender_list, start=1):
        gender_dict.setdefault(elem[0], {'name': elem[1], 'id': num})

    return {'bibliography_dict': bibliography_dict, 'carrier_type_dict': carrier_type_dict,
            'content_type_dict': content_type_dict, 'contribution_dict': contribution_dict,
            'country_dict': country_dict,
            'media_type_dict': media_type_dict, 'publishing_statistics_dict': publishing_statistics_dict,
            'topic_category_dict': topic_category_dict, 'language_dict': language_dict,
            'gender_dict': gender_dict}
