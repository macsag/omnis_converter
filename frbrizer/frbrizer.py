from tqdm import tqdm

from commons.marc_iso_commons import read_marc_from_file, get_values_by_field_and_subfield, get_values_by_field
from objects.work import Work
from converters.converter_work import convert_to_work


# 1
def is_book_ebook_audiobook(pymarc_object):
    val_380a = get_values_by_field_and_subfield(pymarc_object, ('380', ['a']))
    val_ldr67 = pymarc_object.leader[6:8]

    values_380a_to_check = ['Książki', 'Audiobooki', 'E-booki']
    values_ldr67_to_check = ['am', 'im']

    if val_ldr67 in values_ldr67_to_check:
        for value in values_380a_to_check:
            if value in val_380a:
                return True
        else:
            return False
    else:
        return False


# 2.1
def is_single_work(pymarc_object):
    # each and every record MUST have these fields, if it hasn't, it should be treated as invalid and skipped
    try:
        val_245a_last_char = get_values_by_field_and_subfield(pymarc_object, ('245', ['a']))[0][:-1]
        val_245a = get_values_by_field_and_subfield(pymarc_object, ('245', ['a']))[0]
        val_245c = get_values_by_field_and_subfield(pymarc_object, ('245', ['c']))[0]
    except IndexError:
        print('Invalid record.')
        return False

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
        return True
    else:
        return False


def main_loop(**kwargs):
    indexed_works_by_uuid = {}
    indexed_works_by_titles = {}

    indexed_manifestations_bn_by_id = {}

    for bib in tqdm(read_marc_from_file(kwargs['file_in'])):
        if is_book_ebook_audiobook(bib) and is_single_work(bib):
            #print(bib)
            work = Work()
            work.get_manifestation_bn_id(bib)
            work.get_main_creator(bib)
            work.get_titles(bib)
            work.match_with_existing_work_and_index(indexed_works_by_uuid, indexed_works_by_titles)

            indexed_manifestations_bn_by_id.setdefault(get_values_by_field(bib, '001')[0], bib.as_marc())
        else:
            print("Nie będziemy przetwarzać tego rekordu.")
            pass

    for work_uuid, indexed_work in indexed_works_by_uuid.items():
        indexed_work.convert_to_work(indexed_manifestations_bn_by_id)
        # print(f'{work_uuid} : {indexed_work.titles246_title_orig}, {indexed_work.manifestations_bn_ids}, {indexed_work.work_udc}')
        # indexed_work.serialize_work_for_es_dump()

    print(indexed_works_by_uuid)
    print(indexed_works_by_titles)
    print(indexed_manifestations_bn_by_id)


if __name__ == '__main__':

    configs = {'file_in': 'test.mrc'}

    main_loop(**configs)







