import logging
import sys
import os

from tqdm import tqdm
from pymarc import parse_xml_to_array

from exceptions.exceptions import DescriptorNotResolved

from commons.marc_iso_commons import read_marc_from_file, get_values_by_field_and_subfield, get_values_by_field
from commons.json_writer import JsonBufferOut

from manifestation_matcher.manif_matcher import get_titles_for_manifestation_matching, match_manifestation
from institutions_indexer.inst_indexer import create_lib_indexes
from code_value_indexer.code_value_indexer import code_value_indexer
from indexers.descriptors_indexer import index_descriptors
from descriptor_resolver.resolve_record import resolve_record

from objects.work import Work
from objects.item import MakItem


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
        val_245a_last_char = get_values_by_field_and_subfield(pymarc_object, ('245', ['a']))[0][-1]
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


def has_items(pymarc_object):
    return True if get_values_by_field(pymarc_object, '852') else False


def main_loop(**kwargs):
    indexed_works_by_uuid = {}
    indexed_works_by_titles = {}
    indexed_works_by_mat_nlp_id = {}

    indexed_manifestations_bn_by_nlp_id = {}
    indexed_manifestations_bn_by_titles_245 = {}
    indexed_manifestations_bn_by_titles_490 = {}

    # prepare indexes
    logging.info('Indexing institutions...')
    indexed_libs_by_mak_id, indexed_libs_by_es_id = create_lib_indexes(kwargs['inst_file_in'])
    logging.info('DONE!')

    logging.info('Indexing codes and values...')
    indexed_code_values = code_value_indexer(kwargs['code_val_file_in'])
    logging.info('DONE!')

    logging.info('Indexing descriptors...')
    indexed_descriptors = index_descriptors(kwargs['descr_files_path_dir'])
    logging.info('DONE!')

    # start main loop - iterate through all bib records (only books) from BN
    logging.info('Starting main loop...')
    logging.info('FRBRrization step one in progress (first loop)...')

    # used for limit and stats
    counter = 0

    for bib in tqdm(read_marc_from_file(kwargs['file_in'])):
        if is_book_ebook_audiobook(bib) and is_single_work(bib) and has_items(bib):
            if counter == kwargs['limit']:
                break

            try:
                bib = resolve_record(bib, indexed_descriptors)
            except DescriptorNotResolved as error:
                logging.error(error)
                continue

            # create stub work and get from manifestation data needed for work matching
            work = Work()
            work.get_manifestation_bn_id(bib)
            work.get_main_creator(bib, indexed_descriptors)
            work.get_other_creator(bib, indexed_descriptors)
            work.get_titles(bib)

            counter += 1

            # try to match with existing work (and if there is a match: merge to one work and index by all titles)
            # if there is no match, index new work by titles and by uuid
            work.match_with_existing_work_and_index(indexed_works_by_uuid, indexed_works_by_titles)

            # index original bib record by bn_id - fast lookup for conversion and manifestation matching
            indexed_manifestations_bn_by_nlp_id.setdefault(get_values_by_field(bib, '001')[0], bib.as_marc())

            # index manifestation for matching with mak+ by 245 titles and 490 titles
            titles_for_manif_match = get_titles_for_manifestation_matching(bib)

            for title in titles_for_manif_match.get('titles_245'):
                indexed_manifestations_bn_by_titles_245.setdefault(title, set()).add(get_values_by_field(bib, '001')[0])
            for title in titles_for_manif_match.get('titles_490'):
                indexed_manifestations_bn_by_titles_490.setdefault(title, set()).add(get_values_by_field(bib, '001')[0])

    logging.info('DONE!')

    if kwargs['frbr_step_two']:

        logging.info('FRBRrization step two - trying to merge works using broader context (second loop)...')

        for work_uuid, indexed_work in tqdm(indexed_works_by_uuid.items()):
            # check if work exists, it could've been set to None earlier in case of merging more than one work at a time
            if indexed_work:
                result = indexed_work.try_to_merge_possible_duplicates_using_broader_context(indexed_works_by_uuid,
                                                                                             indexed_works_by_titles)
                if result:
                    indexed_works_by_uuid[work_uuid] = None

        logging.info('DONE!')

    logging.info('Conversion in progress...')

    for work_uuid, indexed_work in tqdm(indexed_works_by_uuid.items()):
        # do conversion, upsert expressions and instantiate manifestations and BN items
        if indexed_work:
            indexed_work.convert_to_work(indexed_manifestations_bn_by_nlp_id,
                                         kwargs['buffer'],
                                         indexed_descriptors,
                                         indexed_code_values)

            logging.info(f'\n{indexed_work.mock_es_id}')

            for expression in indexed_work.expressions_dict.values():
                logging.info(f'    {expression}')

                for manifestation in expression.manifestations:
                    # index works by manifestations nlp id for inserting MAK+ items
                    indexed_works_by_mat_nlp_id.setdefault(manifestation.mat_nlp_id, indexed_work.uuid)

                    logging.info(f'        {manifestation}')
                    for i in manifestation.bn_items:
                        logging.info(f'            {i}')

    logging.info('DONE!')

    if kwargs['run_manif_matcher']:

        logging.info('MAK+ manifestation matching in progress...')

        # parse marxml data from MAK+ (one file at a time)
        for file_num, filename in enumerate(os.listdir('../input_files/bib_records/mak/marcxml')):
            path_file = os.sep.join(['../input_files/bib_records/mak/marcxml', filename])

            logging.info(f'Parsing {file_num} MAK+ file...')

            parsed_xml = parse_xml_to_array(path_file)

            # loop through MAK+ bib records from the file
            for r in tqdm(parsed_xml):
                # check if it is not None - there are some problems with parsing
                if r:
                    # try to match with BN manifestation
                    try:
                        match = match_manifestation(r,
                                                    index_245=indexed_manifestations_bn_by_titles_245,
                                                    index_490=indexed_manifestations_bn_by_titles_490,
                                                    index_id=indexed_manifestations_bn_by_nlp_id)
                    except IndexError as error:
                        #print(error)
                        continue

                    if match:
                        list_ava = r.get_fields('AVA')
                        list_mak_items = []

                        w_uuid = indexed_works_by_mat_nlp_id.get(match)
                        ref_to_work = indexed_works_by_uuid.get(w_uuid)

                        # this is definitely not a best way to do it
                        if ref_to_work:
                            for e in ref_to_work.expressions_dict.values():
                                for m in e.manifestations:
                                    if m.mat_nlp_id == match:
                                        logging.debug('Adding mak_items...')
                                        item_counter = 0
                                        item_add_counter = 0
                                        for num, ava in enumerate(list_ava, start=1):
                                            try:
                                                it_to_add = MakItem(ava, indexed_libs_by_mak_id, ref_to_work,
                                                                    e, m, buff, num)
                                                if it_to_add.item_local_bib_id not in m.mak_items:
                                                    logging.debug(f'Added new mak_item - {num}')
                                                    m.mak_items.setdefault(it_to_add.item_local_bib_id, it_to_add)
                                                    item_counter += 1
                                                else:
                                                    existing_it = m.mak_items.get(it_to_add.item_local_bib_id)
                                                    existing_it.add(it_to_add)
                                                    logging.debug(f'Increased item_count in existing mak_item - {num}.')
                                                    item_add_counter += 1
                                            except AttributeError as error:
                                                print(error)
                                                continue
                                        logging.debug(f'Added {item_counter} new mak_items, increased count {item_add_counter} times.')

        logging.info('DONE!')

    for indexed_work in tqdm(indexed_works_by_uuid.values()):
        if indexed_work:
            #print(f'\n{indexed_work.mock_es_id}')
            for expr in indexed_work.expressions_dict.values():
                #print(f'    {expr}')

                for manif in expr.manifestations:

                    for num, it in enumerate(manif.mak_items.values(), start=1):
                        it.mock_es_id = str(num) + str(manif.mock_es_id)
                        it.write_to_dump_file(buff)

                    manif.get_resolve_and_serialize_libraries(indexed_libs_by_es_id)
                    manif.get_mak_item_ids()
                    manif.write_to_dump_file(buff)
                    #print(f'        {manif}')

                    #for i in manif.bn_items:
                        #print(f'            BN - {i}')
                    #for im in manif.mak_items.values():
                        #print(f'            MAK - {im}')

                expr.get_item_ids_item_count_and_libraries()
                expr.write_to_dump_file(buff)

            indexed_work.get_expr_manif_item_ids_and_counts()
            indexed_work.write_to_dump_file(buff)

    #print(indexed_works_by_uuid)
    #print(indexed_works_by_titles)
    #print(indexed_manifestations_bn_by_id)
    #print(indexed_manifestations_bn_by_titles_245)
    #print(indexed_manifestations_bn_by_titles_490)


if __name__ == '__main__':

    logging.root.addHandler(logging.StreamHandler(sys.stdout))
    logging.root.setLevel(level=logging.DEBUG)

    buff = JsonBufferOut('../output/item.json', '../output/materialization.json', '../output/expression.json',
                         '../output/work.json', '../output/expression_data.json', '../output/work_data.json')

    configs = {'file_in': 'quo_vadis.mrc',
               'inst_file_in': '../manager-library.json',
               'code_val_file_in': '../code_value_indexer/code_value_sql_source/001_import.sql',
               'descr_files_path_dir': '../source_files/descriptors',
               'buffer': buff,
               'run_manif_matcher': True,
               'frbr_step_two': True,
               'limit': 50000}

    main_loop(**configs)
    buff.flush()







