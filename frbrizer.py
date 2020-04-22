import logging
import sys
import os
import pickle
import redis

from tqdm import tqdm
from pymarc import parse_xml_to_array

from analyzer import analyze_record_and_produce_frbr_clusters


import commons.validators as c_valid

from objects.frbr_cluster import FRBRCluster
from objects.work import Work
from objects.item import MakItem

from commons.marc_iso_commons import read_marc_from_file, get_values_by_field_and_subfield, get_values_by_field
from commons.json_writer import JsonBufferOut
from commons.debugger import FRBRDebugger

from indexers.descriptors_indexer import index_descriptors
from indexers.code_value_indexer import code_value_indexer
from indexers.inst_indexer import create_lib_indexes

from manifestation_matcher.manif_matcher import get_titles_for_manifestation_matching, match_manifestation, get_data_for_matching

from descriptor_resolver.resolve_record import resolve_record

import config.main_configuration as c_mc


def has_items(pymarc_object):
    return True if get_values_by_field(pymarc_object, '852') else False


def is_245_indicator_2_valid(pymarc_object):
    return True if pymarc_object.get_fields('245')[0].indicators[1] in [str(n) for n in list(range(0, 10))] else False


def main_loop(configuration: dict):
    # dict with real FRBRCluster objects by uuid
    indexed_frbr_clusters_by_uuid = {}

    # helper dict for matching: {'title': [FRBRCluster uuid, ...]}
    indexed_frbr_clusters_by_titles = {}

    # helper dict for garbage collection:
    # {'raw_record_id': {'current_matches': set((frbr_cluster_uuid, expression_uuid, manifestation_uuid), ...),
    # 'previous_matches': set((frbr_cluster_uuid, expression_uuid, manifestation_uuid), ...)}}
    indexed_frbr_clusters_by_raw_record_id = {}

    indexed_manifestations_by_raw_record_id = {}
    indexed_manifestations_bn_by_titles_245 = {}
    indexed_manifestations_bn_by_titles_490 = {}

    # prepare indexes
    # logging.info('Indexing institutions...')
    # indexed_libs_by_mak_id, indexed_libs_by_es_id = create_lib_indexes(configuration['inst_file_in'])
    # logging.info('DONE!')
    #
    # logging.info('Indexing codes and values...')
    # indexed_code_values = code_value_indexer(configuration['code_val_file_in'])
    # logging.info('DONE!')
    #
    # logging.info('Indexing descriptors...')
    # indexed_descriptors = index_descriptors(configuration['descr_files_in'])
    # logging.info('DONE!')

    # start main loop - iterate through all bib records (only books) from BN
    logging.info('Starting main loop...')
    logging.info('FRBRrization step one in progress (first loop)...')

    # used for limit and stats
    counter = 0

    for pymarc_object in tqdm(read_marc_from_file(configuration['bn_file_in'])):
        if c_valid.is_document_type(pymarc_object) and \
                c_valid.is_single_or_multi_work(pymarc_object) == 'single_work' and \
                has_items(pymarc_object) and \
                is_245_indicator_2_valid(pymarc_object):

            if counter > configuration['limit']:
                break

            # analyze bibliographic record
            # and create one or more FRBRCluster instances (one if single work bib, two or more if multi work bib)
            # and get from raw parsed record data needed for matching each or them
            # and for building final works, expressions and manifestations (work_data, expression_data, etc.)
            frbr_clusters_list = analyze_record_and_produce_frbr_clusters(pymarc_object)

            for frbr_cluster in frbr_clusters_list:
                # has the raw record been already matched?
                # has data needed for matching produced clusters changed?
                # check this by looking up in indexed hashes (by raw_record_id)
                # of frbr_cluster_match_data, expression_match_data and manifestation_match_data
                # for each cluster produced by raw record
                # if nothing changed, there is nothing to do in matcher
                # pass record and related frbr_clusters directly to converter
                # (other data still may have been changed)

                # get match_info

                # local dict version
                if type(indexed_frbr_clusters_by_raw_record_id) == dict:
                    frbr_cluster_match_info = indexed_frbr_clusters_by_raw_record_id.get(
                        frbr_cluster.original_raw_record_id)
                # Redis version
                else:
                    frbr_cluster_match_info = pickle.loads(indexed_frbr_clusters_by_raw_record_id.get(
                        frbr_cluster.original_raw_record_id))


                # if record was already indexed, compare match_data
                if frbr_cluster_match_info:
                    changes = frbr_cluster.check_changes_in_match_data(frbr_cluster_match_info)

                    # if nothing changed, rebuild work_data, expression_data and manifestation
                    # using uuids from frbr_cluster_match_info to get frbr_cluster from database
                    # and raw_record_id to get manifestation and data from frbr_cluster
                    if not changes:
                        frbr_cluster_to_rebuild_uuid = frbr_cluster_match_info.get(frbr_cluster.work_match_data_sha_1)
                        new_work_data = frbr_cluster.work_data_by_raw_record_id.get(frbr_cluster.original_raw_record_id)
                        new_expression_data = None # TODO

                        # local dict version
                        if type(indexed_frbr_clusters_by_uuid) == dict:

                            # get cluster from local dict
                            frbr_cluster_to_rebuild = indexed_frbr_clusters_by_uuid.get(frbr_cluster_to_rebuild_uuid)
                            # rebuild work_data and expression_data
                            frbr_cluster_to_rebuild.rebuild_work_and_expression_data(new_work_data,
                                                                                     new_expression_data)

                            # rebuild manifestation (data for manifestation comes from newly created frbr_cluster,
                            # but manifestation uuid must not change, so we create manifestation and replace new uuid
                            # with old uuid from match_info)
                            new_manifestation = frbr_cluster.create_manifestation(pymarc_object)
                            new_manifestation.uuid = frbr_cluster_match_info.get(
                                new_manifestation.manifestation_match_data_sha_1)

                            # now, when we have new manifestation, we can compare items, which were created basing upon
                            # this raw bibliographic record

                            # TODO

                        # Redis version
                        else:
                            pass # TODO

                    # if something has changed, situation gets a bit complicated
                    # since single raw record can produce more than one frbr_cluster, we've got
                    else:
                        pass # TODO

                # try to match each frbr_cluster with existing ones (one or more), merge them and reindex
                # or create and index new frbr_cluster
                frbr_cluster.match_work_and_index(indexed_frbr_clusters_by_uuid,
                                                  indexed_frbr_clusters_by_titles,
                                                  indexed_frbr_clusters_by_raw_record_id,
                                                  indexed_manifestations_by_raw_record_id,
                                                  pymarc_object)

                # switch for initial import (final records are built only once after matching in initial import)
                if not c_mc.IS_INITIAL_IMPORT:
                    # send new/modified clusters to final conversion
                    # TODO
                    pass





            # create stub work and get
            # work = Work()
            # work.get_manifestation_bn_id(bib)
            # work.get_main_creator(bib, indexed_descriptors)
            # work.get_other_creator(bib, indexed_descriptors)
            # work.get_titles(bib)

            counter += 1

            # # try to match with existing work (and if there is a match: merge to one work and index by all titles)
            # # if there is no match, index new work by titles and by uuid
            # work.match_with_existing_work_and_index(indexed_works_by_uuid, indexed_works_by_titles)
            #
            # # decompose raw record and create
            #
            # # index original bib record by bn_id - fast lookup for conversion and manifestation matching
            # indexed_manifestations_bn_by_nlp_id.setdefault(get_values_by_field(bib, '001')[0], bib.as_marc())
            #
            # # index manifestation for matching with mak+ by 245 titles and 490 titles
            # titles_for_manif_match = get_titles_for_manifestation_matching(bib)
            #
            # for title in titles_for_manif_match.get('titles_245'):
            #     indexed_manifestations_bn_by_titles_245.setdefault(title, set()).add(get_values_by_field(bib, '001')[0])
            # for title in titles_for_manif_match.get('titles_490'):
            #     indexed_manifestations_bn_by_titles_490.setdefault(title, set()).add(get_values_by_field(bib, '001')[0])



    # logging.info('DONE!')

    if configuration['frbr_step_two']:

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

    # for work_uuid, indexed_work in tqdm(indexed_works_by_uuid.items()):
    #     # do conversion, upsert expressions and instantiate manifestations and BN items
    #     if indexed_work:
    #         print(indexed_work.titles245)
    #         indexed_work.convert_to_work(indexed_manifestations_bn_by_nlp_id,
    #                                      configuration['buffer'],
    #                                      indexed_descriptors,
    #                                      indexed_code_values)
    #
    #         logging.debug(f'\n{indexed_work.mock_es_id}')
    #
    #         for expression in indexed_work.expressions_dict.values():
    #             logging.debug(f'    {expression}')
    #
    #             for manifestation in expression.manifestations:
    #                 # index works by manifestations nlp id for inserting MAK+ items
    #                 indexed_works_by_mat_nlp_id.setdefault(manifestation.mat_nlp_id, indexed_work.uuid)
    #
    #                 logging.debug(f'        {manifestation}')
    #                 for i in manifestation.bn_items:
    #                     logging.debug(f'            {i}')

    logging.info('DONE!')

    if configuration['run_manif_matcher']:

        logging.info('MAK+ manifestation matching in progress...')
        list_of_files = os.listdir(configuration['mak_files_in'])

        # iterate through marcxml MAK+ files
        for file_num, filename in enumerate(list_of_files, start=1):
            if file_num > configuration['limit_mak']:
                break
            else:
                path_file = os.sep.join([configuration['mak_files_in'], filename])
                logging.info(f'Parsing MAK+ file nr {file_num} - {filename}...')
                parsed_xml = parse_xml_to_array(path_file)

                # iterate through parsed records (pymarc Records objects)
                for r in parsed_xml:
                    # check if it is not None - there are some problems with parsing
                    if r:
                        # try to match with BN manifestation
                        try:
                            match = match_manifestation(r,
                                                        index_245=indexed_manifestations_bn_by_titles_245,
                                                        index_490=indexed_manifestations_bn_by_titles_490,
                                                        index_id=indexed_manifestations_bn_by_nlp_id)
                        except (IndexError, ValueError, TypeError) as error:
                            # print(error)
                            continue

                        if match:
                            list_ava = r.get_fields('AVA')

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
                                                        logging.debug(
                                                            f'Increased item_count in existing mak_item - {num}.')
                                                        item_add_counter += 1
                                                except AttributeError as error:
                                                    logging.debug(error)
                                                    continue
                                            logging.debug(
                                                f'Added {item_counter} new mak_items, increased count {item_add_counter} times.')
        logging.info('DONE!')

    # loop for:
    # - adding mak items mock_es_ids
    # - serializing and writing mak items to json file
    # - getting libraries for manifestation
    # - getting mak item ids and count for manifestation
    # - serializing and writing manifestations to json file
    # - getting mak item ids and count for expression
    # - serializing and writing expressions to json file
    # - getting mak item ids and count, manifestation ids and couun, expresions ids and count for work
    # - serializing and writing works to json file

    # for indexed_work in tqdm(indexed_works_by_uuid.values()):
    #     if indexed_work:
    #         logging.debug(f'\n{indexed_work.mock_es_id}')
    #
    #         for expression in indexed_work.expressions_dict.values():
    #             logging.debug(f'    {expression}')
    #
    #             for manifestation in expression.manifestations:
    #
    #                 for num, item in enumerate(manifestation.mak_items.values(), start=1):
    #                     item.mock_es_id = f'{str(num)}{str(manifestation.mock_es_id)}'
    #                     item.write_to_dump_file(buff)
    #
    #                 manifestation.get_resolve_and_serialize_libraries(indexed_libs_by_es_id)
    #                 manifestation.get_mak_item_ids()
    #                 manifestation.write_to_dump_file(buff)
    #                 logging.debug(f'        {manifestation}')
    #
    #                 #for i in manif.bn_items:
    #                     #print(f'            BN - {i}')
    #                 #for im in manif.mak_items.values():
    #                     #print(f'            MAK - {im}')
    #
    #             expression.get_item_ids_item_count_and_libraries()
    #             expression.write_to_dump_file(buff)
    #
    #         indexed_work.get_expr_manif_item_ids_and_counts()
    #         indexed_work.write_to_dump_file(buff)
    #
    # logging.debug(indexed_works_by_uuid)
    # logging.debug(indexed_works_by_titles)
    # logging.debug(indexed_manifestations_bn_by_nlp_id)
    # logging.debug(indexed_manifestations_bn_by_titles_245)
    # logging.debug(indexed_manifestations_bn_by_titles_490)

    #frbr_debugger = FRBRDebugger()
    #frbr_debugger.log_indexed_works_by_uuid(indexed_works_by_uuid)


if __name__ == '__main__':

    logging.root.addHandler(logging.StreamHandler(sys.stdout))
    logging.root.setLevel(level=logging.DEBUG)

    #r_client_frbr_clusters_by_uuid = redis.Redis(db=0)
    #r_client_frbr_cluster_by_title = redis.Redis(db=1)
    #r_client_frbr_clusters_by_raw_record_id = redis.Redis(db=2)

    buff = JsonBufferOut('./output/item.json', './output/materialization.json', './output/expression.json',
                         './output/work.json', './output/expression_data.json', './output/work_data.json')

    configs = {'bn_file_in': './input_files/bib_records/bn/quo_vadis.mrc',
               'mak_files_in': './input_files/bib_records/mak',
               'inst_file_in': './input_files/institutions/manager-library.json',
               'code_val_file_in': './input_files/code_values/001_import.sql',
               'descr_files_in': './input_files/descriptors',
               'buffer': buff,
               'run_manif_matcher': False,
               'frbr_step_two': False,
               'limit': 10000,
               'limit_mak': 3}

    main_loop(configs)
    buff.flush()
