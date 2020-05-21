import logging
import io
import pickle
import time

import ujson
import redis
from tqdm import tqdm

from analyzer import analyze_record_and_produce_frbr_clusters
import commons.validators as c_valid
from commons.marc_iso_commons import read_marc_from_file, read_marc_from_binary

import config.main_configuration as c_mc


logger = logging.getLogger(__name__)


class FRBRizer(object):
    def __init__(self,
                 indexed_frbr_clusters_by_uuid: redis.Redis,
                 indexed_frbr_clusters_by_titles: redis.Redis,
                 indexed_frbr_clusters_by_raw_record_id: redis.Redis,
                 indexed_manifestations_by_uuid: redis.Redis):

        # Redis connection pools
        # index with real FRBRCluster objects by uuid
        self.indexed_frbr_clusters_by_uuid = indexed_frbr_clusters_by_uuid
        # helper index used for FRBRclusters matching
        # {'title': [FRBRCluster uuid, ...]}
        self.indexed_frbr_clusters_by_titles = indexed_frbr_clusters_by_titles
        self.indexed_frbr_clusters_by_raw_record_id = indexed_frbr_clusters_by_raw_record_id
        self.indexed_manifestations_by_uuid = indexed_manifestations_by_uuid

        self.clusters_to_convert = set()
        self.clusters_to_delete = set()
        self.expressions_to_delete = set()

    def frbrize_from_message(self,
                message: dict):

        records_from_message = message.get('records')
        item_conversion_table_from_message = message.get('job').get('conversionTableItem')
        process_only_single_work = message.get('job').get('singleWork')
        document_types_to_process = message.get('job').get('documentType')
        data_source_id = message.get('job').get('dataSource').get('id')

        item_conversion_table = {"physical_item":
                        {"item_field_tag": "852",
                         "item_count":
                            {"field": None,
                             "subfields": None},
                        "item_url":
                            {"field": None,
                             "subfields": None,
                             "scheme":
                                {"prefix": "https://katalogi.bn.org.pl/discovery/fulldisplay?docid=alma",
                                 "suffix": "&context=L&vid=48OMNIS_NLOP:48OMNIS_NLOP",
                                 "infix": {"field": "009", "subfields": None}}},
                        "item_local_bib_id":
                            {"field": None,
                             "subfields": None,
                             "from_ct": "[]"}
                         },
                    "digital_item":
                        {"item_field_tag": "856",
                         "item_count":
                             {"field": None,
                             "subfields": None},
                         "item_url":
                             {"field": "this_field",
                              "subfields": ["u"],
                              "scheme": None},
                         "item_local_bib_id":
                             {"field": None,
                              "subfields": None,
                              "from_ct": "POLONA"},
                         }
                    }

        for record in records_from_message:

            for pymarc_object in read_marc_from_binary(io.BytesIO(record.get('metaData').encode())):
                logger.debug(f'Processing record: \n{pymarc_object}')

                if c_valid.is_document_type(pymarc_object) and \
                        c_valid.is_single_or_multi_work(pymarc_object) == 'single_work' and \
                        c_valid.has_items(pymarc_object) and \
                        c_valid.is_245_indicator_2_valid(pymarc_object):

                    logger.debug(f'Record will be processed.')

                    # analyze bibliographic record
                    # and create one or more FRBRCluster instances (one if single work bib, two or more if multi work bib)
                    # and get from raw parsed record data needed for matching each or them
                    # and for building final works, expressions and manifestations (work_data, expression_data, etc.)
                    frbr_clusters_list = analyze_record_and_produce_frbr_clusters(pymarc_object)

                    logger.debug(f'Created {frbr_clusters_list}.')

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

                        if frbr_cluster:

                            if not c_mc.IS_INITIAL_IMPORT:
                                # local dict version
                                if type(self.indexed_frbr_clusters_by_raw_record_id) == dict:
                                    frbr_cluster_match_info = self.indexed_frbr_clusters_by_raw_record_id.get(
                                        frbr_cluster.original_raw_record_id)
                                # Redis version
                                else:
                                    frbr_cluster_match_info_raw = self.indexed_frbr_clusters_by_raw_record_id.get(
                                        frbr_cluster.original_raw_record_id)
                                    if frbr_cluster_match_info_raw:
                                        frbr_cluster_match_info = pickle.loads(frbr_cluster_match_info_raw)
                                    else:
                                        frbr_cluster_match_info = None

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
                                        if type(self.indexed_frbr_clusters_by_uuid) == dict:

                                            # get cluster from local dict
                                            frbr_cluster_to_rebuild = self.indexed_frbr_clusters_by_uuid.get(frbr_cluster_to_rebuild_uuid)
                                            # rebuild work_data and expression_data
                                            frbr_cluster_to_rebuild.rebuild_work_and_expression_data(new_work_data,
                                                                                                     new_expression_data)

                                            # rebuild manifestation (data for manifestation comes from newly created frbr_cluster,
                                            # but manifestation uuid must not change, so we create manifestation and replace new uuid
                                            # with old uuid from match_info)
                                            new_manifestation = frbr_cluster.create_manifestation(pymarc_object)
                                            new_manifestation.uuid = frbr_cluster_match_info.get(
                                                new_manifestation.manifestation_match_data_sha_1)

                                            # now, when we have new manifestation, we can compare items, which were created earlier
                                            # from this raw bibliographic record - we have to be sure, that number of item records
                                            # (one record per library) and item count (one library can have more than one phisical item)

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
                            updated_clusters, expressions_to_delete = frbr_cluster.match_work_and_index(
                                self.indexed_frbr_clusters_by_uuid,
                                self.indexed_frbr_clusters_by_titles,
                                self.indexed_frbr_clusters_by_raw_record_id,
                                self.indexed_manifestations_by_uuid,
                                pymarc_object,
                                item_conversion_table)

                            # send new/modified clusters to final converter
                            # only the last cluster needs to be converted
                            self.clusters_to_convert.add(updated_clusters[-1])
                            logger.debug(
                                f'Added: {updated_clusters[-1]} cluster(s) to send to converter (conversion).')

                            # others have been merged with the last one and need to be deleted
                            self.clusters_to_delete.update(updated_clusters[:-1])
                            logger.debug(
                                f'Added: {updated_clusters[:-1]} cluster(s) to send to converter (deletion).')

                            # send deleted expressions to final_converter
                            self.expressions_to_delete.update(expressions_to_delete)
                            logger.debug(
                                f'Added: {expressions_to_delete} expression(s) to send to converter (deletion).')

    def frbrize_from_file(self,
                          filepath: str,
                          connection_to_converter,
                          final_converter_queue_name):

        item_conversion_table = {"physical_item":
                        {"item_field_tag": "852",
                         "item_count":
                            {"field": None,
                             "subfields": None},
                        "item_url":
                            {"field": None,
                             "subfields": None,
                             "scheme":
                                {"prefix": "https://katalogi.bn.org.pl/discovery/fulldisplay?docid=alma",
                                 "suffix": "&context=L&vid=48OMNIS_NLOP:48OMNIS_NLOP",
                                 "infix": {"field": "009", "subfields": None}}},
                        "item_local_bib_id":
                            {"field": None,
                             "subfields": None,
                             "from_ct": "BN"}
                         },
                    "digital_item":
                        {"item_field_tag": "856",
                         "item_count":
                             {"field": None,
                             "subfields": None},
                         "item_url":
                             {"field": "this_field",
                              "subfields": ["u"],
                              "scheme": None},
                         "item_local_bib_id":
                             {"field": None,
                              "subfields": None,
                              "from_ct": "POLONA"},
                         }
                    }

        for pymarc_objects_list in tqdm(read_marc_from_file(filepath)):

            for pymarc_object in pymarc_objects_list:

                if c_valid.is_document_type(pymarc_object) and \
                        c_valid.is_single_or_multi_work(pymarc_object) == 'single_work' and \
                        c_valid.has_items(pymarc_object) and \
                        c_valid.is_245_indicator_2_valid(pymarc_object):

                    logger.debug(f'Record will be processed.')

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

                        if frbr_cluster:

                            if not c_mc.IS_INITIAL_IMPORT:
                                # local dict version
                                if type(self.indexed_frbr_clusters_by_raw_record_id) == dict:
                                    frbr_cluster_match_info = self.indexed_frbr_clusters_by_raw_record_id.get(
                                        frbr_cluster.original_raw_record_id)
                                # Redis version
                                else:
                                    frbr_cluster_match_info_raw = self.indexed_frbr_clusters_by_raw_record_id.get(
                                        frbr_cluster.original_raw_record_id)
                                    if frbr_cluster_match_info_raw:
                                        frbr_cluster_match_info = pickle.loads(frbr_cluster_match_info_raw)
                                    else:
                                        frbr_cluster_match_info = None

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
                                        if type(self.indexed_frbr_clusters_by_uuid) == dict:

                                            # get cluster from local dict
                                            frbr_cluster_to_rebuild = self.indexed_frbr_clusters_by_uuid.get(frbr_cluster_to_rebuild_uuid)
                                            # rebuild work_data and expression_data
                                            frbr_cluster_to_rebuild.rebuild_work_and_expression_data(new_work_data,
                                                                                                     new_expression_data)

                                            # rebuild manifestation (data for manifestation comes from newly created frbr_cluster,
                                            # but manifestation uuid must not change, so we create manifestation and replace new uuid
                                            # with old uuid from match_info)
                                            new_manifestation = frbr_cluster.create_manifestation(pymarc_object)
                                            new_manifestation.uuid = frbr_cluster_match_info.get(
                                                new_manifestation.manifestation_match_data_sha_1)

                                            # now, when we have new manifestation, we can compare items, which were created earlier
                                            # from this raw bibliographic record - we have to be sure, that number of item records
                                            # (one record per library) and item count (one library can have more than one physical item)

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
                            updated_clusters, expressions_to_delete = frbr_cluster.match_work_and_index(
                                self.indexed_frbr_clusters_by_uuid,
                                self.indexed_frbr_clusters_by_titles,
                                self.indexed_frbr_clusters_by_raw_record_id,
                                self.indexed_manifestations_by_uuid,
                                pymarc_object,
                                item_conversion_table)

                            # send new/modified clusters to final converter
                            # only the last cluster needs to be converted
                            self.clusters_to_convert.add(updated_clusters[-1])
                            logger.debug(f'Added: {updated_clusters[-1]} cluster(s) to send to converter (conversion).')

                            # others have been merged with the last one and need to be deleted
                            self.clusters_to_delete.update(updated_clusters[:-1])
                            logger.debug(f'Added: {updated_clusters[:-1]} cluster(s) to send to converter (deletion).')

                            # send deleted expressions to final_converter
                            self.expressions_to_delete.update(expressions_to_delete)
                            logger.debug(f'Added: {expressions_to_delete} expression(s) to send to converter (deletion).')

            self.send_to_final_converter(connection_to_converter,
                                         final_converter_queue_name)
            self.flush_all()

    def send_to_final_converter(self,
                                connection_to_converter,
                                final_converter_queue_name):

        logger.info(f'Sending to final-converter:\n'
                     f'{len(self.clusters_to_convert)} cluster(s) for conversion\n'
                     f'{len(self.clusters_to_delete)} cluster(s) for deletion\n'
                     f'{len(self.expressions_to_delete)} expression(s) for deletion.')

        # timestamp is used to control version of indexed documents in ElasticSearch
        # due to asynchronous nature of fetching messages from queues and horizontal scaling of final-converter
        # we have to be sure, that older version won't get indexed later than the newer one
        message = {'clusters_to_convert': list(self.clusters_to_convert),
                   'clusters_to_delete': list(self.clusters_to_delete),
                   'expressions_to_delete': list(self.expressions_to_delete),
                   'timestamp': time.time_ns()}

        connection_to_converter.send(final_converter_queue_name,
                                     ujson.dumps(message))

        logger.debug('Message successfully sent.')

    def flush_all(self):
        self.clusters_to_convert = set()
        self.clusters_to_delete = set()
