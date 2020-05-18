from typing import List
import logging
import time
import pickle
import ujson

import redis
from elasticsearch import Elasticsearch
from elasticsearch_dsl import MultiSearch, Search

from objects.frbr_cluster import FRBRCluster
from objects.work import FinalWork
from objects.expression import FinalExpression
from objects.manifestation import FRBRManifestation, FinalManifestation
from objects.item import FinalItem


logger = logging.getLogger(__name__)


class FinalConverter(object):
    def __init__(self,
                 indexed_frbr_clusters_by_uuid: redis.Redis,
                 indexed_manifestations_by_uuid: redis.Redis,
                 indexed_frbr_clusters_by_raw_record_id: redis.Redis,
                 es_connection_for_resolver_cache: Elasticsearch,
                 redis_connection_for_resolver_cache: redis.Redis) -> None:

        # Redis connection pools
        self.indexed_frbr_clusters_by_uuid = indexed_frbr_clusters_by_uuid
        self.indexed_manifestations_by_uuid = indexed_manifestations_by_uuid
        self.indexed_frbr_clusters_by_raw_record_id = indexed_frbr_clusters_by_raw_record_id
        self.redis_connection_for_resolver_cache = redis_connection_for_resolver_cache

        # ElasticSearch connection pool
        self.es_connection_for_resolver_cache = es_connection_for_resolver_cache

        # create empty lists for appending final objects
        self.final_works = []
        self.final_work_data_objects = []
        self.final_expressions = []
        self.final_expression_data_objects = []
        self.final_manifestations = []
        self.final_items = []

        # all serialized final records are being collected in a list
        self.bulk_api_requests_to_send_to_indexer_as_a_list = []

        # all codes, ids and names which need to be resolved are being collected in cache - after that they will
        # be resolved using as few requests as possible and put into final objects in second iteration
        # this should work much faster than making request each time when there is something to resolve
        self.resolver_cache = {}

    def convert_and_build_final_records(self,
                                        frbr_cluster_uuids: List[str]):

        # get all FRBRClusters (FRBRWorks and FRBRExpressions) from indexed_frbr_clusters_by_uuid
        frbr_clusters_list = self.get_frbr_clusters(frbr_cluster_uuids)

        # timestamp for ES version control updated upon requests to Redis
        work_expression_timestamp = time.time_ns()
        manifestation_item_timestamp = time.time_ns()

        # start main loop - iterater over FRBRClusters
        for frbr_cluster in frbr_clusters_list:

            # get FRBRmanifestations and appended FRBRitems from indexed_manifestations_by_uuid
            frbr_manifestations_list = self.get_frbr_manifestations(frbr_cluster)

            # update timestamp for manifestation_item after request to Redis
            manifestation_item_timestamp = time.time_ns()

            # create a dict with key=manifestation_uuid for fast access during iteration over expressions
            frbr_manifestations_dict = {}
            for frbr_manifestation in frbr_manifestations_list:
                if frbr_manifestation:
                    frbr_manifestations_dict[frbr_manifestation.uuid] = frbr_manifestation

            # create FinalWork, join and calculate all "pure" work attributes
            # remaining "impure" attributes will be collected when iterating through
            # expressions, manifestations and items
            final_work = FinalWork(frbr_cluster, work_expression_timestamp)
            final_work.join_and_calculate_pure_work_attributes()

            # iterate over expressions
            for expression_uuid, expression_object in frbr_cluster.expressions.items():

                # create FinalExpression, join and calculate all "pure" expression attributes
                # remaining "impure" attributes will be collected when iterating through manifestations
                final_expression = FinalExpression(expression_object, frbr_cluster.uuid)
                final_expression.join_and_calculate_pure_expression_attributes()

                # iterate over manifestations and items, build FinalItems, collect data for FinalManifestations building
                # build FinalManifestations and collect data for FinalExpressions and FinalWorks building
                for frbr_manifestation_uuid in expression_object.manifestations.keys():
                    # get reference to the frbr_manifestation_object
                    frbr_manifestation_object = frbr_manifestations_dict.get(frbr_manifestation_uuid)

                    # if FRBRManifestation contains multiple works and expression, get their ids
                    other_work_ids, other_expression_ids = self.get_other_work_and_expression_ids_if_multiwork(
                        frbr_manifestation_object,
                        frbr_cluster.uuid,
                        expression_uuid)
                    all_work_ids = [frbr_cluster.uuid]
                    all_work_ids.extend(other_work_ids)
                    all_expression_ids = [expression_uuid]
                    all_expression_ids.extend(other_expression_ids)

                    # create FinalManifestation
                    final_manifestation = FinalManifestation(all_work_ids,
                                                             all_expression_ids,
                                                             frbr_manifestation_object)

                    for frbr_item in frbr_manifestation_object.items_by_institution_code.values():
                        final_item = FinalItem(all_work_ids,
                                               all_expression_ids,
                                               frbr_manifestation_object.uuid,
                                               frbr_item)

                        final_item.collect_data_for_resolver_cache(self.resolver_cache)

                        final_manifestation.stat_item_count += final_item.frbr_item.item_count.count
                        final_manifestation.item_ids.add(final_item.frbr_item.uuid)
                        final_manifestation.libraries.add(final_item.library)

                        # final item record is ready
                        self.final_items.append(final_item)

                    final_manifestation.collect_data_for_resolver_cache(self.resolver_cache)
                    self.final_manifestations.append(final_manifestation)

                    # add remaining impure attributes of expression basing on manifestation
                    final_expression.item_ids.update(final_manifestation.item_ids)
                    final_expression.stat_item_count += final_manifestation.stat_item_count
                    final_expression.libraries.update(final_manifestation.libraries)

                    # add remaining impure attributes of work basing on manifestation
                    final_work.join_and_calculate_impure_work_attributes_from_manifestation(final_manifestation)

                # collect all ids and codes to resolve from expression
                final_expression.collect_data_for_resolver_cache(self.resolver_cache)

                # final expression record is ready
                self.final_expressions.append(final_expression)

                # add remaining impure attributes (stats) of work part basing on expression
                final_work.item_ids.update(final_expression.item_ids)
                final_work.stat_item_count += final_expression.stat_item_count
                final_work.libraries.update(final_expression.libraries)

            final_work.join_and_calculate_impure_work_attributes_final()
            final_work.collect_data_for_resolver_cache(self.resolver_cache)

            # final work record is ready
            self.final_works.append(final_work)

        self.get_data_for_resolver_cache()

        self.resolve_and_serialize_all_records_for_bulk_request(work_expression_timestamp,
                                                                manifestation_item_timestamp)

    def resolve_and_serialize_all_records_for_bulk_request(self,
                                                           work_expression_timestamp,
                                                           manifestation_item_timestamp):

        # resolve all attributes in final work records, serialize them and prepare bulk api request
        for final_work in self.final_works:
            bulk_request_list = final_work.prepare_for_indexing_in_es(self.resolver_cache,
                                                                      work_expression_timestamp)
            self.bulk_api_requests_to_send_to_indexer_as_a_list.extend(bulk_request_list)

        # resolve all attributes in final expression records, serialize them and prepare bulk api request
        for final_expression in self.final_expressions:
            bulk_request_list = final_expression.prepare_for_indexing_in_es(self.resolver_cache,
                                                                            work_expression_timestamp)
            self.bulk_api_requests_to_send_to_indexer_as_a_list.extend(bulk_request_list)

        # resolve all attributes in final manifestation records, serialize them and prepare bulk api request
        for final_manifestation in self.final_manifestations:
            bulk_request_list = final_manifestation.prepare_for_indexing_in_es(self.resolver_cache,
                                                                               manifestation_item_timestamp)
            self.bulk_api_requests_to_send_to_indexer_as_a_list.extend(bulk_request_list)

        # resolve all attributes in final item records, serialize them and prepare bulk api request
        for final_item in self.final_items:
            bulk_request_list = final_item.prepare_for_indexing_in_es(self.resolver_cache,
                                                                      manifestation_item_timestamp)
            self.bulk_api_requests_to_send_to_indexer_as_a_list.extend(bulk_request_list)

    def send_bulk_request_to_indexer(self, connection_to_indexer):
        wrapped_request = {'bulk_api_request': self.bulk_api_requests_to_send_to_indexer_as_a_list}
        wrapped_request_in_json = ujson.dumps(wrapped_request, ensure_ascii=False)
        connection_to_indexer.send('/queue/omnis.indexer-bibliographic', wrapped_request_in_json)

    def flush_all(self):
        self.final_works = []
        self.final_work_data_objects = []
        self.final_expressions = []
        self.final_expression_data_objects = []
        self.final_manifestations = []
        self.final_items = []

        self.bulk_api_requests_to_send_to_indexer_as_a_list = []

        self.resolver_cache = {}

    def get_frbr_clusters(self,
                          frbr_cluster_uuids: List[str]):

        frbr_clusters_list_to_return = []

        frbr_clusters_list = self.indexed_frbr_clusters_by_uuid.mget(frbr_cluster_uuids)
        for frbr_cluster in frbr_clusters_list:
            if frbr_cluster:
                frbr_clusters_list_to_return.append(pickle.loads(frbr_cluster))

        return frbr_clusters_list_to_return

    def get_frbr_manifestations(self,
                                frbr_cluster: FRBRCluster) -> List[FRBRManifestation]:

        manifestations_list_to_return = []

        # collect from FRBRCluster all manifestation uuids for mget request
        manifestation_uuids_list = []
        for manifestation_uuid in frbr_cluster.manifestations_by_raw_record_id.values():
            manifestation_uuids_list.append(manifestation_uuid.get('uuid'))

        manifestations_list = self.indexed_manifestations_by_uuid.mget(manifestation_uuids_list)
        for manifestation in manifestations_list:
            manifestations_list_to_return.append(pickle.loads(manifestation))

        return manifestations_list_to_return

    def get_other_work_and_expression_ids_if_multiwork(self,
                                                       frbr_manifestation_object,
                                                       frbr_cluster_uuid,
                                                       expression_uuid):
        other_work_ids = []
        other_expressions_ids = []

        if frbr_manifestation_object.multiwork:
            m_related_ids = self.indexed_frbr_clusters_by_raw_record_id.get(frbr_manifestation_object.raw_record_id)
            m_related_ids = pickle.loads(m_related_ids)

            for work_id in m_related_ids['work_match_data'].values():
                if work_id != frbr_cluster_uuid:
                    other_work_ids.append(work_id)
            for expression_id in m_related_ids['expression_match_data'].values():
                if expression_id != expression_uuid:
                    other_expressions_ids.append(expression_id)

        return other_work_ids, other_expressions_ids

    def get_data_for_resolver_cache(self):
        # ElasticSearch queries
        # instantiate Multisearch query
        ms = MultiSearch(using=self.es_connection_for_resolver_cache)

        # prepare query for all descriptors
        descriptors_dict = self.resolver_cache.get('descriptors')
        if descriptors_dict:
            for descriptor_nlp_id in descriptors_dict.keys():
                ms = ms.add(Search().query('match', descr_nlp_id=descriptor_nlp_id))

        # prepare query for all institution codes
        institution_codes_dict = self.resolver_cache.get('institution_codes')
        if institution_codes_dict:
            for inst_code in institution_codes_dict.keys():
                ms = ms.add(Search().query('match', code=inst_code))

        # execute query
        resp = ms.execute()

        # parse response and collect data for resolver cache
        for single_resp in resp:
            for hit in single_resp:
                if hit.meta.index in ['personal_descriptor', 'corporate_descriptor', 'event_descriptor',
                                      'subject_descriptor', 'geographical_descriptor', 'genre_descriptor']:
                    descriptors_dict[hit.descr_nlp_id] = {'id': hit.meta.id,
                                                          'type': hit.meta.index,
                                                          'value': hit.descr_name}
                if hit.meta.index == 'manager_library':
                    institution_codes_dict[hit.code] = {'digital': hit.digital,
                                                        'localization': hit.localization,
                                                        'country': hit.country,
                                                        'province': hit.province,
                                                        'city': hit.city,
                                                        'name': hit.name,
                                                        'id': hit.code}

        # Redis queries below
        code_types = ['language', 'contribution', 'bibliography',
                      'carrier_type', 'content_type', 'country',
                      'media_type', 'publishing_statistics']

        for code_type in code_types:
            codes_dict = self.resolver_cache.get(code_type)
            if codes_dict:
                # execute query
                resp = self.redis_connection_for_resolver_cache.hmget(code_type, [code for code in codes_dict.keys()])
                # parse response and collect data for resolver cache
                for code, name in zip(codes_dict.keys(), resp):
                    codes_dict[code] = name
