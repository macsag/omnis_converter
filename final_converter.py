from typing import Union, List
import logging
import sys
import time
import pickle

import redis
import stomp

from objects.frbr_cluster import FRBRCluster
from objects.work import FinalWork
from objects.manifestation import FRBRManifestation, FinalManifestation
from objects.item import FRBRItem, FinalItem


class FinalConverter(object):
    def __init__(self,
                 indexed_frbr_clusters_by_uuid,
                 indexed_manifestations_by_uuid,
                 indexed_frbr_clusters_by_raw_record_id):

        # Redis connection pools
        self.indexed_frbr_clusters_by_uuid = indexed_frbr_clusters_by_uuid
        self.indexed_manifestations_by_uuid = indexed_manifestations_by_uuid
        self.indexed_frbr_clusters_by_raw_record_id = indexed_frbr_clusters_by_raw_record_id

        # create empty lists for appending final objects
        self.final_works = []
        self.final_work_data_objects = []
        self.final_expressions = []
        self.final_expression_data_objects = []
        self.final_manifestations = []
        self.final_items = []

        self.bulk_api_requests_to_send_to_indexer_as_a_list = []

        # all codes, ids and names which need to be resolved are being collected in cache - after that they will
        # be resolved using as few requests as possible and put into final objects in second iteration
        # this should work much faster than making request each time when there is something to resolve
        self.resolver_cache = {}

    def convert_and_build_final_records(self,
                                        frbr_cluster_uuids: List[str]):

        # get all FRBRClusters (FRBRWorks and FRBRExpressions) from indexed_frbr_clusters_by_uuid
        frbr_clusters_list = self.get_frbr_clusters(frbr_cluster_uuids)

        # start main loop - iterater over FRBRClusters
        for frbr_cluster in frbr_clusters_list:

            # create FinalWork, join and calculate all "pure" work attributes (no expression, manifestation, item data)
            final_work = FinalWork(frbr_cluster)
            final_work.join_and_calculate_pure_work_attributes(self.resolver_cache)


            # get FRBRmanifestations and appended FRBRitems from indexed_manifestations_by_uuid
            frbr_manifestations_list = self.get_frbr_manifestations(frbr_cluster)

            # iterate over manifestations and items, build FinalItems, collect data for FinalManifestations building
            # build FinalManifestations and collect data for FinalExpressions and FinalWorks building
            item_ids = []

            for frbr_manifestation in frbr_manifestations_list:
                final_manifestation = FinalManifestation()

                for frbr_item in frbr_manifestation.items_by_institution_code.values():
                    final_item = FinalItem(frbr_cluster.uuid,
                                           frbr_manifestation.uuid,
                                           frbr_item)
                    final_item.get_single_expression_id(frbr_cluster.expressions_by_raw_record_id)

                    # if FRBRManifestation contains multiple works and expression, get their ids
                    other_work_ids, other_expression_ids = final_item.get_other_work_and_expression_ids_if_multiwork(
                        self.indexed_frbr_clusters_by_raw_record_id)

                    item_ids.append(frbr_item.uuid)

                    self.resolver_cache.setdefault('institution_codes',
                                                   {}).setdefault(frbr_item.item_local_bib_id, None)
                    self.final_items.append(final_item)

        # iterate over manifestations

        # TODO

    def get_frbr_clusters(self,
                          frbr_cluster_uuids: List[str]):

        frbr_clusters_list_to_return = []

        frbr_clusters_list = self.indexed_frbr_clusters_by_uuid.mget(frbr_cluster_uuids)
        for frbr_cluster in frbr_clusters_list:
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


class MatcherListener(stomp.ConnectionListener):
    def __init__(self,
                 final_converter: FinalConverter):
        self.final_converter = final_converter

    def on_error(self, headers, message):
        print('received an error "%s"' % message)

    def on_message(self, headers, message):
        unpickled_message = pickle.loads(message)
        print('received a message "%s"' % unpickled_message)
        self.final_converter.convert_and_build_final_records(unpickled_message)
        print('processed message')

    def on_disconnected(self):
        print('disconnected')


if __name__ == "__main__":

    logging.root.addHandler(logging.StreamHandler(sys.stdout))
    logging.root.setLevel(level=logging.DEBUG)

    # initialize connection pools for Redis
    indexed_frbr_clusters_by_uuid = redis.Redis(db=0)
    indexed_manifestations_by_uuid = redis.Redis(db=4)
    indexed_frbr_clusters_by_raw_record_id = redis.Redis(db=2)

    # initialize FinalConverter instance
    final_converter = FinalConverter(indexed_frbr_clusters_by_uuid,
                                     indexed_manifestations_by_uuid,
                                     indexed_frbr_clusters_by_raw_record_id)

    # initialize ActiveMQ connection and set listener with FinalConverter wrapped
    c = stomp.Connection([('127.0.0.1', 61613)], heartbeats=(0, 0), keepalive=True, auto_decode=False)
    c.set_listener('matcher_listener', MatcherListener(final_converter))
    c.connect('admin', 'admin', wait=True)
    c.subscribe(destination='/queue/matcher-final-converter', ack='auto', id='matcher_listener')

    while True:
        time.sleep(5)
