from typing import List, Optional

from pymarc import Record

import commons.validators as c_valid
from objects.frbr_cluster import FRBRCluster


def analyze_record_and_produce_frbr_clusters(pymarc_object: Record) -> List[FRBRCluster]:
    list_of_frbr_clusters = []
    is_single_or_multi = c_valid.is_single_or_multi_work(pymarc_object)

    # single work bib record - produce only one FRBRCluster
    if is_single_or_multi == 'single_work':
        frbr_cluster = produce_frbr_cluster_from_single_work(pymarc_object)
        list_of_frbr_clusters.append(frbr_cluster)

    # multiwork bib record
    # CASE A1 - ISBD multiwork punctuation in 245 present, 7XX|t present or 730 != "Katalog wystawy"
    if is_single_or_multi == 'multi_work_A1':
        pass

    return list_of_frbr_clusters


def produce_frbr_cluster_from_single_work(pymarc_object: Record) -> Optional[FRBRCluster]:
    frbr_cluster = FRBRCluster()
    frbr_cluster.get_raw_record_id(pymarc_object)

    try:
        frbr_cluster.get_main_creator(pymarc_object)
    except oe.TooMany1xxFields:
        return None

    frbr_cluster.get_other_creator(pymarc_object)

    try:
        frbr_cluster.get_titles(pymarc_object)
    except (oe.TooMany245Fields, oe.No245FieldFound):
        return None

    frbr_cluster.get_expression_distinctive_tuple(pymarc_object)

    frbr_cluster.get_sha_1_of_work_match_data()
    frbr_cluster.get_sha_1_of_expression_match_data()
    frbr_cluster.get_sha_1_of_manifestation_match_data()


    return frbr_cluster
