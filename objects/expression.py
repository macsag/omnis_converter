import json
from uuid import uuid4
import time
from datetime import datetime, timezone

from resolvers.descriptor_resolvers import resolve_ids_to_dict_objects
from resolvers.codes_resolvers import resolve_institution_codes, resolve_codes_to_dict_objects


class FRBRExpression(object):
    __slots__ = ['uuid', 'manifestations', 'expression_distinctive_tuple', 'expression_match_data_sha_1',
                 'expression_data_by_raw_record_id']

    def __init__(self, expression_distinctive_tuple_nlp_id,
                 expression_match_data_sha_1_nlp_id,
                 expression_data):

        self.uuid = str(uuid4())
        self.expression_distinctive_tuple = expression_distinctive_tuple_nlp_id
        self.expression_match_data_sha_1 = expression_match_data_sha_1_nlp_id

        self.expression_data_by_raw_record_id = {expression_data.raw_record_id: expression_data}
        self.manifestations = {}

    def __repr__(self):
        return f'FRBRExpression(id={self.uuid}, expression_distinctive_tuple={self.expression_distinctive_tuple})'


class FinalExpression(object):
    """
    Wrapper class around FRBRExpression object. Used for building final expression records
    and serializing final expression and expression_data records.
    """
    def __init__(self,
                 frbr_expression: FRBRExpression,
                 work_uuid: str):
        self.frbr_expression = frbr_expression
        self.expr_content_type = []  # TODO (but for now in ES it doesn't work as well)
        self.expr_contributor = None  # TODO (but for now in ES it doesn't work as well)

        self.expr_form = set()
        self.expr_lang = set()
        self.expr_leader_type = set()
        self.expr_title = set()

        # TODO it would be nice to check, what's really used in front-end
        self.expr_work = {'id': work_uuid, 'type': 'work', 'value': work_uuid}
        self.work_ids = [work_uuid]

        self.materialization_ids = [m_id for m_id in frbr_expression.manifestations.keys()]

        self.item_ids = set()
        self.stat_item_count = 0
        self.libraries = set()
        self.phrase_suggest = ['-']
        self.suggest = ['-']

        self.stat_digital = False
        self.stat_digital_library_count = 0
        self.stat_public_domain = False

        self.modificationTime = datetime.fromtimestamp(time.time(), tz=timezone.utc).isoformat()

    def __repr__(self):
        return f'FinalExpression(id={self.frbr_expression.uuid}, lang={self.expr_lang})'

    def join_and_calculate_pure_expression_attributes(self) -> None:

        for expression_data_object in self.frbr_expression.expression_data_by_raw_record_id.values():
            self.expr_form.update(expression_data_object.expr_form)
            self.expr_lang.update(expression_data_object.expr_lang)
            self.expr_leader_type.update(expression_data_object.expr_leader_type)
            self.expr_title.add(expression_data_object.expr_title)

    def collect_data_for_resolver_cache(self,
                                        resolver_cache: dict) -> None:

        descriptor_related_attributes = ['expr_form']

        for attribute in descriptor_related_attributes:
            for descr_nlp_id in getattr(self, attribute):
                resolver_cache.setdefault('descriptors', {}).setdefault(descr_nlp_id, None)

        lang_code_related_attributes = ['expr_lang']

        for attribute in lang_code_related_attributes:
            for lang_code in getattr(self, attribute):
                resolver_cache.setdefault('language_codes', {}).setdefault(lang_code, None)

    def resolve_and_serialize_expression_for_bulk_request(self,
                                                          resolver_cache: dict) -> dict:

        dict_expression = {'expr_content_type': self.expr_content_type,
                           'expr_form': resolve_ids_to_dict_objects(list(self.expr_form), resolver_cache),
                           'expr_lang': resolve_codes_to_dict_objects(list(self.expr_lang), 'language', resolver_cache),
                           'expr_leader_type': list(self.expr_leader_type)[0],
                           'expr_title': list(self.expr_title)[0],
                           'expr_work': self.expr_work,
                           'item_ids': list(self.item_ids),
                           'libraries': resolve_institution_codes(list(self.libraries), resolver_cache),
                           'materialization_ids': self.materialization_ids,
                           'modificationTime': self.modificationTime,
                           'phrase_suggest': self.phrase_suggest,
                           'suggest': self.suggest,
                           'work_ids': self.work_ids}

        return dict_expression

    def prepare_for_indexing_in_es(self, resolver_cache, timestamp):
        dict_expression = self.resolve_and_serialize_expression_for_bulk_request(resolver_cache)
        request = {"index": {"_index": "expression",
                             "_type": "expression",
                             "_id": self.frbr_expression.uuid,
                             "version": timestamp,
                             "version_type": "external"}}

        bulk_list = [request, dict_expression]

        return bulk_list
