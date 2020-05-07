import json
from uuid import uuid4

from commons.marc_iso_commons import get_values_by_field_and_subfield, get_values_by_field, postprocess
from commons.marc_iso_commons import serialize_to_jsonl_descr, truncate_title_proper
from commons.json_writer import write_to_json

from descriptor_resolver.resolve_record import resolve_code_and_serialize, resolve_field_value

from objects.manifestation import Manifestation

import config.mock_es_id_prefixes as esid


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
    def __init__(self,
                 frbr_expression,
                 work_uuid):
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

        self.materialization_ids = set()

        self.item_ids = set()
        self.item_count = 0
        self.libraries = set()
        self.phrase_suggest = ['-']
        self.suggest = ['-']

        self.stat_digital = False
        self.stat_digital_library_count = 0
        self.stat_public_domain = False

    def __repr__(self):
        return f'FinalExpression(id={self.frbr_expression.uuid}, lang={self.expr_lang})'

    def join_and_calculate_pure_expression_attributes(self,
                                                      resolver_cache):

        for expression_data_object in self.frbr_expression.expression_data_by_raw_record_id.values():
            self.expr_form.update(expression_data_object.expr_form)
            self.expr_lang.update(expression_data_object.expr_lang)
            self.expr_leader_type.update(expression_data_object.expr_leader_type)
            self.expr_title.update(expression_data_object.expr_title)


    def add(self, bib_object, work, buffer, descr_index, code_val_index):
        if not self.mock_es_id:
            self.mock_es_id = str(esid.EXPRESSION_PREFIX + get_values_by_field(bib_object, '001')[0][1:])
        if not self.expr_form:
            self.expr_form = serialize_to_jsonl_descr(resolve_field_value(
                get_values_by_field_and_subfield(bib_object, ('380', ['a'])), descr_index))
        if not self.expr_lang:
            self.expr_lang = [get_values_by_field(bib_object, '008')[0][35:38]]
            self.expr_lang = resolve_code_and_serialize(self.expr_lang, 'language_dict', code_val_index)
        if not self.expr_leader_type:
            self.expr_leader_type = bib_object.leader[6]
        if not self.expr_title:
            self.expr_title = postprocess(truncate_title_proper,
                                          get_values_by_field_and_subfield(bib_object, ('245', ['a', 'b'])))[0]
        if not self.work_ids:
            self.work_ids = [int(work.mock_es_id)]
        if not self.expr_work:
            self.expr_work = {'id': int(work.mock_es_id), 'type': 'work', 'value': str(work.mock_es_id)}

        self.materialization_ids.append(int(esid.MANIFESTATION_PREFIX + get_values_by_field(bib_object, '001')[0][1:]))
        self.instantiate_manifestation(bib_object, work, buffer, descr_index, code_val_index)

    def instantiate_manifestation(self, bib_object, work, buffer, descr_index, code_val_index):
        self.manifestations.append(Manifestation(bib_object, work, self, buffer, descr_index, code_val_index))

    def get_item_ids_item_count_and_libraries(self):
        lib_ids = set()

        for m in self.manifestations:
            self.item_ids.extend([int(i_id) for i_id in m.item_ids])

            for lib in m.libraries:
                if lib['id'] not in lib_ids:
                    self.libraries.append(lib)
                    lib_ids.add(lib['id'])
            self.item_count += m.stat_item_count
            self.stat_digital = True if self.stat_digital or m.stat_digital else False
            self.stat_public_domain = True if self.stat_public_domain or m.stat_public_domain else False
            self.stat_digital_library_count = 1 if self.stat_digital_library_count == 1 or m.stat_public_domain == 1 \
                else 0

    def write_to_dump_file(self, buffer):
        write_to_json(self.serialize_expression_for_expr_es_dump(), buffer, 'expr_buffer')

        for jsonl in self.serialize_expression_for_expr_work_es_dump():
            write_to_json(jsonl, buffer, 'expr_data_buffer')

    def serialize_expression_for_expr_es_dump(self):
        dict_expression = {"_index": "expression", "_type": "expression", "_id": str(self.mock_es_id),
                           "_score": 1, "_source": {
                               'expr_content_type': self.expr_content_type,
                               'expr_form': self.expr_form,
                               'expr_lang': self.expr_lang,
                               'expr_leader_type': self.expr_leader_type,
                               'expr_title': self.expr_title,
                               'expr_work': self.expr_work,
                               'item_ids': self.item_ids,
                               'libraries': self.libraries,
                               'materialization_ids': self.materialization_ids,
                               'metadata_source': self.metadata_source,
                               'modificationTime': self.modificationTime,
                               'phrase_suggest': self.phrase_suggest,
                               'suggest': self.suggest,
                               'work_ids': self.work_ids}}

        json_expr = json.dumps(dict_expression, ensure_ascii=False)

        return json_expr

    def serialize_expression_for_expr_work_es_dump(self):
        dict_expr_data_list = []

        for num, manif in enumerate(self.manifestations, start=1):

            dict_expression_data = {"_index": "expression_data", "_type": "expression_data",
                                    "_id": f'{num}{self.mock_es_id}', "_score": 1, "_source": {
                                        'expr_expression':
                                            {'id': int(self.mock_es_id),
                                             'type': 'expression',
                                             'value': str(self.mock_es_id)},
                                        'expr_form': self.expr_form,
                                        'expr_lang': self.expr_lang,
                                        'expr_leader_type': self.expr_leader_type,
                                        'expr_materialization':
                                            {'id': int(manif.mock_es_id),
                                             'type': 'materialization',
                                             'value': str(manif.mock_es_id)},
                                        'expr_title': self.expr_title,
                                        'expr_work': self.expr_work,
                                        'metadata_original': manif.metadata_original,
                                        'metadata_source': self.metadata_source,
                                        'modificationTime': self.modificationTime,
                                        'phrase_suggest': self.phrase_suggest,
                                        'suggest': self.suggest}}

            json_expr_data = json.dumps(dict_expression_data, ensure_ascii=False)
            dict_expr_data_list.append(json_expr_data)

        return dict_expr_data_list
