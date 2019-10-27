import json
import pprint
from uuid import uuid4

from commons.marc_iso_commons import get_values_by_field_and_subfield, get_values_by_field, postprocess
from commons.marc_iso_commons import serialize_to_jsonl_descr, truncate_title_proper
from commons.json_writer import write_to_json

from descriptor_resolver.resolve_record import resolve_code_and_serialize, resolve_field_value

from objects.manifestation import Manifestation


class Expression(object):
    __slots__ = ['uuid', 'manifestations', 'item_count', 'mock_es_id', 'expr_content_type', 'expr_contributor',
                 'expr_form', 'expr_lang', 'expr_leader_type', 'expr_title', 'expr_work', 'item_ids', 'libraries',
                 'materialization_ids', 'metadata_source', 'modificationTime', 'phrase_suggest', 'suggest', 'work_ids']

    def __init__(self):
        self.uuid = uuid4()
        self.manifestations = []
        self.item_count = 0

        # attributes for expression_es_index
        self.mock_es_id = None
        self.expr_content_type = []  # todo (but for now in ES it doesn't work as well)
        self.expr_contributor = None  # todo (but for now in ES it doesn't work as well)
        self.expr_form = None
        self.expr_lang = None
        self.expr_leader_type = None
        self.expr_title = None
        self.expr_work = None
        self.item_ids = []
        self.libraries = []
        self.materialization_ids = []
        self.metadata_source = 'REFERENCE'
        self.modificationTime = "2019-10-01T13:34:23.580"
        self.phrase_suggest = ['-']
        self.suggest = ['-']
        self.work_ids = None

    def __repr__(self):
        return f'Expression(id={self.mock_es_id}, lang={self.expr_lang})'

    def add(self, bib_object, work, buffer, descr_index, code_val_index):
        if not self.mock_es_id:
            self.mock_es_id = str('112' + get_values_by_field(bib_object, '001')[0][1:])
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

        self.materialization_ids.append(int('113' + get_values_by_field(bib_object, '001')[0][1:]))
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

    def write_to_dump_file(self, buffer):
        write_to_json(self.serialize_expression_for_expr_es_dump(), buffer, 'expr_buffer')

        for jsonl in self.serialize_expression_for_expr_work_es_dump():
            write_to_json(jsonl, buffer, 'expr_data_buffer')

    def serialize_expression_for_expr_es_dump(self):
        dict_expression = {"_index": "expression", "_type": "expression", "_id": self.mock_es_id,
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
