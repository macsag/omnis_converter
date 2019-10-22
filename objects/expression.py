from uuid import uuid4

from commons.marc_iso_commons import get_values_by_field_and_subfield, get_values_by_field, postprocess

from objects.manifestation import Manifestation


class Expression(object):
    def __init__(self):
        self.uuid = uuid4()
        self.manifestations = []

        # attributes for expression_es_index
        self.mock_es_id = None
        self.expr_content_type = None
        self.expr_contributor = None
        self.expr_form = None
        self.expr_lang = None
        self.expr_leader_type = None
        self.expr_title = None
        self.expr_work = None
        self.item_ids = None
        self.libraries = None
        self.materialization_ids = []
        self.metadata_source = 'REFERENCE'
        self.modification_time = None
        self.phrase_suggest = ''
        self.suggest = ''
        self.work_ids = None

        # attributes for expression_data_es_index
        self.mock_es_data_id = None

    def __repr__(self):
        return f'Expression(id={self.mock_es_id}, lang={self.expr_lang})'

    def add(self, bib_object, work, buffer, descr_index, code_val_index):
        if not self.mock_es_id:
            self.mock_es_id = str('112' + get_values_by_field(bib_object, '001')[0][1:-1])
        if not self.expr_lang:
            self.expr_lang = get_values_by_field(bib_object, '008')[0][35:38]
        if not self.expr_leader_type:
            self.expr_leader_type = bib_object.leader[6]
        if not self.work_ids:
            self.work_ids = [int(work.mock_es_id)]
        self.materialization_ids.append(str('113' + get_values_by_field(bib_object, '001')[0][1:-1]))
        self.instantiate_manifestation(bib_object, work, buffer, descr_index, code_val_index)

    def instantiate_manifestation(self, bib_object, work, buffer, descr_index):
        self.manifestations.append(Manifestation(bib_object, work, self, buffer, descr_index))




