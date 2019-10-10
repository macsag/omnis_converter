from uuid import uuid4

from commons.marc_iso_commons import get_values_by_field_and_subfield, get_values_by_field, postprocess

from objects.manifestation import Manifestation


class Expression(object):
    def __init__(self, work):
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
        return f'Expression(id={self.mock_es_id}, lang={self.expr_lang}, manifestations={self.manifestations}'

    def add(self, manifestation):
        if not self.mock_es_id:
            self.mock_es_id = str('112' + get_values_by_field(manifestation, '001')[0][1:-1])
        if not self.expr_lang:
            self.expr_lang = get_values_by_field(manifestation, '008')[0][35:38]
        if not self.expr_leader_type:
            self.expr_leader_type = manifestation.leader[6]
        self.materialization_ids.append(str('113' + get_values_by_field(manifestation, '001')[0][1:-1]))
        self.instantiate_manifestation(manifestation)

    def instantiate_manifestation(self, manifestation):
        self.manifestations.append(Manifestation(manifestation))




