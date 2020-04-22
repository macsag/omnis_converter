from commons.marc_iso_commons import get_values_by_field, get_values_by_field_and_subfield, postprocess
from commons.normalization import normalize_title


class ExpressionData(object):
    __slots__ = ['raw_record_id',
                 'expression_match_data_sha_1',
                 'expr_form',
                 'expr_lang',
                 'expr_leader_type',
                 'expr_title']

    def __init__(self, frbr_cluster, pymarc_object):
        self.raw_record_id = frbr_cluster.original_raw_record_id

        # data needed for merging and splitting FRBRClusters and deleting expression_data from FRBRCluster
        self.expression_match_data_sha_1 = frbr_cluster.expression_match_data_sha_1

        # expression_data attributes (no need for calculations or joins)
        self.expr_form = None
        self.expr_lang = None
        self.expr_leader_type = None
        self.expr_title = None

        # call method to populate the attributes
        self.get_attributes_from_pymarc_object(pymarc_object)

    def __repr__(self):
        return f'ExpressionData(id={self.raw_record_id})'

    def get_attributes_from_pymarc_object(self, pymarc_object):
        self.expr_form = get_values_by_field_and_subfield(pymarc_object, ('380', ['a']))
        self.expr_lang = [get_values_by_field(pymarc_object, '008')[0][35:38]]
        self.expr_leader_type = pymarc_object.leader[6]
        self.expr_title = postprocess(normalize_title,
                                      get_values_by_field_and_subfield(pymarc_object, ('245', ['a', 'b'])))[0]
