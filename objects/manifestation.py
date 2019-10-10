from uuid import uuid4

from commons.marc_iso_commons import to_single_value, get_values_by_field_and_subfield, get_values_by_field
from commons.marc_iso_commons import postprocess, truncate_title_proper


class Manifestation(object):
    def __init__(self, manifestation):
        self.uuid = uuid4()

        # attributes for manifestation_es_index
        self.mock_es_id = str('114' + to_single_value(get_values_by_field(manifestation, '001'))[1:-1])
        self.mat_nlp_id = to_single_value(get_values_by_field(manifestation, '001'))
        self.mat_isbn = get_values_by_field_and_subfield(manifestation, ('020', ['a']))
        self.mat_pub_info = get_values_by_field(manifestation, '260')
        self.mat_pub_city = get_values_by_field_and_subfield(manifestation, ('260', ['a']))
        self.mat_physical_info = get_values_by_field(manifestation, '300')
        self.mat_title_proper = to_single_value(
            postprocess(truncate_title_proper, get_values_by_field_and_subfield(manifestation, ('245', ['a']))))
        self.mat_external_id = get_values_by_field_and_subfield(manifestation, ('035', ['a']))
        self.mat_title_and_resp = get_values_by_field(manifestation, '245')
        self.mat_title_variant = get_values_by_field_and_subfield(manifestation, ('246', ['a', 'b']))
        self.mat_edition = get_values_by_field(manifestation, '250')

    def __repr__(self):
        return f'Manifestation(id={self.mock_es_id}, title_and_resp={self.mat_title_and_resp}'
