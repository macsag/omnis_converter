class RawRecordMatchData(object):
    __slots__ = ['raw_record_id',
                 'main_creator', 'other_creator',
                 'titles']

    def __init__(self, raw_record_id):
        self.raw_record_id = raw_record_id

        # data for creating, merging, splitting and deleting frbr_cluster_match_data
        self.main_creator = set()
        self.other_creator = set()
        self.titles = set()

    def __repr__(self):
        return f'RawRecordMatchData(raw_record_id={self.raw_record_id}, ' \
               f'main_creator={self.main_creator}, ' \
               f'other_creator={self.other_creator}, ' \
               f'title={self.titles})'

    # 3.1.1
    def get_main_creator(self, bib_object):
        # get lists of 1XX fields from parsed raw record
        list_val_100abcd = get_values_by_field_and_subfield(bib_object, ('100', ['a', 'b', 'c', 'd']))
        list_val_110abcdn = get_values_by_field_and_subfield(bib_object, ('110', ['a', 'b', 'c', 'd', 'n']))
        list_val_111abcdn = get_values_by_field_and_subfield(bib_object, ('111', ['a', 'b', 'c', 'd', 'n']))

        # validate number of 1XX fields in parsed raw record record and raise exception if record is not valid
        # exception is handled at the frbrizer level
        is_number_of_1xx_fields_valid(list_val_100abcd, list_val_110abcdn, list_val_111abcdn)

        # 3.1.1.1 - there is 1XX field
        if list_val_100abcd or list_val_110abcdn or list_val_111abcdn:
            if list_val_100abcd:
                self.main_creator.add(prepare_name_for_indexing(list_val_100abcd[0]))
            if list_val_110abcdn:
                self.main_creator.add(prepare_name_for_indexing(list_val_110abcdn[0]))
            if list_val_111abcdn:
                self.main_creator.add(prepare_name_for_indexing(list_val_111abcdn[0]))

        # 3.1.1.2 - if there is no 1XX field, check for 7XX [CHECKED]
        else:
            list_val_700abcd = set()
            list_val_710abcdn = set()
            list_val_711abcdn = set()

            main_creators_to_add = set()

            list_700_fields = bib_object.get_fields('700')
            if list_700_fields:
                for field in list_700_fields:
                    e_subflds = field.get_subfields('e')
                    if e_subflds:
                        if 'Autor' in e_subflds or 'Autor domniemany' in e_subflds or 'Wywiad' in e_subflds:
                            list_val_700abcd.add(
                                ' '.join(subfld for subfld in field.get_subfields('a', 'b', 'c', 'd')))
                    else:
                        list_val_700abcd.add(
                            ' '.join(subfld for subfld in field.get_subfields('a', 'b', 'c', 'd')))

            main_creators_to_add.update(postprocess(prepare_name_for_indexing, list_val_700abcd))

            list_710_fields = bib_object.get_fields('710')
            if list_710_fields:
                for field in list_710_fields:
                    e_subflds = field.get_subfields('e')
                    subflds_4 = field.get_subfields('4')
                    if e_subflds:
                        if 'Autor' in e_subflds or 'Autor domniemany' in e_subflds or 'Wywiad' in e_subflds:
                            list_val_710abcdn.add(
                                ' '.join(subfld for subfld in field.get_subfields('a', 'b', 'c', 'd', 'n')))
                    if not e_subflds and not subflds_4:
                        list_val_710abcdn.add(
                            ' '.join(subfld for subfld in field.get_subfields('a', 'b', 'c', 'd', 'n')))

            main_creators_to_add.update(postprocess(prepare_name_for_indexing, list_val_710abcdn))

            list_711_fields = bib_object.get_fields('711')
            if list_711_fields:
                for field in list_711_fields:
                    j_subflds = field.get_subfields('j')
                    subflds_4 = field.get_subfields('4')
                    if j_subflds:
                        if 'Autor' in j_subflds or 'Autor domniemany' in j_subflds or 'Wywiad' in j_subflds:
                            list_val_711abcdn.add(
                                ' '.join(subfld for subfld in field.get_subfields('a', 'b', 'c', 'd', 'n')))
                    if not j_subflds and not subflds_4:
                        list_val_711abcdn.add(
                            ' '.join(subfld for subfld in field.get_subfields('a', 'b', 'c', 'd', 'n')))

            main_creators_to_add.update(postprocess(prepare_name_for_indexing, list_val_711abcdn))

            for main_creator in main_creators_to_add:
                self.main_creator.add(main_creator)

    # 3.1.2
    def get_other_creator(self, bib_object):
        if not self.main_creator:
            list_val_700abcd = set()
            list_val_710abcdn = set()
            list_val_711abcdn = set()

            list_700_fields = bib_object.get_fields('700')
            if list_700_fields:
                for field in list_700_fields:
                    e_subflds = field.get_subfields('e')
                    if e_subflds:
                        e_sub_joined = ' '.join(e_sub for e_sub in e_subflds)
                        if 'Red' in e_sub_joined or 'Oprac' in e_sub_joined or 'Wybór' in e_sub_joined:
                            list_val_700abcd.add(
                                ' '.join(subfld for subfld in field.get_subfields('a', 'b', 'c', 'd')))

            self.other_creator.update(postprocess(prepare_name_for_indexing, list_val_700abcd))

            list_710_fields = bib_object.get_fields('710')
            if list_710_fields:
                for field in list_710_fields:
                    e_subflds = field.get_subfields('e')
                    if e_subflds:
                        e_sub_joined = ' '.join(e_sub for e_sub in e_subflds)
                        if 'Red' in e_sub_joined or 'Oprac' in e_sub_joined or 'Wybór' in e_sub_joined:
                            list_val_710abcdn.add(
                                ' '.join(subfld for subfld in field.get_subfields('a', 'b', 'c', 'd', 'n')))

            self.other_creator.update(postprocess(prepare_name_for_indexing, list_val_710abcdn))

            list_711_fields = bib_object.get_fields('711')
            if list_711_fields:
                for field in list_711_fields:
                    j_subflds = field.get_subfields('j')
                    if j_subflds:
                        j_sub_joined = ' '.join(j_sub for j_sub in j_subflds)
                        if 'Red' in j_sub_joined or 'Oprac' in j_sub_joined or 'Wybór' in j_sub_joined:
                            list_val_711abcdn.add(
                                ' '.join(subfld for subfld in field.get_subfields('a', 'b', 'c', 'd', 'n')))

            self.other_creator.update(postprocess(prepare_name_for_indexing, list_val_711abcdn))

    # 3.1.3
    # to be able to track changes on merging, splitting and deleting FRBR clusters,
    # unique titles can be added only once per raw record (counter.count for each NEW title after merge must == 1)
    def get_titles(self, bib_object):
        titles_to_add = set()

        # get list of raw 245 fields
        titles_245_raw = bib_object.get_fields('245')

        # validate record
        is_field_245_valid(titles_245_raw)

        # get titles from 245 field
        list_val_245ab = postprocess(normalize_title, get_values_by_field_and_subfield(bib_object,
                                                                                             ('245', ['a', 'b'])))
        title_245_raw_ind = titles_245_raw[0].indicators
        list_val_245a = postprocess(normalize_title, get_values_by_field_and_subfield(bib_object,
                                                                                            ('245', ['a'])))
        val_245a_last_char = get_values_by_field_and_subfield(bib_object, ('245', ['a']))[0].strip()[-1]
        list_val_245p = postprocess(normalize_title, get_values_by_field_and_subfield(bib_object, ('245', ['p'])))

        if val_245a_last_char == '=' and not list_val_245p:
            to_add = prepare_name_for_indexing(list_val_245a[0])

            try:
                titles_to_add.add(to_add[int(title_245_raw_ind[1]):])
            except ValueError:
                pass
            titles_to_add.add(to_add)

        if list_val_245p:
            # add only title from last subfield |p
            to_add = prepare_name_for_indexing(list_val_245p[-1])

            titles_to_add.add(to_add)

        else:
            to_add = prepare_name_for_indexing(list_val_245ab[0])

            try:
                titles_to_add.add(to_add[int(title_245_raw_ind[1]):])
            except ValueError:
                pass
            titles_to_add.add(to_add)

        # get titles from 246 fields
        list_fields_246ab = postprocess(normalize_title, get_values_by_field_and_subfield('246', ['a', 'b']))
        titles_to_add.update(list_fields_246ab)

        # get title from 240 field
        title_240_raw_list = bib_object.get_fields('240')
        if title_240_raw_list:
            title_240_raw = title_240_raw_list[0]

            list_val_240 = postprocess(normalize_title,
                                       get_values_by_field_and_subfield(bib_object, ('240', ['a', 'b'])))

            try:
                titles_to_add.add(prepare_name_for_indexing(list_val_240[0][int(title_240_raw.indicators[1]):]))
            except ValueError:
                pass

            titles_to_add.add(prepare_name_for_indexing(list_val_240[0]))

        # finally add all unique titles from single raw record with counters
        for title in titles_to_add:
            self.titles.setdefault(title, ObjCounter()).add(1)

    def get_expression_distinctive_tuple(self, bib_object, is_multiwork=False):
        if not is_multiwork:
            list_fields_700 = bib_object.get_fields('700')
            list_fields_710 = bib_object.get_fields('710')
            list_fields_711 = bib_object.get_fields('711')

            translators = set()

            if list_fields_700:
                for field in list_fields_700:
                    if field.get_subfields('e'):
                        if field.get_subfields('e')[0] in ['Tł.', 'Tł', 'Tłumaczenie']:
                            translators.add(' '.join(field.get_subfields('a', 'b', 'c', 'd')))

            expr_lang = get_values_by_field(bib_object, '008')[0][35:38]
            ldr6 = bib_object.leader[6]

            expression_distinctive_tuple = (expr_lang, frozenset(translators), ldr6)

            self.expression_distinctive_tuple_from_original_raw_record = expression_distinctive_tuple
        else:
            # TODO get expressions when there is more than one work in raw record
            pass

    def get_raw_record_id(self, bib_object):
        self.original_raw_record_id = bib_object.get_fields('001')[0].value()

    def create_manifestation(self):
        self.manifestation_from_original_raw_record = FRBRManifestation()

    def create_expression_and_append_manifestation(self):
        expression_to_add = FRBRExpression(self.expression_distinctive_tuple_from_original_raw_record)

        self.expressions_by_distinctive_tuple.setdefault(self.expression_distinctive_tuple_from_original_raw_record,
                                                         expression_to_add.uuid)
        self.expressions.setdefault(expression_to_add.uuid,
                                    expression_to_add).manifestations.setdefault(self.manifestation_from_original_raw_record.uuid,
                                                                                 self.manifestation_from_original_raw_record)

    def merge_frbr_clusters_and_reindex(self,
                                        matched_frbr_cluster: List[FRBRCluster],
                                        indexed_frbr_clusters_by_uuid: dict,
                                        indexed_frbr_clusters_by_titles: dict,
                                        indexed_frbr_clusters_by_raw_record_id: dict) -> None:

        # perform these actions only if merging stub with existing frbr_cluster
        # in this case stub is never indexed and dies after merging
        if self.stub:
            self.merge_titles(matched_frbr_cluster)
            self.merge_manifestations_by_original_raw_id(matched_frbr_cluster)
            self.merge_children(matched_frbr_cluster,
                                indexed_frbr_clusters_by_uuid,
                                indexed_frbr_clusters_by_titles,
                                indexed_frbr_clusters_by_raw_record_id)
            matched_frbr_cluster.index_frbr_cluster_by_titles(indexed_frbr_clusters_by_titles)

        # perform these actions only if merging two or more existing frbr_clusters
        # in this case all matched frbr_clusters will be merged with first frbr_cluster on the list
        # and will die after that (they will be unindexed and deleted from the main index)
        else:
            self.unindex_frbr_cluster_by_titles(indexed_frbr_clusters_by_titles)
            self.merge_titles(matched_frbr_cluster)
            self.merge_children(matched_frbr_cluster,
                                indexed_frbr_clusters_by_uuid,
                                indexed_frbr_clusters_by_titles,
                                indexed_frbr_clusters_by_raw_record_id)

            indexed_frbr_clusters_by_uuid.pop(self.uuid)

    def merge_titles(self, matched_frbr_cluster: FRBRCluster) -> None:
        for title, counter in self.titles.items():
            matched_frbr_cluster.titles.setdefault(title, ObjCounter()).add(counter.count)

    def merge_manifestations_by_original_raw_id(self, matched_frbr_cluster: FRBRCluster) -> None:
        matched_frbr_cluster.manifestations_by_raw_record_id.update(self.manifestations_by_raw_record_id)

    def merge_children(self,
                       matched_frbr_cluster: FRBRCluster,
                       indexed_frbr_clusters_by_uuid: dict,
                       indexed_frbr_clusters_by_titles: dict,
                       indexed_frbr_clusters_by_raw_record_id: dict) -> None:

        # merge strategy for merging two already existing frbr_clusters
        if self.expressions_by_distinctive_tuple:
            for expression_distinctive_tuple, expression_uuid in self.expressions_by_distinctive_tuple.items():
                expression_to_merge_with_uuid = matched_frbr_cluster.expressions_by_distinctive_tuple.get(
                    expression_distinctive_tuple)

                if expression_to_merge_with_uuid:
                    expression_to_merge_with_object = matched_frbr_cluster.expressions.get(
                        expression_to_merge_with_uuid)
                    for manifestation_uuid, manifestation in self.expressions.get(
                            expression_uuid).manifestations.items():
                        expression_to_merge_with_object.manifestations.setdefault(
                            manifestation_uuid,
                            manifestation)
                else:
                    matched_frbr_cluster.expressions_by_distinctive_tuple.update(
                        {expression_distinctive_tuple: expression_uuid})
                    matched_frbr_cluster.expressions.update(
                        {expression_uuid: self.expressions.get(expression_uuid)})

        # merge strategy for merging stub frbr_cluster with existing frbr_cluster
        else:
            expression_to_merge_with_uuid = matched_frbr_cluster.expressions_by_distinctive_tuple.get(
                self.expression_distinctive_tuple_from_original_raw_record)
            if expression_to_merge_with_uuid:
                expression_to_merge_with_object = matched_frbr_cluster.expressions.get(
                    expression_to_merge_with_uuid)
                expression_to_merge_with_object.manifestations.setdefault(
                    self.manifestation_from_original_raw_record.uuid,
                    self.manifestation_from_original_raw_record)
            else:
                self.create_expression_and_append_manifestation()
                matched_frbr_cluster.expressions_by_distinctive_tuple.update(self.expressions_by_distinctive_tuple)
                matched_frbr_cluster.expressions.update(self.expressions)

            # matched_frbr_cluster.index_frbr_cluster_by_titles(indexed_frbr_clusters_by_titles)
            # matched_frbr_cluster.index_frbr_cluster_by_raw_record_ids(indexed_frbr_clusters_by_raw_record_id)

    # index entire cluster by titles
    # to avoid unnecessary requests (in case of merging) only new (with counter.count == 1) titles are indexed
    # in production dict is replaced by Redis
    def index_frbr_cluster_by_titles(self, indexed_frbr_clusters_by_titles: dict) -> None:
        for title, counter in self.titles.items():
            if counter.count == 1:
                indexed_frbr_clusters_by_titles.setdefault(title, set()).add(self.uuid)

    def unindex_frbr_cluster_by_titles(self, indexed_frbr_clusters_by_titles: dict):
        for title in self.titles.keys():
            indexed_frbr_clusters_by_titles.get(title).pop(self.uuid)

    def index_frbr_cluster_by_uuid(self, indexed_frbr_clusters_by_uuid):
        indexed_frbr_clusters_by_uuid.setdefault(self.uuid, self)

    def index_frbr_cluster_by_raw_record_ids(self, indexed_frbr_clusters_by_raw_record_id: dict) -> None:
        for original_raw_record_id in self.manifestations_by_raw_record_id.keys():
            indexed_frbr_clusters_by_raw_record_id.setdefault(original_raw_record_id,
                                                              {}).setdefault('current_matches',
                                                                             set()).add(self.uuid)

    def set_manifestation_by_raw_record_id_based_on_original_raw_record(self):
        self.manifestations_by_raw_record_id.setdefault(self.original_raw_record_id,
                                                        self.manifestation_from_original_raw_record.uuid)

    def match_work_and_index(self,
                             indexed_frbr_clusters_by_uuid: dict,
                             indexed_frbr_clusters_by_titles: dict,
                             indexed_frbr_clusters_by_raw_record_id: dict) -> None:

        candidate_clusters = set()
        matched_clusters = set()

        for title in self.titles.keys():
            candidates_per_title = indexed_frbr_clusters_by_titles.get(title)
            if candidates_per_title:
                candidate_clusters.update(candidates_per_title)

        for candidate_uuid in candidate_clusters:
            candidate_cluster = indexed_frbr_clusters_by_uuid.get(candidate_uuid)
            if candidate_cluster:
                if candidate_cluster.main_creator:
                    if candidate_cluster.main_creator == self.main_creator:
                        matched_clusters.add(candidate_uuid)
                if candidate_cluster.other_creator:
                    if candidate_cluster.other_creator == self.other_creator:
                        matched_clusters.add(candidate_uuid)
                if not candidate_cluster.main_creator and not candidate_cluster.other_creator \
                        and not self.main_creator and not self.other_creator:
                    matched_clusters.add(candidate_uuid)

        matched_clusters = list(matched_clusters)

        if not matched_clusters:
            self.create_manifestation()
            self.set_manifestation_by_raw_record_id_based_on_original_raw_record()
            self.create_expression_and_append_manifestation()
            self.stub = False

            self.index_frbr_cluster_by_titles(indexed_frbr_clusters_by_titles)
            self.index_frbr_cluster_by_uuid(indexed_frbr_clusters_by_uuid)

            self.index_frbr_cluster_by_raw_record_ids(indexed_frbr_clusters_by_raw_record_id)

        else:
            self.create_manifestation()
            self.set_manifestation_by_raw_record_id_based_on_original_raw_record()

            matched_clusters_to_merge_with = [indexed_frbr_clusters_by_uuid.get(
                matched_cluster) for matched_cluster in matched_clusters]

            self.merge_frbr_clusters_and_reindex(matched_clusters_to_merge_with,
                                                 indexed_frbr_clusters_by_uuid,
                                                 indexed_frbr_clusters_by_titles,
                                                 indexed_frbr_clusters_by_raw_record_id)