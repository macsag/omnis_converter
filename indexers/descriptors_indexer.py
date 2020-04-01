import ujson
import os

from tqdm import tqdm

from commons.marc_iso_commons import prepare_name_for_indexing


def index_descriptors(path_dir):
    descriptors_dict = {}

    for filename in os.listdir(path_dir):
        path_file = os.sep.join([path_dir, filename])
        with open(path_file, 'r', encoding='utf-8') as fp:
            for line in tqdm(fp):
                line_as_dict = ujson.loads(line)
                index = line_as_dict.get('_index')
                es_id = line_as_dict.get('_id')
                name = line_as_dict.get('_source')['descr_name']
                indexing_name = prepare_name_for_indexing(name)
                descriptors_dict.setdefault(indexing_name, (es_id, index, name))

    return descriptors_dict
