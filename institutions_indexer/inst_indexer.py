import json

from objects.library import Library


def read_json_file(file_in):
    with open(file_in, 'r', encoding='utf-8') as fp:
        for line in fp:
            yield line


def index_libraries(library, library_index: dict):
    lib_as_dict = json.loads(library)
    met_orig = lib_as_dict['_source'].get('metadata_original')
    if met_orig and 'makplus' in met_orig:
        library_index.setdefault(lib_as_dict['_source']['code'], Library(lib_as_dict))


def create_index(file_in):
    lib_ind = {}
    for rcd in read_json_file(file_in):
        index_libraries(rcd, lib_ind)
    return lib_ind
