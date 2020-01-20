import json

from objects.library import Library


def read_json_file(file_in):
    with open(file_in, 'r', encoding='utf-8') as fp:
        for line in fp:
            yield line


def index_libraries(library, library_index_mak: dict, library_index_es: dict):
    lib_as_dict = json.loads(library)
    met_orig = lib_as_dict['_source'].get('metadata_original')
    if met_orig and 'makplus' in met_orig:
        library_index_mak.setdefault(lib_as_dict['_source']['code'], Library(lib_as_dict))
        library_index_es.setdefault(lib_as_dict['_id'], Library(lib_as_dict))


def create_lib_indexes(file_in):
    lib_ind_mak = {}
    lib_in_es = {}
    for rcd in read_json_file(file_in):
        index_libraries(rcd, lib_ind_mak, lib_in_es)
    return lib_ind_mak, lib_in_es


if __name__ == '__main__':

    lib_mak, lib_es = create_lib_indexes('../input_files/institutions/manager-library.json')
    for id, lib in lib_mak.items():
        lib_ser = lib.get_serialized()
        with open('libs.csv', 'a', encoding='utf-8') as fp:
            fp.write(''.join([f'{x}|' for x in lib_ser.values()]))
            fp.write('\n')