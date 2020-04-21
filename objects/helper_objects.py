from collections import namedtuple


class ObjCounter(object):
    __slots__ = ['count', 'prev_count']

    def __init__(self):
        self.count = 0
        self.prev_count = 0

    def __repr__(self):
        return f'ObjCounter(title_count={self.count})'

    def add(self, number_to_add):
        self.prev_count = self.count
        self.count += number_to_add

    def subtract(self, number_to_subtract):
        self.prev_count = self.count
        self.count -= number_to_subtract


FRBRClusterMatchInfo = namedtuple('FRBRClusterMatchInfo', [])

ManifestationMatchData = namedtuple('ManifestationMatchData',
                                    ['ldr_67', 'val_008_0614', 'isbn_020_az', 'title_245',
                                     'title_245_no_offset', 'title_245_with_offset', 'titles_490',
                                     'numbers_from_title_245', 'place_pub_260_a_first_word',
                                     'num_of_pages_300_a', 'b_format', 'edition'])
