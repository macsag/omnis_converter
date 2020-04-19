from collections import namedtuple


class ObjCounter(object):
    __slots__ = ['count', 'diff']

    def __init__(self):
        self.count = 0
        self.diff = 0

    def __repr__(self):
        return f'ObjCounter(title_count={self.count})'

    def add(self, number_to_add):
        self.count += number_to_add
        self.diff = number_to_add

    def subtract(self, number_to_subtract):
        self.count -= number_to_subtract
        self.diff = -number_to_subtract


FRBRClusterMatchInfo = namedtuple('FRBRClusterMatchInfo', [])

ManifMatchData = namedtuple('ManifMatchData', ['ldr_67', 'val_008_0614', 'isbn_020_az', 'title_245',
                                               'title_245_no_offset', 'title_245_with_offset', 'titles_490',
                                               'numbers_from_title_245', 'place_pub_260_a_first_word',
                                               'num_of_pages_300_a', 'b_format', 'edition'])
