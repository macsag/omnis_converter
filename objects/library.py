class Library(object):
    __slots__ = ['es_id', 'mak_plus_id', 'source']

    def __init__(self, library_dict):

        self.es_id = library_dict['_id']
        self.mak_plus_id = library_dict['_source']['code']
        self.source = library_dict['_source']

    def __repr__(self):
        return f'Library(es_id={self.es_id}, mak_plus_id={self.mak_plus_id})'

    def get_serialized(self):
        return {'digital': self.source['digital'],
                'localization': self.source['localization'],
                'country': 'Polska',
                'province': self.source['province'],
                'city': self.source['city'],
                'name': self.source['name'],
                'id': int(self.es_id)}
