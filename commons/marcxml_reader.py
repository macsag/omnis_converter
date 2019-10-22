from pymarc import marcxml


def print_rcd(r):
    print(r)


def yield_record(r):
    yield r


def marcxml_reader():
    marcxml.map_xml(yield_record, 'msib_rec_00001.xml')


for r in marcxml_reader():
    print(r)