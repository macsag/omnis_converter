class OmnisConverterException(Exception):
    pass


class TooMany1xxFields(OmnisConverterException):
    def __str__(self):
        return 'Invalid record - more than one 1XX marc field.'


class No245FieldFound(OmnisConverterException):
    def __str__(self):
        return 'Invalid record - no 245 field.'


class TooMany245Fields(OmnisConverterException):
    def __str__(self):
        return 'Invalid record - too many 245 fields.'


class No008FieldFound(OmnisConverterException):
    def __str__(self):
        return 'Invalid record - no 008 field.'


class DescriptorNotResolved(OmnisConverterException):
    def __str__(self):
        return 'Invalid record - descriptor not resolved.'