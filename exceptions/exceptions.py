class OmnisConverterException(Exception):
    pass


class TooMany1xxFields(OmnisConverterException):
    def __str__(self):
        return 'Invalid record - more than one 1XX marc field.'


class No245FieldFoundOrTooMany245Fields(OmnisConverterException):
    def __str__(self):
        return 'Invalid record - no 245 field or too many 245 fields'