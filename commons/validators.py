from typing import Optional

from exceptions.exceptions import TooMany1xxFields, No245FieldFound, TooMany245Fields


def is_number_of_1xx_fields_valid(list_val_100abcd: list,
                                  list_val_110abcdn: list,
                                  list_val_111abcdn: list) -> None:
    if (len(list_val_100abcd) > 1 or len(list_val_110abcdn) > 1 or len(list_val_111abcdn) > 1) \
            or (list_val_100abcd and list_val_110abcdn and list_val_111abcdn):
        raise TooMany1xxFields


def is_field_245_valid(title_245_raw: list) -> None:
    if len(title_245_raw) > 1:
        raise TooMany245Fields
    if not title_245_raw:
        raise No245FieldFound
