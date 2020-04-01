from typing import Optional

from exceptions.exceptions import TooMany1xxFields


def is_number_of_1xx_fields_valid(list_val_100abcd: list,
                                  list_val_110abcdn: list,
                                  list_val_111abcdn: list) -> Optional[bool]:
    if (len(list_val_100abcd) > 1 or len(list_val_110abcdn) > 1 or len(list_val_111abcdn) > 1) \
            or (list_val_100abcd and list_val_110abcdn and list_val_111abcdn):
        raise TooMany1xxFields
    else:
        return True
