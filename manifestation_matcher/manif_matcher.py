import re

from commons.marc_iso_commons import get_values_by_field, get_values_by_field_and_subfield, read_marc_from_binary
from commons.marc_iso_commons import normalize_edition_for_matching, postprocess

from objects.helper_objects import ManifMatchData


def get_data_for_matching(manifestation):
    ldr_67 = manifestation.leader[6:8]
    val_008_0614 = get_values_by_field(manifestation, '008')[0][6:15].replace('+', ' ')
    isbn_020_az = get_values_by_field_and_subfield(manifestation, ('020', ['a', 'z']))
    title_245 = get_values_by_field_and_subfield(manifestation, ('245', ['a', 'b']))[0]
    title_245_no_offset = ' '.join(sf for sf in manifestation.get_fields('245')[0].get_subfields('a', 'b'))[:25]
    title_245_with_offset = ' '.join(sf for sf in manifestation.get_fields('245')[0].get_subfields('a', 'b'))[int(manifestation.get_fields('245')[0].indicators[1]):25]
    titles_490 = get_values_by_field_and_subfield(manifestation, ('490', ['a']))

    numbers_from_title_245 = ''.join(gr for gr in re.findall('\d', title_245))
    place_pub_260_a_first_word = get_values_by_field_and_subfield(manifestation, ('260', ['a']))[0].split()[0]
    num_of_pages_300_a = max(int(gr) for gr in re.findall('\d+',
                                                          get_values_by_field_and_subfield(manifestation,
                                                                                           ('300', ['a']))[0]))
    b_format = int(re.search('\d+', get_values_by_field_and_subfield(manifestation, ('300', ['c']))[0])[0])
    edition = postprocess(normalize_edition_for_matching, get_values_by_field(manifestation, '250'))

    return ManifMatchData(ldr_67=ldr_67, val_008_0614=val_008_0614, isbn_020_az=isbn_020_az, title_245=title_245,
                          title_245_no_offset=title_245_no_offset, title_245_with_offset=title_245_with_offset,
                          titles_490=titles_490, numbers_from_title_245=numbers_from_title_245,
                          place_pub_260_a_first_word=place_pub_260_a_first_word, num_of_pages_300_a=num_of_pages_300_a,
                          b_format=b_format, edition=edition)


def get_titles_for_manifestation_matching(pymarc_object):
    titles_to_index_490 = set()
    titles_to_index_245 = set()

    title_245 = pymarc_object.get_fields('245')[0]
    titles_490 = get_values_by_field_and_subfield(pymarc_object, ('490', ['a']))

    title_245_no_offset = ' '.join(sf for sf in title_245.get_subfields('a', 'b'))[:25]
    title_245_with_offset = ' '.join(sf for sf in title_245.get_subfields('a', 'b'))[int(title_245.indicators[1]):25]

    titles_to_index_490.update(titles_490)
    titles_to_index_245.add(title_245_no_offset)
    titles_to_index_245.add(title_245_with_offset)

    return {'titles_245': list(titles_to_index_245), 'titles_490': list(titles_to_index_490)}


def match_manifestation(mak_manif, index_245=None, index_490=None, index_id=None):

    mak_manif_data = get_data_for_matching(mak_manif)

    candidates_245 = set()

    cand_1_245 = index_245.get(mak_manif_data.title_245_no_offset)
    cand_2_245 = index_245.get(mak_manif_data.title_245_with_offset)

    if cand_1_245:
        candidates_245.update(cand_1_245)
    if cand_2_245:
        candidates_245.update(cand_2_245)

    match = False

    matched_from_245_with_edition = set()
    matched_from_245_without_edition = set()

    for candidate in list(candidates_245):
        bn_manif_data = get_data_for_matching(read_marc_from_binary(index_id.get(candidate)))

        if bn_manif_data.ldr_67 == mak_manif_data.ldr_67 and bn_manif_data.val_008_0614 == mak_manif_data.val_008_0614:
            if (bn_manif_data.isbn_020_az and mak_manif_data.isbn_020_az and \
               bn_manif_data.isbn_020_az != mak_manif_data.isbn_020_az and \
               len(set(bn_manif_data.isbn_020_az) | set(mak_manif_data.isbn_020_az)) <= \
               len(bn_manif_data.isbn_020_az) and \
               len(set(bn_manif_data.isbn_020_az) | set(mak_manif_data.isbn_020_az)) <= \
               len(mak_manif_data.isbn_020_az)) or (not bn_manif_data.isbn_020_az or not \
               mak_manif_data.isbn_020_az):
                #print('ISBN case 1')
                last_5_char_245 = bn_manif_data.title_245[-5:] == mak_manif_data.title_245[-5:]
                num_245 = bn_manif_data.numbers_from_title_245 == mak_manif_data.numbers_from_title_245
                place_pub_260 = bn_manif_data.place_pub_260_a_first_word == mak_manif_data.place_pub_260_a_first_word
                num_pages = bn_manif_data.num_of_pages_300_a in [mak_manif_data.num_of_pages_300_a,
                                                                 mak_manif_data.num_of_pages_300_a - 1,
                                                                 mak_manif_data.num_of_pages_300_a + 1]
                b_form = mak_manif_data.b_format + 0.125 * mak_manif_data.b_format >= bn_manif_data.b_format >= \
                         mak_manif_data.b_format - 0.125 * mak_manif_data.b_format
                edition = bn_manif_data.edition == mak_manif_data.edition

                if last_5_char_245 and num_245 and place_pub_260 and num_pages and b_form and edition:
                    matched_from_245_with_edition.add(candidate)
                    #print('There is a match - no editions or editions are the same.')
                if last_5_char_245 and num_245 and place_pub_260 and num_pages and b_form and not edition:
                    matched_from_245_without_edition.add(candidate)
                    #print('There is a match, but editions are different.')

            if bn_manif_data.isbn_020_az and mak_manif_data.isbn_020_az and \
               bn_manif_data.isbn_020_az == mak_manif_data.isbn_020_az:
                #print('ISBN case 2')
                last_5_char_245 = bn_manif_data.title_245[-5:] == mak_manif_data.title_245[-5:]
                num_245 = bn_manif_data.numbers_from_title_245 == mak_manif_data.numbers_from_title_245
                place_pub_260 = bn_manif_data.place_pub_260_a_first_word == mak_manif_data.place_pub_260_a_first_word
                num_pages = bn_manif_data.num_of_pages_300_a in [mak_manif_data.num_of_pages_300_a,
                                                                 mak_manif_data.num_of_pages_300_a - 1,
                                                                 mak_manif_data.num_of_pages_300_a + 1]
                b_form = mak_manif_data.b_format + 0.125 * mak_manif_data.b_format >= bn_manif_data.b_format >= \
                         mak_manif_data.b_format - 0.125 * mak_manif_data.b_format
                edition = bn_manif_data.edition == mak_manif_data.edition

                if (num_245 and place_pub_260 and num_pages and b_form and edition and not last_5_char_245) or \
                   (last_5_char_245 and place_pub_260 and num_pages and b_form and edition and not num_245) or \
                   (last_5_char_245 and num_245 and num_pages and b_form and edition and not place_pub_260) or \
                   (num_245 and place_pub_260 and num_pages and b_form and edition and last_5_char_245):
                    matched_from_245_with_edition.add(candidate)
                    #print('There is a match - no editions or editions are the same.')

                if last_5_char_245 and place_pub_260 and num_pages and b_form and num_245 and not edition:
                    matched_from_245_without_edition.add(candidate)
                    #print('There is a match, but editions are different.')

    if matched_from_245_with_edition:
        return list(matched_from_245_with_edition)[0]
    if matched_from_245_without_edition:
        return list(matched_from_245_without_edition)[0]

    candidates_490 = set()

    cand_1_490 = index_490.get(mak_manif_data.title_245_no_offset)
    cand_2_490 = index_490.get(mak_manif_data.title_245_with_offset)

    if cand_1_490:
        candidates_490.update(cand_1_245)
    if cand_2_490:
        candidates_490.update(cand_2_245)

    matched_from_490_with_edition = set()
    matched_from_490_without_edition = set()

    for candidate in list(candidates_490):
        bn_manif_data = get_data_for_matching(read_marc_from_binary(index_id.get(candidate)))

        if bn_manif_data.ldr_67 == mak_manif_data.ldr_67 and bn_manif_data.val_008_0614 == mak_manif_data.val_008_0614:
            if (bn_manif_data.isbn_020_az and mak_manif_data.isbn_020_az and
               bn_manif_data.isbn_020_az != mak_manif_data.isbn_020_az and
               len(set(bn_manif_data.isbn_020_az) | set(mak_manif_data.isbn_020_az)) <=
               len(bn_manif_data.isbn_020_az) and
               len(set(bn_manif_data.isbn_020_az) | set(mak_manif_data.isbn_020_az)) <=
               len(mak_manif_data.isbn_020_az)) or (not bn_manif_data.isbn_020_az or not mak_manif_data.isbn_020_az):

                #print('ISBN case 1')
                place_pub_260 = bn_manif_data.place_pub_260_a_first_word == mak_manif_data.place_pub_260_a_first_word
                num_pages = bn_manif_data.num_of_pages_300_a in [mak_manif_data.num_of_pages_300_a,
                                                                     mak_manif_data.num_of_pages_300_a - 1,
                                                                     mak_manif_data.num_of_pages_300_a + 1]
                b_form = mak_manif_data.b_format + 0.125 * mak_manif_data.b_format >= bn_manif_data.b_format >= \
                             mak_manif_data.b_format - 0.125 * mak_manif_data.b_format
                edition = bn_manif_data.edition == mak_manif_data.edition

                if place_pub_260 and num_pages and b_form and edition:
                    matched_from_490_with_edition.add(candidate)
                    #print('There is a match - no editions or editions are the same.')
                if place_pub_260 and num_pages and b_form and not edition:
                    matched_from_490_without_edition.add(candidate)
                    #print('There is a match, but editions are different.')

        if bn_manif_data.isbn_020_az and mak_manif_data.isbn_020_az and \
                bn_manif_data.isbn_020_az == mak_manif_data.isbn_020_az:
            #print('ISBN case 2')
            place_pub_260 = bn_manif_data.place_pub_260_a_first_word == mak_manif_data.place_pub_260_a_first_word
            num_pages = bn_manif_data.num_of_pages_300_a in [mak_manif_data.num_of_pages_300_a,
                                                             mak_manif_data.num_of_pages_300_a - 1,
                                                             mak_manif_data.num_of_pages_300_a + 1]
            b_form = mak_manif_data.b_format + 0.125 * mak_manif_data.b_format >= bn_manif_data.b_format >= \
                     mak_manif_data.b_format - 0.125 * mak_manif_data.b_format
            edition = bn_manif_data.edition == mak_manif_data.edition

            if (place_pub_260 and num_pages and b_form and edition) or \
               (num_pages and b_form and edition and not place_pub_260):
                matched_from_490_with_edition.add(candidate)
                #print('There is a match - no editions or editions are the same.')

            if place_pub_260 and num_pages and b_form and not edition:
                matched_from_490_without_edition.add(candidate)
                #print('There is a match, but editions are different.')

    if matched_from_490_with_edition:
        return list(matched_from_490_with_edition)[0]
    if matched_from_490_without_edition:
        return list(matched_from_490_without_edition)[0]

    return match
