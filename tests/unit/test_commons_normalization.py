import unittest

import commons.normalization as cn


class TestPrepareNameForIndexing(unittest.TestCase):
    def test_4_1(self):
        data = 'Tytuł$ nie tylko z literami i cyframi. cz. 1? \n $%'
        result = cn.prepare_name_for_indexing(data)
        self.assertEqual('TYTUŁ NIE TYLKO Z LITERAMI I CYFRAMI CZ 1', result)

    def test_4_2(self):
        data = 'Wielokrotne   białe    znaki  są redukowane \n do jednej \t spacji.'
        result = cn.prepare_name_for_indexing(data)
        self.assertEqual('WIELOKROTNE BIAŁE ZNAKI SĄ REDUKOWANE DO JEDNEJ SPACJI', result)

    def test_4_3(self):
        data =' Białe znaki z początku i końca są usuwane    \n'
        result = cn.prepare_name_for_indexing(data)
        self.assertEqual('BIAŁE ZNAKI Z POCZĄTKU I KOŃCA SĄ USUWANE', result)

    def test_4_4(self):
        data = 'Wszystko podniesione do WIeLKIch liter'
        result = cn.prepare_name_for_indexing(data)
        self.assertEqual('WSZYSTKO PODNIESIONE DO WIELKICH LITER', result)


class TestNormalizeTitle(unittest.TestCase):
    def test_1(self):
        data = '   usuń białe znaki z początku i końca /  '
        result = cn.normalize_title(data)
        self.assertEqual('usuń białe znaki z początku i końca', result)

    def test_2(self):
        data = ['usuń  /', 'usuń ,', 'usuń ;', 'usuń  :', 'usuń=', 'usuń .']
        result = [cn.normalize_title(title) for title in data]
        self.assertEqual(['usuń', 'usuń', 'usuń', 'usuń', 'usuń', 'usuń'], result)


if __name__ == '__main__':
    unittest.main()
