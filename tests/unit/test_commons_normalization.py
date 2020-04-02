import unittest

import commons.normalization as cn


class TestPrepareNameForIndexing(unittest.TestCase):
    def test_4_1(self):
        data = 'Tytuł$ nie tylko z literami i cyframi. cz. 1? \n $%'
        result = cn.prepare_name_for_indexing(data)
        self.assertEqual(result, 'TYTUŁ NIE TYLKO Z LITERAMI I CYFRAMI CZ 1')
    def test_4_2(self):
        data = 'Wielokrotne   białe    znaki  są redukowane \n do jednej \t spacji.'
        result = cn.prepare_name_for_indexing(data)
        self.assertEqual(result, 'WIELOKROTNE BIAŁE ZNAKI SĄ REDUKOWANE DO JEDNEJ SPACJI')
    def test_4_3(self):
        data =' Białe znaki z początku i końca są usuwane    \n'
        result = cn.prepare_name_for_indexing(data)
        self.assertEqual(result, 'BIAŁE ZNAKI Z POCZĄTKU I KOŃCA SĄ USUWANE')
    def test_4_4(self):
        data = 'Wszystko podniesione do WIeLKIch liter'
        result = cn.prepare_name_for_indexing(data)
        self.assertEqual(result, 'WSZYSTkO PODNIESIONE DO WIELKICH LITER')


if __name__ == '__main__':
    unittest.main()
