from rest_framework.test import APITransactionTestCase

from search.tests.fill_elastic import load_docs


class QueryTest(APITransactionTestCase):
    """
    Testing commonly used datasets
    """

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        load_docs()

    def test_openbare_ruimte(self):
        response = self.client.get(
            "/atlas/search/openbareruimte/", {'q': "Prinsengracht"})
        self.assertEqual(response.status_code, 200)
        self.assertIn('results', response.data)
        self.assertIn('count', response.data)
        # self.assertEqual(response.data['count'], 1)

        self.assertEqual(
            response.data['results'][0]['naam'], "Prinsengracht")

        self.assertEqual(
            response.data['results'][0]['subtype'], "water")

    def test_subject(self):
        """
        We are not authorized. should fail
        """
        response = self.client.get(
            "/search/kadastraalsubject/", {'q': "kikker"})
        self.assertEqual(response.status_code, 200)
        self.assertNotIn('results', response.data)

    def test_bouwblok(self):
        response = self.client.get(
            "/search/bouwblok/", {'q': "RN3"})
        self.assertEqual(response.status_code, 200)
        self.assertIn('results', response.data)
        self.assertIn('count', response.data)
        self.assertEqual(response.data['count'], 1)
        self.assertEqual(
            response.data['results'][0]['code'], "RN35")

    def test_adres(self):
        response = self.client.get(
            "/search/postcode/", {'q': "1016 SZ 228 a 1"})
        self.assertEqual(response.status_code, 200)
        self.assertIn('results', response.data)
        self.assertIn('count', response.data)

        # not due to elk scoring it could happen 228 B, scores better
        # then 228 A
        adres = response.data['results'][0]['adres']
        self.assertTrue(adres.startswith("Rozenstraat 228"))
        self.assertFalse(expr='order' in response.data['results'][0],
                         msg='Order data should be stripped from result')

    # def test_postcode_exact(self):
    #    response = self.client.get(
    #        "/search/postcode/", {'q': "1016 SZ 228 a 1"})
    #    self.assertEqual(response.status_code, 200)

    #    # now due to elk scoring it could happen 228 B, scores better
    #    # then 228 A

    #    adres = response.data['adres']
    #    self.assertTrue(
    #        adres.startswith("Rozenstraat 228")
    #    )

    #def test_postcode_exact_incorrect_house_num(self):
    #    response = self.client.get(
    #        "/search/postcode/", {'q': "1016 SZ 1"})
    #    self.assertEqual(response.status_code, 200)

    #    self.assertNotIn('adres', response.data)

    #def test_postcode_exact_no_house_num(self):
    #    response = self.client.get(
    #        "/search/postcode/", {'q': "1016 SZ"})
    #    self.assertEqual(response.status_code, 200)

    #    # we should get openbare ruimte
    #    self.assertNotIn('adres', response.data)

    # /typeahead/logica

    # /atlas/typeahead/gebieden/
    # /atlas/typeahead/brk/
    # /atlas/typeahead/bag/

    def test_typeahead_gebied(self):
        response = self.client.get(
            "/atlas/typeahead/gebieden/", {'q': "Centrum"})
        self.assertEqual(response.status_code, 200)
        self.assertIn('Centrum', str(response.data))

    def test_typeahead_bag_postcode(self):
        response = self.client.get(
            "/atlas/typeahead/bag/", {'q': "1016 SZ"})
        self.assertEqual(response.status_code, 200)

        self.assertIn('Rozenstraat', str(response.data))

    def test_typeahead_bag_adres(self):
        response = self.client.get(
            "/atlas/typeahead/bag/", {'q': "Rozenstraat 228"})
        self.assertEqual(response.status_code, 200)

        self.assertIn('Rozenstraat 228', str(response.data))

    def test_typeahead_subject(self):
        """
        We are not authorized. should fail
        """
        response = self.client.get(
            "/atlas/typeahead/brk/", {'q': "kikker"})
        self.assertEqual(response.status_code, 200)

        self.assertNotIn('kikker', str(response.data))

    def test_bouwblok_typeahead(self):

        response = self.client.get(
            "/atlas/typeahead/bag/", {'q': "RN35"})
        self.assertEqual(response.status_code, 200)

        self.assertIn("RN35", str(response.data))