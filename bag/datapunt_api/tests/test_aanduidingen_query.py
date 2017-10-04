# Python
# Package
from rest_framework.test import APITestCase
# Project
from batch import batch
import datasets.bag.batch
from datasets.bag.tests import factories as bag_factories
import datasets.brk.batch


class SubjectSearchTest(APITestCase):

    @classmethod
    def setUpClass(cls):

        super().setUpClass()

        ptt_straat = bag_factories.OpenbareRuimteFactory.create(
            naam="Marius Cornelis straat",
            naam_ptt="M Cornelisstr",
            type='01')

        nen_straat = bag_factories.OpenbareRuimteFactory.create(
            naam="Stoom Maker Weg",
            naam_nen="S Maker wg",
            type='01')

        straat = bag_factories.OpenbareRuimteFactory.create(
            naam="Anjeliersstraat", type='01')

        gracht = bag_factories.OpenbareRuimteFactory.create(
            naam="Prinsengracht", type='01')

        bag_factories.NummeraanduidingFactory.create(
            huisnummer=192,
            huisletter='A',
            type='01',  # Verblijfsobject
            openbare_ruimte=gracht
        )

        cls.anje42F = bag_factories.NummeraanduidingFactory.create(
            huisnummer=42,
            huisletter='F',
            huisnummer_toevoeging='1',
            type='01',  # Verblijfsobject
            openbare_ruimte=straat
        )

        bag_factories.NummeraanduidingFactory.create(
            huisnummer=43,
            huisletter='F',
            huisnummer_toevoeging='1',
            type='01',  # Verblijfsobject
            openbare_ruimte=straat
        )

        bag_factories.NummeraanduidingFactory.create(
            huisnummer=99,
            huisletter='',
            type='01',  # Verblijfsobject
            openbare_ruimte=ptt_straat
        )

        bag_factories.NummeraanduidingFactory.create(
            huisnummer=100,
            huisletter='',
            postcode='1234AB',
            type='01',  # Verblijfsobject
            openbare_ruimte=nen_straat
        )

        batch.execute(datasets.bag.batch.IndexBagJob())
        batch.execute(datasets.brk.batch.IndexKadasterJob())

    def test_no_match_query(self):
        response = self.client.get(
            '/atlas/search/adres/', {'q': 'x'})
        self.assertEqual(response.status_code, 200)
        self.assertIn('results', response.data)
        self.assertIn('count', response.data)

        self.assertEqual(response.data['count'], 0)

    def test_straat_query(self):
        response = self.client.get(
            '/atlas/search/adres/', {'q': 'anjel'})
        self.assertEqual(response.status_code, 200)
        self.assertIn('results', response.data)
        self.assertIn('count', response.data)

        self.assertEqual(response.data['count'], 2)

        first = response.data['results'][0]
        self.assertEqual(first['straatnaam'], "Anjeliersstraat")

    def test_straat_vbo_status(self):
        response = self.client.get(
            '/atlas/search/adres/', {'q': 'Anjeliersstraat 42 F'})

        self.assertEqual(response.status_code, 200)
        self.assertIn('results', response.data)
        self.assertIn('count', response.data)

        # exact 1 result
        self.assertEqual(response.data['count'], 1)

        first = response.data['results'][0]

        vbo_status = self.anje42F.adresseerbaar_object.status

        self.assertEqual(
            first['vbo_status'][0]['omschrijving'], vbo_status.omschrijving)

    def test_gracht_query(self):
        response = self.client.get(
            "/atlas/search/adres/", {'q': "prinsengracht 192"})
        self.assertEqual(response.status_code, 200)
        self.assertIn('results', response.data)
        self.assertIn('count', response.data)
        self.assertEqual(response.data['count'], 1)

        self.assertEqual(
            response.data['results'][0]['adres'], "Prinsengracht 192A")

    def test_postcode_huisnummer_query(self):
        response = self.client.get(
            '/atlas/search/adres/', {'q': '1234AB 100'})

        self.assertEqual(response.status_code, 200)
        self.assertIn('results', response.data)
        self.assertIn('count', response.data)

        self.assertEqual(response.data['count'], 1)

        first = response.data['results'][0]
        self.assertEqual(first['straatnaam'], "Stoom Maker Weg")

    def test_nen_query(self):
        response = self.client.get(
            "/atlas/search/adres/", {'q': "s maker wg"})
        self.assertEqual(response.status_code, 200)
        self.assertIn('results', response.data)
        self.assertIn('count', response.data)
        self.assertEqual(response.data['count'], 1)

        self.assertEqual(
            response.data['results'][0]['adres'], "Stoom Maker Weg 100")

    def test_ptt_query(self):
        response = self.client.get(
            "/atlas/search/adres/", {'q': 'M Cornelisstr'})
        self.assertEqual(response.status_code, 200)
        self.assertIn('results', response.data)
        self.assertIn('count', response.data)
        self.assertEqual(response.data['count'], 1)

        self.assertEqual(
            response.data['results'][0]['adres'], "Marius Cornelis straat 99")