import time

from rest_framework.test import APITestCase

import datasets.bag.batch
from datasets.bag.tests import factories as bag_factories
import datasets.brk.batch
from batch import batch


class TypeaheadTest(APITestCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()

        anjeliersstraat = bag_factories.OpenbareRuimteFactory.create(
            naam="Anjeliersstraat")

        bag_factories.NummeraanduidingFactory.create(
            openbare_ruimte=anjeliersstraat,
            postcode='1000AN',
            huisnummer=11, huisletter='A', hoofdadres=True)

        bag_factories.NummeraanduidingFactory.create(
            openbare_ruimte=anjeliersstraat,
            # postcode='1000AN',
            huisnummer=11, huisletter='B', hoofdadres=True)

        bag_factories.NummeraanduidingFactory.create(
            openbare_ruimte=anjeliersstraat,
            # postcode='1000AN',
            huisnummer=11, huisletter='C', hoofdadres=True)

        bag_factories.NummeraanduidingFactory.create(
            # postcode='1000AN',
            openbare_ruimte=anjeliersstraat,
            huisnummer=12, hoofdadres=True)

        marnix_kade = bag_factories.OpenbareRuimteFactory.create(
            naam="Marnixkade")

        bag_factories.NummeraanduidingFactory.create(
            openbare_ruimte=marnix_kade,
            huisnummer=36, huisletter='F',
            hoofdadres=True, postcode='1051XR')

        bag_factories.NummeraanduidingFactory.create(
            openbare_ruimte=marnix_kade,
            huisnummer=36, huisletter='F',
            hoofdadres=True, postcode='1052WR')

        batch.execute(datasets.bag.batch.IndexJob())

        batch.execute(datasets.brk.batch.IndexKadasterJob())

    def test_match_openbare_ruimte(self):
        response = self.client.get('/api/atlas/typeahead/', dict(q="an"))
        self.assertEqual(response.status_code, 200)

        self.assertIn("Anjeliersstraat", str(response.data))

    def test_match_openbare_ruimte_lowercase(self):
        response = self.client.get('/api/atlas/typeahead/', dict(q="AN"))
        self.assertEqual(response.status_code, 200)

        self.assertIn("Anjeliersstraat", str(response.data))

    def test_match_maximum_length(self):
        response = self.client.get('/api/atlas/typeahead/', dict(q="a"))
        self.assertEqual(response.status_code, 200)

        lst = response.data['verblijfsobject']
        self.assertEqual(len(lst), 5)

    def test_match_adresseerbaar_object(self):
        response = self.client.get('/api/atlas/typeahead/', dict(q="anjelier"))
        self.assertEqual(response.status_code, 200)
        # vbos = response.data['verblijfsobject']
        vbo = response.data['openbare ruimte'][0]
        self.assertEqual(vbo['item'], "Anjeliersstraat")

        self.assertIn("Anjeliersstraat 11", str(response.data))

    def test_match_adresseerbaar_object_met_huisnummer(self):
        response = self.client.get(
            '/api/atlas/typeahead/',
            dict(q="anjeliersstraat 11"))

        self.assertIn("Anjeliersstraat 11", str(response.data))

    def test_match_postcode(self):
        response = self.client.get("/api/atlas/typeahead/", dict(q='105'))
        self.assertIn("105", str(response.data))
