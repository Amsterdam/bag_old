# Python
# import datetime
# import json
import logging
import os
# Packages
from django.conf import settings
from django.contrib.gis.geos import Point
from django.db import connection
from django.utils.text import slugify
# import requests
# Project
from batch import batch
from datasets.generic import uva2, index, database, geo, metadata
from . import models, documents

log = logging.getLogger(__name__)


class CodeOmschrijvingUvaTask(batch.BasicTask):
    model = None
    code = None

    def __init__(self, path):
        self.path = path

    def before(self):
        pass

    def after(self):
        pass

    def process(self):
        avrs = uva2.process_uva2(self.path, self.code, self.process_row)
        self.model.objects.bulk_create(avrs, batch_size=database.BATCH_SIZE)

    def process_row(self, r):
        # noinspection PyCallingNonCallable
        return self.model(pk=r['Code'], omschrijving=r['Omschrijving'])


class ImportAvrTask(CodeOmschrijvingUvaTask):
    name = "Import AVR"
    code = "AVR"
    model = models.RedenAfvoer


class ImportOvrTask(CodeOmschrijvingUvaTask):
    name = "Import OVR"
    code = "OVR"
    model = models.RedenOpvoer


class ImportBrnTask(CodeOmschrijvingUvaTask):
    name = "Import BRN"
    code = "BRN"
    model = models.Bron


class ImportStsTask(CodeOmschrijvingUvaTask):
    name = "Import STS"
    code = "STS"
    model = models.Status


class ImportEgmTask(CodeOmschrijvingUvaTask):
    name = "Import EGM"
    code = "EGM"
    model = models.Eigendomsverhouding


class ImportFngTask(CodeOmschrijvingUvaTask):
    name = "Import FNG"
    code = "FNG"
    model = models.Financieringswijze


class ImportLggTask(CodeOmschrijvingUvaTask):
    name = "Import LGG"
    code = "LGG"
    model = models.Ligging


class ImportGbkTask(CodeOmschrijvingUvaTask):
    name = "Import GBK"
    code = "GBK"
    model = models.Gebruik


class ImportLocTask(CodeOmschrijvingUvaTask):
    name = "Import LOC"
    code = "LOC"
    model = models.LocatieIngang


class ImportTggTask(CodeOmschrijvingUvaTask):
    name = "Import TGG"
    code = "TGG"
    model = models.Toegang


class ImportGmeTask(batch.BasicTask):
    name = "Import GME"

    def __init__(self, path):
        self.path = path

    def before(self):
        pass

    def after(self):
        pass

    def process(self):
        gemeentes = uva2.process_uva2(self.path, "GME", self.process_row)
        models.Gemeente.objects.bulk_create(
            gemeentes, batch_size=database.BATCH_SIZE)

    def process_row(self, r):
        if not uva2.geldig_tijdvak(r):
            return

        return models.Gemeente(
            pk=r['sleutelVerzendend'],
            code=r['Gemeentecode'],
            naam=r['Gemeentenaam'],
            verzorgingsgebied=uva2.uva_indicatie(
                r['IndicatieVerzorgingsgebied']),
            vervallen=uva2.uva_indicatie(r['Indicatie-vervallen']),
            begin_geldigheid=uva2.uva_datum(
                r['TijdvakGeldigheid/begindatumTijdvakGeldigheid']),
            einde_geldigheid=uva2.uva_datum(
                r['TijdvakGeldigheid/einddatumTijdvakGeldigheid']),
        )


class ImportSdlTask(batch.BasicTask, metadata.UpdateDatasetMixin):
    name = "Import SDL"
    dataset_id = 'gebieden-stadsdeel'

    def __init__(self, bag_path, shp_path):
        self.shp_path = shp_path
        self.bag_path = bag_path
        self.gemeentes = set()
        self.stadsdelen = dict()

    def before(self):
        self.gemeentes = set(
            models.Gemeente.objects.values_list("pk", flat=True))

    def after(self):
        self.gemeentes.clear()
        self.stadsdelen.clear()
        self.update_metadata_uva2(self.bag_path, 'SDL')

    def process(self):
        self.stadsdelen = dict(
            uva2.process_uva2(self.bag_path, "SDL", self.process_row))
        geo.process_shp(
            self.shp_path, "GBD_Stadsdeel.shp", self.process_feature)

        models.Stadsdeel.objects.bulk_create(
            self.stadsdelen.values(), batch_size=database.BATCH_SIZE)

    def process_row(self, r):
        if not uva2.uva_geldig(
                r['TijdvakGeldigheid/begindatumTijdvakGeldigheid'],
                r['TijdvakGeldigheid/einddatumTijdvakGeldigheid']):
            return None

        if not uva2.uva_geldig(r['SDLGME/TijdvakRelatie/begindatumRelatie'],
                               r['SDLGME/TijdvakRelatie/einddatumRelatie']):
            return None

        pk = r['sleutelVerzendend']
        gemeente_id = r['SDLGME/GME/sleutelVerzendend'] or None

        if gemeente_id not in self.gemeentes:
            log.warn("""
                Stadsdeel {} references non-existing gemeente {};
                skipping""".format(
                pk, gemeente_id))
            return None

        code = r['Stadsdeelcode']
        return code, models.Stadsdeel(
            pk=pk,
            code=code,
            naam=r['Stadsdeelnaam'],
            brondocument_naam=r['Brondocumentverwijzing'],
            brondocument_datum=uva2.uva_datum(r['Brondocumentdatum']),
            ingang_cyclus=uva2.uva_datum(
                r['TijdvakGeldigheid/begindatumTijdvakGeldigheid']),
            vervallen=uva2.uva_indicatie(r['Indicatie-vervallen']),
            gemeente_id=gemeente_id,
            begin_geldigheid=uva2.uva_datum(
                r['TijdvakGeldigheid/begindatumTijdvakGeldigheid']),
            einde_geldigheid=uva2.uva_datum(
                r['TijdvakGeldigheid/einddatumTijdvakGeldigheid']),
        )

    def process_feature(self, feat):
        code = feat.get('CODE')
        if code not in self.stadsdelen:
            log.warning(
                """Stadsdeel/SHP {} references non-existing stadsdeel;
                skipping""".format(code))
            return

        self.stadsdelen[code].geometrie = geo.get_multipoly(feat.geom.wkt)


class ImportBrtTask(batch.BasicTask, metadata.UpdateDatasetMixin):
    name = "Import BRT"
    dataset_id = 'gebieden-buurt'

    def __init__(self, uva_path, shp_path):
        self.shp_path = shp_path
        self.uva_path = uva_path
        self.stadsdelen = set()
        self.buurten = dict()
        self.buurtcombinaties = dict()

    def before(self):
        # database.clear_models(models.Buurt)
        self.stadsdelen = set(
            models.Stadsdeel.objects.values_list("pk", flat=True))
        self.buurtcombinaties = dict(
            models.Buurtcombinatie.objects.values_list("code", "pk"))

    def after(self):
        self.stadsdelen.clear()
        self.buurten.clear()
        self.buurtcombinaties.clear()
        self.update_metadata_uva2(self.uva_path, 'BRT')

    def process(self):
        self.buurten = dict(
            uva2.process_uva2(self.uva_path, "BRT", self.process_row))
        geo.process_shp(
            self.shp_path, "GBD_Buurt.shp", self.process_feature)

        models.Buurt.objects.bulk_create(
            self.buurten.values(), batch_size=database.BATCH_SIZE)

    def process_row(self, r):
        if not uva2.uva_geldig(
                r['TijdvakGeldigheid/begindatumTijdvakGeldigheid'],
                r['TijdvakGeldigheid/einddatumTijdvakGeldigheid']):
            return None

        if not uva2.uva_geldig(r['BRTSDL/TijdvakRelatie/begindatumRelatie'],
                               r['BRTSDL/TijdvakRelatie/einddatumRelatie']):
            return None

        pk = r['sleutelVerzendend']
        stadsdeel_id = r['BRTSDL/SDL/sleutelVerzendend'] or None
        if stadsdeel_id not in self.stadsdelen:
            log.warn("""
            Buurt {} references non-existing stadsdeel {}; skipping
            """.format(pk, stadsdeel_id))
            return None

        code = r['Buurtcode']
        bc_code = code[:-1]
        bc_id = self.buurtcombinaties.get(bc_code)

        if not bc_id:
            log.warn("""
            Buurt {} references non-existing buurtcombinatie {}; ignoring
            """.format(pk, bc_code))

        return code, models.Buurt(
            pk=pk,
            code=code,
            naam=r['Buurtnaam'],
            brondocument_naam=r['Brondocumentverwijzing'],
            brondocument_datum=uva2.uva_datum(r['Brondocumentdatum']),
            ingang_cyclus=uva2.uva_datum(
                r['TijdvakGeldigheid/begindatumTijdvakGeldigheid']),
            stadsdeel_id=stadsdeel_id,
            vervallen=uva2.uva_indicatie(r['Indicatie-vervallen']),
            begin_geldigheid=uva2.uva_datum(
                r['TijdvakGeldigheid/begindatumTijdvakGeldigheid']),
            einde_geldigheid=uva2.uva_datum(
                r['TijdvakGeldigheid/einddatumTijdvakGeldigheid']),
            buurtcombinatie_id=bc_id,
        )

    def process_feature(self, feat):
        vollcode = feat.get('VOLLCODE')
        code = vollcode[1:]
        if code not in self.buurten:
            log.warning("""
            Buurt/SHP {} references non-existing buurt; skipping
            """.format(code))
            return

        self.buurten[code].geometrie = geo.get_multipoly(feat.geom.wkt)
        self.buurten[code].vollcode = vollcode


class ImportBbkTask(batch.BasicTask, metadata.UpdateDatasetMixin):
    name = "Import BBK"
    dataset_id = 'gebieden-bouwblok'

    def __init__(self, uva_path, shp_path):
        self.shp_path = shp_path
        self.uva_path = uva_path
        self.buurten = set()
        self.bouwblokken = dict()

    def before(self):
        self.buurten = set(models.Buurt.objects.values_list("pk", flat=True))

    def after(self):
        self.buurten.clear()
        self.bouwblokken.clear()
        self.update_metadata_uva2(self.uva_path, 'BBK')

    def process(self):
        self.bouwblokken = dict(
            uva2.process_uva2(self.uva_path, "BBK", self.process_row))
        geo.process_shp(
            self.shp_path, "GBD_Bouwblok.shp", self.process_feature)

        models.Bouwblok.objects.bulk_create(
            self.bouwblokken.values(), batch_size=database.BATCH_SIZE)

    def process_row(self, r):
        if not uva2.uva_geldig(
                r['TijdvakGeldigheid/begindatumTijdvakGeldigheid'],
                r['TijdvakGeldigheid/einddatumTijdvakGeldigheid']):
            return None

        if not uva2.uva_geldig(r['BBKBRT/TijdvakRelatie/begindatumRelatie'],
                               r['BBKBRT/TijdvakRelatie/einddatumRelatie']):
            return None

        pk = r['sleutelVerzendend']
        buurt_id = r['BBKBRT/BRT/sleutelVerzendend'] or None
        if buurt_id not in self.buurten:
            log.warning("""
            Bouwblok {} references non-existing buurt {}; ignoring
            """.format(pk, buurt_id))
            buurt_id = None

        code = r['Bouwbloknummer']
        return code, models.Bouwblok(
            pk=pk,
            code=code,
            ingang_cyclus=uva2.uva_datum(
                r['TijdvakGeldigheid/begindatumTijdvakGeldigheid']),
            buurt_id=buurt_id,
            begin_geldigheid=uva2.uva_datum(
                r['TijdvakGeldigheid/begindatumTijdvakGeldigheid']),
            einde_geldigheid=uva2.uva_datum(
                r['TijdvakGeldigheid/einddatumTijdvakGeldigheid']),
        )

    def process_feature(self, feat):
        code = feat.get('CODE')
        if code not in self.bouwblokken:
            log.warning("""
            BBK/SHP {} references non-existing bouwblok; skipping
            """.format(code))
            return

        self.bouwblokken[code].geometrie = geo.get_multipoly(feat.geom.wkt)


class ImportWplTask(batch.BasicTask):
    name = "Import WPL"

    def __init__(self, path):
        self.path = path
        self.gemeentes = set()

    def before(self):
        self.gemeentes = set(
            models.Gemeente.objects.values_list("pk", flat=True))

    def after(self):
        self.gemeentes.clear()

    def process(self):
        woonplaatsen = uva2.process_uva2(self.path, "WPL", self.process_row)
        models.Woonplaats.objects.bulk_create(
            woonplaatsen, batch_size=database.BATCH_SIZE)

    def process_row(self, r):
        if not uva2.geldig_tijdvak(r):
            return None

        if not uva2.geldige_relaties(r, 'WPLGME'):
            return None

        pk = r['sleutelverzendend']
        gemeente_id = r['WPLGME/GME/sleutelVerzendend']
        if gemeente_id not in self.gemeentes:
            log.warning("""
            Woonplaats {} references non-existing gemeente {}; skipping
            """.format(pk, gemeente_id))
            return None

        return models.Woonplaats(
            pk=pk,
            landelijk_id=r['Woonplaatsidentificatie'],
            naam=r['Woonplaatsnaam'],
            document_nummer=r['DocumentnummerMutatieWoonplaats'],
            document_mutatie=uva2.uva_datum(
                r['DocumentdatumMutatieWoonplaats']),
            naam_ptt=r['WoonplaatsPTTSchrijfwijze'],
            vervallen=uva2.uva_indicatie(r['Indicatie-vervallen']),
            gemeente_id=gemeente_id,
            begin_geldigheid=uva2.uva_datum(
                r['TijdvakGeldigheid/begindatumTijdvakGeldigheid']),
            einde_geldigheid=uva2.uva_datum(
                r['TijdvakGeldigheid/einddatumTijdvakGeldigheid']),
            mutatie_gebruiker=r['Mutatie-gebruiker'],
        )


class ImportOprTask(batch.BasicTask):
    name = "Import OPR"

    def __init__(self, path, wkt_path):
        self.path = path
        self.wkt_path = wkt_path
        self.bronnen = set()
        self.statussen = set()
        self.woonplaatsen = set()
        self.landelijke_ids = dict()
        self.openbare_ruimtes = dict()

    def before(self):
        self.bronnen = set(models.Bron.objects.values_list("pk", flat=True))
        self.statussen = set(
            models.Status.objects.values_list("pk", flat=True))
        self.woonplaatsen = set(
            models.Woonplaats.objects.values_list("pk", flat=True))

    def after(self):
        self.bronnen.clear()
        self.statussen.clear()
        self.woonplaatsen.clear()
        self.landelijke_ids.clear()
        self.openbare_ruimtes.clear()

    def process(self):
        self.landelijke_ids = uva2.read_landelijk_id_mapping(self.path, "OPR")
        self.openbare_ruimtes = dict(
            uva2.process_uva2(self.path, "OPR", self.process_row))
        geo.process_wkt(
            self.wkt_path, "BAG_OPENBARERUIMTE_GEOMETRIE.dat",
            self.process_wkt_row)
        models.OpenbareRuimte.objects.bulk_create(
            self.openbare_ruimtes.values(), batch_size=database.BATCH_SIZE)

    def process_row(self, r):
        if not uva2.geldig_tijdvak(r):
            return None

        if not uva2.geldige_relaties(r, 'OPRBRN', 'OPRSTS', 'OPRWPL'):
            return None

        pk = r['sleutelVerzendend']
        bron_id = r['OPRBRN/BRN/Code'] or None
        status_id = r['OPRSTS/STS/Code'] or None
        woonplaats_id = r['OPRWPL/WPL/sleutelVerzendend'] or None
        landelijk_id = self.landelijke_ids.get(pk)

        if not landelijk_id:
            log.error("""
            OpenbareRuimte {} references non-existing landelijk_id {}; skipping
            """.format(pk, pk))
            return

        if bron_id and bron_id not in self.bronnen:
            log.warning("""
            OpenbareRuimte {} references non-existing bron {}; ignoring
            """.format(pk, bron_id))
            bron_id = None

        if status_id not in self.statussen:
            log.warning("""
                OpenbareRuimte {} references non-existing status {}; ignoring
                """.format(pk, status_id))
            status_id = None

        if woonplaats_id not in self.woonplaatsen:
            log.warning("""
            OpenbareRuimte {} references non-existing woonplaats {}; skipping
            """.format(pk, woonplaats_id))
            return None

        return pk, models.OpenbareRuimte(
            pk=pk,
            landelijk_id=landelijk_id,
            type=r['TypeOpenbareRuimteDomein'],
            naam=r['NaamOpenbareRuimte'],
            code=r['Straatcode'],
            document_nummer=r['DocumentnummerMutatieOpenbareRuimte'],
            document_mutatie=uva2.uva_datum(
                r['DocumentdatumMutatieOpenbareRuimte']),
            straat_nummer=r['Straatnummer'],
            naam_nen=r['StraatnaamNENSchrijfwijze'],
            naam_ptt=r['StraatnaamPTTSchrijfwijze'],
            vervallen=uva2.uva_indicatie(r['Indicatie-vervallen']),
            bron_id=bron_id,
            status_id=status_id,
            woonplaats_id=woonplaats_id,
            begin_geldigheid=uva2.uva_datum(
                r['TijdvakGeldigheid/begindatumTijdvakGeldigheid']),
            einde_geldigheid=uva2.uva_datum(
                r['TijdvakGeldigheid/einddatumTijdvakGeldigheid']),
            mutatie_gebruiker=r['Mutatie-gebruiker'],
        )

    def process_wkt_row(self, wkt_id, geometrie):
        key = '0' + wkt_id
        if key not in self.openbare_ruimtes:
            log.warning("""
            OpenbareRuimte/WKT {} references non-existing openbare ruimte {};
            skipping """.format(wkt_id, key))
            return

        self.openbare_ruimtes[key].geometrie = geo.get_multipoly(geometrie)


class ImportNumTask(batch.BasicTask, metadata.UpdateDatasetMixin):
    name = "Import NUM"
    dataset_id = 'BAG'

    def __init__(self, path):
        self.path = path
        self.bronnen = set()
        self.statussen = set()
        self.openbare_ruimtes = set()

        self.ligplaatsen = set()
        self.standplaatsen = set()
        self.verblijfsobjecten = set()

        self.nummeraanduidingen = dict()
        self.landelijke_ids = dict()

    def before(self):
        self.bronnen = set(models.Bron.objects.values_list("pk", flat=True))
        self.statussen = set(
            models.Status.objects.values_list("pk", flat=True))
        self.openbare_ruimtes = set(
            models.OpenbareRuimte.objects.values_list("pk", flat=True))

        self.ligplaatsen = set(
            models.Ligplaats.objects.values_list("pk", flat=True))

        self.standplaatsen = set(
            models.Standplaats.objects.values_list("pk", flat=True))
        self.verblijfsobjecten = set(
            models.Verblijfsobject.objects.values_list("pk", flat=True))

    def after(self):
        self.bronnen.clear()
        self.statussen.clear()
        self.openbare_ruimtes.clear()

        self.ligplaatsen.clear()
        self.standplaatsen.clear()
        self.verblijfsobjecten.clear()

        self.nummeraanduidingen.clear()

        self.update_metadata_uva2(self.path, 'NUM')

    def process(self):
        self.landelijke_ids = uva2.read_landelijk_id_mapping(self.path, "NUM")
        self.nummeraanduidingen = dict(
            uva2.process_uva2(self.path, "NUM", self.process_num_row))
        uva2.process_uva2(self.path, "NUMLIGHFD", self.process_numlig_row)
        uva2.process_uva2(self.path, "NUMSTAHFD", self.process_numsta_row)
        uva2.process_uva2(self.path, "NUMVBOHFD", self.process_numvbo_row)
        uva2.process_uva2(self.path, "NUMVBONVN", self.process_numvbonvn_row)

        models.Nummeraanduiding.objects.bulk_create(
            self.nummeraanduidingen.values(), batch_size=database.BATCH_SIZE)

    def process_num_row(self, r):
        if not uva2.geldig_tijdvak(r):
            return None

        if not uva2.geldige_relaties(r, 'NUMBRN', 'NUMSTS', 'NUMOPR'):
            return None

        pk = r['sleutelVerzendend']
        bron_id = r['NUMBRN/BRN/Code'] or None
        status_id = r['NUMSTS/STS/Code'] or None
        openbare_ruimte_id = r['NUMOPR/OPR/sleutelVerzendend'] or None
        landelijk_id = self.landelijke_ids.get(
            r['IdentificatiecodeNummeraanduiding'])

        if not landelijk_id:
            log.error('Nummeraanduiding {} references non-existing landelijk_id {}; skipping'.format(pk, landelijk_id))
            return

        if bron_id and bron_id not in self.bronnen:
            log.warning('Nummeraanduiding {} references non-existing bron {}; ignoring'.format(pk, bron_id))
            bron_id = None

        if status_id not in self.statussen:
            log.warning('Nummeraanduiding {} references non-existing status {}; ignoring'.format(pk, status_id))
            status_id = None

        if openbare_ruimte_id not in self.openbare_ruimtes:
            log.warning('Nummeraanduiding {} references non-existing openbare ruimte {}; skipping'
                        .format(pk, openbare_ruimte_id))
            return None

        return pk, models.Nummeraanduiding(
            pk=pk,
            landelijk_id=landelijk_id,
            huisnummer=r['Huisnummer'],
            huisletter=r['Huisletter'],
            huisnummer_toevoeging=r['Huisnummertoevoeging'],
            postcode=r['Postcode'],
            document_mutatie=uva2.uva_datum(r['DocumentdatumMutatieNummeraanduiding']),
            document_nummer=r['DocumentnummerMutatieNummeraanduiding'],
            type=r['TypeAdresseerbaarObjectDomein'],
            adres_nummer=r['Adresnummer'],
            vervallen=uva2.uva_indicatie(r['Indicatie-vervallen']),
            bron_id=bron_id,
            status_id=status_id,
            openbare_ruimte_id=openbare_ruimte_id,
            begin_geldigheid=uva2.uva_datum(r['TijdvakGeldigheid/begindatumTijdvakGeldigheid']),
            einde_geldigheid=uva2.uva_datum(r['TijdvakGeldigheid/einddatumTijdvakGeldigheid']),
            mutatie_gebruiker=r['Mutatie-gebruiker'],
        )

    def process_numlig_row(self, r):
        if not uva2.geldig_tijdvak(r):
            return

        if not uva2.geldige_relaties(r, 'NUMLIGHFD'):
            return

        pk = r['sleutelVerzendend']
        ligplaats_id = r['NUMLIGHFD/LIG/sleutelVerzendend']
        if ligplaats_id not in self.ligplaatsen:
            log.warning('Num-Lig-Hfd {} references non-existing ligplaats {}; skipping'.format(pk, ligplaats_id))
            return None

        nummeraanduiding_id = r['IdentificatiecodeNummeraanduiding']
        if nummeraanduiding_id not in self.nummeraanduidingen:
            log.warning(
                'Num-Lig-Hfd {} references non-existing nummeraanduiding {}; skipping'.format(pk, nummeraanduiding_id))
            return None

        nummeraanduiding = self.nummeraanduidingen[nummeraanduiding_id]
        nummeraanduiding.ligplaats_id = ligplaats_id
        nummeraanduiding.hoofdadres = True

    def process_numsta_row(self, r):
        if not uva2.geldig_tijdvak(r):
            return

        if not uva2.geldige_relaties(r, 'NUMSTAHFD'):
            return

        pk = r['sleutelVerzendend']
        standplaats_id = r['NUMSTAHFD/STA/sleutelVerzendend']
        if standplaats_id not in self.standplaatsen:
            log.warning('Num-Sta-Hfd {} references non-existing standplaats {}; skipping'.format(pk, standplaats_id))
            return None

        nummeraanduiding_id = r['IdentificatiecodeNummeraanduiding']
        if nummeraanduiding_id not in self.nummeraanduidingen:
            log.warning(
                'Num-Sta-Hfd {} references non-existing nummeraanduiding {}; skipping'.format(pk, nummeraanduiding_id))
            return None

        nummeraanduiding = self.nummeraanduidingen[nummeraanduiding_id]
        nummeraanduiding.standplaats_id = standplaats_id
        nummeraanduiding.hoofdadres = True

    def process_numvbo_row(self, r):
        if not uva2.geldig_tijdvak(r):
            return

        if not uva2.geldige_relaties(r, 'NUMVBOHFD'):
            return

        pk = r['sleutelVerzendend']
        vbo_id = r['NUMVBOHFD/VBO/sleutelVerzendend']
        if vbo_id not in self.verblijfsobjecten:
            log.warning('Num-Vbo-Hfd {} references non-existing verblijfsobject {}; skipping'.format(pk, vbo_id))
            return None

        nummeraanduiding_id = r['IdentificatiecodeNummeraanduiding']
        if nummeraanduiding_id not in self.nummeraanduidingen:
            log.warning(
                'Num-Vbo-Hfd {} references non-existing nummeraanduiding {}; skipping'.format(pk, nummeraanduiding_id))
            return None

        nummeraanduiding = self.nummeraanduidingen[nummeraanduiding_id]
        nummeraanduiding.verblijfsobject_id = vbo_id
        nummeraanduiding.hoofdadres = True

    def process_numvbonvn_row(self, r):
        if not uva2.geldig_tijdvak(r):
            return

        if not uva2.geldige_relaties(r, 'NUMVBONVN'):
            return

        pk = r['sleutelVerzendend']
        vbo_id = r['NUMVBONVN/sleutelVerzendend']
        if vbo_id not in self.verblijfsobjecten:
            log.warning('Num-Vbo-Nvn {} references non-existing verblijfsobject {}; skipping'.format(pk, vbo_id))
            return None

        nummeraanduiding_id = r['IdentificatiecodeNummeraanduiding']
        if nummeraanduiding_id not in self.nummeraanduidingen:
            log.warning(
                'Num-Vbo-Nvn {} references non-existing nummeraanduiding {}; skipping'.format(pk, nummeraanduiding_id))
            return None

        nummeraanduiding = self.nummeraanduidingen[nummeraanduiding_id]
        nummeraanduiding.verblijfsobject_id = vbo_id
        nummeraanduiding.hoofdadres = False


class ImportLigTask(batch.BasicTask):
    name = "Import LIG"

    def __init__(self, bag_path, wkt_path):
        self.bag_path = bag_path
        self.wkt_path = wkt_path
        self.bronnen = set()
        self.statussen = set()
        self.buurten = set()
        self.landelijke_ids = dict()

        self.ligplaatsen = dict()

    def before(self):
        self.bronnen = set(models.Bron.objects.values_list("pk", flat=True))
        self.statussen = set(models.Status.objects.values_list("pk", flat=True))
        self.buurten = set(models.Buurt.objects.values_list("pk", flat=True))

    def after(self):
        self.bronnen.clear()
        self.statussen.clear()
        self.buurten.clear()

        self.ligplaatsen.clear()

    def process(self):
        self.landelijke_ids = uva2.read_landelijk_id_mapping(self.bag_path, "LIG")

        self.ligplaatsen = dict(uva2.process_uva2(self.bag_path, "LIG", self.process_row))
        geo.process_wkt(self.wkt_path, 'BAG_LIGPLAATS_GEOMETRIE.dat', self.process_wkt_row)

        models.Ligplaats.objects.bulk_create(self.ligplaatsen.values(), batch_size=database.BATCH_SIZE)

    def process_row(self, r):
        if not uva2.geldig_tijdvak(r):
            return None

        if not uva2.geldige_relaties(r, 'LIGBRN', 'LIGSTS', 'LIGBRT'):
            return None

        pk = r['sleutelverzendend']
        bron_id = r['LIGBRN/BRN/Code'] or None
        status_id = r['LIGSTS/STS/Code'] or None
        buurt_id = r['LIGBRT/BRT/sleutelVerzendend'] or None
        landelijk_id = self.landelijke_ids.get(r['Ligplaatsidentificatie'])

        if not landelijk_id:
            log.error('Ligplaats {} references non-existing landelijk_id {}; skipping'.format(pk, landelijk_id))
            return

        if bron_id and bron_id not in self.bronnen:
            log.warning('Ligplaats {} references non-existing bron {}; ignoring'.format(pk, bron_id))
            bron_id = None

        if status_id and status_id not in self.statussen:
            log.warning('Ligplaats {} references non-existing status {}; ignoring'.format(pk, status_id))
            status_id = None

        if buurt_id and buurt_id not in self.buurten:
            log.warning('Ligplaats {} references non-existing buurt {}; ignoring'.format(pk, status_id))
            buurt_id = None

        return pk, models.Ligplaats(
            pk=pk,
            landelijk_id=landelijk_id,
            vervallen=uva2.uva_indicatie(r['Indicatie-vervallen']),
            document_nummer=r['DocumentnummerMutatieLigplaats'],
            document_mutatie=uva2.uva_datum(r['DocumentdatumMutatieLigplaats']),
            bron_id=bron_id,
            status_id=status_id,
            buurt_id=buurt_id,
            begin_geldigheid=uva2.uva_datum(r['TijdvakGeldigheid/begindatumTijdvakGeldigheid']),
            einde_geldigheid=uva2.uva_datum(r['TijdvakGeldigheid/einddatumTijdvakGeldigheid']),
            mutatie_gebruiker=r['Mutatie-gebruiker'],
        )

    def process_wkt_row(self, wkt_id, geometrie):
        key = '0' + wkt_id
        if key not in self.ligplaatsen:
            log.warning('Ligplaats/WKT {} references non-existing ligplaats {}; skipping'.format(wkt_id, key))
            return

        self.ligplaatsen[key].geometrie = geometrie


class ImportStaTask(batch.BasicTask):
    name = "Import STA"

    def __init__(self, bag_path, wkt_path):
        self.bag_path = bag_path
        self.wkt_path = wkt_path
        self.bronnen = set()
        self.statussen = set()
        self.buurten = set()
        self.landelijke_ids = dict()

        self.standplaatsen = dict()

    def before(self):
        self.bronnen = set(models.Bron.objects.values_list("pk", flat=True))
        self.statussen = set(models.Status.objects.values_list("pk", flat=True))
        self.buurten = set(models.Buurt.objects.values_list("pk", flat=True))

    def after(self):
        self.bronnen.clear()
        self.statussen.clear()
        self.buurten.clear()
        self.standplaatsen.clear()

    def process(self):
        self.landelijke_ids = uva2.read_landelijk_id_mapping(self.bag_path, "STA")
        self.standplaatsen = dict(uva2.process_uva2(self.bag_path, "STA", self.process_row))
        geo.process_wkt(self.wkt_path, "BAG_STANDPLAATS_GEOMETRIE.dat", self.process_wkt_row)

        models.Standplaats.objects.bulk_create(self.standplaatsen.values(), batch_size=database.BATCH_SIZE)

    def process_row(self, r):
        if not uva2.geldig_tijdvak(r):
            return

        if not uva2.geldige_relaties(r, 'STABRN', 'STASTS', 'STABRT'):
            return

        pk = r['sleutelverzendend']
        bron_id = r['STABRN/BRN/Code'] or None
        status_id = r['STASTS/STS/Code'] or None
        buurt_id = r['STABRT/BRT/sleutelVerzendend'] or None
        landelijk_id = self.landelijke_ids.get(r['Standplaatsidentificatie'])

        if not landelijk_id:
            log.error('Standplaats {} references non-existing landelijk_id {}; skipping'.format(pk, landelijk_id))
            return

        if bron_id and bron_id not in self.bronnen:
            log.warning('Standplaats {} references non-existing bron {}; ignoring'.format(pk, bron_id))
            bron_id = None

        if status_id and status_id not in self.statussen:
            log.warning('Standplaats {} references non-existing status {}; ignoring'.format(pk, status_id))
            status_id = None

        if buurt_id and buurt_id not in self.buurten:
            log.warning('Standplaats {} references non-existing buurt {}; ignoring'.format(pk, status_id))
            buurt_id = None

        return pk, models.Standplaats(
            pk=pk,
            landelijk_id=landelijk_id,
            vervallen=uva2.uva_indicatie(r['Indicatie-vervallen']),
            document_nummer=r['DocumentnummerMutatieStandplaats'],
            document_mutatie=uva2.uva_datum(r['DocumentdatumMutatieStandplaats']),
            bron_id=bron_id,
            status_id=status_id,
            buurt_id=buurt_id,
            begin_geldigheid=uva2.uva_datum(r['TijdvakGeldigheid/begindatumTijdvakGeldigheid']),
            einde_geldigheid=uva2.uva_datum(r['TijdvakGeldigheid/einddatumTijdvakGeldigheid']),
            mutatie_gebruiker=r['Mutatie-gebruiker'],
        )

    def process_wkt_row(self, wkt_id, geometrie):
        key = '0' + wkt_id
        if key not in self.standplaatsen:
            log.warning('Standplaats/WKT {} references non-existing standplaats {}; skipping'.format(wkt_id, key))
            return

        self.standplaatsen[key].geometrie = geometrie


class ImportVboTask(batch.BasicTask):
    name = "Import VBO"

    def __init__(self, path):
        self.path = path
        self.redenen_afvoer = set()
        self.redenen_opvoer = set()
        self.bronnen = set()
        self.eigendomsverhoudingen = set()
        self.financieringswijzes = set()
        self.gebruik = set()
        self.locaties_ingang = set()
        self.liggingen = set()
        self.toegang = set()
        self.statussen = set()
        self.buurten = set()
        self.landelijke_ids = dict()

    def before(self):
        self.redenen_afvoer = set(models.RedenAfvoer.objects.values_list("pk", flat=True))
        self.redenen_opvoer = set(models.RedenOpvoer.objects.values_list("pk", flat=True))
        self.bronnen = set(models.Bron.objects.values_list("pk", flat=True))
        self.eigendomsverhoudingen = set(models.Eigendomsverhouding.objects.values_list("pk", flat=True))
        self.financieringswijzes = set(models.Financieringswijze.objects.values_list("pk", flat=True))
        self.gebruik = set(models.Gebruik.objects.values_list("pk", flat=True))
        self.locaties_ingang = set(models.LocatieIngang.objects.values_list("pk", flat=True))
        self.liggingen = set(models.Ligging.objects.values_list("pk", flat=True))
        self.toegang = set(models.Toegang.objects.values_list("pk", flat=True))
        self.statussen = set(models.Status.objects.values_list("pk", flat=True))
        self.buurten = set(models.Buurt.objects.values_list("pk", flat=True))

    def after(self):
        self.redenen_afvoer.clear()
        self.redenen_opvoer.clear()
        self.bronnen.clear()
        self.eigendomsverhoudingen.clear()
        self.financieringswijzes.clear()
        self.gebruik.clear()
        self.locaties_ingang.clear()
        self.liggingen.clear()
        self.toegang.clear()
        self.statussen.clear()
        self.buurten.clear()

    def process(self):
        self.landelijke_ids = uva2.read_landelijk_id_mapping(self.path, "VBO")
        verblijfsobjecten = uva2.process_uva2(self.path, "VBO", self.process_row)
        models.Verblijfsobject.objects.bulk_create(verblijfsobjecten, batch_size=database.BATCH_SIZE)

    def process_row(self, r):
        if not uva2.geldig_tijdvak(r):
            return

        if not uva2.geldige_relaties(r, 'VBOAVR', 'VBOOVR', 'VBOBRN', 'VBOEGM', 'VBOFNG', 'VBOGBK', 'VBOLOC', 'VBOLGG', 'VBOMNT',
                                     'VBOTGG', 'VBOOVR', 'VBOSTS', 'VBOBRT'):
            return

        x = r['X-Coordinaat']
        y = r['Y-Coordinaat']
        if x and y:
            geo = Point(int(x), int(y))
        else:
            geo = None

        pk = r['sleutelverzendend']
        reden_afvoer_id = r['VBOAVR/AVR/Code'] or None
        reden_opvoer_id = r['VBOOVR/OVR/Code'] or None
        bron_id = r['VBOBRN/BRN/Code'] or None
        eigendomsverhouding_id = r['VBOEGM/EGM/Code'] or None
        financieringswijze_id = r['VBOFNG/FNG/Code'] or None
        gebruik_id = r['VBOGBK/GBK/Code'] or None
        locatie_ingang_id = r['VBOLOC/LOC/Code'] or None
        ligging_id = r['VBOLGG/LGG/Code'] or None
        toegang_id = r['VBOTGG/TGG/Code'] or None
        status_id = r['VBOSTS/STS/Code'] or None
        buurt_id = r['VBOBRT/BRT/sleutelVerzendend'] or None
        landelijk_id = self.landelijke_ids.get(r['Verblijfsobjectidentificatie'])

        if not landelijk_id:
            log.error('Verblijfsobject {} references non-existing landelijk_id {}; skipping'.format(pk, landelijk_id))
            return

        if reden_afvoer_id and reden_afvoer_id not in self.redenen_afvoer:
            log.warning('Verblijfsobject {} references non-existing reden afvoer {}; ignoring'.format(pk, bron_id))
            reden_afvoer_id = None

        if reden_opvoer_id and reden_opvoer_id not in self.redenen_opvoer:
            log.warning('Verblijfsobject {} references non-existing reden opvoer {}; ignoring'.format(pk, bron_id))
            reden_opvoer_id = None

        if bron_id and bron_id not in self.bronnen:
            log.warning('Verblijfsobject {} references non-existing bron {}; ignoring'.format(pk, bron_id))
            bron_id = None

        if eigendomsverhouding_id and eigendomsverhouding_id not in self.eigendomsverhoudingen:
            log.warning('Verblijfsobject {} references non-existing eigendomsverhouding {}; ignoring'.format(pk,
                                                                                                             eigendomsverhouding_id))
            eigendomsverhouding_id = None

        if financieringswijze_id and financieringswijze_id not in self.financieringswijzes:
            log.warning('Verblijfsobject {} references non-existing financieringswijze {}; ignoring'.format(pk,
                                                                                                            financieringswijze_id))
            financieringswijze_id = None

        if gebruik_id and gebruik_id not in self.gebruik:
            log.warning('Verblijfsobject {} references non-existing gebruik {}; ignoring'.format(pk, gebruik_id))
            gebruik_id = None

        if locatie_ingang_id and locatie_ingang_id not in self.locaties_ingang:
            log.warning(
                'Verblijfsobject {} references non-existing locatie ingang {}; ignoring'.format(pk, locatie_ingang_id))
            locatie_ingang_id = None

        if ligging_id and ligging_id not in self.liggingen:
            log.warning('Verblijfsobject {} references non-existing ligging {}; ignoring'.format(pk, ligging_id))
            ligging_id = None

        if toegang_id and toegang_id not in self.toegang:
            log.warning('Verblijfsobject {} references non-existing toegang {}; ignoring'.format(pk, toegang_id))
            toegang_id = None

        if status_id and status_id not in self.statussen:
            log.warning('Verblijfsobject {} references non-existing status {}; ignoring'.format(pk, status_id))
            status_id = None

        if buurt_id and buurt_id not in self.buurten:
            log.warning('Verblijfsobject {} references non-existing bron {}; ignoring'.format(pk, buurt_id))
            buurt_id = None

        return models.Verblijfsobject(
            pk=pk,
            landelijk_id=landelijk_id,
            geometrie=geo,
            gebruiksdoel_code=(
                r['GebruiksdoelVerblijfsobjectDomein']),
            gebruiksdoel_omschrijving=(
                r['OmschrijvingGebruiksdoelVerblijfsobjectDomein']),
            oppervlakte=uva2.uva_nummer(r['OppervlakteVerblijfsobject']),
            document_mutatie=uva2.uva_datum(
                r['DocumentdatumMutatieVerblijfsobject']),
            document_nummer=(r['DocumentnummerMutatieVerblijfsobject']),
            bouwlaag_toegang=uva2.uva_nummer(r['Bouwlaagtoegang']),
            status_coordinaat_code=(r['StatusCoordinaatDomein']),
            status_coordinaat_omschrijving=(r['OmschrijvingCoordinaatDomein']),
            verhuurbare_eenheden=r['AantalVerhuurbareEenheden'] or None,
            bouwlagen=uva2.uva_nummer(r['AantalBouwlagen']),
            type_woonobject_code=(r['TypeWoonobjectDomein']),
            type_woonobject_omschrijving=(
                r['OmschrijvingTypeWoonobjectDomein']),
            woningvoorraad=uva2.uva_indicatie(r['IndicatieWoningvoorraad']),
            aantal_kamers=uva2.uva_nummer(r['AantalKamers']),
            vervallen=uva2.uva_indicatie(r['Indicatie-vervallen']),
            reden_afvoer_id=reden_afvoer_id,
            reden_opvoer_id=reden_opvoer_id,
            bron_id=bron_id,
            eigendomsverhouding_id=eigendomsverhouding_id,
            financieringswijze_id=financieringswijze_id,
            gebruik_id=gebruik_id,
            locatie_ingang_id=locatie_ingang_id,
            ligging_id=ligging_id,
            # ?=(r['VBOMNT/MNT/Code']),
            toegang_id=toegang_id,
            # ?=(r['VBOOVR/OVR/Code']),
            status_id=status_id,
            buurt_id=buurt_id,
            begin_geldigheid=uva2.uva_datum(
                r['TijdvakGeldigheid/begindatumTijdvakGeldigheid']),
            einde_geldigheid=uva2.uva_datum(
                r['TijdvakGeldigheid/einddatumTijdvakGeldigheid']),
            mutatie_gebruiker=r['Mutatie-gebruiker'],
        )


class ImportPndTask(batch.BasicTask):
    name = "Import PND"

    def __init__(self, bag_path, wkt_path):
        self.wkt_path = wkt_path
        self.bag_path = bag_path
        self.statussen = set()
        self.bouwblokken = set()
        self.panden = dict()
        self.landelijke_ids = dict()

    def before(self):
        self.statussen = set(models.Status.objects.values_list("pk", flat=True))
        self.bouwblokken = set(models.Bouwblok.objects.values_list("pk", flat=True))

    def after(self):
        self.statussen.clear()
        self.panden.clear()

    def process(self):
        self.landelijke_ids = uva2.read_landelijk_id_mapping(self.bag_path, "PND")
        self.panden = dict(uva2.process_uva2(self.bag_path, "PND", self.process_row))
        geo.process_wkt(self.wkt_path, "BAG_PAND_GEOMETRIE.dat", self.process_wkt_row)

        models.Pand.objects.bulk_create(self.panden.values(), batch_size=database.BATCH_SIZE)

    def process_row(self, r):
        if not uva2.geldig_tijdvak(r):
            return

        if not uva2.geldige_relaties(r, 'PNDSTS', 'PNDBBK'):
            return

        pk = r['sleutelverzendend']
        status_id = r['PNDSTS/STS/Code'] or None
        bbk_id = r['PNDBBK/BBK/sleutelVerzendend'] or None
        landelijk_id = self.landelijke_ids.get(r['Pandidentificatie'])

        if not landelijk_id:
            log.error('Pand {} references non-existing landelijk_id {}; skipping'.format(pk, landelijk_id))
            return

        if status_id and status_id not in self.statussen:
            log.warning('Pand {} references non-existing status {}; ignoring'.format(pk, status_id))
            status_id = None

        if bbk_id and bbk_id not in self.bouwblokken:
            log.warning('Pand {} references non-existing bouwblok {}; ignoring'.format(pk, bbk_id))
            bbk_id = None

        return pk, models.Pand(
            pk=pk,
            landelijk_id=landelijk_id,
            document_mutatie=uva2.uva_datum(r['DocumentdatumMutatiePand']),
            document_nummer=(r['DocumentnummerMutatiePand']),
            bouwjaar=uva2.uva_nummer(r['OorspronkelijkBouwjaarPand']),
            laagste_bouwlaag=uva2.uva_nummer(r['LaagsteBouwlaag']),
            hoogste_bouwlaag=uva2.uva_nummer(r['HoogsteBouwlaag']),
            pandnummer=(r['Pandnummer']),
            vervallen=uva2.uva_indicatie(r['Indicatie-vervallen']),
            status_id=status_id,
            begin_geldigheid=uva2.uva_datum(r['TijdvakGeldigheid/begindatumTijdvakGeldigheid']),
            einde_geldigheid=uva2.uva_datum(r['TijdvakGeldigheid/einddatumTijdvakGeldigheid']),
            mutatie_gebruiker=r['Mutatie-gebruiker'],
            bouwblok_id=bbk_id,
        )

    def process_wkt_row(self, wkt_id, geometrie):
        key = '0' + wkt_id
        if key not in self.panden:
            log.warning('Pand/WKT {} references non-existing pand {}; skipping'.format(wkt_id, key))
            return

        self.panden[key].geometrie = geometrie


class ImportPndVboTask(batch.BasicTask):
    name = "Import PNDVBO"

    def __init__(self, path):
        self.path = path
        self.panden = set()
        self.vbos = set()

    def before(self):

        self.panden = frozenset(
            models.Pand.objects.values_list("pk", flat=True))
        self.vbos = frozenset(
            models.Verblijfsobject.objects.values_list("pk", flat=True))

    def after(self):
        self.panden = None
        self.vbos = None

    def process(self):
        relaties = frozenset(
            uva2.process_uva2(self.path, "PNDVBO", self.process_row))
        models.VerblijfsobjectPandRelatie.objects.bulk_create(
            relaties, batch_size=database.BATCH_SIZE)

    def process_row(self, r):
        if not uva2.geldig_tijdvak(r):
            return None

        if not uva2.geldige_relaties(r, 'PNDVBO'):
            return None

        pand_id = r['sleutelverzendend']
        vbo_id = r['PNDVBO/VBO/sleutelVerzendend']

        if vbo_id not in self.vbos:
            log.warning('Pand/VBO {} references non-existing verblijfsobject {}; skipping'.format(pand_id, vbo_id))
            return None

        if pand_id not in self.panden:
            log.warning('Pand/VBO {} references non-existing pand {}; skipping'.format(pand_id, pand_id))
            return None

        return models.VerblijfsobjectPandRelatie(
            verblijfsobject_id=vbo_id,
            pand_id=pand_id,
        )


BAG_DOC_TYPES = [
    documents.Ligplaats,
    documents.Standplaats,
    documents.Verblijfsobject,
    documents.OpenbareRuimte,
    documents.Bouwblok,
    documents.Gebied,
    documents.ExactLocation,
]


class DeleteIndexTask(index.DeleteIndexTask):
    index = settings.ELASTIC_INDICES['BAG']
    doc_types = BAG_DOC_TYPES


class DeleteNummerAanduidingIndexTask(index.DeleteIndexTask):
    index = settings.ELASTIC_INDICES['NUMMERAANDUIDING']
    doc_types = [documents.Nummeraanduiding]


class DeleteNummerAanduidingBackupIndexTask(index.DeleteIndexTask):
    index = settings.ELASTIC_INDICES['NUMMERAANDUIDING'] + 'backup'
    doc_types = [documents.Nummeraanduiding]


class DeleteBackupIndexTask(index.DeleteIndexTask):
    index = settings.ELASTIC_INDICES['BAG'] + 'backup'
    doc_types = BAG_DOC_TYPES


class IndexLigplaatsTask(index.ImportIndexTask):
    name = "index ligplaatsen"
    queryset = models.Ligplaats.objects.\
        prefetch_related('adressen').\
        prefetch_related('adressen__openbare_ruimte')

    def convert(self, obj):
        return documents.from_ligplaats(obj)


class IndexStandplaatsTask(index.ImportIndexTask):
    name = "index standplaatsen"
    queryset = models.Standplaats.objects.\
        prefetch_related('adressen').\
        prefetch_related('adressen__openbare_ruimte')

    def convert(self, obj):
        return documents.from_standplaats(obj)


class IndexVerblijfsobjectTask(index.ImportIndexTask):
    name = "index verblijfsobjecten"
    queryset = models.Verblijfsobject.objects.\
        prefetch_related('adressen').\
        prefetch_related('adressen__openbare_ruimte')

    def convert(self, obj):
        return documents.from_verblijfsobject(obj)


class IndexOpenbareRuimteTask(index.ImportIndexTask):
    name = "index openbare ruimtes"
    queryset = models.OpenbareRuimte.objects.prefetch_related('adressen')

    def convert(self, obj):
        return documents.from_openbare_ruimte(obj)


#########################################################
# gebieden tasks
#########################################################


class IndexUnescoTask(index.ImportIndexTask):
    name = "index unesco"
    queryset = models.Unesco.objects.all()

    def convert(self, obj):
        return documents.from_unesco(obj)


class IndexBuurtTask(index.ImportIndexTask):
    name = "index buurten"
    queryset = models.Buurt.objects.all()

    def convert(self, obj):
        return documents.from_buurt(obj)


class IndexBuurtcombinatieTask(index.ImportIndexTask):
    name = "index buurtcombinaties"
    queryset = models.Buurtcombinatie.objects.all()

    def convert(self, obj):
        return documents.from_buurtcombinatie(obj)


class IndexGebiedsgerichtWerkenTask(index.ImportIndexTask):
    name = "index gebiedsgerichtwerken"
    queryset = models.Gebiedsgerichtwerken.objects.all()

    def convert(self, obj):
        return documents.from_gebiedsgerichtwerken(obj)


class IndexStadsdeelTask(index.ImportIndexTask):
    name = "index stadsdeel"
    queryset = models.Stadsdeel.objects.all()

    def convert(self, obj):
        return documents.from_stadsdeel(obj)


class IndexGrootstedelijkgebiedTask(index.ImportIndexTask):
    name = "Index grootstedelijk"
    queryset = models.Grootstedelijkgebied.objects.all()

    def convert(self, obj):
        return documents.from_grootstedelijk(obj)


class IndexGemeenteTask(index.ImportIndexTask):
    name = "index gemeebten"
    queryset = models.Gemeente.objects.all()

    def convert(self, obj):
        return documents.from_gemeente(obj)


class IndexWoonplaatsTask(index.ImportIndexTask):
    name = "index gemeebten"
    queryset = models.Woonplaats.objects.all()

    def convert(self, obj):
        return documents.from_woonplaats(obj)


##########################################################
##########################################################


class IndexNummerAanduidingTask(index.ImportIndexTask):
    name = "index nummer aanduidingen"
    queryset = models.Nummeraanduiding.objects.\
        prefetch_related('verblijfsobject').\
        prefetch_related('standplaats').\
        prefetch_related('ligplaats').\
        prefetch_related('openbare_ruimte')

    def convert(self, obj):
        return documents.from_nummeraanduiding_ruimte(obj)


class IndexBouwblokTask(index.ImportIndexTask):
    name = "index bouwblokken"
    queryset = models.Bouwblok.objects.all()

    def convert(self, obj):
        return documents.from_bouwblok(obj)


class IndexExactMatchesTask(index.ImportIndexTask):
    name = "index extact matches for postcode geocoding"
    queryset = models.Nummeraanduiding.objects.\
        prefetch_related('verblijfsobject').\
        prefetch_related('standplaats').\
        prefetch_related('ligplaats')

    def convert(self, obj):
        return documents.exact_from_nummeraanduiding(obj)


# these files don't have a UVA file
class ImportBuurtcombinatieTask(batch.BasicTask):
    """
    layer.fields:

    ['ID', 'NAAM', 'CODE', 'VOLLCODE', 'DOCNR',
     'DOCDATUM', 'INGSDATUM', 'EINDDATUM']
    """

    name = "Import GBD Buurtcombinatie"

    def __init__(self, shp_path):
        self.shp_path = shp_path
        self.stadsdelen = dict()

    def before(self):
        self.stadsdelen = dict(
            models.Stadsdeel.objects.values_list("code", "id"))

    def after(self):
        self.stadsdelen.clear()

    def process(self):
        bcs = geo.process_shp(
            self.shp_path, "GBD_Buurtcombinatie.shp", self.process_feature)

        models.Buurtcombinatie.objects.bulk_create(
            bcs, batch_size=database.BATCH_SIZE)

    def process_feature(self, feat):
        vollcode = feat.get('VOLLCODE')

        return models.Buurtcombinatie(
            id=str(int(feat.get('ID'))),
            naam=feat.get('NAAM').encode('utf-8'),
            code=feat.get('CODE').encode('utf-8'),
            vollcode=vollcode,
            brondocument_naam=feat.get('DOCNR').encode('utf-8'),
            brondocument_datum=feat.get('DOCDATUM'),
            ingang_cyclus=feat.get('INGSDATUM'),
            geometrie=geo.get_multipoly(feat.geom.wkt),
            stadsdeel_id=self.stadsdelen.get(vollcode[0]),
            begin_geldigheid=feat.get('INGSDATUM'),
            einde_geldigheid=feat.get('EINDDATUM'),
        )


class ImportGebiedsgerichtwerkenTask(batch.BasicTask):
    """
    layer.fields:

    ['NAAM', 'CODE', 'STADSDEEL', 'INGSDATUM',
     'EINDDATUM', 'DOCNR', 'DOCDATUM']
    """

    name = "Import GBD Gebiedsgerichtwerken"

    def __init__(self, shp_path):
        self.shp_path = shp_path
        self.stadsdelen = dict()

    def before(self):
        self.stadsdelen = dict(
            models.Stadsdeel.objects.values_list("code", "pk"))

    def after(self):
        self.stadsdelen.clear()

    def process(self):
        ggws = geo.process_shp(
            self.shp_path, "GBD_gebiedsgerichtwerken.shp",
            self.process_feature)

        models.Gebiedsgerichtwerken.objects.bulk_create(
            ggws, batch_size=database.BATCH_SIZE)

    def process_feature(self, feat):
        sdl = feat.get('STADSDEEL')
        if sdl not in self.stadsdelen:
            log.warning('Gebiedsgerichtwerken {} references non-existing stadsdeel {}; skipping'.format(sdl, sdl))
            return

        code = feat.get('CODE').encode('utf-8')
        return models.Gebiedsgerichtwerken(
            id=code,
            naam=feat.get('NAAM').encode('utf-8'),
            code=code,
            stadsdeel_id=self.stadsdelen[sdl],
            geometrie=geo.get_multipoly(feat.geom.wkt),
        )


class ImportGrootstedelijkgebiedTask(batch.BasicTask):
    """
    layer.fields:

    ['NAAM']
    """

    name = "Import GBD Grootstedelijkgebied"

    def __init__(self, shp_path):
        self.shp_path = shp_path

    def before(self):
        pass

    def after(self):
        pass

    def process(self):
        ggbs = geo.process_shp(
            self.shp_path,
            "GBD_grootstedelijke_projecten.shp", self.process_feature)

        models.Grootstedelijkgebied.objects.bulk_create(
            ggbs, batch_size=database.BATCH_SIZE)

    def process_feature(self, feat):
        naam = feat.get('NAAM').encode('utf-8')
        return models.Grootstedelijkgebied(
            id=slugify(naam),
            naam=naam,
            geometrie=geo.get_multipoly(feat.geom.wkt),
        )


class ImportUnescoTask(batch.BasicTask):
    """
    layer.fields:

    ['NAAM']
    """

    name = "Import GBD unesco"

    def __init__(self, shp_path):
        self.shp_path = shp_path

    def before(self):
        pass

    def after(self):
        pass

    def process(self):
        unesco = geo.process_shp(
            self.shp_path, "GBD_unesco.shp", self.process_feature)
        models.Unesco.objects.bulk_create(
            unesco, batch_size=database.BATCH_SIZE)

    def process_feature(self, feat):
        naam = feat.get('NAAM').encode('utf-8')
        return models.Unesco(
            id=slugify(naam),
            naam=naam,
            geometrie=geo.get_multipoly(feat.geom.wkt),
        )


class DenormalizeDataTask(batch.BasicTask):
    name = "Denormalize data"

    def before(self):
        pass

    def after(self):
        pass

    def process(self):
        with connection.cursor() as c:
            c.execute("""
UPDATE bag_verblijfsobject vbo
SET _openbare_ruimte_naam = t.naam,
  _huisnummer             = t.huisnummer,
  _huisletter             = t.huisletter,
  _huisnummer_toevoeging  = t.huisnummer_toevoeging
FROM (
       SELECT
         num.verblijfsobject_id    AS vbo_id,
         opr.naam                  AS naam,
         num.huisnummer            AS huisnummer,
         num.huisletter            AS huisletter,
         num.huisnummer_toevoeging AS huisnummer_toevoeging
       FROM bag_nummeraanduiding num
         LEFT JOIN bag_openbareruimte opr ON num.openbare_ruimte_id = opr.id
       WHERE num.hoofdadres
     ) t
WHERE vbo.id = t.vbo_id;
            """)

            c.execute("""
UPDATE bag_ligplaats lig
SET _openbare_ruimte_naam = t.naam,
  _huisnummer             = t.huisnummer,
  _huisletter             = t.huisletter,
  _huisnummer_toevoeging  = t.huisnummer_toevoeging
FROM (
       SELECT
         num.ligplaats_id          AS lig_id,
         opr.naam                  AS naam,
         num.huisnummer            AS huisnummer,
         num.huisletter            AS huisletter,
         num.huisnummer_toevoeging AS huisnummer_toevoeging
       FROM bag_nummeraanduiding num
         LEFT JOIN bag_openbareruimte opr ON num.openbare_ruimte_id = opr.id
       WHERE num.hoofdadres AND num.ligplaats_id IS NOT NULL
     ) t
WHERE lig.id = t.lig_id;
            """)

            c.execute("""
UPDATE bag_standplaats sta
SET _openbare_ruimte_naam = t.naam,
  _huisnummer             = t.huisnummer,
  _huisletter             = t.huisletter,
  _huisnummer_toevoeging  = t.huisnummer_toevoeging
FROM (
       SELECT
         num.standplaats_id        AS sta_id,
         opr.naam                  AS naam,
         num.huisnummer            AS huisnummer,
         num.huisletter            AS huisletter,
         num.huisnummer_toevoeging AS huisnummer_toevoeging
       FROM bag_nummeraanduiding num
         LEFT JOIN bag_openbareruimte opr ON num.openbare_ruimte_id = opr.id
       WHERE num.hoofdadres AND num.standplaats_id IS NOT NULL
     ) t
WHERE sta.id = t.sta_id;
            """)

            c.execute("""
UPDATE bag_nummeraanduiding num
SET _openbare_ruimte_naam = opr.naam
FROM bag_openbareruimte opr
WHERE opr.id = num.openbare_ruimte_id
            """)


class ImportBagJob(object):
    name = "Import BAG"

    def __init__(self):
        diva = settings.DIVA_DIR
        if not os.path.exists(diva):
            raise ValueError("DIVA_DIR not found: {}".format(diva))

        self.bag = os.path.join(diva, 'bag')
        self.bag_wkt = os.path.join(diva, 'bag_wkt')
        self.gebieden = os.path.join(diva, 'gebieden')
        self.gebieden_shp = os.path.join(diva, 'gebieden_shp')

    def tasks(self):
        return [
            ImportAvrTask(self.bag),
            ImportOvrTask(self.bag),
            ImportBrnTask(self.bag),
            ImportEgmTask(self.bag),
            ImportFngTask(self.bag),
            ImportGbkTask(self.bag),
            ImportLggTask(self.bag),
            ImportLocTask(self.bag),
            ImportTggTask(self.bag),
            ImportStsTask(self.bag),

            ImportGmeTask(self.gebieden),
            ImportWplTask(self.bag),
            ImportSdlTask(self.gebieden, self.gebieden_shp),
            ImportBuurtcombinatieTask(self.gebieden_shp),
            ImportBrtTask(self.gebieden, self.gebieden_shp),
            ImportBbkTask(self.gebieden, self.gebieden_shp),
            ImportOprTask(self.bag, self.bag_wkt),

            ImportLigTask(self.bag, self.bag_wkt),
            ImportStaTask(self.bag, self.bag_wkt),
            ImportVboTask(self.bag),

            ImportNumTask(self.bag),

            ImportPndTask(self.bag, self.bag_wkt),
            ImportPndVboTask(self.bag),

            ImportGebiedsgerichtwerkenTask(self.gebieden_shp),
            ImportGrootstedelijkgebiedTask(self.gebieden_shp),
            ImportUnescoTask(self.gebieden_shp),

            DenormalizeDataTask(),
        ]


class IndexBagJob(object):
    name = "Create new search-index for all BAG data from database"

    def tasks(self):
        return [
            DeleteIndexTask(),
            DeleteNummerAanduidingIndexTask(),
            IndexOpenbareRuimteTask(),
            IndexNummerAanduidingTask(),
            IndexExactMatchesTask(),
        ]


class BuildIndexBagJob(object):
    name = "Create new search-index for all BAG data from database"

    def tasks(self):
        return [
            IndexOpenbareRuimteTask(),
            IndexNummerAanduidingTask(),
            IndexExactMatchesTask(),
        ]


class DeleteIndexBagJob(object):

    name = "Delete BAG related indexes"

    def tasks(self):
        return [
            DeleteIndexTask(),
            DeleteNummerAanduidingIndexTask(),
        ]


class IndexNummerAanduidingJob(object):
    name = "Create new search index for Nummeraanduiding"

    def tasks(self):
        return [
            DeleteNummerAanduidingIndexTask(),
            IndexNummerAanduidingTask()
        ]


class IndexGebiedenJob(object):
    """Important! This only adds to the bag index, but does not create it"""

    name = "Create add gebieden to BAG index"

    def tasks(self):
        return [
            IndexBouwblokTask(),

            # NOTE !! DEVELOPMENT -> Only for document changes
            # DeleteIndexTaskTask(),

            IndexUnescoTask(),
            IndexBuurtTask(),
            IndexBuurtcombinatieTask(),
            IndexStadsdeelTask(),
            IndexGrootstedelijkgebiedTask(),
            IndexGebiedsgerichtWerkenTask(),
            IndexGemeenteTask()
        ]


class BackupBagIndexTask(index.CopyIndexTask):
    """
    Backup elastic BAG Index
    """
    index = settings.ELASTIC_INDICES['BAG']
    target = settings.ELASTIC_INDICES['BAG'] + 'backup'
    name = 'Backup BAG index in elastic'


class BackupNummerAanduidingTask(index.CopyIndexTask):
    """
    Backup elastic BAG Index
    """
    index = settings.ELASTIC_INDICES['NUMMERAANDUIDING']
    target = settings.ELASTIC_INDICES['NUMMERAANDUIDING'] + 'backup'
    name = 'Backup Aanduiding index in elastic'


class RestoreBagIndexTask(index.CopyIndexTask):
    """
    Restore elastic BAG Index
    """
    name = 'Restore backup bag index in elastic'

    index = settings.ELASTIC_INDICES['BAG'] + 'backup'
    target = settings.ELASTIC_INDICES['BAG']


class RestoreNummerAanduidingIndexTask(index.CopyIndexTask):
    """
    Restore elastic BAG Index
    """
    name = 'Restore backup nummeraanduiding index in elastic'

    index = settings.ELASTIC_INDICES['NUMMERAANDUIDING'] + 'backup'
    target = settings.ELASTIC_INDICES['NUMMERAANDUIDING']


class BackupBagJob(object):
    """
    Backup elastic BAG documents
    """
    name = "Backup elastic-index BAG"

    def tasks(self):
        return [
            DeleteBackupIndexTask,
            BackupBagIndexTask(),
        ]


class RestoreBagJob(object):

    name = "Restore Backup elastic-index BAG"

    def tasks(self):
        return [
            DeleteIndexTask(),
            RestoreBagIndexTask()
        ]


class BackupNummerAanduidingJob(object):
    """
    Nummeraanduiding elastic index Backup
    """

    name = "Backup elastic-index NUMMERAANDUIDING"

    def tasks(self):
        return [
            DeleteNummerAanduidingBackupIndexTask(),
            BackupNummerAanduidingTask(),
        ]


class RestoreNummerAanduidingJob(object):
    """
    Nummeraanduiding elastic index Restore
    """

    def tasks(self):
        return [
            DeleteNummerAanduidingIndexTask(),
            RestoreNummerAanduidingIndexTask()
        ]