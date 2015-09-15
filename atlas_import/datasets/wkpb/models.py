from django.db import models
from datasets.generic import mixins
from datasets.lki.models import KadastraalObject


# Wkpb

class Beperkingcode(mixins.ImportStatusMixin, mixins.CodeOmschrijvingMixin, models.Model):
    """
    Kadastrale code voor de type beperking.
    """

    class Meta:
        verbose_name = "Beperkingcode"
        verbose_name_plural = "Beperkingcodes"


class Broncode(mixins.ImportStatusMixin, mixins.CodeOmschrijvingMixin, models.Model):
    """
    Het orgaan dat de beperking heeft opgelegd.
    """

    class Meta:
        verbose_name = "Broncode"
        verbose_name_plural = "Broncodes"


class Brondocument(mixins.ImportStatusMixin, models.Model):
    """
    Het document dat aan de beperking ten grondslag ligt.
    """

    id = models.IntegerField(null=False, primary_key=True)
    documentnummer = models.IntegerField(null=False)
    bron = models.ForeignKey(Broncode, null=True)
    documentnaam = models.CharField(max_length=21, null=False)
    persoonsgegeven_afschermen = models.BooleanField(null=False)
    soort_besluit = models.CharField(max_length=60, null=True)

    class Meta:
        verbose_name = "Brondocument"
        verbose_name_plural = "Brondocumenten"

    def __str__(self):
        return "{}".format(self.documentnummer)


class Beperking(mixins.ImportStatusMixin, models.Model):
    """
    Beperking van de eigendom, zoals door een publiekrechtelijke beperking als beschermd monument of een
    aanschrijving op
    grond van de Woningwet.

    http://www.amsterdam.nl/stelselpedia/wkpb-index/catalogus/beperking/
    """

    id = models.IntegerField(null=False, primary_key=True)
    inschrijfnummer = models.IntegerField(null=False)
    beperkingtype = models.ForeignKey(Beperkingcode, null=False)
    datum_in_werking = models.DateField(null=False)
    datum_einde = models.DateField(null=True)

    class Meta:
        verbose_name = "Beperking"
        verbose_name_plural = "Beperkingen"

    def __str__(self):
        return "Beperking({})".format(self.id)


class BeperkingKadastraalObject(mixins.ImportStatusMixin, models.Model):
    """
    n:n-relaties: Beperking <> KadastraalObject
    """

    id = models.CharField(max_length=33, null=False, primary_key=True)
    beperking = models.ForeignKey(Beperking, null=False)
    kadastraal_object = models.ForeignKey(KadastraalObject, null=False)

    def __str__(self):
        return "{}-{}".format(self.beperking_id, self.kadastraal_object_id)
