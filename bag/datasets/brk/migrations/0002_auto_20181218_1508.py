# Generated by Django 2.0.7 on 2018-12-18 15:08

import django.contrib.gis.db.models.fields
from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('brk', '0001_initial'),
    ]

    operations = [
        migrations.AddField(
            model_name='kadastralegemeente',
            name='geometrie_lines',
            field=django.contrib.gis.db.models.fields.MultiLineStringField(null=True, srid=28992),
        ),
        migrations.AddField(
            model_name='kadastralesectie',
            name='geometrie_lines',
            field=django.contrib.gis.db.models.fields.MultiLineStringField(null=True, srid=28992),
        ),
    ]