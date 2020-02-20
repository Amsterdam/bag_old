"""
Module Contains logic to get the latest most up to date
files to import in the BAG / BRK / WKPB database

Goal is to assure we load Datapunt with accurate and current data

checks:

   check AGE of filenames
     - we do not work with old data
   check filename changes
     - we do not work of old files because new files are renamed

We download specific zip files:

Unzip target data in to empty new location and start
import proces.


"""
import argparse
import datetime
import logging
import os
import re
import time
import zipfile

from functools import lru_cache
from pathlib import Path
from dateutil import parser

from swiftclient.client import Connection

log = logging.getLogger(__name__)
logging.basicConfig(level=os.environ.get("LOGLEVEL", "INFO"))

logging.getLogger("requests").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("swiftclient").setLevel(logging.WARNING)

connections = {
    'bag_brk': {
        'auth_version': '2.0',
        'authurl': 'https://identity.stack.cloudvps.com/v2.0',
        'user': 'bag_brk',
        'key': os.getenv('BAG_OBJECTSTORE_PASSWORD', 'insecure'),
        'tenant_name': 'BGE000081_BAG',
        'os_options': {
            'tenant_id': '4f2f4b6342444c84b3580584587cfd18',
            'region_name': 'NL',
        }
    },
    'GOB_user': {
        'auth_version': '2.0',
        'authurl': 'https://identity.stack.cloudvps.com/v2.0',
        'user': 'GOB_user',
        'key': os.getenv('GOB_OBJECTSTORE_PASSWORD', 'insecure'),
        'tenant_name': 'BGE000081_GOB',
        'os_options': {
            'tenant_id': '2ede4a78773e453db73f52500ef748e5',
            'region_name': 'NL',
        }
    }
}

# zet in data directory laat diva voor test data.
# in settings een verschil maken
DIVA_DIR = os.getenv('DIVA_DIR', '/app/data')


@lru_cache(maxsize=None)
def get_conn(connect):
    assert (connect == 'bag_brk' and os.getenv('BAG_OBJECTSTORE_PASSWORD')) or (
            connect == 'GOB_user' and os.getenv('GOB_OBJECTSTORE_PASSWORD'))
    return Connection(**connections[connect])


def get_full_container_list(connect, container_name, **kwargs):
    """
    Return a listing of filenames in container `container_name`
    :param container_name:
    :param kwargs:
    :return:
    """
    limit = 10000
    kwargs['limit'] = limit
    seed = []
    _, page = get_conn(connect).get_container(container_name, **kwargs)
    seed.extend(page)

    while len(page) == limit:
        # keep getting pages..
        kwargs['marker'] = seed[-1]['name']
        _, page = get_conn(connect).get_container(container_name, **kwargs)
        seed.extend(page)

    return seed


def delete_from_objectstore(connect, container, object_name):
    """
    remove file `object_name` fronm `container`
    :param container: Container name
    :param object_name:
    :return:
    """
    return get_conn(connect).delete_object(container, object_name)


def download_file(connect, container_name, file_path, target_path=None, target_root=DIVA_DIR, file_last_modified=None):
    path = file_path.split('/')

    file_name = path[-1]
    log.info(f"Create file {file_name} in {target_root}")
    file_name = path[-1]

    if target_path:
        newfilename = '{}/{}'.format(target_root, target_path)
    else:
        newfilename = '{}/{}'.format(target_root, file_name)

    (directory, _) = os.path.split(newfilename)
    if not os.path.exists(directory):
        os.makedirs(directory)
    if file_exists(newfilename):
        log.debug('Skipped file exists: %s', newfilename)
        return

    with open(newfilename, 'wb') as newfile:
        data = get_conn(connect).get_object(container_name, file_path)[1]
        newfile.write(data)
    if file_last_modified:
        epoch_modified = file_last_modified.timestamp()
        os.utime(newfilename, (epoch_modified, epoch_modified))


def download_file_data(connect, container_name, file_path):
    return get_conn(connect).get_object(container_name, file_path)[1]


def download_wkpb_file_data(file_path):
    return download_file_data('GOB_user', 'productie', file_path)


def download_diva_file(container_name, file_path, target_path=None):
    """
    Download a diva file
    """
    download_file('bag_brk', container_name, file_path, target_path=target_path)


def file_exists(target):
    target = Path(target)
    return target.is_file()


def download_zips(container_name, zips_mapper):
    """
    Download latest zips
    """

    for _, zipfiles in zips_mapper.items():
        zipfiles.sort(reverse=True)
        zip_name = zipfiles[0][1]['name']
        download_diva_file(container_name, zip_name)


def delete_old_zips(container_name, zips_mapper):
    """
    Cleanup old zips
    """
    for _zipkey, zipfiles in zips_mapper.items():
        log.debug('KEEP : %s', zipfiles[0][1]['name'])
        if len(zipfiles) > 1:
            # delete old files
            for _, zipobject in zipfiles[1:]:
                zippath = zipobject['name']
                log.debug('PURGE: %s', zippath)
                delete_from_objectstore('bag_brk', container_name, zippath)


"""
Originele mappen gebruikt door import
bag, bag_wkt, beperkingen, brk, brk_shp
gebieden, gebieden_shp, kbk10, kbk50
"""

path_mapping = {
    'Gebieden/UVA2/GEB_Actueel/ASCII': 'gebieden',
    'Gebieden/Objecten/Esri_Shape': 'gebieden_shp',
    'BRK/BRK_Totaal/ASCII': 'brk',
    'BRK/BRK_Totaal/Esri_Shape': 'brk_shp',
    'BAG/BAG_Geometrie/WKT': 'bag_wkt',
    'BAG/UVA2/BAG_Actueel/ASCII': 'bag',
    'BAG/BAG_LandelijkeSleutel/ASCII': 'bag',
    'WKPB/beperkingen/ASCII': 'beperkingen',
    'BAG/BAG_Authentiek/ASCII': 'bag',
    'bestaatnietinzip': 'bag_openbareruimte_beschrijving',
}


def create_target_directories():
    """
    the directories where the import proces expects the import source files
    should be created before unzipping files.
    """

    # Make sure target directories exist
    for target in path_mapping.values():
        directory = os.path.join(DIVA_DIR, target)
        if not os.path.exists(directory):
            os.makedirs(directory)


def unzip_files(zipsource, mtime):
    """
    Unzip single files to the right target directory
    """

    # Extract files to the expected location
    directory = os.path.join(DIVA_DIR)

    for fullname in zipsource.namelist():
        zipsource.extract(fullname, directory)
        file_name = fullname.split('/')[-1]
        for path, target in path_mapping.items():
            if path in fullname:
                source = f"{directory}/{fullname}"
                target = f'{directory}/{target}/{file_name}'
                # relocate fiel to expected location
                print(source)
                print(target)
                os.rename(source, target)
                os.utime(target, (mtime, mtime))


# list of exceptions which are not in the 'official zips'
exception_list = [
    ('bag_geometrie/BAG_OPENBARERUIMTE_GEOMETRIE.dat',
     'bag_wkt/BAG_OPENBARERUIMTE_GEOMETRIE.dat'),
    ('gebieden_shp/GBD_gebiedsgerichtwerken.shp', ''),
    ('gebieden_shp/GBD_gebiedsgerichtwerken_praktijk.shp', ''),
    ('gebieden_shp/GBD_grootstedelijke_projecten.shp', ''),
    ('gebieden_shp/GBD_unesco.shp', ''),
    ('bag_openbareruimte_beschrijving/OPR_beschrijving.csv', ''),
]


def get_specific_files(container_name, exception_list1=exception_list):
    """
    There are some files not contained in the zips.
    Lets pick them up separately.
    """
    for specific_file, target in exception_list1:

        if not target:
            target = specific_file

        if specific_file.endswith('shp'):
            for ext in ['.dbf', '.prj', '.shx']:
                also_get = specific_file.replace('.shp', ext)
                new_target = target.replace('.shp', ext)
                download_diva_file(
                    container_name, also_get, target_path=new_target)

        download_diva_file(container_name, specific_file, target_path=target)


def unzip_data(zips_mapper):
    """
    unzip the zips
    """

    for _zipkey, zipfiles in zips_mapper.items():
        latestzip = zipfiles[0][1]

        filepath = latestzip['name'].split('/')
        file_name = filepath[-1]
        zip_path = '{}/{}'.format(DIVA_DIR, file_name)

        log.info(f"Unzip {zip_path}")

        zip_date = file_name.split('_')[0]
        log.debug('ZIP_DATE: %s', zip_date)
        zip_date = parser.parse(zip_date)
        zip_seconds = time.mktime(zip_date.timetuple())

        zipsource = zipfile.ZipFile(zip_path, 'r')
        unzip_files(zipsource, zip_seconds)


zip_age_limits = {
    'GEBASCII.zip': 5,
    'GEBSHAPE.zip': 365,
    'BAGACTUEEL.zip': 5,
    'BAGGEOMETRIE.zip': 5,
    'BAGLSLEUTEL.zip': 5,
    'BRKASCII.zip': 10,
    'BRKSHAPE.zip': 10,
    'WKPB.zip': 5,
}


def check_age(zip_created, file_key, file_object):
    """
    Do basic sanity check on zip delivery..
    """

    now = datetime.datetime.today()
    delta = now - zip_created
    log.debug('AGE: %2d days', delta.days)
    source_name = file_object['name']

    log.debug('%s_%s', zip_created.strftime('%Y%m%d'), file_key)

    for key, _agelimit in zip_age_limits.items():
        if file_key.endswith(key):
            if zip_age_limits[key] < delta.days:
                raise ValueError(
                    f"""

        Zip delivery is late!

        {key} age: {delta.days}  max_age: {zip_age_limits[key]}

        from {source_name}

                    """)


def validate_age(zips_mapper):
    """
    Check if the files we want to import are not to old!
    """
    log.debug('validating age..')

    for zipkey, zipfiles in zips_mapper.items():
        # this is the file we want to import
        age, importsource = zipfiles[0]

        check_age(age, zipkey, importsource)

        log.debug('OK: %s %s', age, zipkey)


def fetch_diva_zips(container_name, zipfolder):
    """
    fetch files from folder in an objectstore container
    :param container_name:
    :param zipfolder:
    :return:
    """
    log.info(f"import files from {zipfolder}")

    zips_mapper = {}

    for file_object in get_full_container_list(
            'bag_brk', container_name, prefix=zipfolder):

        if file_object['content_type'] == 'application/directory':
            continue

        path = file_object['name'].split('/')
        file_name = path[-1]

        if not file_name.endswith('.zip'):
            continue

        # not of interest for bag / brk
        exclude = ['BRT', 'NAP', 'MBT']

        if any(REGTYPE in file_name for REGTYPE in exclude):
            continue

        dt = parser.parse(file_object['last_modified'])

        file_key = "".join(file_name.split('_')[1:])

        zips_mapper.setdefault(file_key, []).append((dt, file_object))

    download_zips(container_name, zips_mapper)
    delete_old_zips(container_name, zips_mapper)

    validate_age(zips_mapper)

    unzip_data(zips_mapper)


def fetch_gob_files(container_name: str):
    """
    data/bag/AOT_geconstateerd-inonderzoek_20200210.csv    X
    data/bag/PND_naam_20200210.csv                         X
    data/bag/AVR_20200210_N_20200210_20200210.UVA2   bag/UVA2_Actueel/
    data/bag/OVR_20200210_N_20200210_20200210.UVA2   bag/UVA2_Actueel/
    data/bag/FNG_20200210_N_20200210_20200210.UVA2   bag/UVA2_Actueel/
    data/bag/LGG_20200210_N_20200210_20200210.UVA2   bag/UVA2_Actueel/
    data/bag/GBK_20200210_N_20200210_20200210.UVA2   bag/UVA2_Actueel/
    data/bag/LOC_20200210_N_20200210_20200210.UVA2   bag/UVA2_Actueel/
    data/bag/TGG_20200210_N_20200210_20200210.UVA2   bag/UVA2_Actueel/
    data/bag/VBO_gebruiksdoelen_20200210.csv         X

    data/gebieden/GME_20190411_N_20190411_20190411.UVA2  gebieden/UVA2_Actueel/
    data/gebieden/SDL_20190411_N_20190411_20190411.UVA2  gebieden/UVA2_Actueel/
    data/gebieden_shp/GBD_Stadsdeel.shp etc              gebieden/SHP/
    data/gebieden/BRT_20190411_N_20190411_20190411.UVA2  gebieden/UVA2_Actueel/
    data/gebieden_shp/GBD_Buurt.shp                      gebieden/SHP/
    data/gebieden/BBK_20191024_N_20191024_20191024.UVA2  gebieden/UVA2_Actueel/
    data/gebieden_shp/GBD_Bouwblok.shp                   gebieden/SHP/

    data/bag/WPL_20200210_N_20200210_20200210.UVA2       bag/UVA2_Actueel/

    data/bag_openbareruimte_beschrijving/OPR_beschrijving.csv bag/\
    CSV_Actueel/BAG_openbare_ruimte_beschrijving_Actueel.csv
    data/bag/OPR_20200210_N_20200210_20200210.UVA2 bag/UVA2_Actueel/

    data/bag_wkt/BAG_OPENBARERUIMTE_GEOMETRIE.dat  bag/BAG_Geometrie/

    data/bag/NUM_20200210.dat                      bag/BAG_LandelijkeSleutel/
    data/bag/NUM_20200210_N_20200210_20200210.UVA2 bag/UVA2_Actueel/

    data/bag/LIG_20200210.dat                       bag/BAG_LandelijkeSleutel/
    data/bag/LIG_20200210_N_20200210_20200210.UVA2  bag/UVA2_Actueel/
    data/bag_wkt/BAG_LIGPLAATS_GEOMETRIE.dat        bag/BAG_Geometrie/

    data/bag/STA_20200210.dat                       bag/BAG_LandelijkeSleutel/
    data/bag/STA_20200210_N_20200210_20200210.UVA2  bag/UVA2_Actueel/
    data/bag_wkt/BAG_STANDPLAATS_GEOMETRIE.dat      bag/BAG_Geometrie/


    data/bag/VBO_20200210.dat                       bag/BAG_LandelijkeSleutel/
    data/bag/VBO_20200210_N_20200210_20200210.UVA2  bag/UVA2_Actueel/

    data/bag/PND_20200210.dat                       bag/BAG_LandelijkeSleutel/
    data/bag/PND_20200210_N_20200210_20200210.UVA2  bag/UVA2_Actueel/
    data/bag_wkt/BAG_PAND_GEOMETRIE.dat             bag/BAG_Geometrie/

    data/bag/PNDVBO_20200210_N_20200210_20200210.UVA2 bag/UVA2_Actueel/
    """
    all_gob_file_prefixes = {
        'bag/UVA2_Actueel/AVR': {'dest': 'bag', 'age_limit': 365},
        'bag/UVA2_Actueel/BRN': {'dest': 'bag', 'age_limit': 365},
        'bag/UVA2_Actueel/EGM': {'dest': 'bag', 'age_limit': 365},
        'bag/UVA2_Actueel/FNG': {'dest': 'bag', 'age_limit': 365},
        'bag/UVA2_Actueel/LGG': {'dest': 'bag', 'age_limit': 365},
        'bag/UVA2_Actueel/GBK': {'dest': 'bag', 'age_limit': 365},
        'bag/UVA2_Actueel/LOC': {'dest': 'bag', 'age_limit': 365},
        'bag/UVA2_Actueel/OVR': {'dest': 'bag', 'age_limit': 365},
        'bag/UVA2_Actueel/STS': {'dest': 'bag', 'age_limit': 365},
        'bag/UVA2_Actueel/TGG': {'dest': 'bag', 'age_limit': 365},

        'gebieden/UVA2_Actueel/GME': {'dest': 'gebieden', 'age_limit': 365},
        'gebieden/UVA2_Actueel/SDL': {'dest': 'gebieden', 'age_limit': 365},

        'gebieden/UVA2_Actueel/BRT': {'dest': 'gebieden', 'age_limit': 365},
        'gebieden/SHP/GBD_buurt.shp': {'dest': 'gebieden_shp', 'age_limit': 365},
        'gebieden/SHP/GBD_buurt.dbf': {'dest': 'gebieden_shp', 'age_limit': 365},
        'gebieden/SHP/GBD_buurt.prj': {'dest': 'gebieden_shp', 'age_limit': 365},
        'gebieden/SHP/GBD_buurt.shx': {'dest': 'gebieden_shp', 'age_limit': 365},

        'gebieden/UVA2_Actueel/BBK': {'dest': 'gebieden', 'age_limit': 365},
        'gebieden/SHP/GBD_bouwblok.shp': {'dest': 'gebieden_shp', 'age_limit': 365},
        'gebieden/SHP/GBD_bouwblok.dbf': {'dest': 'gebieden_shp', 'age_limit': 365},
        'gebieden/SHP/GBD_bouwblok.prj': {'dest': 'gebieden_shp', 'age_limit': 365},
        'gebieden/SHP/GBD_bouwblok.shx': {'dest': 'gebieden_shp', 'age_limit': 365},

        'bag/UVA2_Actueel/WPL': {'dest': 'bag', 'age_limit': 365},

        'bag/CSV_Actueel/BAG_openbare_ruimte_beschrijving_Actueel.csv': {
            'dest': 'bag_openbareruimte_beschrijving/OPR_beschrijving.csv', 'age_limit': 365},
        'bag/BAG_LandelijkeSleutel/OPR': {'dest': 'bag', 'age_limit': 365},
        'bag/UVA2_Actueel/OPR': {'dest': 'bag', 'age_limit': 365},
        'bag/BAG_Geometrie/BAG_OPENBARERUIMTE_GEOMETRIE.dat': {'dest': 'bag_wkt', 'age_limit': 365},

        'bag/BAG_LandelijkeSleutel/NUM': {'dest': 'bag', 'age_limit': 5},
        'bag/UVA2_Actueel/NUM': {'dest': 'bag', 'age_limit': 5},

        'bag/BAG_LandelijkeSleutel/LIG': {'dest': 'bag', 'age_limit': 5},
        'bag/UVA2_Actueel/LIG': {'dest': 'bag', 'age_limit': 5},
        'bag/BAG_Geometrie/BAG_LIGPLAATS_GEOMETRIE.dat': {'dest': 'bag_wkt', 'age_limit': 5},

        'bag/BAG_LandelijkeSleutel/STA': {'dest': 'bag', 'age_limit': 5},
        'bag/UVA2_Actueel/STA': {'dest': 'bag', 'age_limit': 5},
        'bag/BAG_Geometrie/BAG_STANDPLAATS_GEOMETRIE.dat': {'dest': 'bag_wkt', 'age_limit': 5},

        'bag/BAG_LandelijkeSleutel/VBO': {'dest': 'bag', 'age_limit': 5},
        'bag/UVA2_Actueel/VBO': {'dest': 'bag', 'age_limit': 5},
        'bag/BAG_LandelijkeSleutel/PND': {'dest': 'bag', 'age_limit': 5},
        'bag/UVA2_Actueel/PND': {'dest': 'bag', 'age_limit': 5},
        'bag/BAG_Geometrie/BAG_PAND_GEOMETRIE.dat': {'dest': 'bag_wkt', 'age_limit': 5},
        'bag/UVA2_Actueel/PNDVBO': {'dest': 'bag', 'age_limit': 5},

        'gebieden/SHP/GBD_stadsdeel.shp': {'dest': 'gebieden_shp', 'age_limit': 365},
        'gebieden/SHP/GBD_stadsdeel.dbf': {'dest': 'gebieden_shp', 'age_limit': 365},
        'gebieden/SHP/GBD_stadsdeel.prj': {'dest': 'gebieden_shp', 'age_limit': 365},
        'gebieden/SHP/GBD_stadsdeel.shx': {'dest': 'gebieden_shp', 'age_limit': 365},

        'gebieden/SHP/GBD_wijk.shp': {'dest': 'gebieden_shp', 'age_limit': 365},
        'gebieden/SHP/GBD_wijk.dbf': {'dest': 'gebieden_shp', 'age_limit': 365},
        'gebieden/SHP/GBD_wijk.prj': {'dest': 'gebieden_shp', 'age_limit': 365},
        'gebieden/SHP/GBD_wijk.shx': {'dest': 'gebieden_shp', 'age_limit': 365},

        'gebieden/SHP/GBD_ggw_gebied.shp': {'dest': 'gebieden_shp', 'age_limit': 365},
        'gebieden/SHP/GBD_ggw_gebied.dbf': {'dest': 'gebieden_shp', 'age_limit': 365},
        'gebieden/SHP/GBD_ggw_gebied.prj': {'dest': 'gebieden_shp', 'age_limit': 365},
        'gebieden/SHP/GBD_ggw_gebied.shx': {'dest': 'gebieden_shp', 'age_limit': 365},

        'gebieden/SHP/GBD_ggw_praktijkgebied.shp': {'dest': 'gebieden_shp', 'age_limit': 365},
        'gebieden/SHP/GBD_ggw_praktijkgebied.dbf': {'dest': 'gebieden_shp', 'age_limit': 365},
        'gebieden/SHP/GBD_ggw_praktijkgebied.prj': {'dest': 'gebieden_shp', 'age_limit': 365},
        'gebieden/SHP/GBD_ggw_praktijkgebied.shx': {'dest': 'gebieden_shp', 'age_limit': 365},

        'gebieden/SHP/GBD_grootstedelijke_projecten.shp': {'dest': 'gebieden_shp', 'age_limit': 365},
        'gebieden/SHP/GBD_grootstedelijke_projecten.dbf': {'dest': 'gebieden_shp', 'age_limit': 365},
        'gebieden/SHP/GBD_grootstedelijke_projecten.prj': {'dest': 'gebieden_shp', 'age_limit': 365},
        'gebieden/SHP/GBD_grootstedelijke_projecten.shx': {'dest': 'gebieden_shp', 'age_limit': 365},

        'gebieden/SHP/GBD_unesco.shp': {'dest': 'gebieden_shp', 'age_limit': 365},
        'gebieden/SHP/GBD_unesco.dbf': {'dest': 'gebieden_shp', 'age_limit': 365},
        'gebieden/SHP/GBD_unesco.prj': {'dest': 'gebieden_shp', 'age_limit': 365},
        'gebieden/SHP/GBD_unesco.shx': {'dest': 'gebieden_shp', 'age_limit': 365},

        'bag/UVA2_Actueel/NUMLIGHFD': {'dest': 'bag', 'age_limit': 5},
        'bag/UVA2_Actueel/NUMSTAHFD': {'dest': 'bag', 'age_limit': 5},
        'bag/UVA2_Actueel/NUMVBOHFD': {'dest': 'bag', 'age_limit': 5},
        'bag/UVA2_Actueel/NUMVBONVN': {'dest': 'bag', 'age_limit': 5},
    }

    connection = 'GOB_user'
    log.info(f"import files from {container_name}")

    underscore_date = re.compile(r'_\d{8}')
    date_match = re.compile(r'_(?P<y>\d{4})(?P<m>\d{2})(?P<d>\d{2})_')

    to_download = {}
    now = datetime.date.today()

    for prefix in ('bag', 'gebieden'):
        for file_object in get_full_container_list(connection, container_name, prefix=prefix):
            if file_object['content_type'] == 'application/directory':
                continue
            path = file_object['name']
            m = underscore_date.search(path)
            date1 = None
            if m:
                path_prefix = path[0: m.span()[0]]
                d1 = date_match.search(path)
                if d1:
                    date1 = datetime.date(int(d1.group('y')), int(d1.group('m')), int(d1.group('d')))
            else:
                path_prefix = path

            if not date1:
                dt1 = parser.parse(file_object['last_modified'])
                date1 = datetime.date(dt1.year, dt1.month, dt1.day)

            if path_prefix in all_gob_file_prefixes:
                if path_prefix not in to_download or (
                        to_download[path_prefix]['date'] and date1 > to_download[path_prefix]['date']):
                    to_download[path_prefix] = {'path': path, 'date': date1}

    if len(to_download.keys()) < len(all_gob_file_prefixes.keys()):
        raise ValueError("Missing files: " + str(set(all_gob_file_prefixes.keys()) - set(to_download.keys())))

    for key, entry in to_download.items():
        if entry['date'] and abs((now - entry['date']).days) > all_gob_file_prefixes[key]['age_limit']:
            raise ValueError(f"File {entry['path']} is too old")
        filename = os.path.split(entry['path'])[1]
        dest = all_gob_file_prefixes[key]['dest']
        if len(dest.split('/')) > 1:
            target = dest
        else:
            target = os.path.join(all_gob_file_prefixes[key]['dest'], filename)
        download_file(connection, container_name, entry['path'], target_path=target)


def fetch_diva_files():
    """
    Er zijn nog geen zip files, selecteer de individuele files.
    totdat de zips gerealiseerd zijn alleen de .csvs en .uva2s
    :return:
    """
    logging.basicConfig(level=logging.DEBUG)
    # creat folders where files are expected.
    create_target_directories()
    # download the exceptions not in zip files
    # these are special cases manual made by some people
    get_specific_files('Diva')
    # download and unpack the zip files
    # These come from a more official product
    fetch_diva_zips('Diva', 'Zip_bestanden')


if __name__ == "__main__":
    argparser = argparse.ArgumentParser()
    argparser.add_argument('-g', '--gob', action='store_true', help='Do GOB import')
    args = argparser.parse_args()
    # Download files from objectstore
    log.info("Start downloading files from objectstore")
    if args.gob:
        fetch_gob_files('acceptatie')
    else:
        fetch_diva_files()
