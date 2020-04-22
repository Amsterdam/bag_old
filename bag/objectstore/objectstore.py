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

environment = os.getenv('GOB_OBJECTSTORE_ENV', 'productie')

connection = {
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


# zet in data directory laat diva voor test data.
# in settings een verschil maken
DIVA_DIR = os.getenv('DIVA_DIR', '/app/data')


@lru_cache(maxsize=None)
def get_conn():
    assert os.getenv('GOB_OBJECTSTORE_PASSWORD')
    return Connection(**connection)


def get_full_container_list(container_name, **kwargs):
    """
    Return a listing of filenames in container `container_name`
    :param container_name:
    :param kwargs:
    :return:
    """
    limit = 10000
    kwargs['limit'] = limit
    seed = []
    _, page = get_conn().get_container(container_name, **kwargs)
    seed.extend(page)

    while len(page) == limit:
        # keep getting pages..
        kwargs['marker'] = seed[-1]['name']
        _, page = get_conn().get_container(container_name, **kwargs)
        seed.extend(page)

    return seed


def delete_from_objectstore(container, object_name):
    """
    remove file `object_name` fronm `container`
    :param container: Container name
    :param object_name:
    :return:
    """
    return get_conn().delete_object(container, object_name)


def download_file(container_name, file_path, target_path=None, target_root=DIVA_DIR, file_last_modified=None):
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
        data = get_conn().get_object(container_name, file_path)[1]
        newfile.write(data)
    if file_last_modified:
        epoch_modified = file_last_modified.timestamp()
        os.utime(newfilename, (epoch_modified, epoch_modified))


def download_file_data(container_name, file_path):
    return get_conn().get_object(container_name, file_path)[1]


def download_wkpb_file_data(file_path):
    return download_file_data('productie', file_path)


def file_exists(target):
    target = Path(target)
    return target.is_file()


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

    log.info(f"import files from {container_name}")

    underscore_date = re.compile(r'_\d{8}')
    date_match = re.compile(r'_(?P<y>\d{4})(?P<m>\d{2})(?P<d>\d{2})_')

    to_download = {}
    now = datetime.date.today()

    for prefix in ('bag', 'gebieden'):
        for file_object in get_full_container_list(container_name, prefix=prefix):
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
        download_file(container_name, entry['path'], target_path=target)


if __name__ == "__main__":
    argparser = argparse.ArgumentParser()
    # Always do GOB import now. Can be removed if parameter is not used anymore
    argparser.add_argument('-g', '--gob', action='store_true', help='Do GOB import')
    args = argparser.parse_args()
    # Download files from objectstore
    log.info("Start downloading files from objectstore")
    fetch_gob_files(environment)
