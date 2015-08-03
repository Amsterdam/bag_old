import re
from .settings import *


def _get_docker_host():
    d_host = os.getenv('DOCKER_HOST', None)
    if d_host:
        return re.match(r'tcp://(.*?):\d+', d_host).group(1)
    return 'localhost'


DATABASES = {
    'default': {
        'ENGINE': 'django.contrib.gis.db.backends.postgis',
        'NAME': 'postgres',
        'USER': 'postgres',
        'PASSWORD': 'insecure',
        'HOST': _get_docker_host(),
        'PORT': 5432,
        'CONN_MAX_AGE': 60,
    }
}


LOGGING = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'console': {
            'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        },
    },
    'handlers': {
        'console': {
            'level': 'DEBUG',
            'class': 'logging.StreamHandler',
            'formatter': 'console',
        }
    },
    'root': {
        'level': 'WARNING',
        'handlers': ['console'],
    },
    'loggers': {
        # The actual soap server
        'batch': {
            'handlers': ['console'],
            'level': 'DEBUG',
            'propagate': False,
        },
    },
}

ELASTIC_SEARCH_HOSTS = [_get_docker_host()]

PROJECT_DIR = os.path.abspath(os.path.join(BASE_DIR, '..', '..'))
DIVA_DIR = os.path.abspath(os.path.join(PROJECT_DIR, '..', 'diva'))
if not os.path.exists(DIVA_DIR):
    DIVA_DIR = os.path.abspath(os.path.join(PROJECT_DIR, 'atlas_import', 'atlas_jobs', 'fixtures', 'testset'))
    print("Geen lokale DIVA bestanden gevonden, maak gebruik van testset onder", DIVA_DIR)

DEBUG = True
