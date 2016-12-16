"""
Django settings for BAG project.

Basis Administratie Gebouwen

Generated by 'django-admin startproject' using Django 1.8.3.

more information on this file, see
https://docs.djangoproject.com/en/1.8/topics/settings/

For the full list of settings and their values, see
https://docs.djangoproject.com/en/1.8/ref/settings/
"""

# Build paths inside the project like this: os.path.join(BASE_DIR, ...)
import datetime
import os
import sys
import re


def _get_docker_host():
    d_host = os.getenv('DOCKER_HOST', None)
    if d_host:
        return re.match(r'tcp://(.*?):\d+', d_host).group(1)
    return '0.0.0.0'

NO_INTEGRATION_TEST = os.getenv('NO_INTEGRATION_TEST')

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Quick-start development settings - unsuitable for production
# See https://docs.djangoproject.com/en/1.8/howto/deployment/checklist/

# SECURITY WARNING: keep the secret key used in production secret!
insecure_key = 'insecure'

SECRET_KEY = os.getenv('BAG_SECRET_KEY', insecure_key)

DEBUG = SECRET_KEY == insecure_key

# Application definition
PARTIAL_IMPORT = dict(
    numerator=0,
    denominator=1
)

NO_INTEGATION_TEST = True

DATAPUNT_API_URL = os.getenv(
    # note the ending /
    'DATAPUNT_API_URL', 'https://api.datapunt.amsterdam.nl/')

INSTALLED_APPS = (
    'django.contrib.admin',
    'django.contrib.auth',
    'django.contrib.contenttypes',
    'django.contrib.sessions',
    'django.contrib.messages',
    'django.contrib.sites',

    'django.contrib.staticfiles',
    'django_extensions',

    'oauth2_provider',

    'batch',
    'atlas',

    'datasets.bag',
    'datasets.brk',
    'datasets.wkpb',

    'geo_views',
    'atlas_api',
    'health',

    'django.contrib.gis',
    'rest_framework',
    'rest_framework_gis',
    'rest_framework_swagger',
)


MIDDLEWARE_CLASSES = (
    'django.contrib.sessions.middleware.SessionMiddleware',
    'django.middleware.common.CommonMiddleware',
    'django.middleware.csrf.CsrfViewMiddleware',
    'django.contrib.auth.middleware.AuthenticationMiddleware',
    'django.contrib.auth.middleware.SessionAuthenticationMiddleware',
    'django.contrib.messages.middleware.MessageMiddleware',
    'django.middleware.clickjacking.XFrameOptionsMiddleware',
    'django.middleware.security.SecurityMiddleware',
    'debug_toolbar.middleware.DebugToolbarMiddleware',
)

if DEBUG:
    MIDDLEWARE_CLASSES += ('debug_toolbar.middleware.DebugToolbarMiddleware',)

ROOT_URLCONF = 'bag.urls'

TEMPLATES = [
    {
        'BACKEND': 'django.template.backends.django.DjangoTemplates',
        'DIRS': [],
        'APP_DIRS': True,
        'OPTIONS': {
            'context_processors': [
                'django.template.context_processors.debug',
                'django.template.context_processors.request',
                'django.contrib.auth.context_processors.auth',
                'django.contrib.messages.context_processors.messages',
            ],
        },
    },
]

WSGI_APPLICATION = 'bag.wsgi.application'

def _get_docker_host():
    d_host = os.getenv('DOCKER_HOST', None)
    if d_host:
        return re.match(r'tcp://(.*?):\d+', d_host).group(1)
    return '0.0.0.0'

# Database
# https://docs.djangoproject.com/en/1.8/ref/settings/#databases

DATABASES = {
    'default': {
        'ENGINE': 'django.contrib.gis.db.backends.postgis',
        'NAME': os.getenv('DATABASE_NAME', 'atlas'),
        'USER': os.getenv('DATABASE_USER', 'atlas'),
        'PASSWORD': os.getenv('DATABASE_PASSWORD', 'insecure'),
        'HOST': os.getenv('DATABASE_PORT_5432_TCP_ADDR', _get_docker_host()),
        'PORT': os.getenv('DATABASE_PORT_5432_TCP_PORT', 5434),
        'CONN_MAX_AGE': 15,
    }
}


# Internationalization
# https://docs.djangoproject.com/en/1.8/topics/i18n/

LANGUAGE_CODE = 'en-us'

TIME_ZONE = 'Europe/Amsterdam'

USE_I18N = True

USE_L10N = True

USE_TZ = True

TESTING = len(sys.argv) > 1 and sys.argv[1] == 'test'

DIVA_DIR = '/app/diva/'

ELASTIC_INDICES = {
    'BAG': 'bag',
    'BRK': 'brk',
    'NUMMERAANDUIDING': 'nummeraanduiding',
    'MEETBOUTEN': 'meetbouten',
}

if TESTING:
    for k, v in ELASTIC_INDICES.items():
        ELASTIC_INDICES[k] += 'test'

ELASTIC_SEARCH_HOSTS = ["{}:{}".format(
    os.getenv('ELASTICSEARCH_PORT_9200_TCP_ADDR', _get_docker_host()),
    os.getenv('ELASTICSEARCH_PORT_9200_TCP_PORT', 9200))]

BATCH_SETTINGS = dict(
    batch_size=4000
)

STATIC_URL = '/static/'

ALLOWED_HOSTS = [
    '127.0.0.1',
    'localhost',
    '.localdomain',
    '.datapunt.amsterdam.nl',
    '.amsterdam.nl',
    '.service.consul',
]

REST_FRAMEWORK = dict(
    PAGE_SIZE=25,
    MAX_PAGINATE_BY=100,
    DEFAULT_AUTHENTICATION_CLASSES=(
        'rest_framework_jwt.authentication.JSONWebTokenAuthentication',
        'oauth2_provider.ext.rest_framework.OAuth2Authentication',
        'rest_framework.authentication.BasicAuthentication',
        'rest_framework.authentication.SessionAuthentication',
    ),
    DEFAULT_PAGINATION_CLASS='drf_hal_json.pagination.HalPageNumberPagination',
    DEFAULT_PARSER_CLASSES=(
        'drf_hal_json.parsers.JsonHalParser',
    ),
    DEFAULT_RENDERER_CLASSES=(
        'rest_framework.renderers.JSONRenderer',
        'rest_framework.renderers.BrowsableAPIRenderer'
    ),
    DEFAULT_FILTER_BACKENDS=(
        'django_filters.rest_framework.DjangoFilterBackend',
    )
)

# Security

SECURE_CONTENT_TYPE_NOSNIFF = True
SECURE_BROWSER_XSS_FILTER = True
# CSRF_COOKIE_SECURE = True
CSRF_COOKIE_HTTPONLY = True
X_FRAME_OPTIONS = 'DENY'

SWAGGER_SETTINGS = {
    'exclude_namespaces': [],
    'api_version': '0.1',
    'api_path': '/',

    'enabled_methods': [
        'get',
    ],

    'USE_SESSION_AUTH': False,
    'VALIDATOR_URL': None,

    'api_key': '',

    'is_authenticated': False,
    'is_superuser': False,

    'unauthenticated_user': 'django.contrib.auth.models.AnonymousUser',
    'permission_denied_handler': None,
    'resource_access_handler': None,

    'protocol': 'https' if not DEBUG else '',
    'base_path': 'api-acc.datapunt.amsterdam.nl/bag/docs' if DEBUG else '127.0.0.1:8000/bag/docs',

    'info': {
        'contact': 'atlas.basisinformatie@amsterdam.nl',
        'description': 'This is the BAG API server.',
        'license': 'Mozilla Public License Version 2.0',
        'licenseUrl': 'https://www.mozilla.org/en-US/MPL/2.0/',
        'termsOfServiceUrl': 'https://atlas.amsterdam.nl/terms/',
        'title': 'BAG, BRK en WKPB API',
    },
    'doc_expansion': 'list',
}

OBJECTSTORE = {
    'auth_version': '2.0',
    'authurl': 'https://identity.stack.cloudvps.com/v2.0',
    'user': os.getenv('OBJECTSTORE_USER', 'bag_brk'),
    'key': os.getenv('OS_PASSWORD', 'insecure'),
    'tenant_name': 'BGE000081_BAG',
    'os_options': {
        'tenant_id': '4f2f4b6342444c84b3580584587cfd18',
        'region_name': 'NL',
        'endpoint_type': 'internalURL'}
}

JWT_AUTH = {
    'JWT_ENCODE_HANDLER':
        'rest_framework_jwt.utils.jwt_encode_handler',

    'JWT_DECODE_HANDLER':
        'rest_framework_jwt.utils.jwt_decode_handler',

    'JWT_PAYLOAD_HANDLER':
        'rest_framework_jwt.utils.jwt_payload_handler',

    'JWT_PAYLOAD_GET_USER_ID_HANDLER':
        'rest_framework_jwt.utils.jwt_get_user_id_from_payload_handler',

    'JWT_RESPONSE_PAYLOAD_HANDLER':
        'rest_framework_jwt.utils.jwt_response_payload_handler',

    'JWT_SECRET_KEY': os.getenv('JWT_SHARED_SECRET_KEY', 'some_shared_secret'),
    'JWT_ALGORITHM': 'HS256',
    'JWT_VERIFY': True,
    'JWT_VERIFY_EXPIRATION': True,
    'JWT_LEEWAY': 0,
    'JWT_EXPIRATION_DELTA': datetime.timedelta(seconds=300),
    'JWT_AUDIENCE': None,
    'JWT_ISSUER': None,

    'JWT_ALLOW_REFRESH': False,
    'JWT_REFRESH_EXPIRATION_DELTA': datetime.timedelta(days=7),

    'JWT_AUTH_HEADER_PREFIX': 'JWT',
}


ELASTIC_SEARCH_HOSTS = ["{}:{}".format(
    os.getenv('ELASTICSEARCH_PORT_9200_TCP_ADDR', _get_docker_host()),
    os.getenv('ELASTICSEARCH_PORT_9200_TCP_PORT', 9200))]

PROJECT_DIR = os.path.abspath(os.path.join(BASE_DIR, '..', '..'))
DIVA_DIR = os.path.abspath(os.path.join(PROJECT_DIR, 'data'))

if not os.path.exists(DIVA_DIR):
    DIVA_DIR = os.path.abspath(os.path.join(PROJECT_DIR, 'bag', 'diva'))
    print("Geen lokale DIVA bestanden gevonden, maak gebruik van testset onder", DIVA_DIR)

# noinspection PyUnresolvedReferences
from .checks import *  # used for ./manage.py check
