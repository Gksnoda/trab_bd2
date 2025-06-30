from pathlib import Path
from decouple import config

# --------------------------------------------------
# BASE DIR
# --------------------------------------------------
BASE_DIR = Path(__file__).resolve().parent.parent

STATIC_URL = 'static/'

STATICFILES_DIRS = [
    BASE_DIR / 'static',
]


TEMPLATES = [
    {
        'BACKEND': 'django.template.backends.django.DjangoTemplates',
        'DIRS': [ BASE_DIR / 'templates' ],  # <-- isso deve estar aqui
        'APP_DIRS': True,
        'OPTIONS': {
            'context_processors': [
                'django.template.context_processors.request',
                'django.contrib.auth.context_processors.auth',
                'django.contrib.messages.context_processors.messages',
            ],
        },
    },
]


# --------------------------------------------------
# SECRET KEY / DEBUG / ALLOWED HOSTS
# --------------------------------------------------
SECRET_KEY = config('DJANGO_SECRET_KEY',
                    default='django-insecure-m!o$7_%$_p3mm6vfd75%ixk@537r%peh5dx3ip3=m)l#vn)mjt')

DEBUG = config('DEBUG', cast=bool, default=True)

ALLOWED_HOSTS = [
    'localhost',
    '127.0.0.1',
]


# --------------------------------------------------
# APPLICATIONS
# --------------------------------------------------
INSTALLED_APPS = [
    'django.contrib.admin',
    'django.contrib.auth',
    'django.contrib.contenttypes',
    'django.contrib.sessions',
    'django.contrib.messages',
    'django.contrib.staticfiles',
    'django.contrib.humanize',

    # seu app de relatórios ad-hoc
    'reports',
]


# --------------------------------------------------
# MIDDLEWARE
# --------------------------------------------------
MIDDLEWARE = [
    'django.middleware.security.SecurityMiddleware',
    'django.contrib.sessions.middleware.SessionMiddleware',
    'django.middleware.common.CommonMiddleware',
    'django.middleware.csrf.CsrfViewMiddleware',
    'django.contrib.auth.middleware.AuthenticationMiddleware',
    'django.contrib.messages.middleware.MessageMiddleware',
    'django.middleware.clickjacking.XFrameOptionsMiddleware',
]


# --------------------------------------------------
# URLS & WSGI
# --------------------------------------------------
ROOT_URLCONF = 'ad_hoc_django.urls'
WSGI_APPLICATION = 'ad_hoc_django.wsgi.application'



# --------------------------------------------------
# DATABASES
# --------------------------------------------------
DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.postgresql',
        'HOST':   config('DB_HOST', default='localhost'),
        'PORT':   config('DB_PORT', default='5432'),
        'NAME':   config('DB_NAME', default='twitch_2'),
        'USER':   config('DB_USER', default='postgres'),
        'PASSWORD': config('DB_PASSWORD', default='postgres'),
    }
}


# --------------------------------------------------
# AUTH PASSWORD VALIDATION
# --------------------------------------------------
AUTH_PASSWORD_VALIDATORS = [
    {'NAME': 'django.contrib.auth.password_validation.UserAttributeSimilarityValidator',},
    {'NAME': 'django.contrib.auth.password_validation.MinimumLengthValidator',},
    {'NAME': 'django.contrib.auth.password_validation.CommonPasswordValidator',},
    {'NAME': 'django.contrib.auth.password_validation.NumericPasswordValidator',},
]


# --------------------------------------------------
# INTERNATIONALIZATION
# --------------------------------------------------
LANGUAGE_CODE = 'pt-br'
TIME_ZONE = 'America/Sao_Paulo'
USE_I18N = True
USE_TZ = True


# --------------------------------------------------
# STATIC FILES
# --------------------------------------------------
STATIC_URL = 'static/'


# --------------------------------------------------
# DEFAULT AUTO FIELD
# --------------------------------------------------
DEFAULT_AUTO_FIELD = 'django.db.models.BigAutoField'


# --------------------------------------------------
# CREDENCIAIS DA TWITCH (opcionalmente disponíveis via settings)
# --------------------------------------------------
TWITCH_CLIENT_ID     = config('TWITCH_CLIENT_ID', default='')
TWITCH_CLIENT_SECRET = config('TWITCH_CLIENT_SECRET', default='')
TWITCH_REDIRECT_URI  = config('TWITCH_REDIRECT_URI', default='')
TWITCH_TOKEN         = config('TWITCH_TOKEN', default='')
