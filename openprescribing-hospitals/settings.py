from pathlib import Path
from environs import Env
import re
import os

env = Env()
env.read_env()

BASE_DIR = Path(__file__).resolve().parent.parent

# Quick-start development settings - unsuitable for production
# See https://docs.djangoproject.com/en/5.1/howto/deployment/checklist/

# SECURITY WARNING: keep the secret key used in production secret!
SECRET_KEY = env.str("SECRET_KEY")

DEBUG = env.bool("DEBUG", False) 
DIGITAL_OCEAN_HOSTNAME = env.str("DIGITAL_OCEAN_HOSTNAME", None)

ALLOWED_HOSTS = []

if DEBUG:
    ALLOWED_HOSTS.append("localhost")
    ALLOWED_HOSTS.append("127.0.0.1")

if DIGITAL_OCEAN_HOSTNAME:
    ALLOWED_HOSTS.append(DIGITAL_OCEAN_HOSTNAME)


# Application definition

INSTALLED_APPS = [
    "django.contrib.admin",
    "django.contrib.auth",
    "django.contrib.contenttypes",
    "django.contrib.sessions",
    "django.contrib.messages",
    "django.contrib.staticfiles",
    "viewer",
    "django_vite",
    "django_browser_reload",
]

MIDDLEWARE = [
    "django.middleware.security.SecurityMiddleware",
    "whitenoise.middleware.WhiteNoiseMiddleware",
    "django.contrib.sessions.middleware.SessionMiddleware",
    "django.middleware.common.CommonMiddleware",
    "django.middleware.csrf.CsrfViewMiddleware",
    "django.contrib.auth.middleware.AuthenticationMiddleware",
    "django.contrib.messages.middleware.MessageMiddleware",
    "django.middleware.clickjacking.XFrameOptionsMiddleware",
    "viewer.middleware.MaintenanceModeMiddleware",
]

ROOT_URLCONF = "openprescribing-hospitals.urls"

TEMPLATES = [
    {
        "BACKEND": "django.template.backends.django.DjangoTemplates",
        "DIRS": [BASE_DIR / "templates"],
        "APP_DIRS": True,
        "OPTIONS": {
            "context_processors": [
                "django.template.context_processors.debug",
                "django.template.context_processors.request",
                "django.contrib.auth.context_processors.auth",
                "django.contrib.messages.context_processors.messages",
            ],
        },
    },
]

WSGI_APPLICATION = "openprescribing-hospitals.wsgi.application"


# Database
# https://docs.djangoproject.com/en/5.1/ref/settings/#databases

DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.postgresql',
        'NAME': env.str("DATABASE_NAME"),
        'USER': env.str("DATABASE_USER"),
        'PASSWORD': env.str("DATABASE_PASSWORD"),
        'HOST': env.str("DATABASE_HOST"),
        'PORT': env.str("DATABASE_PORT"),
    }
}

CACHES = {
    'default': {
        'BACKEND': 'django.core.cache.backends.db.DatabaseCache',
        'LOCATION': 'django_cache_table',
        'TIMEOUT': None,
        'OPTIONS': {
            'MAX_ENTRIES': 1000,
            'CULL_FREQUENCY': 3,
        }
    }
}

# Password validation
# https://docs.djangoproject.com/en/5.1/ref/settings/#auth-password-validators

AUTH_PASSWORD_VALIDATORS = [
    {
        "NAME": "django.contrib.auth.password_validation.UserAttributeSimilarityValidator",
    },
    {
        "NAME": "django.contrib.auth.password_validation.MinimumLengthValidator",
    },
    {
        "NAME": "django.contrib.auth.password_validation.CommonPasswordValidator",
    },
    {
        "NAME": "django.contrib.auth.password_validation.NumericPasswordValidator",
    },
]


# Internationalization
# https://docs.djangoproject.com/en/5.1/topics/i18n/

LANGUAGE_CODE = "en-us"

TIME_ZONE = "UTC"

USE_I18N = True

USE_TZ = True


# Static files (CSS, JavaScript, Images)
# https://docs.djangoproject.com/en/5.1/howto/static-files/

STATIC_URL = "/static/"
STATIC_ROOT = BASE_DIR / "staticfiles"
STATICFILES_DIRS = [
    BASE_DIR / "static",
    BASE_DIR / "assets" / "dist",
]

STATICFILES_STORAGE = 'whitenoise.storage.CompressedManifestStaticFilesStorage'

# Default primary key field type
# https://docs.djangoproject.com/en/5.1/ref/settings/#default-auto-field

DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"


DJANGO_VITE = {
    "default": {
        "manifest_path": BASE_DIR / "assets" / "dist" / ".vite" / "manifest.json",
        "dev_mode": DEBUG,
    }
}

if DEBUG:
    CSP_DEFAULT_SRC = ("'self'",)
    CSP_SCRIPT_SRC = ("'self' 'unsafe-inline' http://localhost:5173 http://localhost:3000 https://plausible.io https://cdn.jsdelivr.net/npm/clipboard@2/dist/clipboard.min.js")
    CSP_SCRIPT_SRC_ELEM = ("'self' 'unsafe-inline' http://localhost:5173 http://localhost:3000 https://plausible.io https://cdn.jsdelivr.net/npm/clipboard@2/dist/clipboard.min.js",)
    CSP_STYLE_SRC = ("'self'", "'unsafe-inline'")
    CSP_IMG_SRC = ("'self'", "data:")
    CSP_FONT_SRC = ("'self'",)
    CSP_CONNECT_SRC = ("'self' http://localhost:5173 ws://localhost:5173 http://localhost:3000 ws://localhost:3000 https://plausible.io")

else:
    CSP_DEFAULT_SRC = ("'self'",)
    CSP_SCRIPT_SRC = ("'self'", "https://plausible.io", "https://cdn.jsdelivr.net/npm/clipboard@2/dist/clipboard.min.js")
    CSP_SCRIPT_SRC_ELEM = ("'self'", "https://plausible.io", "https://cdn.jsdelivr.net/npm/clipboard@2/dist/clipboard.min.js")
    CSP_STYLE_SRC = ("'self'",)
    CSP_IMG_SRC = ("'self'", "data:")
    CSP_FONT_SRC = ("'self'",)
    CSP_CONNECT_SRC = ("'self'", "https://plausible.io")

def immutable_file_test(path, url):
    # Match filename with 12 hex digits before the extension
    # e.g. app.db8f2edc0c8a.js
    return re.match(r"^.+[\.\-][0-9a-f]{8,12}\..+$", url)


WHITENOISE_IMMUTABLE_FILE_TEST = immutable_file_test

LOGIN_REDIRECT_URL = '/'
LOGOUT_REDIRECT_URL = '/'
LOGIN_URL = '/login/'

CSRF_COOKIE_SECURE = not DEBUG
SESSION_COOKIE_SECURE = not DEBUG
CSRF_TRUSTED_ORIGINS = [f"https://{DIGITAL_OCEAN_HOSTNAME}"] if DIGITAL_OCEAN_HOSTNAME else []


LOGGING = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'verbose': {
            'format': '{levelname} {asctime} {module} {process:d} {thread:d} {message}',
            'style': '{',
        },
        'simple': {
            'format': '{levelname} {message}',
            'style': '{',
        },
    },
    'handlers': {
        'console': {
            'level': 'INFO',
            'class': 'logging.StreamHandler',
            'formatter': 'simple',
        },
        'file': {
            'level': 'INFO',
            'class': 'logging.handlers.RotatingFileHandler',
            'filename': os.path.join(BASE_DIR, 'logs', 'django.log'),
            'maxBytes': 1024 * 1024 * 5,  # 5 MB
            'backupCount': 5,
            'formatter': 'verbose',
        },
    },
    'root': {
        'handlers': ['console', 'file'],
        'level': 'INFO',
    },
    'loggers': {
        'django': {
            'handlers': ['console', 'file'],
            'level': 'INFO',
            'propagate': False,
        },
    },
}

os.makedirs(os.path.join(BASE_DIR, 'logs'), exist_ok=True)

CSRF_FAILURE_VIEW = 'viewer.views.csrf_failure'
