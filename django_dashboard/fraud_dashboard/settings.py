"""
Django settings for the fraud detection dashboard (Step 6).

The dashboard runs on Windows and reads the predictions that the Spark
Stage 3 consumer writes into MySQL (Docker container 'mysql', published
on localhost:3307).
"""
import os
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
PROJECT_ROOT = BASE_DIR.parent  # the "BIG DATA ESSENTIAL PROJECT" folder

SECRET_KEY = "dev-only-key-for-bde-fraud-dashboard-change-if-deployed"
DEBUG = True
ALLOWED_HOSTS = ["*"]

INSTALLED_APPS = [
    "django.contrib.staticfiles",
    "monitor",
]

MIDDLEWARE = [
    "django.middleware.security.SecurityMiddleware",
    "django.middleware.common.CommonMiddleware",
]

ROOT_URLCONF = "fraud_dashboard.urls"

TEMPLATES = [
    {
        "BACKEND": "django.template.backends.django.DjangoTemplates",
        "DIRS": [],
        "APP_DIRS": True,
        "OPTIONS": {"context_processors": ["django.template.context_processors.request"]},
    },
]

WSGI_APPLICATION = "fraud_dashboard.wsgi.application"

# MySQL in Docker (see docker-compose.yml). Override with environment variables if needed.
DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.mysql",
        "NAME": os.environ.get("FRAUD_DB_NAME", "fraud_db"),
        "USER": os.environ.get("FRAUD_DB_USER", "bde_user"),
        "PASSWORD": os.environ.get("FRAUD_DB_PASSWORD", "bde_pw"),
        "HOST": os.environ.get("FRAUD_DB_HOST", "127.0.0.1"),
        "PORT": os.environ.get("FRAUD_DB_PORT", "3307"),
    }
}

# Model training results (copied from spark-master after training)
TRAINING_METRICS_PATH = PROJECT_ROOT / "models" / "training_metrics.json"

LANGUAGE_CODE = "en-us"
TIME_ZONE = "UTC"
USE_I18N = False
USE_TZ = False  # the Spark job writes plain (naive) datetimes

STATIC_URL = "static/"
DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"
