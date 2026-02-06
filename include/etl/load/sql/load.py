import logging
import io
from typing import List, Dict
import pandas as pd

from airflow.utils.context import Context
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from config.env_config import config




class Loader:
    def __init__(self, connection):
        pass
