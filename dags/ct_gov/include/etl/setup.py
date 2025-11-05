from typing import Dict
from ct_gov.include.handlers.s3_handler import S3Handler
from ct_gov.include.notification_middleware.generic_middleware import (
    persist_before_failure, persist_before_exit
)

from airflow.utils.log.logging_mixin import LoggingMixin
log = LoggingMixin().log



class Setup:
    def __init__(self, context: Dict, s3_hook):
        self.context = context
        self.s3 = s3_hook

    def create_s3_folder(self, bucket: str):
        folder_name = self.context["ds"]
        folder_key = f"{folder_name}"

        try:
            self.s3.load_string(
                string_data="",
                key=folder_key,
                bucket_name=bucket,
                replace=True,
            )
            destination = f"{bucket}/{folder_key}"

            metadata = {
                'destination_bucket': destination,
                'date': self.context["ds"]
            }
            persist_before_exit(self.context, metadata)
            return destination

        except Exception as e:
            persist_before_failure(
                error=e,
                context=self.context,
                metadata={
                    'attempted_object': f"{bucket}/{folder_name}"
                }
            )
            raise e  # todo: create notification