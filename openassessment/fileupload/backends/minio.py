import logging

import boto3
from botocore.client import Config
from django.conf import settings

from .base import BaseBackend
from ..exceptions import FileUploadInternalError

logger = logging.getLogger("openassessment.fileupload.api")


class Backend(BaseBackend):

    def get_upload_url(self, key, content_type):
        bucket_name, key_name = self._retrieve_parameters(key)

        try:
            client = _connect_to_s3()
            client.put_object(
                ACL="public-read",
                Bucket=bucket_name,
                Key=key_name,
                ContentType=content_type
            )

            upload_url = "{}/{}/{}".format(client.meta.endpoint_url, bucket_name, key_name)
            return upload_url
        except Exception as ex:
            logger.exception(
                u"An internal exception occurred while generating an upload URL."
            )
            raise FileUploadInternalError(ex)

    def get_download_url(self, key):
        bucket_name, key_name = self._retrieve_parameters(key)
        try:
            client = _connect_to_s3()
            if client.get_object(Bucket=bucket_name, Key=key_name):
                return "{}/{}/{}".format(client.meta.endpoint_url, bucket_name, key_name)
            else:
                return None
        except Exception as ex:
            logger.exception(
                u"An internal exception occurred while generating a download URL."
            )
            raise FileUploadInternalError(ex)

    def remove_file(self, key):
        bucket_name, key_name = self._retrieve_parameters(key)

        client = _connect_to_s3()
        try:
            client.delete_object(Bucket=bucket_name, Key=key_name)
        except:
            return False


def _connect_to_s3():
    """Connect to s3

    Creates a connection to s3 for file URLs.

    """
    # Try to get the AWS credentials from settings if they are available
    # If not, these will default to `None`, and boto will try to use
    # environment vars or configuration files instead.
    minio_access_key_id = getattr(settings, 'MINIO_ACCESS_KEY_ID', None)
    minio_secret_access_key = getattr(settings, 'MINIO_SECRET_ACCESS_KEY', None)
    minio_custom_domain = getattr(settings, 'MINIO_S3_CUSTOM_DOMAIN', None)

    return boto3.client('s3',
                        endpoint_url=minio_custom_domain,
                        aws_access_key_id=minio_access_key_id,
                        aws_secret_access_key=minio_secret_access_key,
                        config=Config(signature_version='s3v4'), region_name='')
