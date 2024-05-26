import os
import boto3
from tqdm import tqdm
import posixpath
import botocore.exceptions
import hashlib
import json
import pandas as pd
from pprint import pformat
from pathlib import Path
from urllib.parse import urljoin, urlparse, urlunparse

from pdb import set_trace

from . import functional as F
from . import auth
from .utils import get_url

class S3Logger(object):
    def __init__(self, bucket_name, profile='wasabi', endpoint_url=None, acl='public-read', hash_length=10, cache_dir=None):        
        if cache_dir is None: cache_dir = F.CACHE_DIR

        self.cache_dir = cache_dir
        self.profile = profile
        if endpoint_url is None:
            self.endpoint_url = auth.WASABI_ENDPOINT if 'wasabi' in profile else auth.AWS_ENDPOINT
        else:
            self.endpoint_url = endpoint_url
        
        self.acl = acl
        self.hash_length = hash_length
        self.bucket_name = bucket_name
        self.set_session_bucket()

    def set_session_bucket(self):
        # temporary session without region name
        tmp_session = auth.get_session_with_userdata(self.profile, region_name=None)
        # get region name for this bucket, add to endpoint_url
        region_name = auth.get_bucket_region(tmp_session, self.bucket_name, self.endpoint_url)
        endpoint_url = self.endpoint_url.replace("s3.", f"s3.{region_name}.")

        # now we can start a proper session and setup bucket access:
        self.bucket_region = region_name
        self.session = auth.get_session_with_userdata(self.profile, region_name=region_name)
        self.s3 = self.session.resource('s3', endpoint_url=endpoint_url)
        self.bucket = self.s3.Bucket(self.bucket_name)
        self.bucket.region = region_name

    def list_objects(self, prefix='', depth=None, include_directories=True):
        """
        List objects in an S3 bucket with optional depth and directory exclusion.
        
        Parameters:
        - prefix: The prefix (subfolder) to filter objects.
        - depth: The maximum depth of subfolders to include.
        - include_directories: Whether to include directories in the listing.
        """
        bucket = self.bucket
        for obj in bucket.objects.filter(Prefix=prefix):
            # Check if the key is directly within the specified depth
            if depth is None or (obj.key[len(prefix):].count('/') - 1) <= depth:
                # If include_directories is False, skip keys that end with a '/'
                if not include_directories and obj.key.endswith('/'):
                    continue
                print(obj.key)
    
    def load_file(self, filename):
        return F.load_file(filename)

    def download_object_from_bucket(self, bucket_key, cache_dir=None, progress=True, check_hash=True):
        if cache_dir is None: cache_dir = self.cache_dir
        return F.download_object(self.bucket.name, bucket_key, self.profile, cache_dir=cache_dir, progress=progress, check_hash=check_hash)

    def download_file_from_url(self, url, cache_dir=None, progress=True, check_hash=True):
        if cache_dir is None: cache_dir = self.cache_dir
        return F.download_if_needed(url, cache_dir=cache_dir, progress=progress, check_hash=check_hash)

    def upload_file(self, local_filename, bucket_subfolder, new_filename=None, acl=None, hash_length=None, verbose=True):
        if acl is None: acl = self.acl
        if hash_length is None: hash_length = self.hash_length
        if not bucket_subfolder.endswith('/'): bucket_subfolder += '/'

        object_name = F.get_object_name_with_hash_id(local_filename, object_name=new_filename, hash_length=hash_length)
        object_key = urljoin(bucket_subfolder, object_name)
        object_url = F.upload_file(self.s3, self.bucket, local_filename, object_key, acl=acl, verbose=verbose)

        return object_url

    def __repr__(self):
        return (f"{self.__class__.__name__}(bucket_name={self.bucket_name!r}, profile={self.profile!r}, "
                f"endpoint_url={self.endpoint_url!r}, bucket_region={self.bucket_region!r},\n"
                f"\t acl={self.acl!r}, hash_length={self.hash_length!r}, "
                f"cache_dir={self.cache_dir!r})")

    