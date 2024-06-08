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
from . import api
from .utils import get_object_name_with_hash_id, parse_s3_url
from .data import prepare_data_for_upload

class S3FileStore(object):
    def __init__(self, bucket_name, profile='wasabi', endpoint_url=None, acl='public-read', hash_length=10, cache_dir=None, expires_in_seconds=3600):        
        if cache_dir is None: cache_dir = F.CACHE_DIR

        self.cache_dir = cache_dir
        self.profile = profile
        if endpoint_url is None:
            self.endpoint_url = auth.WASABI_ENDPOINT if 'wasabi' in profile else auth.AWS_ENDPOINT
        else:
            self.endpoint_url = endpoint_url
        
        self.acl = acl
        self.expires_in_seconds = expires_in_seconds
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
        self.s3_client = self.session.client('s3', endpoint_url=endpoint_url)
        self.s3 = self.session.resource('s3', endpoint_url=endpoint_url)
        self.bucket = self.s3.Bucket(self.bucket_name)
        self.bucket.region = region_name

    def list_objects(self, prefix='', depth=None, directory_filter=None, verbose=False):
        """
        List objects in an S3 bucket with optional depth and directory exclusion.

        Parameters:
        - prefix: The prefix (subfolder) to filter objects.
        - depth: The maximum depth of subfolders to include.
        - include_directories: Whether to include directories in the listing.
        """
        objects = F.list_objects(self.bucket,
                                 prefix=prefix,
                                 depth=depth,
                                 directory_filter=directory_filter,
                                 verbose=verbose)

        return objects  

    def list_urls(self, prefix='', depth=None, verbose=False):
        objects = self.list_objects(prefix=prefix, depth=depth, include_directories=False, verbose=verbose)
        urls = [auth.generate_url(self.s3_client, self.bucket.name, bucket_key, bucket_region=self.bucket.region, 
                                  profile=self.profile, expires_in_seconds=self.expires_in_seconds) 
                for bucket_key in objects]
        return urls 
    
    def list_s3_urls(self, prefix='', depth=None, verbose=False):
        objects = self.list_objects(prefix=prefix, depth=depth, include_directories=False, verbose=verbose)
        urls = [f"s3://{self.bucket.name}/{bucket_key}" for bucket_key in objects]
        return urls

    def load_file(self, filename, cache_dir=None, progress=True, check_hash=True):
        if cache_dir is None: cache_dir = self.cache_dir
        if filename.startswith("https://"):
            local_filename = self.download_url(filename, cache_dir=cache_dir, progress=progress, check_hash=check_hash)
            return F.load_file(local_filename)
        elif filename.startswith("s3://"):
            bucket_name, bucket_key = parse_s3_url(filename)
            bucket_region = self.bucket.region if bucket_name==self.bucket.name else None
            local_filename = F.download_object(self.s3_client, bucket_name, bucket_key, self.profile, bucket_region=bucket_region,
                                               cache_dir=cache_dir, progress=progress, check_hash=check_hash)
            return F.load_file(local_filename)
        return F.load_file(filename)

    def load_object(self, bucket_key, cache_dir=None, progress=True, check_hash=True):
        if cache_dir is None: cache_dir = self.cache_dir
        local_filename = F.download_object(self.s3_client, self.bucket.name, bucket_key, self.profile, bucket_region=self.bucket.region,
                                           cache_dir=cache_dir, progress=progress, check_hash=check_hash)
        return F.load_file(local_filename)

    def download_object(self, bucket_key, cache_dir=None, progress=True, check_hash=True):
        if cache_dir is None: cache_dir = self.cache_dir
        return F.download_object(self.s3_client, self.bucket.name, bucket_key, self.profile, bucket_region=self.bucket.region,
                                 cache_dir=cache_dir, progress=progress, check_hash=check_hash)

    def download_objects(self, objects, cache_dir=None, progress=True, check_hash=True):
        filenames = [self.download_object(object_key, cache_dir=cache_dir, progress=progress, check_hash=check_hash) 
                     for object_key in objects]

        return filenames

    def download_url(self, url, cache_dir=None, progress=True, check_hash=True):
        if cache_dir is None: cache_dir = self.cache_dir
        return F.download_if_needed(url, cache_dir=cache_dir, progress=progress, check_hash=check_hash)

    def download_urls(self, urls, cache_dir=None, progress=True, check_hash=True):
        filenames = [self.download_url(url, cache_dir=cache_dir, progress=progress, check_hash=check_hash) 
                     for url in urls]
        
        return filenames    

    def upload_file(self, local_filename, bucket_subfolder, new_filename=None, acl=None, hash_length=None, verbose=True, profile=None, expires_in_seconds=None):        
        if acl is None: acl = self.acl
        if hash_length is None: hash_length = self.hash_length
        if not bucket_subfolder.endswith('/'): bucket_subfolder += '/'
        if profile is None: profile = self.profile
        if expires_in_seconds is None: expires_in_seconds = self.expires_in_seconds

        object_name = get_object_name_with_hash_id(local_filename, object_name=new_filename, hash_length=hash_length)
        object_key = urljoin(bucket_subfolder, object_name)
        object_url = F.upload_file(self.s3_client, self.bucket, local_filename, object_key, acl=acl, 
                                   verbose=verbose, profile=profile, expires_in_seconds=expires_in_seconds)

        return object_url

    def upload_files(self, filenames, bucket_subfolder, acl=None, hash_length=None, verbose=True, profile=None, expires_in_seconds=None):
        if isinstance(bucket_subfolder, (str)):
            bucket_subfolders = [bucket_subfolder]*len(filenames)
        elif isinstance(bucket_subfolders, (list,tuple)):
            bucket_subfolders = bucket_subfolder

        urls = [self.upload_file(local_filename, bucket_subfolder, acl=acl, hash_length=hash_length, verbose=verbose, 
                                 profile=profile, expires_in_seconds=expires_in_seconds)
                for local_filename,bucket_subfolder in zip(filenames, bucket_subfolders)]
        return urls

    def update_object_acl(self, object_key, acl, verbose=True):
        return api.update_object_acl(self.s3_client, self.bucket.name, object_key, acl, verbose=verbose)

    def upload_data(self, data, bucket_key, data_format=None, acl=None, hash_length=None, verbose=True, profile=None, expires_in_seconds=None):
        if acl is None: acl = self.acl
        if hash_length is None: hash_length = self.hash_length
        if profile is None: profile = self.profile
        if expires_in_seconds is None: expires_in_seconds = self.expires_in_seconds

        # get the buffer and hash_id
        buf, hash_id, data_format = prepare_data_for_upload(data, hash_length, data_format=data_format)

        # new filename with hash_id
        path = Path(bucket_key)
        bucket_subfolder = str(path.parent)
        if not bucket_subfolder.endswith('/'): bucket_subfolder += '/'
        filename_with_hash_id = f"{path.stem}-{hash_id}{path.suffix}"
        bucket_key = urljoin(bucket_subfolder, filename_with_hash_id)

        url = F.upload_buffer(self.s3_client, self.bucket, buf, bucket_key, acl=acl, 
                              verbose=verbose, profile=profile, expires_in_seconds=expires_in_seconds)

        return bucket_key, url

    def __repr__(self):
        return (f"{self.__class__.__name__}(bucket_name={self.bucket_name!r}, profile={self.profile!r}, "
                f"endpoint_url={self.endpoint_url!r}, bucket_region={self.bucket_region!r},\n"
                f"\t acl={self.acl!r}, expires_in_seconds={self.expires_in_seconds!r}, hash_length={self.hash_length!r}, "
                f"cache_dir={self.cache_dir!r})")

