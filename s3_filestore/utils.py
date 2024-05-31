import os
import boto3
import hashlib
import requests
import re

from pathlib import Path
from urllib.parse import urlparse

from pdb import set_trace

def is_url_public_readable(url):
    try:
        response = requests.head(url)
        # Check if the status code is 200
        if response.status_code == 200:
            return True
        else:
            return False
    except requests.RequestException as e:
        print(f"Error checking URL: {e}")
        return False
    
def parse_s3_uri(s3_url):
    parsed_url = urlparse(s3_url)
    bucket_name = parsed_url.netloc
    
    # Remove leading '/' from the path and then remove the bucket name from it
    full_path = parsed_url.path.lstrip('/')
    
    # Strip the bucket name from the beginning of the full_path
    if full_path.startswith(bucket_name + '/'):
        bucket_key = full_path[len(bucket_name) + 1:]
    else:
        bucket_key = full_path

    return bucket_name, bucket_key

def parse_s3_url(url):
    default_region = 'us-east-1'
    bucket_name = None
    object_key = None
    domain = None
    region = default_region

    parsed_url = urlparse(url)
    hostname = parsed_url.hostname
    path = parsed_url.path

    if hostname.endswith('amazonaws.com'):
        domain = 'amazonaws.com'
    elif hostname.endswith('wasabisys.com'):
        domain = 'wasabisys.com'
    else:
        set_trace()
        raise ValueError("URL is neither an AWS nor a Wasabi S3 URL")

    # Virtual-hosted-style URL
    match = re.match(r'^(?P<bucket_name>[^.]+)\.s3\.(?P<region>[^.]+)\.' + re.escape(domain), hostname)
    if match:
        bucket_name = match.group('bucket_name')
        region = match.group('region')
        object_key = path.lstrip('/')
    else:
        # Path-style URL
        match = re.match(r'^s3\.(?P<region>[^.]+)\.' + re.escape(domain), hostname)
        if match:
            region = match.group('region')
            # The path is of the form /bucket_name/object_key
            path_parts = path.lstrip('/').split('/', 1)
            if len(path_parts) == 2:
                bucket_name, object_key = path_parts
            elif len(path_parts) == 1:
                bucket_name = path_parts[0]
                object_key = ''
    
    return bucket_name, object_key, domain, region
    
def append_hash_id_to_objectname(local_filename, object_name, hash_length):
    hash_id = get_file_hash(local_filename, hash_length=hash_length)
    object_name = f"{Path(object_name).stem}-{hash_id}{Path(object_name).suffix}"
    return object_name

def get_object_name_with_hash_id(local_filename, object_name=None, hash_length=None):
    if object_name is None:
        object_name = Path(local_filename).name

    object_name_hash_id = append_hash_id_to_objectname(local_filename, object_name, hash_length)    
    
    return object_name_hash_id
    
def has_hash(filename):
    return len(Path(filename).stem.split("-")) == 2

def get_file_hash(filename, hash_length=None):
    with open(filename,"rb") as f:
        bytes = f.read() # read entire file as bytes
        readable_hash = hashlib.sha256(bytes).hexdigest();
    
    if isinstance(hash_length, (int)):
        readable_hash = readable_hash[0:hash_length]  

    return readable_hash

def check_hashid(hashid, weights_path):
    weights_hash = get_file_hash(weights_path)
    assert weights_hash.startswith(hashid), f"Oops, expected weights_hash to start with {hashid}, got {weights_hash}"
    return True

def get_subfolder(weight_file, split_from="logs"):
    parts = weight_file.parts
    subfolder = f"{os.path.sep}".join(parts[parts.index(split_from):-1])
    
    return subfolder
    
  