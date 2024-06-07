import os
import sys
import torch
import pandas as pd
import botocore
import re
import json 

from torch.hub import download_url_to_file
from typing import Any, Callable, Dict, List, Mapping, Optional, Type, TypeVar, Union
from urllib.parse import urlparse

from . import auth

HASH_REGEX = re.compile(r'-([a-f0-9]*)\.')
CACHE_DIR = torch.hub.get_dir().replace("/hub", "/results")

def download_object(s3_client, bucket_name, bucket_key, profile, bucket_region=None, 
                    cache_dir=None, progress=True, check_hash=True, expires_in_seconds=3600):
    if cache_dir is None: cache_dir = CACHE_DIR
    url = auth.generate_url(s3_client, bucket_name, bucket_key, bucket_region=bucket_region, profile=profile, expires_in_seconds=expires_in_seconds)    
    response = download_if_needed(url, cache_dir=cache_dir, progress=progress, check_hash=check_hash)
    return response

def download_if_needed(url, cache_dir=None, progress=True, check_hash=True) -> Mapping[str, Any]:
    '''Download a file given a url. 

      File is stored in the cache_dir, which defaults to torch.hub.get_dir().replace("/hub", "/results").      
    '''  
    if cache_dir is None: cache_dir = CACHE_DIR

    os.makedirs(cache_dir, exist_ok=True)

    filename = os.path.basename(urlparse(url).path)
    cache_filename = os.path.join(cache_dir, filename)

    if not os.path.exists(cache_filename):
        sys.stderr.write(f'Downloading: "{url}" to {cache_filename}\n')
        hash_prefix = None
        if check_hash:
            r = HASH_REGEX.search(filename)  # r is Optional[Match[str]]
            hash_prefix = r.group(1) if r else None
        download_url_to_file(url, cache_filename, hash_prefix, progress=progress)

    return cache_filename

def upload_file(s3_client, bucket, local_filename, object_key, acl=None, verbose=True, profile='wasabi', expires_in_seconds=3600):        
    
    # try getting the remote file size and comparing to local
    # if remote not found (404), continue and upload the file
    try:
        s3_file_size = bucket.Object(object_key).content_length
        local_file_size = os.path.getsize(local_filename)
        if s3_file_size == local_file_size:
            object_url = auth.generate_url(s3_client, bucket.name, object_key, bucket_region=bucket.region, 
                                           profile=profile, expires_in_seconds=expires_in_seconds)
            if verbose: 
                print(f"The file '{object_key}' already exists in the S3 bucket '{bucket.name}' and has the same size. The file will not be re-uploaded.\n")
                print(object_url+"\n")
            return object_url
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            # The key does not exist.
            pass
        elif e.response['Error']['Code'] == 403:
            # Unauthorized, including invalid bucket
            raise e
        else:
          # Something else has gone wrong.
          raise e

    # Upload the file
    bucket.Object(object_key).put(Body=open(local_filename, 'rb'), ACL=acl)
    object_url = auth.generate_url(s3_client, bucket.name, object_key, bucket_region=bucket.region, 
                                   profile=profile, expires_in_seconds=expires_in_seconds)
    if verbose: 
        print(f"The file '{object_key}' has been uploaded to the S3 bucket '{bucket.name}'.\n")            
        print(object_url+"\n")
    
    return object_url  

def upload_buffer(s3_client, bucket, buf, object_key, acl=None, verbose=True, profile='wasabi', expires_in_seconds=3600):
    """
    Upload a buffer to an S3 bucket, comparing sizes to avoid redundant uploads.
    
    Parameters:
    - s3_client: The S3 client.
    - bucket: The S3 bucket object.
    - buf: The buffer containing the data to upload.
    - object_key: The key for the S3 object.
    - acl: The ACL for the uploaded object.
    - verbose: Whether to print verbose messages.
    - profile: The profile to use for generating the URL.
    - expires_in_seconds: The expiry time for the generated URL.
    
    Returns:
    - The URL of the uploaded object.
    """
    
    # Move the buffer to the beginning
    buf.seek(0)
    buffer_size = len(buf.getvalue())
    
    try:
        s3_file_size = bucket.Object(object_key).content_length
        if s3_file_size == buffer_size:
            object_url = auth.generate_url(s3_client, bucket.name, object_key, bucket_region=bucket.region, 
                                           profile=profile, expires_in_seconds=expires_in_seconds)
            if verbose: 
                print(f"The file '{object_key}' already exists in the S3 bucket '{bucket.name}' and has the same size. The file will not be re-uploaded.\n")
                print(object_url + "\n")
            return object_url
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            # The key does not exist.
            pass
        elif e.response['Error']['Code'] == 403:
            # Unauthorized, including invalid bucket
            raise e
        else:
            # Something else has gone wrong.
            raise e

    # Upload the buffer
    buf.seek(0)
    bucket.Object(object_key).put(Body=buf, ACL=acl)
    object_url = auth.generate_url(s3_client, bucket.name, object_key, bucket_region=bucket.region, 
                                   profile=profile, expires_in_seconds=expires_in_seconds)
    if verbose: 
        print(f"The file '{object_key}' has been uploaded to the S3 bucket '{bucket.name}'.\n")            
        print(object_url + "\n")
    
    return object_url    

def load_file(filename):
    local_filename = filename    
    
    assert os.path.isfile(local_filename), f"File not found: {local_filename}"

    if local_filename.endswith(".csv"):
        df = pd.read_csv(local_filename)
        return df
    elif local_filename.endswith(".json"):
        with open(local_filename, 'r') as file:
            data = json.load(file)
        return data
    elif local_filename.endswith(".txt"):
        with open(local_filename, 'r') as file:
            lines = file.readlines()

        # Print each line or process them as needed
        for line in lines:
            print(line.strip())  # .strip() removes leading/trailing whitespace including newlines
        return lines
    elif local_filename.endswith(".pth") or local_filename.endswith(".pt") or local_filename.endswith(".pth.tar"):
        data = torch.load(local_filename, map_location='cpu')
        return data
    else:
        raise ValueError(f'Filetype must be one of csv, json, txt, pth, pt, pth.tar, np, got {local_filename}')      

def list_objects(bucket, prefix='', depth=None, directory_filter=True, verbose=True):
        """
        List objects in an S3 bucket with optional depth and directory exclusion.

        Parameters:
        - prefix: The prefix (subfolder) to filter objects.
        - depth: The maximum depth of subfolders to include.
        - directory_filter: Filter to include everything (None), directories only (True), or files only (False).
        - verbose: Whether to print the object keys.
        """
        objects = []
        directories = set()
        
        # make sure prefix ends with /
        prefix = prefix.strip("/") + "/"
        
        # iterate over objects with this prefix
        for obj in bucket.objects.filter(Prefix=prefix):
            # Calculate the depth of the object's key relative to the prefix
            relative_key = obj.key[len(prefix):]
            key_depth = relative_key.count('/')
            
            # Track implicit directories 
            implicit_folders = relative_key.split("/")[0:key_depth]
            for d in range(0, key_depth):
                folder = prefix + "/".join(implicit_folders[0:d+1]) + "/"
                if folder not in obj.key: continue
                if depth is None or d==depth:
                    directories.add(folder)

            # Check if the key is directly within the specified depth
            if depth is None or key_depth == depth:
                # Directory filter logic
                if directory_filter is None:
                    pass  # Include everything
                elif directory_filter and not obj.key.endswith('/'):
                    continue  # Skip files if directory_filter is True
                elif not directory_filter and obj.key.endswith('/'):
                    continue  # Skip directories if directory_filter is False

                if verbose: 
                    print(obj.key)
                objects.append(obj.key)
        
        if directory_filter:
            return sorted(list(directories))
        return objects  