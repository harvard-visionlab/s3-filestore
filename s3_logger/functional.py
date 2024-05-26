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

from .utils import get_url

HASH_REGEX = re.compile(r'-([a-f0-9]*)\.')
CACHE_DIR = torch.hub.get_dir().replace("/hub", "/results")

def download_object(bucket_name, bucket_key, profile, bucket_region=None, cache_dir=None, progress=True, check_hash=True):
    if cache_dir is None: cache_dir = CACHE_DIR
    url = get_url(bucket_name, bucket_key, bucket_region=bucket_region, profile=profile)
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

def upload_file(s3, bucket, local_filename, object_key, acl=None, verbose=True):    

    object_url = get_url(bucket.name, object_key, bucket_region=bucket.region)
    
    try:
        s3_file_size = bucket.Object(object_key).content_length
        local_file_size = os.path.getsize(local_filename)
        if s3_file_size == local_file_size:
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
    s3.Object(bucket.name, object_key).put(Body=open(local_filename, 'rb'), ACL=acl)
    if verbose: 
        print(f"The file '{object_key}' has been uploaded to the S3 bucket '{bucket.name}'.\n")            
        print(object_url+"\n")
    
    return object_url  

def load_file(filename):
    local_filename = filename
    if filename.startswith("https://"):
        local_filename = download_if_needed(filename)
    elif filename.startswith("s3://"):
        local_filename = download_if_needed(filename)
    
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
    elif local_filename.endswith(".pth") or local_filename.endswith(".pt") or local_filename.endswith(".pth.tar") or local_filename.endswith(".np"):
        data = torch.load(local_filename, map_location='cpu')
        return data
    else:
        raise ValueError(f'Filetype must be one of csv, json, txt, pth, pt, pth.tar, np, got {local_filename}')      

