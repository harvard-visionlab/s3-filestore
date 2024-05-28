import os
import boto3
import hashlib

from pathlib import Path
from urllib.parse import urlparse

def parse_s3_url(s3_url):
    parsed_url = urlparse(s3_url)
    
    # Remove leading '/' from the path to get the bucket key
    bucket_key = parsed_url.path.lstrip('/')
    
    return parsed_url.netloc, bucket_key
    
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
    
  