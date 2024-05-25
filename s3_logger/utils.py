
from . import auth

def get_url(bucket_name, object_name, bucket_region=None, profile='wasabi'):
    domain = 'wasabisys.com' if 'wasabi' in profile else 'amazonaws.com'

    if bucket_region is None:
        bucket_region = auth.get_bucket_location(bucket_name, profile=profile)

    # Construct the object URL
    object_url = f"https://s3.{bucket_region}.{domain}/{bucket_name}/{object_name}"
    
    return object_url