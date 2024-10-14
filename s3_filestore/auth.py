import os
import boto3

from .utils import is_url_public_readable, parse_s3_url

WASABI_ENDPOINT = 'https://s3.wasabisys.com'
AWS_ENDPOINT = 'https://s3.amazonaws.com'

def get_session_with_profile(profile_name):
    session = boto3.Session(profile_name=profile_name)
    return session

def get_credentials(profile_name):

    # if no profile_name, check environ for credentials
    if profile_name is None:
        return dict(
            S3_ACCESS_KEY_ID=os.environ['S3_ACCESS_KEY_ID'],
            S3_SECRET_ACCESS_KEY=os.environ['S3_SECRET_ACCESS_KEY'],
            S3_REGION=os.environ['S3_REGION'],
            S3_ENDPOINT_URL=os.environ['S3_ENDPOINT_URL']
        )

    # otherwise, use the profile to get credentials
    session = get_session_with_profile(profile_name)
    credentials = session.get_credentials()
    current_credentials = credentials.get_frozen_credentials()
    config = session._session.get_scoped_config()
    region = config.get('region', 'us-east-1')
    endpoint_url = config.get('s3api', {}).get('endpoint_url', WASABI_ENDPOINT if 'wasabi' in profile_name else AWS_ENDPOINT)

    return dict(
        S3_ACCESS_KEY_ID=current_credentials.access_key,
        S3_SECRET_ACCESS_KEY=current_credentials.secret_key,
        S3_REGION=region,
        S3_ENDPOINT_URL=endpoint_url
    )

def get_userdata(profile=os.environ.get('S3_PROFILE', None)):
    try:
        from google.colab import userdata
    except ImportError:
        userdata = get_credentials(profile)
    
    return userdata

def get_session_with_userdata(profile, region_name=None):
    userdata = get_userdata(profile=profile)
    session = boto3.Session(aws_access_key_id=userdata.get('S3_ACCESS_KEY_ID'),
                            aws_secret_access_key=userdata.get('S3_SECRET_ACCESS_KEY'),
                            region_name=region_name)
    return session

def get_bucket_region(session, bucket_name, endpoint_url):
    s3_client = session.client('s3', endpoint_url=endpoint_url)
    bucket_location = s3_client.get_bucket_location(Bucket=bucket_name)['LocationConstraint']

    return bucket_location 

def get_bucket_location(bucket_name, profile=os.environ.get('S3_PROFILE', None)):
    s3_client = get_client_with_userdata(profile)
    bucket_location = s3_client.get_bucket_location(Bucket=bucket_name)['LocationConstraint']
    return bucket_location 

def get_client_with_userdata(profile=os.environ.get('S3_PROFILE', None)):
    userdata = get_userdata(profile=profile)
    s3_client = boto3.client('s3', 
                             aws_access_key_id=userdata.get('S3_ACCESS_KEY_ID'),
                             aws_secret_access_key=userdata.get('S3_SECRET_ACCESS_KEY'),
                             endpoint_url=userdata.get('S3_ENDPOINT_URL'))

    return s3_client

def get_public_s3_object_url(bucket_name, object_name):
    s3_client = boto3.client('s3')
    response = s3_client.generate_presigned_url('get_object',
                                                Params={'Bucket': bucket_name,
                                                        'Key': object_name},
                                                ExpiresIn=0,
                                                HttpMethod='GET')

    return response    

def generate_url(s3_client, bucket_name, bucket_key, bucket_region=None, profile=os.environ.get('S3_PROFILE', None), expires_in_seconds=3600):
    if is_object_private(s3_client, bucket_name, bucket_key):        
        url = generate_presigned_url(s3_client, bucket_name, bucket_key, expires_in_seconds=expires_in_seconds)
    else:
        url = get_url(bucket_name, bucket_key, bucket_region=bucket_region, profile=profile)
    
    return url

def get_url(bucket_name, object_name, bucket_region=None, profile=os.environ.get('S3_PROFILE', None)):
    domain = 'wasabisys.com' if 'wasabi' in profile else 'amazonaws.com'

    if bucket_region is None:
        bucket_region = get_bucket_location(bucket_name, profile=profile)

    # Construct the object URL
    if bucket_region is None:
        object_url = f"https://s3.{domain}/{bucket_name}/{object_name}"
    else:
        object_url = f"https://s3.{bucket_region}.{domain}/{bucket_name}/{object_name}"
    
    return object_url

def generate_presigned_url(s3_client, bucket_name, object_name, expires_in_seconds=3600):
    signed_url = s3_client.generate_presigned_url('get_object', Params={'Bucket': bucket_name,
                                                  'Key': object_name},
                                                   ExpiresIn=expires_in_seconds,
                                                   HttpMethod='GET')
    return signed_url  

def is_object_private(s3_client, bucket_name, object_key):
    is_public = is_object_public(s3_client, bucket_name, object_key)
    return is_public==False

def is_object_public(s3_client, bucket_name, object_key):
    try:
        # Get the ACL of the object
        acl = s3_client.get_object_acl(Bucket=bucket_name, Key=object_key)
        # Check if the ACL grants public read access
        for grant in acl['Grants']:
            grantee = grant.get('Grantee', {})
            permission = grant.get('Permission')
            if grantee.get('URI') == 'http://acs.amazonaws.com/groups/global/AllUsers' and permission == 'READ':
                return True
        
        return False
    
    except Exception as e:
        print(f"Error getting ACL for {object_key} in {bucket_name}: {e}")
        return False   
    
def sign_url_if_needed(url, expires_in_seconds=3600, profile=os.environ.get('S3_PROFILE', None)):
    if is_url_public_readable(url):
        return url
        
    bucket_name, object_name, _, _ = parse_s3_url(url)
    s3_client = get_client_with_userdata(profile=profile)
    response = s3_client.generate_presigned_url('get_object',
                                                Params={'Bucket': bucket_name,
                                                        'Key': object_name},
                                                ExpiresIn=expires_in_seconds,
                                                HttpMethod='GET')

    return response     

