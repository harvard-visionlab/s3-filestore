import os
import boto3

WASABI_ENDPOINT = 'https://s3.wasabisys.com'
AWS_ENDPOINT = 'https://s3.amazonaws.com'

def get_session_with_profile(profile_name):
    session = boto3.Session(profile_name=profile_name)
    return session

def get_credentials(profile_name):

    # if no profile_name, check environ for credentials
    if profile_name is None:
        return dict(
            S3_ACCESS_KEY=os.environ['S3_ACCESS_KEY'],
            S3_SECRET_KEY=os.environ['S3_SECRET_KEY'],
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
        S3_ACCESS_KEY=current_credentials.access_key,
        S3_SECRET_KEY=current_credentials.secret_key,
        S3_REGION=region,
        S3_ENDPOINT_URL=endpoint_url
    )

def get_userdata(profile='wasabi'):
    try:
        from google.colab import userdata
    except ImportError:
        userdata = get_credentials(profile)
    
    return userdata

def get_session_with_userdata(profile, region_name=None):
    userdata = get_userdata(profile=profile)
    session = boto3.Session(aws_access_key_id=userdata.get('S3_ACCESS_KEY'),
                            aws_secret_access_key=userdata.get('S3_SECRET_KEY'),
                            region_name=region_name)
    return session

def get_bucket_region(session, bucket_name, endpoint_url):
    s3_client = session.client('s3', endpoint_url=endpoint_url)
    bucket_location = s3_client.get_bucket_location(Bucket=bucket_name)['LocationConstraint']

    return bucket_location 

def get_bucket_location(bucket_name, profile='wasabi'):
    s3_client = get_client_with_userdata(profile)
    bucket_location = s3_client.get_bucket_location(Bucket=bucket_name)['LocationConstraint']
    return bucket_location 

def get_client_with_userdata(profile='wasabi'):
    userdata = get_userdata(profile=profile)
    s3_client = boto3.client('s3', 
                             aws_access_key_id=userdata.get('S3_ACCESS_KEY'),
                             aws_secret_access_key=userdata.get('S3_SECRET_KEY'),
                             endpoint_url=userdata.get('S3_ENDPOINT_URL'))

    return s3_client

