import requests

def update_object_acl(s3_client, bucket_name, object_key, acl, verbose=True):
    """
    Update the ACL of an S3 object.
    
    Parameters:
    - bucket: The s3 bucket connection.
    - object_key: The key of the S3 object.
    - acl: The ACL to set (e.g., 'private', 'public-read', 'public-read-write').
    """

    try:
        # Update the object's ACL
        response = s3_client.put_object_acl(Bucket=bucket_name, Key=object_key, ACL=acl)
        if verbose: print(f"Successfully updated ACL for {object_key} to {acl}.")
        return response
    except Exception as e:
        print(f"Error updating ACL for {object_key} in {bucket_name}: {e}")
        
def get_s3_object_metadata(s3_client, bucket_name, object_key, key=None):
    """
    Get Metadata from s3 object using s3_client
    
    """
    try:
        # Get the object metadata
        response = s3_client.head_object(Bucket=bucket_name, Key=object_key)
        
        # return the metadata
        if key is not None:
            return response['Metadata'].get(key.lower(), None)

        return response['Metadata']
    except s3_client.exceptions.NoSuchKey:
        print(f"Object {object_key} does not exist in bucket {bucket_name}.")
        return None
    except Exception as e:
        print(f"An error occurred: {e}")
        return None 
    
def get_s3_url_metadata(url, key=None):
    """
    Get Metadata from s3 object using https:// url
    
    """    
    try:
        # Send a HEAD request to the S3 object URL
        response = requests.head(url)
        response.raise_for_status()  # Raise an HTTPError for bad responses

        # Extract the 'x-amz-meta-' header if it exists
        if key is not None:
            metadata = response.headers.get(f'x-amz-meta-{key.lower()}', None)
        else:
            metadata = {k.replace('x-amz-meta-',''):v for k,v in response.headers.items() 
                        if k.startswith('x-amz-meta-') }
        
        return metadata
    except requests.exceptions.RequestException as e:
        print(f"An error occurred: {e}")
        return None        