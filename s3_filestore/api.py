
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