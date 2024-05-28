import io
import hashlib
import pandas as pd
import json
import torch
import numpy as np

def contains_numpy_or_torch(data):
    """Recursively check if a dictionary contains any numpy arrays or torch tensors."""
    if isinstance(data, dict):
        for value in data.values():
            if isinstance(value, (np.ndarray, torch.Tensor)):
                return True
            if isinstance(value, dict):
                if contains_numpy_or_torch(value):
                    return True
            if isinstance(value, list):
                if any(isinstance(item, (np.ndarray, torch.Tensor)) for item in value):
                    return True
                if any(isinstance(item, dict) and contains_numpy_or_torch(item) for item in value):
                    return True
    return False

def prepare_data_for_upload(data, hash_length, data_format=None):
    """
    Prepare data for upload to S3 without writing to a file.
    
    Parameters:
    - data: The data to be uploaded. Can be a DataFrame, dict, or PyTorch tensor.
    - data_format: The format of the data ('.csv' for DataFrames, '.json' for dicts, '.pth' for PyTorch tensors).
    
    Returns:
    - buffer: The buffer containing the data ready for upload.
    - sha256sum: The SHA-256 checksum of the data.
    """
    # Determine the data format if not provided
    if data_format is None:
        if isinstance(data, pd.DataFrame):
            data_format = '.csv'
        elif isinstance(data, dict):
            if contains_numpy_or_torch(data):
                data_format = '.pth'
            else:
                data_format = '.json'
        else:
            raise ValueError(f"Unsupported data type: {type(data)}")

    buffer = io.BytesIO()
    sha256 = hashlib.sha256()

    if data_format == '.csv' and isinstance(data, pd.DataFrame):
        data.to_csv(buffer, index=False)
    elif data_format == '.json' and isinstance(data, dict):
        buffer.write(json.dumps(data).encode('utf-8'))
    elif data_format == '.pth' and isinstance(data, dict):
        torch.save(data, buffer)
    else:
        raise ValueError(f"Unsupported data format: {data_format} or data type: {type(data)}")
    
    # Compute SHA-256 checksum
    buffer.seek(0)
    sha256.update(buffer.read())
    readable_hash = sha256.hexdigest()

    if isinstance(hash_length, (int)):
        readable_hash = readable_hash[0:hash_length] 

    # Reset buffer position to the beginning
    buffer.seek(0)
    
    return buffer, readable_hash, data_format