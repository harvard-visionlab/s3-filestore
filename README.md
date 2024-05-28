# s3-filestore
A Python package for uploading/downloading research outputs to an s3 bucket

Part of the harvard-visionlab-stability-through-agility-code-for-science-initiative (HVSTACSI, pronounced "Ha-va-stacks-see"), which is definitely a real thing.

The goal is to easily store research outputs (model weights, .csv files, .json files, figures, etc.) in an s3 bucket (with either public-read or private access), and to just as easily download and access those files.

For example:
```
from s3_filestore import S3FileStore

# on your current workstation (laptop, cluster, anywhere)
s3 = S3FileStore('visionlab-results') # connect to an s3 bucket to which you have write access
url = s3.upload_file('results/output-file.csv',
                     bucket_subfolder='alvarez/Projects/testing1234') 

# on the same workstation or any other workstation
s3 = S3FileStore('visionlab-results')
df = s3.load_file(url) # csv files automatically loaded as pandas dataframe

# If you forgot what files you uploaded or their urls
urls = s3.list_s3_urls('alvarez/Projects/testing1234')
print(urls) 
df = s3.load_file(urls[0])
```

## Installation

boto3 will be installed with this package, but additional dependencies (torch, torchvision, numpy, pandas) are not automatically installed so that you can install s3-logger without fear of mucking up your environment.

```bash
pip install git+https://github.com/harvard-visionlab/s3-filestore.git
```

## Basic Usage

The main S3FileStore class supports basic file upload/download and find operations.

**init logger:**
```
from s3_filestore import S3FileStore

s3_logger = S3FileStore('visionlab-results')
s3_logger
```

## TODO
- [ ] add detailed walkthrough
- [ ] add demo colab notebook
- [ ] add sync method
- [ ] rename "load_file" and "upload_data" to "read" and "write"?
