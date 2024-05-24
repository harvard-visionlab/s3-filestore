# s3-logger
Code for logging experiment results in an s3 bucket

## Installation

```bash
pip install git+https://github.com/harvard-visionlab/s3-logger.git
```

## Basic Usage

The main S3Logger class supports basic file upload/download and find operations.

**init logger:**
```
from s3_logger import S3Logger

s3_logger = S3Logger('path/to/data')
s3_logger
```
