# ETL Unit Test Setup

## Getting Started

This repository contains strucrure to setup ETL Unit Test environment
To setup the project you can follow the following steps

### Prerequisites

To install the pre-requisites, install the requirements.

* python 3.7+
* requirements
  ```sh
  pip install -r requirements.txt
  ```

### Unit Testing

1. Start LocalStack Docker Container
```commandline
docker compose up
```
2. Run Test
```commandline
pytest test/
```
3. List Bucket
```commandline
aws --endpoint-url=http://localhost:4566 s3api list-buckets
```
4. List Objects
```commandline
aws --endpoint-url=http://localhost:4566 s3api list-objects --bucket raw
```
