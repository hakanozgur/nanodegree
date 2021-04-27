# Commands
## Data
- original source -> http://millionsongdataset.com/
- udacity version from s3
  - aws cli
  - aws --version
  - aws configure (access key, secret, region & output needed) 
  - aws s3 cp --recursive s3://udacity-dend/song_data song_data
  - aws s3 cp --recursive s3://udacity-dend/log_data log_data
  - aws s3 cp s3://udacity-dend/log_json_path.json . 

## Localstack
- https://github.com/localstack/localstack
- pip install localstack
- pip install awscli-local
- https://getcommandeer.com/
- docker-compose up -d
- http://localhost:4566/health
- awslocal s3 mb s3://udacity-dend/
- awslocal s3 cp ..\data\log_json_path.json s3://udacity-dend
- awslocal s3 ls s3://udacity-dend
- awslocal s3 sync ..\data\ s3://udacity-dend/


## copy
- aws s3 cp --recursive s3://udacity-dend/song_data s3://udacity-dend-hkn/song_data