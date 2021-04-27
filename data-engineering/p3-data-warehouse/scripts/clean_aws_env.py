import pandas as pd
import boto3
import json
import configparser
import boto3
from botocore.exceptions import ClientError


# Configs from `dwh.cfg` file
config = configparser.ConfigParser()
config.read_file(open('dwh.cfg'))

KEY = config.get('AWS','KEY')
SECRET = config.get('AWS','SECRET')
DWH_CLUSTER_TYPE = config.get("DWH","DWH_CLUSTER_TYPE")
DWH_NUM_NODES = config.get("DWH","DWH_NUM_NODES")
DWH_NODE_TYPE = config.get("DWH","DWH_NODE_TYPE")
DWH_CLUSTER_IDENTIFIER = config.get("DWH","DWH_CLUSTER_IDENTIFIER")
DWH_DB = config.get("DWH","DWH_DB")
DWH_DB_USER = config.get("DWH","DWH_DB_USER")
DWH_DB_PASSWORD = config.get("DWH","DWH_DB_PASSWORD")
DWH_PORT = config.get("DWH","DWH_PORT")
DWH_IAM_ROLE_NAME = config.get("DWH", "DWH_IAM_ROLE_NAME")

# same as `aws configure get region`
default_region = 'us-west-2'


redshift = boto3.client(
    'redshift',
    region_name=default_region,
    aws_access_key_id=KEY,
    aws_secret_access_key=SECRET
    )

iam = boto3.client(
    'iam',
    region_name=default_region,
    aws_access_key_id=KEY,
    aws_secret_access_key=SECRET
    )

redshift.delete_cluster( ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,  SkipFinalClusterSnapshot=True)


def prettyRedshiftProps(props):
    pd.set_option('display.max_colwidth', None)
    keysToShow = ["ClusterIdentifier", "NodeType", "ClusterStatus", "MasterUsername", "DBName", "Endpoint", "NumberOfNodes", 'VpcId']
    x = [(k, v) for k,v in props.items() if k in keysToShow]
    return pd.DataFrame(data=x, columns=["Key", "Value"])



myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
prettyRedshiftProps(myClusterProps)

# Remove iam role
try:
    iam.detach_role_policy(RoleName=DWH_IAM_ROLE_NAME, PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")
    response = iam.delete_role(RoleName=DWH_IAM_ROLE_NAME)
    if response['ResponseMetadata']['HTTPStatusCode'] == 200:
        print(f'{DWH_IAM_ROLE_NAME} role deleted')
    else:
        print('Error: role is not deleted')
except Exception as e:
    print(e)