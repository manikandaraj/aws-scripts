import argparse
import boto3
import os
import configparser
from botocore.exceptions import ClientError

def verify_paths(folder_path):
    if folder_path and not os.path.isdir(folder_path):
        raise NotADirectoryError(f"'{folder_path}' is not a valid directory")

def read_aws_credentials(aws_config):
    config = configparser.ConfigParser()
    config.read(aws_config)
    try:
        access_key = config['aws']['access_key']
        secret_key = config['aws']['secret_key']
        return access_key, secret_key
    except KeyError:
        raise ValueError("AWS credentials not found in the specified config file")

def validate_credentials(access_key, secret_key):
    try:
        boto3.client('sts', aws_access_key_id=access_key, aws_secret_access_key=secret_key).get_caller_identity()
        print("AWS credentials are valid.")
    except ClientError as e:
        raise ValueError(f"Invalid AWS credentials: {e}")

def download_from_s3_recursively(s3_client, bucket_name, bucket_path, local_path):
    try:
        # List objects in the S3 bucket under the given prefix
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=bucket_path)

        # Check if 'Contents' key is present in the response
        if 'Contents' in response:
            for object_summary in response['Contents']:
                key = object_summary['Key']
                # Construct local file path by stripping bucket_path
                relative_key = key[len(bucket_path):] if key.startswith(bucket_path) else key
                local_file_path = os.path.join(local_path, relative_key)
                
                # Create directory if it doesn't exist
                os.makedirs(os.path.dirname(local_file_path), exist_ok=True)
                
                # Download file from S3
                s3_client.download_file(bucket_name, key, local_file_path)
                print(f"Downloaded file: s3://{bucket_name}/{key} to {local_file_path}")

        # Recursively download files from subdirectories
        for prefix in response.get('CommonPrefixes', []):
            subdir = prefix['Prefix']
            download_from_s3_recursively(s3_client, bucket_name, subdir, os.path.join(local_path, os.path.basename(subdir)))

    except Exception as e:
        print(f"An unexpected error occurred in download_from_s3_recursively :{e}:")

def download_from_s3(aws_region, bucket_name, bucket_path, local_path, access_key=None, secret_key=None):
    try:
        # Create S3 client
        if access_key and secret_key:
            s3_client = boto3.client('s3', region_name=aws_region, aws_access_key_id=access_key, aws_secret_access_key=secret_key)
        else:
            s3_client = boto3.client('s3', region_name=aws_region)

        if not os.path.exists(local_path):
            os.makedirs(local_path)

        # If bucket_path is empty, download all files in the bucket
        if not bucket_path or bucket_path == '/':
            bucket_path = ''

        # Download files recursively
        download_from_s3_recursively(s3_client, bucket_name, bucket_path, local_path)
    except Exception as e:
        print(f"An unexpected error occurred in download_files_from_s3 :{e}:")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Download file or folder from S3")
    parser.add_argument("--aws-region", help="AWS region", required=True)
    parser.add_argument("--bucket-name", help="S3 bucket name", required=True)
    parser.add_argument("--bucket-path", help="S3 bucket path", required=True)
    parser.add_argument("--local-path", help="Local path to save files", required=True)
    parser.add_argument("--aws-config", help="AWS credentials config file")
    args = parser.parse_args()

    try:
        if args.local_path:
            verify_paths(args.local_path)
            if args.aws_config:
                access_key, secret_key = read_aws_credentials(args.aws_config)
                validate_credentials(access_key, secret_key)
                download_from_s3(args.aws_region, args.bucket_name, args.bucket_path, args.local_path, access_key, secret_key)
            else:
                download_from_s3(args.aws_region, args.bucket_name, args.bucket_path, args.local_path)
    except Exception as e:
        print(f"Error in main :{e}:")

