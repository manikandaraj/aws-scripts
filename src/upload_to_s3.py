import argparse
import boto3
import os
import configparser
from botocore.exceptions import ClientError

def verify_paths(file_paths, folder_path):
    if(file_paths):
        for file_path in file_paths:
            if not os.path.isfile(file_path):
                raise FileNotFoundError(f"'{file_path}' is not a valid file path")

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

def upload_file_to_s3(aws_region, bucket_name, bucket_path, file_paths, access_key=None, secret_key=None):
    try:
        # Create S3 client
        if access_key and secret_key:
            s3_client = boto3.client('s3', region_name=aws_region, aws_access_key_id=access_key, aws_secret_access_key=secret_key)
        else:
            s3_client = boto3.client('s3', region_name=aws_region)

        for file_path in file_paths:
            s3_key = os.path.join(bucket_path, os.path.basename(file_path))
            s3_client.upload_file(file_path, bucket_name, s3_key)
            print(f"Uploaded file: {file_path} to s3://{bucket_name}/{s3_key}")
    except ClientError as e:
        print(f"Error uploading file to S3: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

def upload_folder_to_s3(aws_region, bucket_name, bucket_path, folder_path, access_key=None, secret_key=None):
    try:
        # Create S3 client
        if access_key and secret_key:
            s3_client = boto3.client('s3', region_name=aws_region, aws_access_key_id=access_key, aws_secret_access_key=secret_key)
        else:
            s3_client = boto3.client('s3', region_name=aws_region)
        
        # Iterate through all files and subdirectories in the given folder
        for root, dirs, files in os.walk(folder_path):
            for file_name in files:
                print(type(file_name))
                local_file_path = os.path.join(root, file_name)
                s3_key = os.path.join(bucket_path, os.path.relpath(local_file_path, folder_path))
                
                # Upload file to S3
                s3_client.upload_file(local_file_path, bucket_name, s3_key)
                print(f"Uploaded file: {local_file_path} to s3://{bucket_name}/{s3_key}")
                
        print("Folder uploaded successfully to S3.")
    except ClientError as e:
        print(f"Error uploading folder to S3: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Upload file or folder to S3")
    parser.add_argument("--aws-region", help="AWS region", required=True)
    parser.add_argument("--bucket-name", help="S3 bucket name", required=True)
    parser.add_argument("--bucket-path", help="S3 bucket path", required=True)
    parser.add_argument("--aws-config", help="AWS credentials config file")
    parser.add_argument("--file-path", help="Local file path", nargs='+')
    parser.add_argument("--folder-path", help="Local folder path")
    args = parser.parse_args()

    try:
        if not args.file_path and not args.folder_path:
            parser.error("Please provide either --file-path or --folder-path")

        if args.file_path:
            verify_paths(args.file_path, None)
            if args.aws_config:
                access_key, secret_key = read_aws_credentials(args.aws_config)
                validate_credentials(access_key, secret_key)
                upload_file_to_s3(args.aws_region, args.bucket_name, args.bucket_path, args.file_path, access_key, secret_key)
            else:
                upload_file_to_s3(args.aws_region, args.bucket_name, args.bucket_path, args.file_path)

        if args.folder_path:
            verify_paths(None, args.folder_path)
            if args.aws_config:
                access_key, secret_key = read_aws_credentials(args.aws_config)
                validate_credentials(access_key, secret_key)
                upload_folder_to_s3(args.aws_region, args.bucket_name, args.bucket_path, args.folder_path, access_key, secret_key)
            else:
                upload_folder_to_s3(args.aws_region, args.bucket_name, args.bucket_path, args.folder_path)
    except Exception as e:
        print(f"Error in main: {e}")

