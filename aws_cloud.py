import boto3
from botocore.exceptions import NoCredentialsError

def connect_to_s3(access_key, secret_key, region):
    """
    Connect to an AWS S3 bucket using the provided credentials and region.

    Parameters:
        - access_key (str): AWS access key ID.
        - secret_key (str): AWS secret access key.
        - region (str): AWS region.

    Returns:
        - boto3.resource: S3 resource object.
    """
    try:
        s3 = boto3.resource(
            's3',
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name=region
        )
        return s3
    except NoCredentialsError:
        print("Credentials not available or incorrect.")
        return None

def upload_file_to_s3(bucket_name, file_path, object_key, access_key, secret_key, region):
    """
    Upload a file to an AWS S3 bucket.

    Parameters:
        - bucket_name (str): S3 bucket name.
        - file_path (str): Local path to the file to be uploaded.
        - object_key (str): Key to assign to the uploaded object in the bucket.
        - access_key (str): AWS access key ID.
        - secret_key (str): AWS secret access key.
        - region (str): AWS region.

    Returns:
        - bool: True if the file is uploaded successfully, False otherwise.
    """
    s3 = connect_to_s3(access_key, secret_key, region)
    
    if s3 is not None:
        try:
            s3.Bucket(bucket_name).upload_file(file_path, object_key)
            print(f"File '{file_path}' uploaded to '{bucket_name}' with key '{object_key}'.")
            return True
        except FileNotFoundError:
            print(f"File '{file_path}' not found.")
        except NoCredentialsError:
            print("Credentials not available or incorrect.")
    
    return False

# Example usage:
access_key = "your_access_key"
secret_key = "your_secret_key"
region = "your_region"
bucket_name = "your_bucket_name"
file_path = "path/to/your/file.txt"
object_key = "your_object_key"

upload_file_to_s3(bucket_name, file_path, object_key, access_key, secret_key, region)
