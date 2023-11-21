import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError

def upload_to_s3(local_file_path, bucket_name, s3_file_name):
    try:
        # Create a Boto3 S3 client
        s3 = boto3.client('s3', region_name='us-east-1')

        # Upload the local file to S3 bucket
        s3.upload_file(local_file_path, bucket_name, s3_file_name)

        print(f'Successfully uploaded {local_file_path} to {bucket_name}/{s3_file_name}')

    except FileNotFoundError:
        print(f'The file {local_file_path} was not found.')
    except NoCredentialsError:
        print('Credentials not available')
    except PartialCredentialsError:
        print('Partial credentials provided')
    except Exception as e:
        print(f'Error: {str(e)}')

# Example usage
local_file_path = 'C:/Users/rolls/Downloads/streaming_viewership_data.csv'  # Path to your local file
bucket_name = 'ottstreamrawdata'        # Name of your S3 bucket
s3_file_name = 'tobeprocessdata/streaming_viewership_data.csv'          # Desired name for the file in S3 bucket

upload_to_s3(local_file_path, bucket_name, s3_file_name)