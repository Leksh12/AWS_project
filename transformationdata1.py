import pandas as pd
import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError

def save_to_csv_s3(df, s3_bucket, s3_key):
    """
    Save DataFrame to CSV and upload to S3.

    Args:
    - df (DataFrame): Data to save.
    - s3_bucket (str): S3 bucket name.
    - s3_key (str): S3 object key (file path within the bucket).
    """
    # Save DataFrame to a local CSV file
    local_filename = "transformed_data.csv"
    df.to_csv(local_filename, index=False)

    # Upload the local file to S3 bucket
    try:
        s3 = boto3.client('s3')
        s3.upload_file(local_filename, s3_bucket, s3_key)
        print(f"Data saved and uploaded to S3: s3://{s3_bucket}/{s3_key}")
    except FileNotFoundError:
        print(f'The file {local_filename} was not found.')
    except NoCredentialsError:
        print('Credentials not available')
    except PartialCredentialsError:
        print('Partial credentials provided')
    except Exception as e:
        print(f'Error: {str(e)}')
def map_ratings(rating):
    if rating == 1:
        return 'Poor'
    elif rating == 2:
        return 'Below Average'
    elif rating == 3:
        return 'Average'
    elif rating == 4:
        return 'Good'
    elif rating == 5:
        return 'Excellent'
    else:
        return 'Unknown'

df = pd.read_csv("C:/Users/rolls/Downloads/streaming_viewership_data.csv")
age_bins = [0, 12, 19, 29, 59, float('inf')]
age_labels = ['Childhood', 'Teenagers', 'Young Adults', 'Adults', 'Seniors']

# Add a new column 'Age_Group' based on the specified age bins
df['Age_Group'] = pd.cut(df['Age'], bins=age_bins, labels=age_labels, right=False)

df['Rating_Description'] = df['Ratings'].apply(map_ratings)

s3_bucket_name = 'ottstreamrawdata'
s3_object_key = 'transformeddata/streaming_viewership_transform_data.csv'

save_to_csv_s3(df, s3_bucket_name, s3_object_key)

