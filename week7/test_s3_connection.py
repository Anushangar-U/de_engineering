import boto3
import os
from dotenv import load_dotenv

# Load credentials from .env file
load_dotenv()

# Create an S3 client
s3 = boto3.client(
    's3',
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
    region_name='eu-north-1'
)

# List all buckets in your account
response = s3.list_buckets()

print("Your S3 buckets:")
for bucket in response['Buckets']:
    print(f"  - {bucket['Name']}")