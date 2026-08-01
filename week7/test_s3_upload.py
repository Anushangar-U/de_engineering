import boto3
import os
from dotenv import load_dotenv

load_dotenv()

s3 = boto3.client(
    's3',
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
    region_name='eu-north-1'
)

bucket_name = 'anushangar-crypto-pipeline-raw'

# Upload a small test file
s3.put_object(
    Bucket=bucket_name,
    Key='test/hello.txt',
    Body='Hello from my crypto pipeline project'
)

print("File uploaded successfully")

# List what's in the bucket now, to confirm
response = s3.list_objects_v2(Bucket=bucket_name)
print("\nContents of bucket:")
for obj in response.get('Contents', []):
    print(f"  - {obj['Key']}")