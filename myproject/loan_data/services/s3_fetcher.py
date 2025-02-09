import boto3
import os
import pandas as pd
import io

class S3Fetcher:
    def __init__(self):
        """Initialize S3 client with AWS credentials."""
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
            region_name=os.getenv("AWS_REGION")
        )
        self.bucket_name = os.getenv("AWS_S3_BUCKET")

    def list_files(self, prefix=""):
        """List all files in the S3 bucket with an optional prefix (folder)."""
        response = self.s3_client.list_objects_v2(Bucket=self.bucket_name, Prefix=prefix)
        return [file['Key'] for file in response.get('Contents', [])]

    def download_file(self, s3_key, local_path):
        """Download a file from S3 to a local path."""
        self.s3_client.download_file(self.bucket_name, s3_key, local_path)
        print(f"Downloaded {s3_key} to {local_path}")

    def read_csv_from_s3(self, s3_key):
        """Read a CSV file directly from S3 as a Pandas DataFrame."""
        obj = self.s3_client.get_object(Bucket=self.bucket_name, Key=s3_key)
        df = pd.read_csv(io.BytesIO(obj['Body'].read()))
        return df
