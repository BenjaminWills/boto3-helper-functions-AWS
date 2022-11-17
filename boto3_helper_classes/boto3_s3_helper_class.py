import logging

import boto3
from botocore.exceptions import ClientError



class S3:
    """
    A helper class for boto3 s3 operations.
    """
    def __init__(self,ACCESS_KEY_ID:str,SECRET_ACCESS_KEY:str, region_name:str) -> None:
        self.client = boto3.client(
                's3',
                aws_access_key_id = ACCESS_KEY_ID,
                aws_secret_access_key = SECRET_ACCESS_KEY,
                region_name = region_name
                )
        self.resource = boto3.resource(
                's3',
                aws_access_key_id = ACCESS_KEY_ID,
                aws_secret_access_key = SECRET_ACCESS_KEY,
                region_name = region_name
                )
    
    def create_bucket(self,bucket_name:str,region:str = None) -> bool:
        try:
            if region is None:
                s3_client = self.client
                s3_client.create_bucket(Bucket = bucket_name)
            else:
                s3_client = boto3.client('s3',region_name = region)
                location = {'LocationConstraint':region}
                s3_client.create_bucket(
                    Bucket = bucket_name,
                    CreateBucketConfiguration = location
                )
        except ClientError as e:
            logging.error(e)
            return False
        return True
    
    def delete_bucket(self,bucket_name:str) -> bool:
        try:
            s3_client = self.client
            objects = s3_client.list_objects_v2(Bucket=bucket_name)
            file_count = objects['KeyCount']
            if file_count == 0:
                s3_client.delete_bucket(Bucket=bucket_name)
            else:
                return False
        except ClientError as e:
            logging.error(e)
            return False
        return True

    def add_item_to_bucket(self,file_path:str,bucket_name:str,file_name:str)->bool:
        try:
            s3_client = self.client
            bucket = s3_client.Bucket(bucket_name)
            bucket.upload_file(file_path,file_name)
        except ClientError as e:
            logging.error(e)
            return False
        return True

    def list_bucket_contents(self,bucket_name:str) -> list:
        try:
            s3_resource = self.resource
            bucket = s3_resource.Bucket(bucket_name)
            contents = [
                file.key 
                for file in bucket.objects.all()
                ]
        except ClientError as e:
            logging.error(e)
            return False
        return contents
    
    def delete_bucket_item(self,bucket_name:str,file_name:str) -> bool:
        try:
            s3_resource = self.resource
            target = s3_resource.Object(bucket_name,file_name)
            target.delete()
        except ClientError as e:
            logging.error(e)
            return False
        return True
    
    def list_s3_buckets(self,verbose:bool = True) -> list:
        try:
            s3 = self.client
            buckets = s3.list_buckets()['Buckets']
            if verbose:
                print('-'*60)
                print('Bucket names:')
                for index,bucket in enumerate(buckets):
                    print(f'{index+1} : ',bucket['Name'])
                print('-'*60)
            return buckets
        except ClientError as e:
            logging.error(e)
            return False