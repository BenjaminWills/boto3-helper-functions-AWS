import logging

import boto3
from botocore.exceptions import ClientError


class EC2:
    """
    A helper class for using EC2 instances with boto3
    """
    def __init__(self,ACCESS_KEY_ID:str,SECRET_ACCESS_KEY:str,IAM_USER:str) -> None:
        self.client = boto3.client(
                'ec2',
                aws_access_key_id = ACCESS_KEY_ID,
                aws_secret_access_key = SECRET_ACCESS_KEY,
                )
        self.resource = boto3.resource(
                'ec2',
                aws_access_key_id = ACCESS_KEY_ID,
                aws_secret_access_key = SECRET_ACCESS_KEY,
                )
        self.iam_user = IAM_USER
    
    def create_ec2_instance(
        self,
        min_count:int,
        max_count:int,
        SSH_key_name:str,
        instance_type:str = 't2.micro',
        image_id:str = 'ami-0648ea225c13e0729'):

        EC2_RESOURCE = self.resource
        EC2_CLIENT = self.client

        try:
            instances = EC2_RESOURCE.create_instances(
                MinCount = min_count,
                MaxCount = max_count,
                ImageId=image_id,
                InstanceType=instance_type,
                KeyName = SSH_key_name,
                TagSpecifications=[
                    {
                        'ResourceType': 'instance',
                        'Tags': [
                            {
                                'Key': 'Name',
                                'Value': 'my-ec2-instance'
                            },
                        ]
                    },
                ]
            )
            for instance in instances:
                print(f'EC2 instance "{instance.id}" has been launched')
                
                instance.wait_until_running()
                
                EC2_CLIENT.associate_iam_instance_profile(
                    IamInstanceProfile = {'Name': self.iam_user},
                    InstanceId = instance.id,
                )

                print(f'EC2 Instance Profile "{self.iam_user}" has been attached')
                print(f'EC2 instance "{instance.id}" has been started')
        except ClientError as e:
            logging.error(e)
            return False
        return True
