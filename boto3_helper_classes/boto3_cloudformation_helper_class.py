import boto3
import logging
import botocore.exceptions
from typing import List

class Cloudformation:
    def __init__(self,ACCESS_KEY_ID:str,SECRET_ACCESS_KEY:str) -> None:
        self.cloud_formation = boto3.client(
            'cloudformation',
            aws_access_key_id = ACCESS_KEY_ID,
            aws_secret_access_key = SECRET_ACCESS_KEY,)

    def create_stack(
        self,
        stack_name:str,
        config_path:str,
        parameters:List[dict],
    ) -> bool:
        try:
            self.cloud_formation.create_stack(
                StackName = stack_name,
                TemplateURL = config_path,
                Parameters = parameters
            )
            return True, "Cloudformation Successful"
        except botocore.Exception as e:
            logging.error(e)
            return False, "Cloudformation Failed."