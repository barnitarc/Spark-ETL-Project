import boto3
from lib.logging_conf import *
from botocore.client import Config
from configuration.dev import conf

def s3_instance():
    logger.info("Creating S3 imstance for DEV environment")
    s3=boto3.resource('s3',aws_access_key_id=conf.aws_access_key,aws_secret_access_key=conf.aws_secret_key,config=Config(signature_version='s3v4'))
    return s3

def list_of_s3_files(s3):
    bucket=s3.Bucket(conf.bucket_name)
    list_of_files=[]
    file_list=''
    
    for file in bucket.objects.all():
        if file.key.startswith("source/"):
            file_list=file_list+file.key+'\n'
            list_of_files.append(file.key)
    logger.info("Showing the files available in S3"+'\n'+file_list)
    return list_of_files





