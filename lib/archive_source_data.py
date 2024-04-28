import boto3
from lib.logging_conf import *
from botocore.client import Config
from configuration.dev import conf
import datetime

def archive_s3_files(s3,list_of_files):
    currentdate=datetime.datetime.now().strftime("%Y_%m_%d")
    try:
        for i in list_of_files:
            s3.Object(conf.bucket_name,'archive/'+currentdate+'/'+i.replace('source/','')).copy_from(CopySource=conf.bucket_name+'/'+i)
            s3.Object(conf.bucket_name, i).delete()
            logger.info(f"Data Moved succesfully from {i} to "+'archive/'+currentdate+'/'+i.replace('source/',''))
    except Exception as e:
        logger.error(f"Error moving file : {str(e)}")
        print(e)
        