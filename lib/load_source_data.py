import boto3
from pyspark.sql import SparkSession
from botocore.client import Config
from lib.S3Resource import *
from lib.spark_session import *
from configuration.dev import conf
from pyspark.sql.types import StructType, StructField, StringType, IntegerType,BooleanType
from lib.logging_conf import *

def define_schemas():
    logger.info("Defining various schemas")
    hospital_schema = StructType([
    StructField("hospital_id", IntegerType(), nullable=False),
    StructField("hospital_name", StringType(), nullable=False),
    StructField("location", StringType(), nullable=False),
    StructField("contact_number", StringType(), nullable=True),  
    StructField("email", StringType(), nullable=True) 
    ])
    employee_hospital_schema = StructType([
    StructField("employee_id", IntegerType(), nullable=False),
    StructField("hospital_id", IntegerType(), nullable=False),
    StructField("is_active", BooleanType(), nullable=False)
    ])
    employee_schema = StructType([
    StructField("employee_id", IntegerType(), nullable=False),
    StructField("employee_name", StringType(), nullable=False),
    StructField("contact_number", StringType(), nullable=True), 
    StructField("email", StringType(), nullable=True),
    StructField("manager_id", IntegerType()) 
    ])
    doctor_specialty_schema = StructType([
    StructField("doctor_id", IntegerType(), nullable=False),
    StructField("specialty_id", IntegerType(), nullable=False)
    ])
    doctor_hospital_schema = StructType([
    StructField("doctor_id", IntegerType(), nullable=False),
    StructField("hospital_id", IntegerType(), nullable=False),
    StructField("is_active", BooleanType(), nullable=False)
    ])
    specialty_name_schema = StructType([
    StructField("specialty_id", IntegerType(), nullable=False),
    StructField("specialty_name", StringType(), nullable=False),
    StructField("Priority", IntegerType(), nullable=False)
    ])
    doctor_schema = StructType([
    StructField("doctor_id", IntegerType(), nullable=False),
    StructField("doctor_name", StringType(), nullable=False),
    StructField("qualification", StringType(), nullable=True), 
    StructField("experience_years", IntegerType(), nullable=True),  
    StructField("contact_number", StringType(), nullable=True),
    StructField("email", StringType(), nullable=True) 
    ])

    schemas={
        "employee":employee_schema,
        "employee_hospital":employee_hospital_schema,
        "hospital":hospital_schema,
        "doctor":doctor_schema,
        "doctor_hospital":doctor_hospital_schema,
        "doctor_specialty":doctor_specialty_schema,
        "specialty_names":specialty_name_schema
    }
    return schemas


def create_source_dataframes(env,list_of_files,spark):
    schemas=define_schemas()
    if env=='DEV':
        # in Dev we will download the S3 files in local and then load in dataframes
        s3=s3_instance()
        logger.info("Downloading S3 files in local system")
        # for i in list_of_files:
        #     s3.meta.client.download_file(conf.bucket_name,i,'D:\SparkProject_ETL\Data/'+i.replace('source/',''))
        logger.info("S3 files downloaded successfully in local system")
        logger.info("Creating Doctor dataframe")
        doctor=spark.read.csv("D:\SparkProject_ETL\Data\doctor_details.csv",schema=schemas['doctor'],header=True)
        logger.info("Doctor dataframe Created successfully")
        logger.info("Showing Doctor data")
        doctor.show(5)
        logger.info("Creating Doctor-Hospital-Relationship dataframe")
        doctor_hospital=spark.read.csv("D:\SparkProject_ETL\Data\doctor_hospital_relation.csv",schema=schemas['doctor_hospital'],header=True)
        logger.info("Doctor-Hospital-Relationship dataframe Created successfully")
        logger.info("Showing Doctor-Hospital-Relationship data")
        doctor_hospital.show(5)
        logger.info("Creating Employee dataframe")
        employee=spark.read.csv("D:\SparkProject_ETL\Data\employee_details.csv",schema=schemas['employee'],header=True)
        logger.info("Employee dataframe Created successfully")
        logger.info("Showing Employee data")
        employee.show(5)
        logger.info("Creating Employee-Hospital-Relationship dataframe")
        employee_hospital=spark.read.csv("D:\SparkProject_ETL\Data\employee_hospital_relation.csv",schema=schemas['employee_hospital'],header=True)
        logger.info("Employee-Hospital-Relationship dataframe Created successfully")
        logger.info("Showing Employee-Hospital-Relationship data")
        employee_hospital.show(5)
        logger.info("Creating Hospital dataframe")
        hospital=spark.read.csv("D:\SparkProject_ETL\Data\hospital_details.csv",schema=schemas['hospital'],header=True)
        logger.info("Hospital dataframe Created successfully")
        logger.info("Showing Hospital data")
        hospital.show(5)
        logger.info("Creating Doctor-Spacialty dataframe")
        doctor_specialty=spark.read.csv("D:\SparkProject_ETL\Data\doctor_specialty.csv",schema=schemas['doctor_specialty'],header=True)
        logger.info("Doctor-Spacialty dataframe Created successfully")
        logger.info("Showing Doctor-Spacialty data")
        doctor_specialty.show(5)
        logger.info("Creating Specialty-Names dataframe")
        specialty_names=spark.read.csv("D:\SparkProject_ETL\Data\specialty_names.csv",schema=schemas['specialty_names'],header=True)
        logger.info("Spacialty-Names dataframe Created successfully")
        logger.info("Showing Spacialty-Names data")
        specialty_names.show(5)

    return {
        "doctor":doctor,
        "doctor_hospital":doctor_hospital,
        "doctor_specialty":doctor_specialty,
        "employee":employee,
        "employee_hospital":employee_hospital,
        "hospital":hospital,
        "specialty_names":specialty_names
    }
















