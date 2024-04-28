import os

#AWS Access And Secret key
aws_access_key = "insert your access key"
aws_secret_key = "insert your secret key"
bucket_name = "bucket name"
s3_log_file_location="logs/"
s3_final_file_location='Master/'
s3_source_location="source/"


#Database credential
# MSSQL database connection properties
database_name = "practice"
url = f"jdbc:sqlserver://DESKTOP-G7H3F97;databaseName={database_name};"
properties = {
    "user": "spark",
    "password": "spark",
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

# Table name
prescriber_table_name = "ETL.Prescriber"
organisation_table_name="ETL.Organisation"
employee_table_name="ETL.Employee"

# File Download location for Dev
local_directory = "D:\SparkProject_ETL\Data"
