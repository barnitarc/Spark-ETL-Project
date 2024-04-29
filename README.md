
# Spark ETL Project

This project is designed to perform Extract, Transform, and Load (ETL) operations using PySpark. It fetches data from an Amazon S3 bucket, transforms it using PySpark DataFrames, and then writes the processed data into Microsoft SQL Server tables. Additionally, it archives the processed files back to the S3 bucket. The entire process is logged using Python's logging module.

# Table of Contents
* Overview
* File Descriptions
    * main.py
    * S3Resource.py
    * spark_session.py
    * logging_conf.py
    * config_loader.py
    * load_source_data.py
    * transform.py
    * database_write.py
    * archive_source_data.py
* Setup
* Usage
* Contributing

# Overview
The project consists of several Python scripts organized into modules, each responsible for a specific task in the ETL process.

# File Descriptions
- main.py
  
  This script orchestrates the entire ETL process. It imports functions from other modules, initializes necessary resources, and executes the ETL pipeline.

- S3Resource.py

  S3Resource module contains functions for interacting with the Amazon S3 bucket. It includes functions for creating an S3 resource instance and listing files        available in the bucket.

- spark_session.py
  
  The spark_session module is responsible for creating a Spark session based on the specified environment (DEV, PROD, or QA) and loading configurations           
  dynamically. It sets up the Spark session with appropriate configurations.

- logging_conf.py
  
  The logging_conf module configures logging settings for the project. It initializes a logger named "Spark_ETL-project" and sets up logging to a file (logs/app-     {date}.log) and console.

- configLoader.py
  
  config_loader module loads Spark configurations from the configuration/spark.conf file based on the specified environment. It reads the configurations and       
  returns a SparkConf object.

- load_source_data.py
  
  load_source_data module defines schemas for various data entities and loads source data from S3 into PySpark DataFrames. It also includes functions for     
  downloading files from S3 to the local system and creating DataFrames.

- transform.py
  
  The transform module contains functions for transforming data. It includes functions for transforming doctor, employee, and hospital data, such as adding   
  calculated columns and joining DataFrames.

- database_write.py
  
  database_write module handles writing transformed DataFrames into MSSQL tables. It includes a function that writes DataFrames to the database using JDBC.

- archive_source_data.py
  
  The archive_source_data module archives processed files from S3 to a designated folder. It includes a function for moving files from the source directory to the    archive directory in the S3 bucket.

# Setup
Clone the repository:
```
git clone <repository-url>
```
Install dependencies:
```
pip install -r requirements.txt
```
Ensure that your environment variables are correctly set, including AWS credentials and SQL Server connection details.
# Usage
To run the ETL process, execute the main.py script with the desired environment as an argument:

```
python main.py DEV
```
Replace DEV with PROD or QA depending on the environment you want to run the process for.

# Contributing
Contributions are welcome! Please open an issue or submit a pull request with any improvements or fixes.

