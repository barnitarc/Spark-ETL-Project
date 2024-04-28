from lib.spark_session import *
from lib.logging_conf import *
import sys
from lib.S3Resource import *
from lib.load_source_data import *
from lib.transform import *
from lib.database_write import *
from configuration.dev import conf
from lib.archive_source_data import *

if __name__=="__main__":
    env=sys.argv[1].upper()
    spark=create_spark(env)
    s3=s3_instance()
    l=list_of_s3_files(s3)
    logger.info("Starting loading source datasets in dataframes")
    dfs=create_source_dataframes(env,l,spark)
    logger.info("Starting Transformation of Doctor dataframes")
    doc_df=transform_doctor(dfs['doctor'],dfs['doctor_hospital'],dfs['doctor_specialty'],dfs['specialty_names'])
    logger.info("Starting Transformation of Employee dataframes")
    emp_df=transform_employee(dfs['employee'],dfs['employee_hospital'])
    logger.info("Starting Transformation of Hospital dataframes")
    hos_df=transform_hospital(dfs['hospital'],dfs['employee_hospital'],dfs['doctor_hospital'],dfs['employee'])
    logger.info("Writing Doctor dataframes into MSSQL table")
    write_to_database(doc_df,conf.url,conf.properties,conf.prescriber_table_name)
    logger.info("Writing Hospital dataframes into MSSQL table")
    write_to_database(hos_df,conf.url,conf.properties,conf.organisation_table_name)
    logger.info("Writing Employee dataframes into MSSQL table")
    write_to_database(emp_df,conf.url,conf.properties,conf.employee_table_name)
    logger.info("Archiving S3 files")
    archive_s3_files(s3,l)
    logger.info("ETL completed successfully")
    input("done")