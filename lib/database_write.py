from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from lib.logging_conf import *

def write_to_database(df,url,properties,tablename):
    
    try:
        df.write\
        .option("driver","com.microsoft.sqlserver.jdbc.SQLServerDriver")\
        .jdbc(url=url,
            table=tablename,
            mode="overwrite",
            properties=properties)
        logger.info(f"Data successfully written into {tablename} table ")
    except Exception as e:
        logger.error(f"Message: Error occured {e}")
        