from lib.logging_conf import *
from pyspark.sql import SparkSession
from lib.configLoader import *
from configuration.dev import conf as dev_conf
from configuration.prod import conf as prod_conf
from configuration.qa import conf as qa_conf

def create_spark(env):
    logger.info("Creating Spark session")
    if env=='DEV':
        spark = SparkSession.builder\
            .master("local[3]") \
            .config(conf=get_spark_conf(env)) \
            .config('spark.sql.autoBroadcastJoinThreshold',100000) \
            .config('spark.sql.adaptive.enabled','false') \
            .getOrCreate()
    elif env=='PROD':
        spark=SparkSession.builder\
            .congif(conf=get_spark_conf(env))\
            .getOrCreate()
        spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.access.key",prod_conf.aws_access_key)
        spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.secret.key",prod_conf.aws_secret_key)
    elif env=='QA':
        spark=SparkSession.builder\
            .congif(conf=get_spark_conf(env))\
            .getOrCreate()
        spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.access.key",qa_conf.aws_access_key)
        spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.secret.key",qa_conf.aws_secret_key)
    logger.info("Spark session created successfully")
    return spark


