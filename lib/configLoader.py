import configparser
from pyspark import SparkConf
def get_spark_conf(env):
    spark_conf=SparkConf()
    parser=configparser.ConfigParser()
    parser.read('configuration/spark.conf')
    for key,value in parser.items(env):
        spark_conf.set(key,value)
    return spark_conf
