import sys
from collections import namedtuple
from pyspark import SparkConf, SparkContext
#from pyspark.sql import SparkSession
from pyspark.sql import *
from lib.logger import Log4J


if __name__ == '__main__':

    # sc = SparkContext(conf=conf)
    spark = SparkSession \
         .builder \
         .master("local[3]") \
         .getOrCreate()

    #sc = SparkContext(conf=conf)

    logger = Log4J(spark)

    if len(sys.argv) != 2:
        logger.error("Usage: HelloSparkSQL <filename>")
        sys.exit(-1)

    surveyDF = spark.read \
               .option("header", "true") \
               .option("inferSchema", "true") \
               .csv(sys.argv[1])

    surveyDF.createOrReplaceTempView("survey_tbl")
    countDF = spark.sql("select country, count(1) as count from survey_tbl where Age<40 group by country")
    countDF.show()
