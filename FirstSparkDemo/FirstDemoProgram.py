import sys
from pyspark import SparkConf
from pyspark.sql import *
from lib.logger import Log4J
from lib.utils import get_spark_app_config, load_survey_df, count_by_country

if __name__ == "__main__":
	#print("Starting First PySpark Program.")
	#conf = SparkConf()
	conf = get_spark_app_config()
	#conf.set("spark.app.name", "Demo Spark")
	#conf.set("spark.master", "local[3]")
	spark = SparkSession.builder \
		.config(conf=conf)\
		.getOrCreate()
	sc = spark.sparkContext
	sc.setLogLevel("INFO")

	logger = Log4J(spark)

	if len(sys.argv) != 2:
		logger.error("Usage: DemoSpark <filename>")
		sys.exit(-1)

	logger.info("Starting DemoSpark")
	#conf_out = sc.getConf()
	#logger.info(conf_out.toDebugString())

	#survey_df = spark.read \
	#			 .option("header", "true") \
	#			 .option("inferSchema", "true") \
	#			 .csv(sys.argv[1])

	survey_df = load_survey_df(spark, sys.argv[1])
	#filtered_survey_df = survey_df.where("Age < 40") \
	#		.select("Age", "Gender", "Country", "state") \

	#grouped_df = filtered_survey_df.groupBy("Country")
	#count_df = grouped_df.count()
	partioned_survey_df = survey_df.repartition(2)
	#count_df = count_by_country(survey_df)
	#count_df.show()
	#survey_df.show()
	count_df = count_by_country(partioned_survey_df)
	logger.info(count_df.collect())

	input("Press Enter") # remove this line in production. It is only for debugging purpose
	logger.info("Finished DemoSpark")


	#spark.stop()