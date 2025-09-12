import re

from pyspark.sql import *
from pyspark.sql.functions import udf, expr
from pyspark.sql.types import *

from lib.logger import Log4J

def parse_gender(gender):
    female_pattern = r"^f$|f.m|w.m"
    male_pattern = r"^m$|ma|m.l"

    if re.search(female_pattern, gender.lower()):
        return "Female"
    elif re.search(male_pattern, gender.lower()):
        return "Male"
    else:
        return "Unknown"

## Note: works well in python 3.9

if __name__ == '__main__':


    spark = SparkSession \
            .builder  \
            .appName("UDFDemo") \
            .master("local[2]") \
            .getOrCreate()

    logger = Log4J(spark)

    survey_df = spark.read \
                .option("header", "true") \
                .option("inferSchema", "true") \
                .csv("data/survey.csv")

    survey_df.show(10)

    parse_gender_udf = udf(parse_gender, returnType=StringType())  ## This is use function in df column  expression
    logger.info("Catalog Entry :")
    [logger.info(f) for f in spark.catalog.listFunctions() if "parse_gender" in f.name]
    survey_df2 = survey_df.withColumn("Gender", parse_gender_udf("Gender"))
    survey_df2.show(10)

    spark.udf.register("parse_gender_udf", parse_gender, StringType())  ## If you want to use function in Sql expression
    logger.info("Catalog Entry :")
    [logger.info(f) for f in spark.catalog.listFunctions() if "parse_gender" in f.name]
    survey_df3 = survey_df.withColumn("Gender", expr("parse_gender_udf(Gender)"))
    survey_df3.show(10)


