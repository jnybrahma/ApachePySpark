from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as f


from lib.logger import Log4J

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("Windowing Demo") \
        .master("local[2]") \
        .getOrCreate()

    logger = Log4J(spark)

    summary_df = spark.read.parquet("data/summary.parquet")

    # summary_df.show(10)
    # This is an example of Windowing Aggregate

    running_total_window = Window.partitionBy("Country") \
                    .orderBy("WeekNumber") \
                    .rowsBetween(Window.unboundedPreceding, Window.currentRow)

    summary_df.withColumn("RunningTotal",
                          f.sum("InvoiceValue").over(running_total_window))\
                        .show()





