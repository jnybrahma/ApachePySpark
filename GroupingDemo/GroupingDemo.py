from pyspark.sql import SparkSession
from pyspark.sql import functions as f


from lib.logger import Log4J

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("Grouping Demo") \
        .master("local[2]") \
        .getOrCreate()

    logger = Log4J(spark)

    invoice_df = spark.read \
                .format("csv") \
                .option("header", "true") \
                .option("inferSchema", "true") \
                .load("data/invoices.csv")


    #invoice_df.show(10)
    # This is an example of Grouping Aggregate

    NumInvoices = f.countDistinct("InvoiceNo").alias("NumInvoices")
    TotalQuantity = f.sum("Quantity").alias("TotalQuantity")
    InvoiceValue = f.expr("round(sum(Quantity * UnitPrice),2) as InvoiceValue")

    exSummary_df = invoice_df \
                .withColumn("InvoiceDate", f.to_date(f.col("InvoiceDate"),"dd-MM-yyyy H.mm")) \
                .where("year(InvoiceDate) == 2010") \
                .withColumn("WeekNumber", f.weekofyear(f.col("InvoiceDate"))) \
                .groupBy("Country","WeekNumber") \
                .agg(NumInvoices, TotalQuantity, InvoiceValue)

    exSummary_df.coalesce(1) \
            .write \
            .format("parquet") \
            .mode("overwrite") \
            .save("output")

    exSummary_df.sort("Country", "WeekNumber").show()







