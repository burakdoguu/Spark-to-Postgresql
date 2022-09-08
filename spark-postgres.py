from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from lib.logger import Log4j
from pyspark.sql.types import *

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("File Streaming Demo") \
        .master("local[3]") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .config("spark.sql.shuffle.partitions", "3") \
        .getOrCreate()

    logger = Log4j(spark)

    schema = StructType([
        StructField("InvoiceNumber", StringType()),
        StructField("CreatedTime", LongType()),
        StructField("StoreID", StringType()),
        StructField("PosID", StringType()),
        StructField("CashierID", StringType()),
        StructField("CustomerType", StringType()),
        StructField("CustomerCardNo", StringType()),
        StructField("TotalAmount", DoubleType()),
        StructField("NumberOfItems", IntegerType()),
        StructField("PaymentMethod", StringType()),
        StructField("CGST", DoubleType()),
        StructField("SGST", DoubleType()),
        StructField("CESS", DoubleType()),
        StructField("DeliveryType", StringType()),
        StructField("DeliveryAddress", StructType([
            StructField("AddressLine", StringType()),
            StructField("City", StringType()),
            StructField("State", StringType()),
            StructField("PinCode", StringType()),
            StructField("ContactNumber", StringType())
        ])),
        StructField("InvoiceLineItems", ArrayType(StructType([
            StructField("ItemCode", StringType()),
            StructField("ItemDescription", StringType()),
            StructField("ItemPrice", DoubleType()),
            StructField("ItemQty", IntegerType()),
            StructField("TotalValue", DoubleType())
        ]))),
    ])

    raw_df = spark.readStream \
        .format("json") \
        .schema(schema) \
        .option("path", "input") \
        .option("maxFilesPerTrigger", "1") \
        .option("cleanSource", "delete") \
        .load()

    # raw_df.printSchema()

    explode_df = raw_df.selectExpr("InvoiceNumber", "CreatedTime", "StoreID", "PosID", "CustomerType", "PaymentMethod",
                                   "DeliveryAddress.City", "DeliveryAddress.State", "DeliveryAddress.PinCode",
                                   "explode(InvoiceLineItems) as LineItem")

    flattened_df = explode_df \
        .withColumn("ItemCode", expr("LineItem.ItemCode")) \
        .withColumn("ItemDescription", expr("LineItem.ItemDescription")) \
        .withColumn("ItemPrice", expr("LineItem.ItemPrice")) \
        .withColumn("ItemQty", expr("LineItem.ItemQty")) \
        .withColumn("TotalValue", expr("LineItem.TotalValue")) \
        .drop("LineItem")

    # flattened_df.printSchema()

    def _write_streaming(
            df,
            epoch_id
    ) -> None:
        df.write \
            .mode('append') \
            .format("jdbc") \
            .option("url", f"jdbc:postgresql://localhost:5432/postgres") \
            .option("driver", "org.postgresql.Driver") \
            .option("dbtable", 'test') \
            .option("user", '{your_username}') \
            .option("password", '{your_password}') \
            .save()


    flattened_df.writeStream \
        .foreachBatch(_write_streaming) \
        .option("checkpointLocation", "chk-point-dir") \
        .start() \
        .awaitTermination()
