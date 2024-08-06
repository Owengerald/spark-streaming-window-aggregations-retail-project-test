from pyspark.sql import SparkSession
from pyspark.sql.functions import *

if __name__ == '__main__':
    
    print("Creating Spark Session")

    spark = SparkSession.builder \
            .appName("streaming application") \
            .config("spark.sql.shuffle.partitions", 3) \
            .master("local[2]") \
            .getOrCreate()

    orders_schema = "order_id long, order_date timestamp, order_customer_id long, order_status string, amount long"

    # 1. reading the data
    orders_df = spark \
        .readStream \
        .format("socket") \
        .option("host", "localhost") \
        .option("port", "9971") \
        .load()
    
    # parsing json data in dataframe
    value_df = orders_df.select(from_json(col("value"), orders_schema).alias("value"))

    # refining the dataframe
    refined_orders_df = value_df.select("value.*")

    # performing window aggregation
    window_agg_df = refined_orders_df \
        .withWatermark("order_date", "30 minute") \
        .groupBy(window(col("order_date"), "15 minutes")) \
        .agg(sum("amount").alias("total_invoice"))

    # creating an output dataframe by selecting from window struct dataframe
    output_df = window_agg_df \
        .select("window.start", "window.end", "total_invoice")

    # writing the output dataframe to console
    streaming_query = output_df \
        .writeStream \
        .format("console") \
        .outputMode("update") \
        .option("checkpointLocation", "checkpointdir1") \
        .trigger(processingTime = "15 seconds") \
        .start()

    
    

    