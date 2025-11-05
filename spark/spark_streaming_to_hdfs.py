# spark/spark_streaming_to_hdfs.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp, concat_ws, lit
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("kafka-to-hdfs-smart-meter") \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()

    kafka_bootstrap = "kafka:9092"  # when running inside Docker network; if local spark, use localhost:9093

    # Schema for JSON messages
    schema = StructType([
        StructField("Date", StringType(), True),
        StructField("Time", StringType(), True),
        StructField("Global_active_power", DoubleType(), True),
        StructField("Global_reactive_power", DoubleType(), True),
        StructField("Voltage", DoubleType(), True),
        StructField("Global_intensity", DoubleType(), True),
        StructField("Sub_metering_1", DoubleType(), True),
        StructField("Sub_metering_2", DoubleType(), True),
        StructField("Sub_metering_3", DoubleType(), True),
    ])

    # Read from Kafka
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9093") \
        .option("subscribe", "smart_meter") \
        .option("startingOffsets", "earliest") \
        .load()

    # value is bytes -> string -> json -> columns
    json_df = df.selectExpr("CAST(value AS STRING) as json_str")
    parsed = json_df.select(from_json(col("json_str"), schema).alias("data")).select("data.*")

    # Build a timestamp column from Date + Time (dataset uses dd/mm/yyyy)
    # We'll parse Date+Time into a timestamp
    # Create an ISO timestamp string
    combined = parsed.withColumn("datetime_str", concat_ws(" ", col("Date"), col("Time")))

    # Convert to timestamp; custom parsing using to_timestamp with format
    combined = combined.withColumn("timestamp", to_timestamp(col("datetime_str"), "dd/MM/yyyy HH:mm:ss"))
    # Partition by date for writes
    combined = combined.withColumn("date", col("timestamp").cast("date"))

    # Choose output HDFS path (ensure HDFS is available)
    output_path = "hdfs://namenode:9000/user/data/smart_meter"

    query = combined.writeStream \
        .format("parquet") \
        .option("checkpointLocation", "/tmp/checkpoints/smart_meter_checkpoint") \
        .option("path", output_path) \
        .partitionBy("date") \
        .outputMode("append") \
        .start()

    query.awaitTermination()
