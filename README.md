8) Quick run instructions (summary)

Install dependencies:

Use Python 3.10+ virtualenv:

python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt


Install spark (or use pip install pyspark which includes a local Spark; using local Spark with Kafka might require additional packages spark-sql-kafka-0-10 in spark-submit).

Start services:

docker-compose up -d


Wait for Kafka and HDFS to be ready.

Download dataset:

bash download_data.sh


Create Kafka topic (optional; producer will auto-create depending on Kafka config):

docker exec -it <kafka-container-name> bash
# inside container
kafka-topics --create --topic smart_meter --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1


If using the Confluent image, you can also use kafka-topics from the host if it’s available. Alternatively, just run the producer; Kafka often creates topics on the fly.

Start Spark streaming job to read from Kafka and write to HDFS (or local file):

If using local host Spark: spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1 spark/spark_streaming_to_hdfs.py

Ensure kafka.bootstrap.servers argument in the script matches your setup (localhost:9093 or kafka:9092).

Start the Kafka producer to stream the dataset:

python3 producer/kafka_producer.py --broker localhost:9093 --topic smart_meter --interval 0.01


Wait for Spark to write Parquet files into HDFS path (or local path if changed). Once you have Parquet files or use raw CSV, train the model:

python3 model/preprocess_and_train.py --parquet_dir /path/to/parquet_or_leave_default


Serve the model:

python3 model/serve_model.py
curl -X POST localhost:5000/predict -H "Content-Type: application/json" -d '{"hour": 14, "dayofweek":2, "is_weekend":0, "gap_mean_lag_1":0.5, "gap_max_lag_1":0.8, "gap_mean_lag_2":0.6, "gap_max_lag_2":0.9, "gap_mean_lag_3":0.5, "gap_max_lag_3":0.8, "gap_mean_lag_6":0.4, "gap_max_lag_6":0.7, "gap_mean_lag_12":0.55, "gap_max_lag_12":0.9, "gap_mean_lag_24":0.6, "gap_max_lag_24":0.9, "rolling_3h_max":0.85, "rolling_24h_mean":0.55}'
