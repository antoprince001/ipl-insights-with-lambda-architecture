from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('com.spark-ipl-stream-analysis').getOrCreate()


def stream_job():
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "ipl_event") \
        .load()
    print('stream')
    # Write the streaming data to console and then stop
    query = df \
        .writeStream \
        .outputMode("append") \
        .format("console") \
        .start()
    process_and_stop_query(query)


def process_and_stop_query(query):
    query.awaitTermination(timeout=60)  # Wait for 60 seconds or until termination
    query.stop()  # Stop the query
