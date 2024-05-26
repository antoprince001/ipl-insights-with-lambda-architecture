from pyspark.sql import SparkSession
from src.batch_layer.batch_processor import process_batch

spark = SparkSession.builder.appName('com.spark-ipl-analysis').getOrCreate()
file_path = './ipl_data.csv'
process_batch(spark, file_path)
