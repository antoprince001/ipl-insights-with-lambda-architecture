from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, sum
from pyspark.sql.types import StructType, StructField, StringType

spark = SparkSession.builder.appName('com.spark-ipl-stream-analysis')    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1").getOrCreate()

schema = StructType([
    StructField("match_id", StringType(), True),
    StructField("season", StringType(), True),
    StructField("start_date", StringType(), True),
    StructField("venue", StringType(), True),
    StructField("innings", StringType(), True),
    StructField("ball", StringType(), True),
    StructField("batting_team", StringType(), True),
    StructField("bowling_team", StringType(), True),
    StructField("striker", StringType(), True),
    StructField("non_striker", StringType(), True),
    StructField("bowler", StringType(), True),
    StructField("runs_off_bat", StringType(), True),
    StructField("extras", StringType(), True),
    StructField("wides", StringType(), True),
    StructField("noballs", StringType(), True),
    StructField("byes", StringType(), True),
    StructField("legbyes", StringType(), True),
    StructField("penalty", StringType(), True),
    StructField("wicket_type", StringType(), True),
    StructField("player_dismissed", StringType(), True),
    StructField("other_wicket_type", StringType(), True),
    StructField("other_player_dismissed", StringType(), True)
])

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "ipl_event") \
    .load()
print('stream')

# Convert the value column from Kafka to a StringType
df = df.selectExpr("CAST(value AS STRING) as json_value")

# Parse the JSON data and apply the schema
parsed_df = df.withColumn("data", from_json(col("json_value"), schema)).select("data.*")

# df = df.withColumn("data", from_json(col("json_value"), schema)).select("data.*")

# Perform some transformations (e.g., filter, select specific columns)
transformed_df = parsed_df.select("match_id", "season", "start_date", "venue", "innings", "ball", 
                           "batting_team", "bowling_team", "striker", "non_striker", "bowler", 
                           "runs_off_bat", "extras", "wides", "noballs", "byes", "legbyes", 
                           "penalty", "wicket_type", "player_dismissed", "other_wicket_type", 
                           "other_player_dismissed") 
                #    / .filter(col("batting_team") == "Kolkata Knight Riders")  # Example filter

transformed_df = transformed_df.groupBy("match_id") \
    .agg(sum("runs_off_bat").alias("total_runs"))

# Write the streaming data to console and then stop
query = transformed_df \
    .writeStream \
    .outputMode("update") \
    .format("console") \
    .start()

query.awaitTermination(timeout=60)  # Wait for 60 seconds or until termination


# def process_and_stop_query(query):
#     query.awaitTermination(timeout=60)  # Wait for 60 seconds or until termination
#     query.stop()  # Stop the query
