from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, FloatType
from textblob import TextBlob

def get_sentiment(comment):
    blob = TextBlob(comment)
    return blob.sentiment.polarity

spark = SparkSession.builder \
    .appName("YouTubeCommentAnalysis") \
    .getOrCreate()

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "youtube-comments") \
    .load()

df = df.selectExpr("CAST(value AS STRING) as comment")

sentiment_udf = udf(get_sentiment, FloatType())
df = df.withColumn("sentiment", sentiment_udf(df.comment))

query = df.writeStream \
    .outputMode("append") \
    .format("csv") \
    .option("path", "../data") \
    .option("checkpointLocation", "../checkpoint") \
    .start()

query.awaitTermination()
