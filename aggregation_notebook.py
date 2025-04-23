# Databricks notebook parameters
dbutils.widgets.text("silver_table", "")
dbutils.widgets.text("gold_table_path", "")
dbutils.widgets.text("business_date", "")

silver_table = dbutils.widgets.get("silver_table")
gold_table_path = dbutils.widgets.get("gold_table_path")
business_date = dbutils.widgets.get("business_date")

from pyspark.sql.functions import col, udf, to_date, when, count, sum as sum_
from pyspark.sql.types import FloatType
from textblob import TextBlob

# Load data from temporary view
df = spark.sql(f"SELECT * FROM {silver_table}")

# Sentiment UDF
def get_sentiment(text):
    try:
        return TextBlob(text).sentiment.polarity
    except:
        return None

sentiment_udf = udf(get_sentiment, FloatType())
df = df.withColumn("sentiment_score", sentiment_udf(col("comment_message")))

# Sentiment categorization
df = df.withColumn("comment_date", to_date("comment_created_time"))
df = df.withColumn(
    "sentiment_category",
    when(col("sentiment_score") >= 0.5, "Promoter")
    .when((col("sentiment_score") >= 0.1) & (col("sentiment_score") < 0.5), "Passive")
    .otherwise("Detractor")
).withColumn(
    "csat_flag", when(col("sentiment_score") >= 0.4, 1).otherwise(0)
)

# Daily aggregation
daily_agg = df.groupBy("comment_date").agg(
    count("*").alias("total_comments"),
    count(when(col("sentiment_category") == "Promoter", 1)).alias("promoters"),
    count(when(col("sentiment_category") == "Detractor", 1)).alias("detractors"),
    sum_("csat_flag").alias("csat_positive")
).withColumn("NPS", ((col("promoters") - col("detractors")) / col("total_comments")) * 100)\
 .withColumn("CSAT", (col("csat_positive") / col("total_comments")) * 100)

# Save to Delta (partitioned by comment_date)
(
    daily_agg.write
    .format("delta")
    .mode("overwrite")
    .partitionBy("comment_date")
    .option("overwriteSchema", "true")
    .saveAsTable(gold_table_path)
)

dbutils.notebook.exit("Aggregated and saved to Gold layer")
