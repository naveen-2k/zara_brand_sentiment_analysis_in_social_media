from pyspark.sql.functions import col, udf
from textblob import TextBlob
from pyspark.sql.types import FloatType
# 2. Define a UDF for sentiment polarity using TextBlob
required_fields=df_final.select("id", "created_time",  "message", "total_count", "comment_id", "comment_created_time",  "comment_message", "comment_likes")
def get_sentiment(text):
    try:
        return TextBlob(text).sentiment.polarity
    except:
        return None

# Register UDF
sentiment_udf = udf(get_sentiment, FloatType())
df_with_sentiment = required_fields.withColumn("sentiment_score", sentiment_udf(col("comment_message")))
df_with_sentiment.display()

############################################################################################################################
from pyspark.sql.functions import when, count, lit

# Define sentiment categories
df_scored = df_with_sentiment.withColumn(
    "sentiment_category",
    when(col("sentiment_score") >= 0.5, "Promoter")
    .when((col("sentiment_score") >= 0.1) & (col("sentiment_score") < 0.5), "Passive")
    .otherwise("Detractor")
)

# CSAT: % of Promoters + Passives (optional: define own CSAT logic)
csat_df = df_scored.withColumn(
    "csat_flag", when(col("sentiment_score") >= 0.1, 1).otherwise(0)
)

csat_score = csat_df.agg((count(when(col("csat_flag") == 1, 1)) / count("*")) * 100).collect()[0][0]

# Count promoters, detractors, total
agg = df_scored.groupBy("sentiment_category").count().collect()
counts = {row['sentiment_category']: row['count'] for row in agg}
total = sum(counts.values())

promoters = counts.get("Promoter", 0)
detractors = counts.get("Detractor", 0)

# Calculate NPS
nps = ((promoters - detractors) / total) * 100 if total > 0 else None

# Print results
print(f"✅ CSAT Score: {csat_score:.2f}%")
print(f"🌟 NPS Score: {nps:.2f}")


##################################################################################################################

from pyspark.sql.functions import to_date

# Extract date from timestamp
df_with_date = df_with_sentiment.withColumn("comment_date", to_date("comment_created_time"))
from pyspark.sql.functions import when

df_with_category = df_with_date.withColumn(
    "sentiment_category",
    when(col("sentiment_score") >= 0.5, "Promoter")
    .when((col("sentiment_score") >= 0.1) & (col("sentiment_score") < 0.5), "Passive")
    .otherwise("Detractor")
)
from pyspark.sql.functions import count, sum as sum_

# Flag for CSAT (1 = satisfied, 0 = not)
df_with_flag = df_with_category.withColumn("csat_flag", when(col("sentiment_score") >= 0.4, 1).otherwise(0))

# Group by comment_date
daily_agg = df_with_flag.groupBy("comment_date").agg(
    count("*").alias("total_comments"),
    count(when(col("sentiment_category") == "Promoter", 1)).alias("promoters"),
    count(when(col("sentiment_category") == "Detractor", 1)).alias("detractors"),
    sum_("csat_flag").alias("csat_positive")
)

# Calculate NPS and CSAT %
daily_agg_final = daily_agg.withColumn("NPS", ((col("promoters")-col("detractors"))/col("total_comments")) * 100)\
                           .withColumn("CSAT", (col("csat_positive") / col("total_comments")) * 100)
display(daily_agg_final.orderBy("comment_date"))


##############################################################################################################

import matplotlib.pyplot as plt
import pandas as pd

# Convert to Pandas for plotting
pdf = daily_agg_final.orderBy("comment_date").toPandas()

# Bar chart with two bars per date (grouped)
x = range(len(pdf))
width = 0.35

plt.figure(figsize=(12, 6))
plt.bar([i - width/2 for i in x], pdf['NPS'], width, label='NPS', color='skyblue')
plt.bar([i + width/2 for i in x], pdf['CSAT'], width, label='CSAT', color='lightgreen')

plt.xlabel('Date')
plt.ylabel('Score (%)')
plt.title('Daily NPS and CSAT Scores')
plt.xticks(x, pdf['comment_date'], rotation=45)
plt.legend()
plt.tight_layout()
plt.grid(axis='y')
plt.show()

############################################################################################

from pyspark.sql.functions import when, col, count

# Step 1: Add sentiment category
df_with_date = df_with_sentiment.withColumn("comment_date", to_date("comment_created_time"))
df_sentiment = df_with_date.withColumn(
    "sentiment_label",
    when(col("sentiment_score") > 0, "Positive")
    .when(col("sentiment_score") < 0, "Negative")
    .otherwise("Neutral")
)

# Step 2: Group by date and sentiment
daily_sentiment_trend = df_sentiment.groupBy("comment_date", "sentiment_label").agg(
    count("*").alias("count")
).orderBy("comment_date")

display(daily_sentiment_trend)

###################################################################################################
import matplotlib.pyplot as plt

# Convert to pandas for plotting
pdf = daily_net_sentiment.toPandas()

plt.figure(figsize=(10, 5))
plt.plot(pdf['comment_date'], pdf['net_sentiment_score'], marker='o', color='royalblue', linewidth=2)

plt.title("Net Sentiment Trend Over Time")
plt.xlabel("Date")
plt.ylabel("Net Sentiment Score")
plt.grid(True)
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()
