#!/usr/bin/env python3
"""
Spark Streaming 实时处理社交电商数据
窗口5分钟，并行度2，写入MySQL
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder \
    .appName("SocialEcommerceStreaming") \
    .config("spark.sql.shuffle.partitions", "2") \
    .config("spark.eventLog.enabled", "false") \
    .config("spark.hadoop.fs.defaultFS", "file:///") \
    .config("spark.sql.adaptive.enabled", "false") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "master:9092") \
    .option("subscribe", "social-ecommerce") \
    .option("startingOffsets", "earliest") \
    .load()

df_parsed = df_raw.select(
    from_csv(col("value").cast("string"), 
             "user_id STRING, item_id STRING, age INT, gender INT, user_level INT, purchase_freq INT, total_spend FLOAT, register_days INT, follow_num INT, fans_num INT, price FLOAT, discount_rate FLOAT, category STRING, title_length INT, title_emo_score FLOAT, img_count INT, has_video INT, like_num INT, comment_num INT, share_num INT, collect_num INT, is_follow_author INT, add2cart INT, coupon_received INT, coupon_used INT, pv_count INT, last_click_gap FLOAT, interaction_rate FLOAT, purchase_intent FLOAT, freshness_score FLOAT, social_influence FLOAT, label INT",
             {"sep": ",", "header": "false"}).alias("data")
).select("data.*")

df_with_time = df_parsed.withColumn("processing_time", current_timestamp())

# 窗口改为5分钟
hot_items = df_with_time \
    .withColumn("hot_score", col("like_num") + col("comment_num") + col("share_num") + col("collect_num")) \
    .groupBy(window("processing_time", "5 minutes"), "item_id") \
    .agg(sum("hot_score").alias("total_hot_score")) \
    .select(col("window.start").alias("window_start"), col("window.end").alias("window_end"), "item_id", "total_hot_score")

behavior_stats = df_with_time \
    .withColumn("behavior_flag",
                when(col("label") == 1, "purchase")
                .when(col("like_num") + col("comment_num") + col("share_num") + col("collect_num") > 0, "interaction")
                .otherwise("view")) \
    .groupBy(window("processing_time", "5 minutes"), "behavior_flag") \
    .agg(count("*").alias("total_count")) \
    .select(col("window.start").alias("window_start"), col("window.end").alias("window_end"), col("behavior_flag").alias("behavior_type"), "total_count")

def write_to_mysql_hot(df, epoch_id):
    if df.count() > 0:
        df.write \
          .mode("append") \
          .format("jdbc") \
          .option("url", "jdbc:mysql://localhost:3306/social_analysis") \
          .option("dbtable", "hot_products") \
          .option("user", "root") \
          .option("password", "123456") \
          .option("driver", "com.mysql.cj.jdbc.Driver") \
          .save()

def write_to_mysql_behavior(df, epoch_id):
    if df.count() > 0:
        df.write \
          .mode("append") \
          .format("jdbc") \
          .option("url", "jdbc:mysql://localhost:3306/social_analysis") \
          .option("dbtable", "behavior_stats") \
          .option("user", "root") \
          .option("password", "123456") \
          .option("driver", "com.mysql.cj.jdbc.Driver") \
          .save()

query1 = hot_items.writeStream \
    .outputMode("update") \
    .foreachBatch(write_to_mysql_hot) \
    .option("checkpointLocation", "/tmp/spark-checkpoint-hot") \
    .trigger(processingTime="10 seconds") \
    .start()

query2 = behavior_stats.writeStream \
    .outputMode("update") \
    .foreachBatch(write_to_mysql_behavior) \
    .option("checkpointLocation", "/tmp/spark-checkpoint-behavior") \
    .trigger(processingTime="10 seconds") \
    .start()

spark.streams.awaitAnyTermination()
