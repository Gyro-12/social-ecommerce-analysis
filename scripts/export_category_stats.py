#!/usr/bin/env python3
import pyspark
from pyspark.sql import SparkSession
import pymysql
import pymysql.cursors

# 初始化 Spark 会话（启用 Hive 支持）
spark = SparkSession.builder \
    .appName("ExportCategoryStats") \
    .config("spark.sql.shuffle.partitions", "1") \
    .enableHiveSupport() \
    .getOrCreate()

# 读取 ads_category_daily 表
df = spark.sql("SELECT dt, category, total_pv, total_likes, total_purchase FROM social_dw.ads_category_daily")

# 转换为 pandas DataFrame（数据量不大，可以直接 collect）
data = df.collect()
print(f"从 Hive 读取到 {len(data)} 条记录")

# MySQL 连接配置（明确指定字符集）
conn = pymysql.connect(
    host='localhost',
    user='root',
    password='123456',
    database='social_analysis',
    charset='utf8mb4',
    use_unicode=True,
    cursorclass=pymysql.cursors.DictCursor
)
cursor = conn.cursor()

# 清空目标表
cursor.execute("TRUNCATE TABLE category_daily_stats")

# 批量插入（提高性能）
insert_sql = "INSERT INTO category_daily_stats (dt, category, total_pv, total_likes, total_purchase) VALUES (%s, %s, %s, %s, %s)"
batch_size = 1000
batch = []
for i, row in enumerate(data):
    batch.append((row.dt, row.category, row.total_pv, row.total_likes, row.total_purchase))
    if len(batch) >= batch_size:
        cursor.executemany(insert_sql, batch)
        conn.commit()
        batch = []
if batch:
    cursor.executemany(insert_sql, batch)
    conn.commit()

print(f"成功插入 {len(data)} 条记录")

cursor.close()
conn.close()
spark.stop()
