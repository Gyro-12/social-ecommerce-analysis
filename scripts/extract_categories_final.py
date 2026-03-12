#!/usr/bin/env python3
import csv
import pymysql
import sys

conn = pymysql.connect(
    host='localhost',
    user='root',
    password='123456',
    database='social_analysis',
    charset='utf8mb4',
    cursorclass=pymysql.cursors.DictCursor
)
cursor = conn.cursor()
print("✅ MySQL 连接成功")

# 删除旧表并重建
cursor.execute("DROP TABLE IF EXISTS item_categories")
cursor.execute("""
    CREATE TABLE item_categories (
        item_id VARCHAR(50) PRIMARY KEY,
        category VARCHAR(50)
    ) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci
""")
print("✅ 已重建 item_categories 表 (utf8mb4)")

csv_file = '/home/hadoop/social_ecommerce_data.csv'
item_category_map = {}

with open(csv_file, 'r', encoding='utf-8') as f:
    reader = csv.reader(f)
    header = next(reader)
    print(f"📋 CSV 表头: {header}")
    row_count = 0
    for row in reader:
        row_count += 1
        if len(row) < 13:
            print(f"⚠️ 第 {row_count} 行列数不足: {row}")
            continue
        item_id = row[1].strip()
        category = row[12].strip()
        # 如果 item_id 已经存在，保留第一次出现的类别
        if item_id not in item_category_map:
            item_category_map[item_id] = category
        else:
            # 可选：检查类别是否一致，不一致可记录日志
            if item_category_map[item_id] != category:
                print(f"⚠️ 商品 {item_id} 出现不同类别: {item_category_map[item_id]} vs {category}，保留原类别")

print(f"📊 共读取 {row_count} 行数据，去重后得到 {len(item_category_map)} 个商品类别")

# 准备数据插入
data = list(item_category_map.items())  # (item_id, category)

insert_sql = "INSERT INTO item_categories (item_id, category) VALUES (%s, %s)"
cursor.executemany(insert_sql, data)
conn.commit()
print(f"✅ 成功插入 {len(data)} 条记录")

cursor.close()
conn.close()
