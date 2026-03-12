#!/usr/bin/env python3
"""
社交电商数据实时模拟生成器
从 CSV 文件读取数据，按随机间隔发送到 Flume 的 netcat 源
"""

import csv
import time
import socket
import random
import sys

CSV_FILE = '/home/hadoop/social_ecommerce_data.csv'  # 你的 CSV 路径
FLUME_HOST = 'localhost'
FLUME_PORT = 10050

def send_data():
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((FLUME_HOST, FLUME_PORT))
        print(f"连接到 Flume {FLUME_HOST}:{FLUME_PORT} 成功")
    except Exception as e:
        print(f"连接失败: {e}")
        sys.exit(1)

    with open(CSV_FILE, 'r') as f:
        reader = csv.reader(f)
        header = next(reader)  # 跳过表头
        for row in reader:
            line = ','.join(row) + '\n'
            sock.send(line.encode())
            print(f"发送: {line[:50]}...")  # 只打印前50字符
            time.sleep(random.uniform(0.5, 3))  # 随机间隔 0.5~3 秒

    sock.close()
    print("数据发送完毕")

if __name__ == '__main__':
    send_data()
