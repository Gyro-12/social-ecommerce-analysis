CREATE DATABASE IF NOT EXISTS social_analysis;
USE social_analysis;

-- 实时结果表
CREATE TABLE IF NOT EXISTS hot_products (
    id INT AUTO_INCREMENT PRIMARY KEY,
    window_start DATETIME,
    window_end DATETIME,
    item_id VARCHAR(50),
    total_hot_score INT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS behavior_stats (
    id INT AUTO_INCREMENT PRIMARY KEY,
    window_start DATETIME,
    window_end DATETIME,
    behavior_type VARCHAR(20),
    total_count INT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 商品类别维表
CREATE TABLE IF NOT EXISTS item_categories (
    item_id VARCHAR(50) PRIMARY KEY,
    category VARCHAR(50)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 离线统计结果表
CREATE TABLE IF NOT EXISTS category_daily_stats (
    dt VARCHAR(10),
    category VARCHAR(50),
    total_pv INT,
    total_likes INT,
    total_purchase INT,
    PRIMARY KEY (dt, category)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
