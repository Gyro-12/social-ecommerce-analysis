-- 创建 ODS 外部表
CREATE DATABASE IF NOT EXISTS social_dw;
USE social_dw;
CREATE EXTERNAL TABLE IF NOT EXISTS ods_social_behavior (
    user_id STRING,
    item_id STRING,
    age INT,
    gender INT,
    user_level INT,
    purchase_freq INT,
    total_spend FLOAT,
    register_days INT,
    follow_num INT,
    fans_num INT,
    price FLOAT,
    discount_rate FLOAT,
    category STRING,
    title_length INT,
    title_emo_score FLOAT,
    img_count INT,
    has_video INT,
    like_num INT,
    comment_num INT,
    share_num INT,
    collect_num INT,
    is_follow_author INT,
    add2cart INT,
    coupon_received INT,
    coupon_used INT,
    pv_count INT,
    last_click_gap FLOAT,
    interaction_rate FLOAT,
    purchase_intent FLOAT,
    freshness_score FLOAT,
    social_influence FLOAT,
    label INT
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/social_ecommerce/raw/'
TBLPROPERTIES ("skip.header.line.count"="1");

-- DWD 层
CREATE TABLE IF NOT EXISTS dwd_user_behavior (
    user_id STRING,
    item_id STRING,
    age INT,
    gender INT,
    user_level INT,
    purchase_freq INT,
    total_spend FLOAT,
    register_days INT,
    follow_num INT,
    fans_num INT,
    price FLOAT,
    discount_rate FLOAT,
    category STRING,
    title_length INT,
    title_emo_score FLOAT,
    img_count INT,
    has_video INT,
    like_num INT,
    comment_num INT,
    share_num INT,
    collect_num INT,
    is_follow_author INT,
    add2cart INT,
    coupon_received INT,
    coupon_used INT,
    pv_count INT,
    last_click_gap FLOAT,
    interaction_rate FLOAT,
    purchase_intent FLOAT,
    freshness_score FLOAT,
    social_influence FLOAT,
    label INT
)
PARTITIONED BY (dt STRING)
STORED AS ORC;

-- DWS 层
CREATE TABLE IF NOT EXISTS dws_user_daily (
    user_id STRING,
    dt STRING,
    total_views INT,
    total_likes INT,
    total_comments INT,
    total_shares INT,
    total_collects INT,
    total_cart INT,
    total_purchase INT,
    total_spend FLOAT
)
STORED AS ORC;

-- ADS 层
CREATE TABLE IF NOT EXISTS ads_category_daily (
    dt STRING,
    category STRING,
    total_pv INT,
    total_likes INT,
    total_purchase INT
)
STORED AS ORC;
